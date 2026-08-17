package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.exception.PaimonDynamicBucketPollutedException;
import io.tapdata.connector.paimon.commit.PaimonCommitStateStore;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;
import io.tapdata.connector.paimon.service.PaimonDynamicBucketPreflight;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import io.tapdata.entity.utils.cache.KVMap;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonTableWriteContextIntegrationTest {

    private static final String DATABASE = "default";

    @TempDir
    java.nio.file.Path tempDir;

    private Catalog catalog;

    @AfterEach
    void closeCatalog() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    void hashDynamicShouldKeepOneLatestRowAcrossContextRestart() throws Exception {
        Table table = createHashDynamicTable("hash_dynamic");
        String ioPath = Files.createDirectory(tempDir.resolve("hash-io")).toString();
        String commitUser = "stable-hash-user";

        try (PaimonTableWriteContext context = PaimonTableWriteContext.create(
                "default.hash_dynamic", "hash_dynamic", table, commitUser, ioPath)) {
            assertEquals(BucketMode.HASH_DYNAMIC, context.bucketMode());
            context.write(hashRow(1, "v1"));
            context.write(hashRow(2, "other"));
            context.commit();
        }

        table = catalog.getTable(Identifier.create(DATABASE, "hash_dynamic"));
        try (PaimonTableWriteContext restarted = PaimonTableWriteContext.create(
                "default.hash_dynamic", "hash_dynamic", table, commitUser, ioPath)) {
            restarted.write(hashRow(1, "v2"));
            restarted.commit();
        }

        List<InternalRow> rows = readRows(catalog.getTable(Identifier.create(DATABASE, "hash_dynamic")));
        assertEquals(2, rows.size());
        assertEquals(1, rows.stream().filter(row -> row.getInt(0) == 1).count());
        InternalRow latest =
                rows.stream()
                        .filter(row -> row.getInt(0) == 1)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("Missing primary key 1"));
        assertEquals("v2", latest.getString(1).toString());
    }

    @Test
    void keyDynamicShouldMovePrimaryKeyAcrossPartitionsWithoutDuplicate() throws Exception {
        Table table = createKeyDynamicTable("key_dynamic");
        String ioPath = Files.createDirectory(tempDir.resolve("key-io")).toString();
        String commitUser = "stable-key-user";

        try (PaimonTableWriteContext context = PaimonTableWriteContext.create(
                "default.key_dynamic", "key_dynamic", table, commitUser, ioPath)) {
            assertEquals(BucketMode.KEY_DYNAMIC, context.bucketMode());
            context.write(keyRow(RowKind.INSERT, 1, 10, "old"));
            context.commit();
        }

        table = catalog.getTable(Identifier.create(DATABASE, "key_dynamic"));
        try (PaimonTableWriteContext restarted = PaimonTableWriteContext.create(
                "default.key_dynamic", "key_dynamic", table, commitUser, ioPath)) {
            restarted.write(keyRow(RowKind.UPDATE_AFTER, 2, 10, "middle"));
            restarted.commit();
            restarted.write(keyRow(RowKind.UPDATE_AFTER, 3, 10, "new"));
            restarted.commit();
        }

        List<InternalRow> rows = readRows(catalog.getTable(Identifier.create(DATABASE, "key_dynamic")));
        assertEquals(1, rows.size());
        assertEquals(3, rows.get(0).getInt(0));
        assertEquals(10, rows.get(0).getInt(1));
        assertEquals("new", rows.get(0).getString(2).toString());
    }

    @Test
    void fixedBucketPrimaryKeyTableShouldKeepNativeInsertUpdateDeleteBehavior() throws Exception {
        Table table = createFixedBucketTable("fixed_bucket");
        String ioPath = Files.createDirectory(tempDir.resolve("fixed-io")).toString();

        try (PaimonTableWriteContext context = PaimonTableWriteContext.create(
                "default.fixed_bucket", "fixed_bucket", table, "fixed-user", ioPath)) {
            assertEquals(BucketMode.HASH_FIXED, context.bucketMode());
            context.write(hashRow(1, "v1"));
            context.commit();
            context.write(GenericRow.ofKind(
                    RowKind.UPDATE_AFTER, 1, BinaryString.fromString("v2")));
            context.commit();
        }

        List<InternalRow> updated = readRows(catalog.getTable(
                Identifier.create(DATABASE, "fixed_bucket")));
        assertEquals(1, updated.size());
        assertEquals("v2", updated.get(0).getString(1).toString());

        table = catalog.getTable(Identifier.create(DATABASE, "fixed_bucket"));
        try (PaimonTableWriteContext restarted = PaimonTableWriteContext.create(
                "default.fixed_bucket", "fixed_bucket", table, "fixed-user", ioPath)) {
            restarted.write(GenericRow.ofKind(
                    RowKind.DELETE, 1, BinaryString.fromString("v2")));
            restarted.commit();
        }
        assertEquals(0, readRows(catalog.getTable(
                Identifier.create(DATABASE, "fixed_bucket"))).size());
    }

    @Test
    void landedDirectCommitFailureMustBeConfirmedWithoutReapplyingData() throws Exception {
        FileStoreTable table = (FileStoreTable) createFixedBucketTable("direct_landed");
        String commitUser = "direct-landed-user";
        StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        StreamTableWrite writer = builder.newWrite();
        writer.write(hashRow(1, "once"));
        AmbiguousCommitter committer =
                new AmbiguousCommitter(
                        new PaimonStreamTableCommitter(builder.newCommit()),
                        DirectFailure.AFTER_DELEGATE);

        try (PaimonTableWriteContext context =
                injectedContext(table, commitUser, writer, committer)) {
            assertEquals(0L, context.commit());
        }

        assertEquals(1, committer.directCalls);
        assertEquals(1, committer.filterCalls);
        assertEquals(0, committer.lastFilterResult);
        assertEquals(1, readRows(table).size());
        assertValidSnapshotIdentities(table, commitUser, 0L);
    }

    @Test
    void failedDirectCommitBeforeSnapshotMustRecoverTheOriginalMessages() throws Exception {
        FileStoreTable table = (FileStoreTable) createFixedBucketTable("direct_not_landed");
        String commitUser = "direct-not-landed-user";
        StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(commitUser);
        StreamTableWrite writer = builder.newWrite();
        writer.write(hashRow(1, "recovered"));
        AmbiguousCommitter committer =
                new AmbiguousCommitter(
                        new PaimonStreamTableCommitter(builder.newCommit()),
                        DirectFailure.BEFORE_DELEGATE);

        try (PaimonTableWriteContext context =
                injectedContext(table, commitUser, writer, committer)) {
            assertEquals(0L, context.commit());
        }

        assertEquals(1, committer.directCalls);
        assertEquals(1, committer.filterCalls);
        assertEquals(1, committer.lastFilterResult);
        assertEquals(1, readRows(table).size());
        assertValidSnapshotIdentities(table, commitUser, 0L);
    }

    @Test
    void firstAttemptMustSkipRecoveryDataFileExistenceChecks() throws Exception {
        FileStoreTable original = (FileStoreTable) createFixedBucketTable("direct_file_checks");
        FileIO countingFileIO = spy(original.fileIO());
        FileStoreTable table =
                FileStoreTableFactory.create(
                        countingFileIO, original.location(), original.schema());
        StreamWriteBuilder builder =
                table.newStreamWriteBuilder().withCommitUser("file-check-user");

        try (StreamTableWrite writer = builder.newWrite();
             PaimonStreamTableCommitter committer =
                     new PaimonStreamTableCommitter(builder.newCommit())) {
            writer.write(hashRow(1, "direct"));
            List<CommitMessage> directMessages = writer.prepareCommit(false, 0L);
            clearInvocations(countingFileIO);

            committer.commit(0L, directMessages);

            List<Path> directExists = dataFileExistsCalls(countingFileIO);
            assertTrue(
                    directExists.isEmpty(),
                    "Direct commit unexpectedly ran recovery data-file checks: " + directExists);

            writer.write(hashRow(2, "recovery"));
            List<CommitMessage> recoveryMessages = writer.prepareCommit(false, 1L);
            Map<Long, List<CommitMessage>> pending = new LinkedHashMap<>();
            pending.put(1L, recoveryMessages);
            clearInvocations(countingFileIO);

            assertEquals(1, committer.filterAndCommit(pending));

            // Paimon 1.3.2 recovery verifies data/changelog/index/compact-after files before
            // committing. Source: paimon-core/src/main/java/org/apache/paimon/table/sink/
            // TableCommitImpl.java#checkFilesExistence, lines 265-345. Baseline:
            // apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e.
            assertFalse(dataFileExistsCalls(countingFileIO).isEmpty());
        }
    }

    @Test
    void committedSnapshotMustRecoverIdentifierAfterStateSaveFailure() throws Exception {
        FileStoreTable table = (FileStoreTable) createFixedBucketTable("state_rebind");
        KVMap<Object> taskState = stateMap();
        String warehouseIdentity = tempDir.resolve("state-warehouse").toUri().toString();
        String ioPath = Files.createDirectory(tempDir.resolve("state-rebind-io")).toString();
        PaimonCommitStateStore.Binding initial =
                PaimonCommitStateStore.bind(taskState, warehouseIdentity, table);
        RuntimeException stateFailure = new RuntimeException("injected task-state failure");

        try (PaimonTableWriteContext first =
                PaimonTableWriteContext.create(
                        "default.state_rebind",
                        "state_rebind",
                        table,
                        initial.commitUser(),
                        ioPath,
                        initial.nextCommitIdentifier(),
                        ignored -> {
                            throw stateFailure;
                        })) {
            first.write(hashRow(1, "committed-before-state-save"));
            assertSame(stateFailure, assertThrows(RuntimeException.class, first::commit));
            assertFalse(first.hasPendingCommit());
            assertThrows(IllegalStateException.class, first::commit);
        }

        FileStoreTable reloaded =
                (FileStoreTable)
                        catalog.getTable(Identifier.create(DATABASE, "state_rebind"));
        PaimonCommitStateStore.Binding recovered =
                PaimonCommitStateStore.bind(taskState, warehouseIdentity, reloaded);

        // Paimon scans retained snapshots backwards for the latest snapshot of this stable user;
        // the connector persists snapshot.identifier + 1 before publishing a replacement writer.
        // Source: paimon-core/src/main/java/org/apache/paimon/utils/
        // SnapshotManager.java#latestSnapshotOfUserFromFilesystem, lines 586-630. Baseline:
        // apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e.
        assertEquals(initial.commitUser(), recovered.commitUser());
        assertEquals(1L, recovered.nextCommitIdentifier());

        try (PaimonTableWriteContext second =
                PaimonTableWriteContext.create(
                        "default.state_rebind",
                        "state_rebind",
                        reloaded,
                        recovered.commitUser(),
                        ioPath,
                        recovered.nextCommitIdentifier(),
                        recovered.store())) {
            second.write(hashRow(2, "after-rebind"));
            assertEquals(1L, second.commit());
        }

        FileStoreTable finalTable =
                (FileStoreTable)
                        catalog.getTable(Identifier.create(DATABASE, "state_rebind"));
        PaimonCommitStateStore.Binding finalBinding =
                PaimonCommitStateStore.bind(taskState, warehouseIdentity, finalTable);
        assertEquals(2L, finalBinding.nextCommitIdentifier());
        assertEquals(2, readRows(finalTable).size());
        assertValidSnapshotIdentities(finalTable, initial.commitUser(), 0L);
        assertValidSnapshotIdentities(finalTable, initial.commitUser(), 1L);
    }

    @Test
    void appendOnlyBucketMinusOneTableShouldContinueUsingNativeWriter() throws Exception {
        Table table = createAppendOnlyTable("append_bucket_minus_one");
        String ioPath = Files.createDirectory(tempDir.resolve("append-io")).toString();

        try (PaimonTableWriteContext context = PaimonTableWriteContext.create(
                "default.append_bucket_minus_one",
                "append_bucket_minus_one",
                table,
                "append-user",
                ioPath)) {
            assertFalse(context.bucketMode() == BucketMode.HASH_DYNAMIC
                    || context.bucketMode() == BucketMode.KEY_DYNAMIC);
            context.write(hashRow(1, "a"));
            context.write(hashRow(1, "b"));
            context.commit();
        }

        assertEquals(2, readRows(catalog.getTable(
                Identifier.create(DATABASE, "append_bucket_minus_one"))).size());
    }

    @Test
    void postponeBucketTableShouldCommitFilesWithoutAssigningFinalBucket() throws Exception {
        FileStoreTable table =
                (FileStoreTable) createPostponeBucketTable("postpone_bucket_minus_two");
        String ioPath = Files.createDirectory(tempDir.resolve("postpone-io")).toString();

        try (PaimonTableWriteContext context =
                PaimonTableWriteContext.create(
                        "default.postpone_bucket_minus_two",
                        "postpone_bucket_minus_two",
                        table,
                        "postpone-user",
                        ioPath)) {
            assertEquals(BucketMode.POSTPONE_MODE, context.bucketMode());
            context.write(hashRow(1, "postponed"));
            context.commit();
        }

        table =
                (FileStoreTable)
                        catalog.getTable(
                                Identifier.create(DATABASE, "postpone_bucket_minus_two"));
        Snapshot snapshot =
                table.latestSnapshot()
                        .orElseThrow(() -> new AssertionError("Missing committed snapshot"));
        List<ManifestEntry> entries = new ArrayList<>();
        for (ManifestFileMeta manifest :
                table.manifestListReader().read(snapshot.deltaManifestList())) {
            entries.addAll(table.manifestFileReader().read(manifest.fileName()));
        }
        assertFalse(entries.isEmpty());
        assertTrue(
                entries.stream()
                        .allMatch(entry -> entry.bucket() == BucketMode.POSTPONE_BUCKET));
    }

    @Test
    void bucketUnawareShouldAcceptUpdateAfterWithoutChangingRowKindInConnector() throws Exception {
        Table table = createAppendOnlyTable("append_update_after");

        try (PaimonTableWriteContext context =
                PaimonTableWriteContext.create(
                        "default.append_update_after",
                        "append_update_after",
                        table,
                        "append-update-after-user",
                        null)) {
            assertEquals(BucketMode.BUCKET_UNAWARE, context.bucketMode());
            context.write(
                    GenericRow.ofKind(
                            RowKind.UPDATE_AFTER, 1, BinaryString.fromString("after")));
            context.commit();
        }

        List<InternalRow> rows =
                readRows(
                        catalog.getTable(
                                Identifier.create(DATABASE, "append_update_after")));
        assertEquals(1, rows.size());
        assertEquals("after", rows.get(0).getString(1).toString());
    }

    @Test
    void bucketUnawareShouldKeepPaimonDefaultRetractRejection() throws Exception {
        assertAppendRetractRejected("append_update_before", RowKind.UPDATE_BEFORE);
        assertAppendRetractRejected("append_delete", RowKind.DELETE);
    }

    @Test
    void bucketUnawareShouldLetPaimonIgnoreConfiguredRetracts() throws Exception {
        Table table = createAppendOnlyTable("append_ignore_retract", true);

        try (PaimonTableWriteContext context =
                PaimonTableWriteContext.create(
                        "default.append_ignore_retract",
                        "append_ignore_retract",
                        table,
                        "append-ignore-retract-user",
                        null)) {
            context.write(hashRow(1, "kept"));
            context.write(
                    GenericRow.ofKind(
                            RowKind.UPDATE_BEFORE, 1, BinaryString.fromString("before")));
            context.write(
                    GenericRow.ofKind(RowKind.DELETE, 1, BinaryString.fromString("deleted")));
            context.commit();
        }

        List<InternalRow> rows =
                readRows(
                        catalog.getTable(
                                Identifier.create(DATABASE, "append_ignore_retract")));
        assertEquals(1, rows.size());
        assertEquals("kept", rows.get(0).getString(1).toString());
    }

    @Test
    void keyDynamicBootstrapShouldRejectAlreadyPollutedPrimaryKeys() throws Exception {
        FileStoreTable table = (FileStoreTable) createKeyDynamicTable("key_dynamic_polluted");
        StreamWriteBuilder builder =
                table.newStreamWriteBuilder().withCommitUser("legacy-polluting-writer");
        try (StreamTableWrite writer = builder.newWrite();
             StreamTableCommit committer = builder.newCommit()) {
            // Reproduce the legacy connector bug: the same PK is forced into unrelated buckets
            // and partitions without GlobalIndexAssigner generating the old-partition DELETE.
            writer.write(keyRow(RowKind.INSERT, 1, 10, "old"), 0);
            writer.write(keyRow(RowKind.INSERT, 2, 10, "duplicate"), 1);
            List<CommitMessage> messages = writer.prepareCommit(false, 0L);
            Map<Long, List<CommitMessage>> commits = new LinkedHashMap<>();
            commits.put(0L, messages);
            committer.filterAndCommit(commits);
        }
        assertEquals(2, readRows(catalog.getTable(
                Identifier.create(DATABASE, "key_dynamic_polluted"))).size());

        table = (FileStoreTable) catalog.getTable(
                Identifier.create(DATABASE, "key_dynamic_polluted"));
        String ioPath = Files.createDirectory(tempDir.resolve("polluted-key-io")).toString();
        FileStoreTable pollutedTable = table;
        PaimonDynamicBucketPollutedException error = assertThrows(
                PaimonDynamicBucketPollutedException.class,
                () -> PaimonTableWriteContext.create(
                        "default.key_dynamic_polluted",
                        "key_dynamic_polluted",
                        pollutedTable,
                        "new-stable-user",
                        ioPath));
        assertTrue(error.getMessage().contains("deduplicate or rebuild"));
        assertTrue(containsMessage(error, "data contains duplicates"));
    }

    @Test
    void hashDynamicPreflightShouldRejectLegacyCrossBucketDuplicates() throws Exception {
        FileStoreTable table = (FileStoreTable) createHashDynamicTable("hash_dynamic_polluted");
        StreamWriteBuilder builder =
                table.newStreamWriteBuilder().withCommitUser("legacy-hash-polluter");
        try (StreamTableWrite writer = builder.newWrite();
             StreamTableCommit committer = builder.newCommit()) {
            writer.write(hashRow(10, "old"), 0);
            writer.write(hashRow(10, "duplicate"), 1);
            Map<Long, List<CommitMessage>> commits = new LinkedHashMap<>();
            commits.put(0L, writer.prepareCommit(false, 0L));
            committer.filterAndCommit(commits);
        }
        assertEquals(2, readRows(catalog.getTable(
                Identifier.create(DATABASE, "hash_dynamic_polluted"))).size());

        String ioPath = Files.createDirectory(tempDir.resolve("polluted-hash-io")).toString();
        PaimonDynamicBucketPollutedException error = assertThrows(
                PaimonDynamicBucketPollutedException.class,
                () -> PaimonDynamicBucketPreflight.ensureHashDynamicValidated(
                        stateMap(),
                        tempDir.toUri().toString(),
                        "default.hash_dynamic_polluted",
                        table,
                        ioPath));
        assertTrue(error.getMessage().contains("deduplicate or rebuild"));
    }

    @Test
    void legacyExplicitBucketHashTableShouldKeepExistingBucketAfterUpgrade() throws Exception {
        FileStoreTable table = (FileStoreTable) createHashDynamicTable("hash_dynamic_legacy_clean");
        StreamWriteBuilder legacyBuilder =
                table.newStreamWriteBuilder().withCommitUser("legacy-manual-bucket-writer");
        try (StreamTableWrite legacyWriter = legacyBuilder.newWrite();
             StreamTableCommit legacyCommitter = legacyBuilder.newCommit()) {
            // This is the old connector path. Paimon's writer still persists the HASH index for
            // the explicitly selected bucket via DynamicBucketIndexMaintainer.
            legacyWriter.write(hashRow(7, "legacy"), 3);
            Map<Long, List<CommitMessage>> commits = new LinkedHashMap<>();
            commits.put(0L, legacyWriter.prepareCommit(false, 0L));
            legacyCommitter.filterAndCommit(commits);
        }

        KVMap<Object> stateMap = stateMap();
        String ioPath = Files.createDirectory(tempDir.resolve("legacy-clean-hash-io")).toString();
        table = (FileStoreTable) catalog.getTable(
                Identifier.create(DATABASE, "hash_dynamic_legacy_clean"));
        PaimonDynamicBucketPreflight.ensureHashDynamicValidated(
                stateMap,
                tempDir.toUri().toString(),
                "default.hash_dynamic_legacy_clean",
                table,
                ioPath);

        try (PaimonTableWriteContext upgraded = PaimonTableWriteContext.create(
                "default.hash_dynamic_legacy_clean",
                "hash_dynamic_legacy_clean",
                table,
                "new-stable-hash-user",
                ioPath)) {
            upgraded.write(hashRow(7, "upgraded"));
            upgraded.commit();
        }

        List<InternalRow> rows = readRows(catalog.getTable(
                Identifier.create(DATABASE, "hash_dynamic_legacy_clean")));
        assertEquals(1, rows.size());
        assertEquals("upgraded", rows.get(0).getString(1).toString());
    }

    @Test
    void hashDynamicPreflightMarkerShouldUseStableTableUuidAcrossReload() throws Exception {
        FileStoreTable first = (FileStoreTable) createHashDynamicTable("hash_dynamic_marker");
        KVMap<Object> stateMap = stateMap();
        String ioPath = Files.createDirectory(tempDir.resolve("hash-marker-io")).toString();

        PaimonDynamicBucketPreflight.ensureHashDynamicValidated(
                stateMap,
                tempDir.toUri().toString(),
                "default.hash_dynamic_marker",
                first,
                ioPath);

        FileStoreTable reloaded = (FileStoreTable) catalog.getTable(
                Identifier.create(DATABASE, "hash_dynamic_marker"));
        assertEquals(first.uuid(), reloaded.uuid());
        clearInvocations(stateMap);

        PaimonDynamicBucketPreflight.ensureHashDynamicValidated(
                stateMap,
                tempDir.toUri().toString(),
                "default.hash_dynamic_marker",
                reloaded,
                ioPath);

        // A matching stable marker returns before creating another IOManager/full-table scan and
        // therefore performs no marker mutation on a catalog reload.
        verify(stateMap, never()).put(anyString(), org.mockito.ArgumentMatchers.any());
        verify(stateMap, never()).putIfAbsent(anyString(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void hashDynamicPreflightCopyShouldRemoveIndexTtlWithoutMutatingTable() throws Exception {
        FileStoreTable table = (FileStoreTable) createHashDynamicTable("hash_dynamic_ttl");
        FileStoreTable withTtl = table.copy(Collections.singletonMap(
                CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key(), "1 d"));

        FileStoreTable validationTable = PaimonDynamicBucketPreflight.withoutIndexTtl(withTtl);

        assertFalse(validationTable.options().containsKey(
                CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key()));
        assertTrue(withTtl.options().containsKey(
                CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key()));
        assertSame(table, PaimonDynamicBucketPreflight.withoutIndexTtl(table));
    }

    private PaimonTableWriteContext injectedContext(
            FileStoreTable table,
            String commitUser,
            StreamTableWrite writer,
            PaimonTableCommitter committer) {
        return new PaimonTableWriteContext(
                "default." + table.name(),
                table.name(),
                commitUser,
                new RawWriterStrategy(table.bucketMode(), writer),
                committer,
                null,
                Collections.emptyList(),
                0L);
    }

    private void assertValidSnapshotIdentities(
            FileStoreTable table, String commitUser, long identifier) throws Exception {
        List<Snapshot> matching = new ArrayList<>();
        Iterator<Snapshot> snapshots = table.snapshotManager().snapshots();
        while (snapshots.hasNext()) {
            Snapshot snapshot = snapshots.next();
            if (commitUser.equals(snapshot.commitUser())
                    && identifier == snapshot.commitIdentifier()) {
                matching.add(snapshot);
            }
        }

        // One identifier may legally produce APPEND and COMPACT snapshots. Retry identity is the
        // (commitUser, identifier, commitKind) tuple, not a one-snapshot-per-identifier rule.
        // Source: paimon-core/src/main/java/org/apache/paimon/table/sink/
        // StreamTableCommit.java#commit, lines 35-38; and paimon-core/src/main/java/org/apache/
        // paimon/operation/FileStoreCommitImpl.java#tryCommitOnce, lines 955-970. Baseline:
        // apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e.
        assertFalse(matching.isEmpty());
        assertTrue(matching.size() <= 2);
        Set<String> identities = new HashSet<>();
        for (Snapshot snapshot : matching) {
            assertTrue(
                    identities.add(
                            snapshot.commitUser()
                                    + ':'
                                    + snapshot.commitIdentifier()
                                    + ':'
                                    + snapshot.commitKind()));
        }
    }

    private List<Path> dataFileExistsCalls(FileIO fileIO) {
        List<Path> paths = new ArrayList<>();
        mockingDetails(fileIO).getInvocations().forEach(invocation -> {
            if ("exists".equals(invocation.getMethod().getName())
                    && invocation.getArguments().length == 1
                    && invocation.getArgument(0) instanceof Path) {
                Path path = invocation.getArgument(0);
                if (path.toString().contains("/bucket-")) {
                    paths.add(path);
                }
            }
        });
        return paths;
    }

    private Table createHashDynamicTable(String tableName) throws Exception {
        initCatalog();
        Identifier identifier = Identifier.create(DATABASE, tableName);
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("value", DataTypes.STRING())
                .primaryKey("id")
                .option("bucket", "-1")
                .option("dynamic-bucket.target-row-num", "1")
                .option("write-buffer-size", "8mb")
                .build();
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    private Table createKeyDynamicTable(String tableName) throws Exception {
        initCatalog();
        Identifier identifier = Identifier.create(DATABASE, tableName);
        Schema schema = Schema.newBuilder()
                .column("pt", DataTypes.INT())
                .column("id", DataTypes.INT())
                .column("value", DataTypes.STRING())
                .partitionKeys("pt")
                .primaryKey("id")
                .option("bucket", "-1")
                .option("dynamic-bucket.target-row-num", "1")
                .option("write-buffer-size", "8mb")
                .build();
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    private Table createFixedBucketTable(String tableName) throws Exception {
        initCatalog();
        Identifier identifier = Identifier.create(DATABASE, tableName);
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("value", DataTypes.STRING())
                .primaryKey("id")
                .option("bucket", "2")
                .option("write-buffer-size", "8mb")
                .build();
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    private Table createPostponeBucketTable(String tableName) throws Exception {
        initCatalog();
        Identifier identifier = Identifier.create(DATABASE, tableName);
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", String.valueOf(BucketMode.POSTPONE_BUCKET))
                        .option("write-buffer-size", "8mb")
                        .build();
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    private Table createAppendOnlyTable(String tableName) throws Exception {
        return createAppendOnlyTable(tableName, false);
    }

    private Table createAppendOnlyTable(String tableName, boolean ignoreRetracts) throws Exception {
        initCatalog();
        Identifier identifier = Identifier.create(DATABASE, tableName);
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .option("bucket", "-1")
                        .option("write-buffer-size", "8mb");
        if (ignoreRetracts) {
            builder.option("ignore-delete", "true").option("ignore-update-before", "true");
        }
        Schema schema = builder.build();
        catalog.createTable(identifier, schema, false);
        return catalog.getTable(identifier);
    }

    private void assertAppendRetractRejected(String tableName, RowKind rowKind) throws Exception {
        Table table = createAppendOnlyTable(tableName);
        try (PaimonTableWriteContext context =
                PaimonTableWriteContext.create(
                        "default." + tableName,
                        tableName,
                        table,
                        "append-retract-user-" + rowKind,
                        null)) {
            IllegalStateException error =
                    assertThrows(
                            IllegalStateException.class,
                            () ->
                                    context.write(
                                            GenericRow.ofKind(
                                                    rowKind,
                                                    1,
                                                    BinaryString.fromString("retract"))));
            assertTrue(error.getMessage().contains("Append only writer can not accept"));
        }
    }

    private void initCatalog() throws Exception {
        catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tempDir.toUri())));
        catalog.createDatabase(DATABASE, true);
    }

    private GenericRow hashRow(int id, String value) {
        return GenericRow.of(id, BinaryString.fromString(value));
    }

    private GenericRow keyRow(RowKind kind, int partition, int id, String value) {
        return GenericRow.ofKind(kind, partition, id, BinaryString.fromString(value));
    }

    private List<InternalRow> readRows(Table table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                     readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        rows.add(serializer.copy(row));
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        return rows;
    }

    private boolean containsMessage(Throwable error, String text) {
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(text)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private enum DirectFailure {
        BEFORE_DELEGATE,
        AFTER_DELEGATE
    }

    private static final class AmbiguousCommitter implements PaimonTableCommitter {
        private final PaimonTableCommitter delegate;
        private final DirectFailure directFailure;
        private int directCalls;
        private int filterCalls;
        private int lastFilterResult = -1;

        private AmbiguousCommitter(
                PaimonTableCommitter delegate, DirectFailure directFailure) {
            this.delegate = delegate;
            this.directFailure = directFailure;
        }

        @Override
        public void commit(long identifier, List<CommitMessage> messages) {
            directCalls++;
            if (directFailure == DirectFailure.BEFORE_DELEGATE) {
                throw new RuntimeException("Injected failure before Paimon commit");
            }
            delegate.commit(identifier, messages);
            throw new RuntimeException("Injected ambiguous failure after Paimon commit");
        }

        @Override
        public int filterAndCommit(Map<Long, List<CommitMessage>> pendingCommits) {
            filterCalls++;
            lastFilterResult = delegate.filterAndCommit(pendingCommits);
            return lastFilterResult;
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }

    private static final class RawWriterStrategy implements PaimonBucketWriterStrategy {
        private final BucketMode bucketMode;
        private final StreamTableWrite writer;
        private final PaimonWriteSemanticContract semanticContract =
                mock(PaimonWriteSemanticContract.class);

        private RawWriterStrategy(BucketMode bucketMode, StreamTableWrite writer) {
            this.bucketMode = bucketMode;
            this.writer = writer;
        }

        @Override
        public BucketMode bucketMode() {
            return bucketMode;
        }

        @Override
        public PaimonWriteSemanticContract writeSemanticContract() {
            return semanticContract;
        }

        @Override
        public void validateRoutingRow(InternalRow row, String operation) {}

        @Override
        public void write(InternalRow row) throws Exception {
            writer.write(row);
        }

        @Override
        public List<CommitMessage> prepareCommit(long commitIdentifier) throws Exception {
            return writer.prepareCommit(false, commitIdentifier);
        }

        @Override
        public void close() throws Exception {
            writer.close();
        }
    }

    @SuppressWarnings("unchecked")
    private KVMap<Object> stateMap() {
        Map<String, Object> values = new ConcurrentHashMap<>();
        KVMap<Object> stateMap = mock(KVMap.class);
        when(stateMap.get(anyString())).thenAnswer(invocation -> values.get(invocation.getArgument(0)));
        when(stateMap.putIfAbsent(anyString(), org.mockito.ArgumentMatchers.any()))
                .thenAnswer(invocation ->
                        values.putIfAbsent(invocation.getArgument(0), invocation.getArgument(1)));
        doAnswer(invocation -> {
            values.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(stateMap).put(anyString(), org.mockito.ArgumentMatchers.any());
        return stateMap;
    }
}
