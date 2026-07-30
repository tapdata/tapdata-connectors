package io.tapdata.connector.paimon.service;

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
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.FileStoreTable;
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
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
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
import static org.mockito.Mockito.never;
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
