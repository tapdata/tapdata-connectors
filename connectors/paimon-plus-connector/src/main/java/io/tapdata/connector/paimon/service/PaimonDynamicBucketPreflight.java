package io.tapdata.connector.paimon.service;

import io.tapdata.entity.utils.cache.KVMap;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.paimon.disk.IOManagerImpl.splitPaths;

/** One-time exact-primary-key pollution preflight for legacy HASH_DYNAMIC tables. */
final class PaimonDynamicBucketPreflight {

    private static final String MARKER_PREFIX = "paimon.hash-dynamic-preflight-v1.";

    private PaimonDynamicBucketPreflight() {
    }

    static void ensureHashDynamicValidated(
            KVMap<Object> stateMap,
            String warehouse,
            String tableKey,
            FileStoreTable table,
            String configuredTmpDirs) throws Exception {
        String markerKey = MARKER_PREFIX + PaimonCommitStateStore.stateKey(
                warehouse, table.location().toUri().toString());
        // FileStorePathFactory.uuid() is intentionally a per-instance random file-name prefix in
        // Paimon 1.3.1. It changes whenever the table/store is reloaded and therefore must never be
        // used as a durable table identity. Table.uuid() is the metastore UUID (or the filesystem
        // table's stable creation identity) and changes when the table is recreated.
        String expectedMarker = Objects.requireNonNull(
                table.uuid(), "Paimon table UUID is required for HASH_DYNAMIC preflight");
        Object existing = stateMap.get(markerKey);
        if (expectedMarker.equals(existing)) {
            return;
        }
        if (existing != null && !(existing instanceof String)) {
            throw new IllegalStateException("Invalid HASH_DYNAMIC preflight marker type");
        }

        validateExactPrimaryKeyUniqueness(tableKey, table, configuredTmpDirs);

        if (existing == null) {
            Object raced = stateMap.putIfAbsent(markerKey, expectedMarker);
            if (raced != null && !expectedMarker.equals(raced)) {
                throw new IllegalStateException(
                        "HASH_DYNAMIC table identity changed during pollution preflight");
            }
        } else {
            // The physical path was recreated with a new Paimon table UUID. The single-writer
            // lifecycle lock permits replacing only this validation marker after a fresh scan.
            stateMap.put(markerKey, expectedMarker);
        }
        if (!expectedMarker.equals(stateMap.get(markerKey))) {
            throw new IllegalStateException("HASH_DYNAMIC preflight marker was not durably observable");
        }
    }

    private static void validateExactPrimaryKeyUniqueness(
            String tableKey, FileStoreTable table, String configuredTmpDirs) throws Exception {
        String tmpDirs = configuredTmpDirs;
        if (tmpDirs == null || tmpDirs.trim().isEmpty()) {
            tmpDirs = System.getProperty("java.io.tmpdir", new File(".").getAbsolutePath());
        }

        IOManager ioManager = null;
        GlobalIndexAssigner checker = null;
        List<String> spillDirs = Collections.emptyList();
        Long snapshotBefore = table.snapshotManager().latestSnapshotIdFromFileSystem();
        Exception failure = null;
        try {
            ioManager = IOManager.create(splitPaths(tmpDirs));
            spillDirs = PaimonSpillDirCleaner.registerLiveDirs(ioManager);
            FileStoreTable validationTable = withoutIndexTtl(table);
            checker = new GlobalIndexAssigner(validationTable);
            checker.open(0L, ioManager, 1, 0, (row, bucket) -> { });
            try (RecordReader<InternalRow> reader =
                         new IndexBootstrap(validationTable).bootstrap(1, 0)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    try {
                        InternalRow row;
                        while ((row = batch.next()) != null) {
                            checker.bootstrapKey(row);
                        }
                    } finally {
                        batch.releaseBatch();
                    }
                }
            }
            checker.endBoostrap(false);
            Long snapshotAfter = table.snapshotManager().latestSnapshotIdFromFileSystem();
            if (!Objects.equals(snapshotBefore, snapshotAfter)) {
                throw new IllegalStateException(
                        "Paimon table changed during HASH_DYNAMIC pollution preflight; "
                                + "only one write job per physical table is supported");
            }
        } catch (Exception e) {
            failure = containsMessage(e, "data contains duplicates")
                    ? new PaimonDynamicBucketPollutedException(tableKey, e)
                    : e;
        } finally {
            failure = close(checker, failure);
            failure = close(ioManager, failure);
            PaimonSpillDirCleaner.unregisterLiveDirs(spillDirs);
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * IndexBootstrap applies cross-partition-upsert.index-ttl by dropping old splits. That behavior
     * is correct for a live KEY_DYNAMIC index but invalid for an exact historical-pollution scan.
     * Use a read-only dynamic table copy with the TTL removed so every latest-snapshot split is
     * examined; the persisted table options are not changed.
     */
    static FileStoreTable withoutIndexTtl(FileStoreTable table) {
        if (!table.options().containsKey(CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key())) {
            return table;
        }
        return table.copy(Collections.singletonMap(
                CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key(), null));
    }

    private static Exception close(AutoCloseable closeable, Exception failure) {
        if (closeable == null) {
            return failure;
        }
        try {
            closeable.close();
        } catch (Exception closeError) {
            if (failure == null) {
                return closeError;
            }
            failure.addSuppressed(closeError);
        }
        return failure;
    }

    private static boolean containsMessage(Throwable error, String text) {
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(text)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
