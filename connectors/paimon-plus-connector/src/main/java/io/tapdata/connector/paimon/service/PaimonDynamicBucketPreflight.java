package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.commit.PaimonCommitStateStore;

import io.tapdata.connector.paimon.exception.PaimonDynamicBucketPollutedException;
import io.tapdata.connector.paimon.util.PaimonSpillDirCleaner;
import io.tapdata.connector.paimon.write.PaimonTableWriteContextFactory.IncompleteCleanupException;
import io.tapdata.entity.utils.cache.KVMap;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** One-time exact-primary-key pollution preflight for legacy HASH_DYNAMIC tables. */
public final class PaimonDynamicBucketPreflight {

    private static final String MARKER_PREFIX = "paimon.hash-dynamic-preflight-v1.";

    private PaimonDynamicBucketPreflight() {
    }

    public static void ensureHashDynamicValidated(
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
        IOManager ioManager = null;
        GlobalIndexAssigner checker = null;
        List<String> spillDirs = Collections.emptyList();
        Long snapshotBefore = table.snapshotManager().latestSnapshotIdFromFileSystem();
        Throwable failure = null;
        boolean cleanupComplete = false;
        try {
            PaimonSpillDirCleaner.IOManagerBuildResult built =
                    PaimonSpillDirCleaner.resolveAndCreateIOManager(configuredTmpDirs);
            ioManager = built.ioManager();
            spillDirs = built.spillDirs();
            FileStoreTable validationTable = withoutIndexTtl(table);
            checker = new GlobalIndexAssigner(validationTable);
            // 与写入策略共享约束视图；原始 IOManager 仍由本方法在 checker 关闭后释放。
            // Paimon 1.3.2 GlobalIndexAssigner.open: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java#L136
            checker.open(0L, PaimonSpillDirCleaner.withConfinedTempDirs(ioManager, spillDirs),
                    1, 0, (row, bucket) -> { });
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
        } catch (Exception | Error e) {
            failure = e instanceof Exception
                    ? PaimonDynamicBucketPollutedException.wrapIfPolluted(tableKey, e) : e;
        } finally {
            // 原生 GlobalIndexAssigner.open 持有 RocksDB / bootstrap Spill；close 失败不能
            // 当作“业务预检失败但资源已安全释放”。只有 checker 正常关闭才允许删除其 IO 目录。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java#L114
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java#L276
            Throwable checkerFailure = close(checker);
            failure = appendFailure(failure, checkerFailure);
            if (checkerFailure == null) {
                Throwable ioFailure = close(ioManager);
                failure = appendFailure(failure, ioFailure);
                cleanupComplete = ioFailure == null;
                PaimonSpillDirCleaner.releaseAfterClose(spillDirs, cleanupComplete);
            }
        }
        if (!cleanupComplete) { throw new IncompleteCleanupException(tableKey, failure); }
        if (failure instanceof Error) { throw (Error) failure; }
        if (failure != null) { throw (Exception) failure; }
    }

    /**
     * IndexBootstrap applies cross-partition-upsert.index-ttl by dropping old splits. That behavior
     * is correct for a live KEY_DYNAMIC index but invalid for an exact historical-pollution scan.
     * Use a read-only dynamic table copy with the TTL removed so every latest-snapshot split is
     * examined; the persisted table options are not changed.
     */
    public static FileStoreTable withoutIndexTtl(FileStoreTable table) {
        if (!table.options().containsKey(CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key())) {
            return table;
        }
        return table.copy(Collections.singletonMap(
                CoreOptions.CROSS_PARTITION_UPSERT_INDEX_TTL.key(), null));
    }

    private static Throwable close(AutoCloseable closeable) {
        if (closeable == null) { return null; }
        try { closeable.close(); return null; }
        catch (Exception | Error failure) { return failure; }
    }

    private static Throwable appendFailure(Throwable first, Throwable next) {
        if (first == null) { return next; }
        if (next != null && next != first) { first.addSuppressed(next); }
        return first;
    }
}
