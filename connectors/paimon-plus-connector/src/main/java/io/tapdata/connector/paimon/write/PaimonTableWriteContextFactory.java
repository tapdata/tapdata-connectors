package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.config.PaimonSyncExpireMode;
import io.tapdata.connector.paimon.write.bucket.DefaultPaimonBucketWriterRuntimeFactory;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterRuntimeFactory;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategyContext;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategyFactory;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractResolver;

import io.tapdata.connector.paimon.commit.PaimonCommitStateStore;
import io.tapdata.connector.paimon.exception.PaimonDynamicBucketPollutedException;
import io.tapdata.connector.paimon.util.PaimonSpillDirCleaner;
import org.apache.paimon.Snapshot;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableWriteImpl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Creates a connector-owned write context and confines Paimon's raw writer and committer APIs to
 * one construction boundary.
 *
 * <p>The returned context only exposes the connector strategy and committer abstractions. This
 * keeps bucket-specific APIs out of the service and transaction state machine while retaining the
 * original resource ownership and rollback order.
 */
public final class PaimonTableWriteContextFactory {

    private PaimonTableWriteContextFactory() {}

    public static PaimonTableWriteContext create(
            String tableKey,
            String tableName,
            Table paimonTable,
            String commitUser,
            String configuredTmpDirs)
            throws Exception {
        if (!(paimonTable instanceof FileStoreTable)) {
            throw new IllegalArgumentException(
                    "Only FileStoreTable supports connector writes, but got "
                            + paimonTable.getClass().getName());
        }

        FileStoreTable fileStoreTable = (FileStoreTable) paimonTable;
        PaimonSyncExpireMode.requireSync(tableKey, fileStoreTable);
        Optional<Snapshot> latestUserSnapshot =
                fileStoreTable.snapshotManager().latestSnapshotOfUserFromFilesystem(commitUser);
        long nextCommitIdentifier =
                latestUserSnapshot.map(PaimonCommitStateStore::nextIdentifier).orElse(0L);
        return create(
                tableKey,
                tableName,
                fileStoreTable,
                commitUser,
                configuredTmpDirs,
                nextCommitIdentifier,
                PaimonTableWriteContext.CommitStateStore.NOOP);
    }

    public static PaimonTableWriteContext create(
            String tableKey,
            String tableName,
            FileStoreTable fileStoreTable,
            String commitUser,
            String configuredTmpDirs,
            long nextCommitIdentifier,
            PaimonTableWriteContext.CommitStateStore commitStateStore)
            throws Exception {
        return create(
                tableKey,
                tableName,
                fileStoreTable,
                commitUser,
                configuredTmpDirs,
                nextCommitIdentifier,
                commitStateStore,
                DefaultPaimonBucketWriterRuntimeFactory.INSTANCE);
    }

    public static PaimonTableWriteContext create(
            String tableKey,
            String tableName,
            FileStoreTable fileStoreTable,
            String commitUser,
            String configuredTmpDirs,
            long nextCommitIdentifier,
            PaimonTableWriteContext.CommitStateStore commitStateStore,
            PaimonBucketWriterRuntimeFactory runtimeFactory)
            throws Exception {
        PaimonSyncExpireMode.requireSync(tableKey, fileStoreTable);
        PaimonWriteSemanticContract writeSemanticContract =
                PaimonWriteSemanticContractResolver.resolve(tableKey, fileStoreTable);
        return create(
                tableKey,
                tableName,
                fileStoreTable,
                commitUser,
                configuredTmpDirs,
                nextCommitIdentifier,
                commitStateStore,
                runtimeFactory,
                writeSemanticContract);
    }

    public static PaimonTableWriteContext create(
            String tableKey,
            String tableName,
            FileStoreTable fileStoreTable,
            String commitUser,
            String configuredTmpDirs,
            long nextCommitIdentifier,
            PaimonTableWriteContext.CommitStateStore commitStateStore,
            PaimonBucketWriterRuntimeFactory runtimeFactory,
            PaimonWriteSemanticContract writeSemanticContract)
            throws Exception {
        Objects.requireNonNull(fileStoreTable, "fileStoreTable");
        PaimonSyncExpireMode.requireSync(tableKey, fileStoreTable);
        Objects.requireNonNull(writeSemanticContract, "writeSemanticContract");
        if (nextCommitIdentifier < 0L) {
            throw new IllegalArgumentException("Negative Paimon commit identifier for " + tableKey);
        }
        if (writeSemanticContract.bucketMode() != fileStoreTable.bucketMode()) {
            throw new IllegalArgumentException(
                    "Paimon write semantic contract mode mismatch for " + tableKey);
        }
        // Build writer and committer from the same StreamWriteBuilder so both carry one stable
        // commitUser. Paimon 1.3.2 forwards that user to both newWrite and newCommit; separating
        // builders/users would break exact-envelope filterAndCommit recovery.
        // Source: paimon-core/src/main/java/org/apache/paimon/table/sink/
        // StreamWriteBuilderImpl.java#withCommitUser/#newWrite/#newCommit, lines 64-76.
        // Baseline: apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e.
        StreamWriteBuilder builder =
                fileStoreTable.newStreamWriteBuilder().withCommitUser(commitUser);
        boolean requiresIoManager =
                PaimonBucketWriterStrategyFactory.requiresIoManager(fileStoreTable.bucketMode())
                        || fileStoreTable.coreOptions().writeBufferSpillable();
        // When the writer strategy needs an IOManager, always create one. Otherwise still honor an
        // explicitly configured tmp dir (diskTmpDir defaults to "/tmp") so users can opt into spill
        // for append-only / bucket-unaware writers; only skip when neither applies.
        boolean createIoManager =
                requiresIoManager || (configuredTmpDirs != null && !configuredTmpDirs.trim().isEmpty());

        IOManager ioManager = null;
        List<String> spillDirs = Collections.emptyList();
        StreamTableWrite rawWriter = null;
        StreamTableCommit rawCommitter = null;
        PaimonNativeWriteAccess nativeWriteAccess = null;
        PaimonTableCommitter tableCommitter = null;
        PaimonBucketWriterStrategy writerStrategy = null;
        PaimonCompactionLifecycle compactionLifecycle = PaimonCompactionLifecycle.forTable(tableKey);
        try {
            // Connector-owned compaction executor, injected before the writer's first use. Paimon
            // then never shuts this executor down itself, so only the connector can produce the
            // termination proof that gates IOManager close.
            // Source: paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java#
            // withCompactExecutor, lines 134-137; AbstractFileStoreWrite.java#withCompactExecutor,
            // lines 146-150 (closeCompactExecutorWhenLeaving=false). Baseline:
            // apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e (= 1.3.2@c05f7d1f).
            rawWriter = builder.newWrite();
            TableWriteImpl<?> tableWrite = requireTableWriteImpl(rawWriter, tableKey);
            nativeWriteAccess = PaimonNativeWriteAccess.of(rawWriter);
            tableWrite = tableWrite.withCompactExecutor(compactionLifecycle.compactionExecutor());
            if (createIoManager) {
                PaimonSpillDirCleaner.IOManagerBuildResult built =
                        PaimonSpillDirCleaner.resolveAndCreateIOManager(configuredTmpDirs);
                ioManager = built.ioManager();
                spillDirs = built.spillDirs();
                tableWrite = tableWrite.withIOManager(ioManager);
            }
            rawWriter = tableWrite;

            rawCommitter = builder.newCommit();
            if (!(rawCommitter instanceof org.apache.paimon.table.sink.TableCommitImpl)) {
                throw new IllegalArgumentException("Paimon committer type drift: expected TableCommitImpl");
            }
            tableCommitter = new PaimonStreamTableCommitter(rawCommitter);
            // GlobalIndexAssigner.open 使用 tempDirs() 创建 rocksdb-*，必须约束在已登记目录内。
            // Paimon 1.3.2: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java#L136
            IOManager strategyIoManager = ioManager == null ? null
                    : PaimonSpillDirCleaner.withConfinedTempDirs(ioManager, spillDirs);
            writerStrategy =
                    PaimonBucketWriterStrategyFactory.create(
                            new PaimonBucketWriterStrategyContext(
                                    tableKey,
                                    fileStoreTable,
                                    rawWriter,
                                    commitUser,
                                    strategyIoManager,
                                    writeSemanticContract),
                            runtimeFactory);

            return new PaimonTableWriteContext(
                    tableKey,
                    tableName,
                    commitUser,
                    writerStrategy,
                    tableCommitter,
                    ioManager,
                    spillDirs,
                    nextCommitIdentifier,
                    commitStateStore,
                    compactionLifecycle,
                    nativeWriteAccess);
        } catch (Exception | Error original) {
            boolean safe = !(original instanceof IncompleteCleanupException);
            Throwable failure = !safe ? original.getCause()
                    : original instanceof Exception && fileStoreTable.bucketMode() == BucketMode.KEY_DYNAMIC
                    ? PaimonDynamicBucketPollutedException.wrapIfPolluted(tableKey, (Exception) original) : original;
            // 构造失败同样先停止实际任务，再消费所有 bucket Future；不能先 rawWriter.close
            // (原生会 cancel) 或 IOManager.close (递归删除)。所有原始引用在类型检查前保存。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java#L343
            InterruptedException interruption = compactionLifecycle.shutdownAndAwaitCompletion();
            if (interruption != null) { failure.addSuppressed(interruption); }
            if (nativeWriteAccess != null) {
                try {
                    for (Throwable syncFailure : nativeWriteAccess.syncAll()) {
                        if (syncFailure != failure) { failure.addSuppressed(syncFailure); }
                    }
                } catch (Exception | Error syncFailure) {
                    if (syncFailure != failure) { failure.addSuppressed(syncFailure); }
                }
            }
            // strategy 已拥有 rawWriter 时只关闭一次；committer 同理。
            safe &= closeSuppressed(writerStrategy != null ? writerStrategy : rawWriter, failure);
            safe &= closeSuppressed(tableCommitter != null ? tableCommitter : rawCommitter, failure);
            if (ioManager != null && safe) {
                boolean deleted = closeSuppressed(ioManager, failure);
                PaimonSpillDirCleaner.releaseAfterClose(spillDirs, deleted);
                safe = deleted;
            }
            if (interruption != null) { Thread.currentThread().interrupt(); }
            if (!safe) { throw new IncompleteCleanupException(tableKey, failure); }
            if (failure instanceof Error) { throw (Error) failure; }
            throw (Exception) failure;
        }
    }

    /** Service 不能在半构造资源未完整关闭时无条件释放物理 owner。 */
    public static final class IncompleteCleanupException extends Exception {
        public IncompleteCleanupException(String tableKey, Throwable cause) {
            super("Paimon construction cleanup is incomplete for " + tableKey, cause);
        }
    }

    private static TableWriteImpl<?> requireTableWriteImpl(
            StreamTableWrite rawWriter, String tableKey) {
        if (!(rawWriter instanceof TableWriteImpl)) {
            throw new IllegalArgumentException(
                    "Paimon writer type drift for "
                            + tableKey
                            + ": expected TableWriteImpl to inject the connector compaction"
                            + " executor, but got "
                            + rawWriter.getClass().getName());
        }
        return (TableWriteImpl<?>) rawWriter;
    }

    private static boolean closeSuppressed(AutoCloseable closeable, Throwable original) {
        if (closeable == null) { return true; }
        try {
            closeable.close();
            return true;
        } catch (Exception | Error closeError) {
            if (closeError != original) { original.addSuppressed(closeError); }
            return false;
        }
    }
}
