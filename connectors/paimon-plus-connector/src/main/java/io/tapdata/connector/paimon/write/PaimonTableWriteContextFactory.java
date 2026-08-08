package io.tapdata.connector.paimon.write;

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
        PaimonTableCommitter tableCommitter = null;
        PaimonBucketWriterStrategy writerStrategy = null;
        try {
            if (createIoManager) {
                PaimonSpillDirCleaner.IOManagerBuildResult built =
                        PaimonSpillDirCleaner.resolveAndCreateIOManager(configuredTmpDirs);
                ioManager = built.ioManager();
                spillDirs = built.spillDirs();
                rawWriter = (StreamTableWrite) builder.newWrite().withIOManager(ioManager);
            } else {
                rawWriter = builder.newWrite();
            }

            StreamTableCommit rawCommitter = builder.newCommit();
            tableCommitter = new PaimonStreamTableCommitter(rawCommitter);
            writerStrategy =
                    PaimonBucketWriterStrategyFactory.create(
                            new PaimonBucketWriterStrategyContext(
                                    tableKey,
                                    fileStoreTable,
                                    rawWriter,
                                    commitUser,
                                    ioManager,
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
                    commitStateStore);
        } catch (Exception e) {
            Exception failure =
                    fileStoreTable.bucketMode() == BucketMode.KEY_DYNAMIC
                            ? PaimonDynamicBucketPollutedException.wrapIfPolluted(tableKey, e)
                            : e;
            if (writerStrategy != null) {
                closeSuppressed(writerStrategy, failure);
            }
            closeSuppressed(tableCommitter, failure);
            if (writerStrategy == null) {
                closeSuppressed(rawWriter, failure);
            }
            if (ioManager != null) {
                try {
                    ioManager.close();
                } catch (Exception closeError) {
                    failure.addSuppressed(closeError);
                } finally {
                    PaimonSpillDirCleaner.unregisterLiveDirs(spillDirs);
                }
            }
            throw failure;
        }
    }

    private static void closeSuppressed(AutoCloseable closeable, Exception original) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception closeError) {
            original.addSuppressed(closeError);
        }
    }
}
