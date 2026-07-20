package io.tapdata.connector.paimon.service;

import org.apache.paimon.Snapshot;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.paimon.disk.IOManagerImpl.splitPaths;

/**
 * Creates a connector-owned write context and confines Paimon's raw writer and committer APIs to
 * one construction boundary.
 *
 * <p>The returned context only exposes the connector strategy and committer abstractions. This
 * keeps bucket-specific APIs out of the service and transaction state machine while retaining the
 * original resource ownership and rollback order.
 */
final class PaimonTableWriteContextFactory {

    private PaimonTableWriteContextFactory() {}

    static PaimonTableWriteContext create(
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
                latestUserSnapshot.map(PaimonTableWriteContextFactory::nextIdentifier).orElse(0L);
        return create(
                tableKey,
                tableName,
                fileStoreTable,
                commitUser,
                configuredTmpDirs,
                nextCommitIdentifier,
                PaimonTableWriteContext.CommitStateStore.NOOP);
    }

    static PaimonTableWriteContext create(
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

    static PaimonTableWriteContext create(
            String tableKey,
            String tableName,
            FileStoreTable fileStoreTable,
            String commitUser,
            String configuredTmpDirs,
            long nextCommitIdentifier,
            PaimonTableWriteContext.CommitStateStore commitStateStore,
            PaimonBucketWriterRuntimeFactory runtimeFactory)
            throws Exception {
        if (nextCommitIdentifier < 0L) {
            throw new IllegalArgumentException("Negative Paimon commit identifier for " + tableKey);
        }

        StreamWriteBuilder builder =
                fileStoreTable.newStreamWriteBuilder().withCommitUser(commitUser);
        boolean requiresIoManager =
                PaimonBucketWriterStrategyFactory.requiresIoManager(fileStoreTable.bucketMode())
                        || fileStoreTable.coreOptions().writeBufferSpillable();
        String tmpDirs = configuredTmpDirs;
        if ((tmpDirs == null || tmpDirs.trim().isEmpty()) && requiresIoManager) {
            tmpDirs = System.getProperty("java.io.tmpdir", new File(".").getAbsolutePath());
        }

        IOManager ioManager = null;
        List<String> spillDirs = Collections.emptyList();
        StreamTableWrite rawWriter = null;
        PaimonTableCommitter tableCommitter = null;
        PaimonBucketWriterStrategy writerStrategy = null;
        try {
            if (tmpDirs == null || tmpDirs.trim().isEmpty()) {
                rawWriter = builder.newWrite();
            } else {
                ioManager = IOManager.create(splitPaths(tmpDirs));
                spillDirs = PaimonSpillDirCleaner.registerLiveDirs(ioManager);
                rawWriter = (StreamTableWrite) builder.newWrite().withIOManager(ioManager);
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
                                    ioManager),
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
                                    && containsMessage(e, "data contains duplicates")
                            ? new PaimonDynamicBucketPollutedException(tableKey, e)
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

    private static long nextIdentifier(Snapshot snapshot) {
        if (snapshot.commitIdentifier() == Long.MAX_VALUE) {
            throw new IllegalStateException("Paimon commit identifier is exhausted");
        }
        return snapshot.commitIdentifier() + 1L;
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
