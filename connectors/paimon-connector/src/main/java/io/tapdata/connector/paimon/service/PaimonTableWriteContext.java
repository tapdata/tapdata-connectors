package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Owns the connector transaction state for one physical Paimon table.
 *
 * <p>Bucket-specific routing and raw writer ownership belong exclusively to {@link
 * PaimonBucketWriterStrategy}; raw commit ownership belongs exclusively to {@link
 * PaimonTableCommitter}. This context only coordinates prepare, pending retry and durable commit
 * identity.
 */
final class PaimonTableWriteContext implements AutoCloseable {

    private final String tableKey;
    private final String tableName;
    private final String commitUser;
    private final PaimonBucketWriterStrategy writerStrategy;
    private final PaimonTableCommitter tableCommitter;
    private final CommitStateStore commitStateStore;
    private final IOManager ioManager;
    private final List<String> spillDirs;
    private final Map<Long, List<CommitMessage>> pendingCommits = new LinkedHashMap<>();

    private long nextCommitIdentifier;
    private boolean closed;
    private boolean failed;

    static PaimonTableWriteContext create(
            String tableKey,
            String tableName,
            Table paimonTable,
            String commitUser,
            String configuredTmpDirs)
            throws Exception {
        return PaimonTableWriteContextFactory.create(
                tableKey, tableName, paimonTable, commitUser, configuredTmpDirs);
    }

    static PaimonTableWriteContext create(
            String tableKey,
            String tableName,
            FileStoreTable fileStoreTable,
            String commitUser,
            String configuredTmpDirs,
            long nextCommitIdentifier,
            CommitStateStore commitStateStore)
            throws Exception {
        return PaimonTableWriteContextFactory.create(
                tableKey,
                tableName,
                fileStoreTable,
                commitUser,
                configuredTmpDirs,
                nextCommitIdentifier,
                commitStateStore);
    }

    static PaimonTableWriteContext create(
            String tableKey,
            String tableName,
            FileStoreTable fileStoreTable,
            String commitUser,
            String configuredTmpDirs,
            long nextCommitIdentifier,
            CommitStateStore commitStateStore,
            PaimonBucketWriterRuntimeFactory runtimeFactory)
            throws Exception {
        return PaimonTableWriteContextFactory.create(
                tableKey,
                tableName,
                fileStoreTable,
                commitUser,
                configuredTmpDirs,
                nextCommitIdentifier,
                commitStateStore,
                runtimeFactory);
    }

    PaimonTableWriteContext(
            String tableKey,
            String tableName,
            String commitUser,
            PaimonBucketWriterStrategy writerStrategy,
            PaimonTableCommitter tableCommitter,
            IOManager ioManager,
            List<String> spillDirs,
            long nextCommitIdentifier) {
        this(
                tableKey,
                tableName,
                commitUser,
                writerStrategy,
                tableCommitter,
                ioManager,
                spillDirs,
                nextCommitIdentifier,
                CommitStateStore.NOOP);
    }

    PaimonTableWriteContext(
            String tableKey,
            String tableName,
            String commitUser,
            PaimonBucketWriterStrategy writerStrategy,
            PaimonTableCommitter tableCommitter,
            IOManager ioManager,
            List<String> spillDirs,
            long nextCommitIdentifier,
            CommitStateStore commitStateStore) {
        this.tableKey = tableKey;
        this.tableName = tableName;
        this.commitUser = commitUser;
        this.writerStrategy = writerStrategy;
        this.tableCommitter = tableCommitter;
        this.ioManager = ioManager;
        this.spillDirs = spillDirs;
        this.nextCommitIdentifier = nextCommitIdentifier;
        this.commitStateStore = commitStateStore;
    }

    String tableKey() {
        return tableKey;
    }

    String tableName() {
        return tableName;
    }

    String commitUser() {
        return commitUser;
    }

    BucketMode bucketMode() {
        return writerStrategy.bucketMode();
    }

    void validateRequiredRoutingFields(Map<String, ?> data, String operation) {
        for (String field : writerStrategy.requiredRoutingFields()) {
            if (data == null || !data.containsKey(field) || data.get(field) == null) {
                throw new PaimonFatalWriteException(
                        "Missing non-null Paimon routing field '"
                                + field
                                + "' for "
                                + operation
                                + " on dynamic-bucket table "
                                + tableKey);
            }
        }
    }

    synchronized boolean hasPendingCommit() {
        return !pendingCommits.isEmpty();
    }

    synchronized long commit() throws Exception {
        ensureOpen();
        if (pendingCommits.isEmpty()) {
            long identifier = nextCommitIdentifier;
            if (identifier == Long.MAX_VALUE) {
                failed = true;
                throw new IllegalStateException(
                        "Paimon commit identifier is exhausted for " + tableKey);
            }
            try {
                List<CommitMessage> messages = writerStrategy.prepareCommit(identifier);
                pendingCommits.put(identifier, messages);
            } catch (Exception e) {
                failed = true;
                throw e;
            }
        }
        return retryPendingCommit();
    }

    synchronized long retryPendingCommit() throws Exception {
        ensureOpen();
        if (pendingCommits.isEmpty()) {
            return nextCommitIdentifier - 1L;
        }

        Map<Long, List<CommitMessage>> snapshot = new LinkedHashMap<>(pendingCommits);
        int committed = tableCommitter.filterAndCommit(snapshot);
        if (committed < 0 || committed > snapshot.size()) {
            failed = true;
            throw new IllegalStateException(
                    "Invalid filterAndCommit result "
                            + committed
                            + " for "
                            + snapshot.size()
                            + " pending commits");
        }
        long lastIdentifier =
                snapshot.keySet().stream().mapToLong(Long::longValue).max().orElse(-1L);
        if (lastIdentifier == Long.MAX_VALUE) {
            failed = true;
            throw new IllegalStateException(
                    "Paimon commit identifier is exhausted for " + tableKey);
        }
        pendingCommits.clear();
        nextCommitIdentifier = Math.max(nextCommitIdentifier, lastIdentifier + 1L);
        try {
            commitStateStore.save(nextCommitIdentifier);
        } catch (Exception e) {
            // The snapshot is already confirmed. Keep pending empty and fence until restart can
            // reconcile this stable commit user against Paimon's latest user snapshot.
            failed = true;
            throw e;
        }
        return lastIdentifier;
    }

    public synchronized void write(InternalRow row) throws Exception {
        ensureWritable();
        try {
            writerStrategy.write(row);
        } catch (Exception e) {
            failed = true;
            throw e;
        }
    }

    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;

        List<Exception> errors = new ArrayList<>();
        closeAndCollect(writerStrategy, errors);
        closeAndCollect(tableCommitter, errors);
        if (ioManager != null) {
            closeAndCollect(ioManager, errors);
            PaimonSpillDirCleaner.unregisterLiveDirs(spillDirs);
        }

        if (!errors.isEmpty()) {
            Exception first = errors.get(0);
            for (int i = 1; i < errors.size(); i++) {
                first.addSuppressed(errors.get(i));
            }
            throw first;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(
                    "Paimon table write context is already closed: " + tableKey);
        }
        if (failed) {
            throw new IllegalStateException(
                    "Paimon table write context has failed and must be rebuilt: " + tableKey);
        }
    }

    private void ensureWritable() {
        ensureOpen();
        if (!pendingCommits.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot write while a Paimon commit outcome is pending for table " + tableKey);
        }
    }

    private static void closeAndCollect(AutoCloseable closeable, List<Exception> errors) {
        try {
            closeable.close();
        } catch (Exception e) {
            errors.add(e);
        }
    }

    @FunctionalInterface
    interface CommitStateStore {
        CommitStateStore NOOP = nextCommitIdentifier -> { };

        void save(long nextCommitIdentifier) throws Exception;
    }
}
