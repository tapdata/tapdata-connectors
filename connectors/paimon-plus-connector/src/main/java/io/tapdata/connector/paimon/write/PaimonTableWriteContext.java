package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.write.bucket.DefaultPaimonBucketWriterRuntimeFactory;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterRuntimeFactory;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;

import io.tapdata.connector.paimon.util.PaimonSpillDirCleaner;
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
import java.util.Objects;

/**
 * Owns the connector transaction state for one physical Paimon table.
 *
 * <p>Bucket-specific routing and raw writer ownership belong exclusively to {@link
 * PaimonBucketWriterStrategy}; raw commit ownership belongs exclusively to {@link
 * PaimonTableCommitter}. This context only coordinates prepare, pending retry and task-state commit
 * identity. {@link #pendingCommits} is deliberately the in-process retry envelope for one table;
 * unlike Paimon's Flink sink it is not operator state and cannot restore CommitMessages after a
 * process crash. Paimon's Flink integration persists those committables as operator state; this
 * connector does not. Source: {@code paimon-flink/paimon-flink-common/src/main/java/org/apache/
 * paimon/flink/sink/RestoreCommittableStateManager.java}, lines 36-87. Baseline: {@code
 * apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e}.
 */
public final class PaimonTableWriteContext implements AutoCloseable {

    private final String tableKey;
    private final String tableName;
    private final String commitUser;
    private final PaimonBucketWriterStrategy writerStrategy;
    private final PaimonTableCommitter tableCommitter;
    private final CommitStateStore commitStateStore;
    private final IOManager ioManager;
    private final List<String> spillDirs;
    // Keep the exact identifier/message pair before the first commit I/O and across every
    // ambiguous retry. Paimon's filter is a latest-same-user identifier threshold (<= latest), not
    // an exact message lookup, so recovery must reuse the original user/id/messages. Safety
    // requires one owner and forbids any higher same-user identifier while this envelope is
    // pending; recovered task state may be ahead of a retained snapshot but never moves backwards.
    // The deployment contract explicitly excludes cross-JVM writers.
    // Source: paimon-core/src/main/java/org/apache/paimon/operation/
    // FileStoreCommitImpl.java#filterCommitted, lines 260-287; and paimon-core/src/main/java/
    // org/apache/paimon/table/sink/StreamWriteBuilder.java, lines 27-38.
    // Baseline: apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e.
    private final Map<Long, List<CommitMessage>> pendingCommits = new LinkedHashMap<>();

    private long nextCommitIdentifier;
    private volatile boolean closed;
    private volatile boolean failed;

    public static PaimonTableWriteContext create(
            String tableKey,
            String tableName,
            Table paimonTable,
            String commitUser,
            String configuredTmpDirs)
            throws Exception {
        return PaimonTableWriteContextFactory.create(
                tableKey, tableName, paimonTable, commitUser, configuredTmpDirs);
    }

    public static PaimonTableWriteContext create(
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

    public static PaimonTableWriteContext create(
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

    public static PaimonTableWriteContext create(
            String tableKey,
            String tableName,
            FileStoreTable fileStoreTable,
            String commitUser,
            String configuredTmpDirs,
            long nextCommitIdentifier,
            CommitStateStore commitStateStore,
            PaimonWriteSemanticContract writeSemanticContract)
            throws Exception {
        return PaimonTableWriteContextFactory.create(
                tableKey,
                tableName,
                fileStoreTable,
                commitUser,
                configuredTmpDirs,
                nextCommitIdentifier,
                commitStateStore,
                DefaultPaimonBucketWriterRuntimeFactory.INSTANCE,
                writeSemanticContract);
    }

    public PaimonTableWriteContext(
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

    public PaimonTableWriteContext(
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

    public String tableKey() {
        return tableKey;
    }

    public String tableName() {
        return tableName;
    }

    public String commitUser() {
        return commitUser;
    }

    public BucketMode bucketMode() {
        return writerStrategy.bucketMode();
    }

    public PaimonWriteSemanticContract writeSemanticContract() {
        return writerStrategy.writeSemanticContract();
    }

    public void validateRoutingRow(InternalRow row, String operation) {
        writerStrategy.validateRoutingRow(row, operation);
    }

    public synchronized boolean hasPendingCommit() {
        return !pendingCommits.isEmpty();
    }

    public synchronized long commit() throws Exception {
        ensureOpen();
        if (!pendingCommits.isEmpty()) {
            // A previous direct attempt has an unknown outcome. Never issue another direct commit
            // and never prepare new messages until Paimon confirms this exact pending envelope.
            return retryPendingCommit();
        }

        long identifier = nextCommitIdentifier;
        if (identifier == Long.MAX_VALUE) {
            failed = true;
            throw new IllegalStateException(
                    "Paimon commit identifier is exhausted for " + tableKey);
        }

        List<CommitMessage> messages;
        try {
            // All bucket strategies eventually use TableWriteImpl#prepareCommit; the dynamic hash
            // strategy first persists its bucket-assignment delta with the same identifier.
            // Source: paimon-core/src/main/java/org/apache/paimon/table/sink/
            // TableWriteImpl.java#prepareCommit, lines 259-263; and paimon-core/src/main/java/
            // org/apache/paimon/index/HashBucketAssigner.java#prepareCommit, lines 101-124.
            // Baseline: apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e.
            messages = Objects.requireNonNull(
                    writerStrategy.prepareCommit(identifier),
                    "Paimon prepareCommit messages for " + tableKey);
            // Publish pending before the first external commit I/O. A direct failure is always an
            // unknown outcome until filterAndCommit confirms the same envelope.
            pendingCommits.put(identifier, messages);
        } catch (Exception e) {
            failed = true;
            throw e;
        }

        try {
            // Paimon 1.3.2 contract: direct commit is faster because it skips committed-identifier
            // filtering. It is safe here only for this newly prepared, strictly monotonic
            // identifier while the single owner has no older pending envelope.
            // Source: paimon-core/src/main/java/org/apache/paimon/table/sink/
            // StreamTableCommit.java#commit, lines 52-62. Baseline:
            // apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e.
            tableCommitter.commit(identifier, messages);
        } catch (RuntimeException directFailure) {
            try {
                return retryPendingCommit();
            } catch (Exception recoveryFailure) {
                if (recoveryFailure != directFailure) {
                    recoveryFailure.addSuppressed(directFailure);
                }
                throw recoveryFailure;
            }
        }
        return completeConfirmedPending(new LinkedHashMap<>(pendingCommits));
    }

    public synchronized long retryPendingCommit() throws Exception {
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
        return completeConfirmedPending(snapshot);
    }

    private long completeConfirmedPending(Map<Long, List<CommitMessage>> confirmed) throws Exception {
        long lastIdentifier =
                confirmed.keySet().stream().mapToLong(Long::longValue).max().orElse(-1L);
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

    public void write(InternalRow row) throws Exception {
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
    public interface CommitStateStore {
        CommitStateStore NOOP = nextCommitIdentifier -> { };

        void save(long nextCommitIdentifier) throws Exception;
    }
}
