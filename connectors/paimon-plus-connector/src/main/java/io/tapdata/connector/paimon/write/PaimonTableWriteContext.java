package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.write.bucket.DefaultPaimonBucketWriterRuntimeFactory;
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
    private final PaimonCompactionLifecycle compactionLifecycle;
    private final PaimonNativeWriteAccess nativeWriteAccess;
    private boolean gracefulCloseFinished;
    private Throwable gracefulCloseFailure;
    private volatile boolean cleanupComplete;
    private StopOutcome stopOutcome = StopOutcome.SUCCESS;
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
    private volatile CloseState closeState = CloseState.ACTIVE;
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
        this(
                tableKey,
                tableName,
                commitUser,
                writerStrategy,
                tableCommitter,
                ioManager,
                spillDirs,
                nextCommitIdentifier,
                commitStateStore,
                // Direct construction implies no connector-managed async producers; the Factory
                // always passes its real lifecycle and verified native writer access.
                PaimonCompactionLifecycle.withoutCompactionExecutor());
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
            CommitStateStore commitStateStore,
            PaimonCompactionLifecycle compactionLifecycle) {
        this(tableKey, tableName, commitUser, writerStrategy, tableCommitter, ioManager, spillDirs,
                nextCommitIdentifier, commitStateStore, compactionLifecycle,
                PaimonNativeWriteAccess.withoutNativeWriters());
    }

    public PaimonTableWriteContext(
            String tableKey, String tableName, String commitUser,
            PaimonBucketWriterStrategy writerStrategy, PaimonTableCommitter tableCommitter,
            IOManager ioManager, List<String> spillDirs, long nextCommitIdentifier,
            CommitStateStore commitStateStore, PaimonCompactionLifecycle compactionLifecycle,
            PaimonNativeWriteAccess nativeWriteAccess) {
        this.tableKey = tableKey;
        this.tableName = tableName;
        this.commitUser = commitUser;
        this.writerStrategy = writerStrategy;
        this.tableCommitter = tableCommitter;
        this.ioManager = ioManager;
        this.spillDirs = spillDirs;
        this.nextCommitIdentifier = nextCommitIdentifier;
        this.commitStateStore = commitStateStore;
        this.compactionLifecycle = compactionLifecycle;
        this.nativeWriteAccess = Objects.requireNonNull(nativeWriteAccess, "nativeWriteAccess");
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
            compactionLifecycle.auditControlFailure();
            // Publish pending before the first external commit I/O. A direct failure is always an
            // unknown outcome until filterAndCommit confirms the same envelope.
            pendingCommits.put(identifier, messages);
        } catch (Exception | Error e) {
            failed = true;
            throw e;
        }
        try { return commitPrepared(identifier, messages); }
        catch (Error failure) { failed = true; throw failure; }
    }

    private long commitPrepared(long identifier, List<CommitMessage> messages) throws Exception {
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
                return confirmPendingCommit();
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
        return confirmPendingCommit();
    }

    private long confirmPendingCommit() throws Exception {
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
        } catch (Exception | Error e) {
            // The snapshot is already confirmed. Keep pending empty and fence until restart can
            // reconcile this stable commit user against Paimon's latest user snapshot.
            failed = true;
            throw e;
        }
        return lastIdentifier;
    }

    /**
     * Service 已取得准入归零及业务确认屏障后调用；DDL 和硬失败清理传 false。
     * 终态异常与资源清理证明独立，重复调用复用同一次结果。
     */
    public synchronized StopOutcome closeForStop(boolean finalizeCompaction,
            CloseObserver progress) throws Exception {
        if (gracefulCloseFinished) {
            throwIfPresent(gracefulCloseFailure);
            return stopOutcome;
        }
        if (compactionLifecycle == null) {
            throw new IllegalStateException("Paimon spill barrier is missing for table " + tableKey);
        }
        closeState = CloseState.CLOSING;
        Throwable failure = null;
        boolean discarded = false;
        if (finalizeCompaction) {
            try {
                if (failed || !pendingCommits.isEmpty()) {
                    throw new IllegalStateException("Final Compaction requires a confirmed business barrier for " + tableKey);
                }
                phase(progress, "FINAL_PREPARE");
                List<CommitMessage> messages = null;
                Throwable prepareFailure = null;
                boolean hasFinalCompaction = false;
                try {
                    messages = Objects.requireNonNull(writerStrategy.prepareFinalCommit(nextCommitIdentifier));
                    hasFinalCompaction = PaimonNativeWriteAccess.hasFinalCompaction(messages);
                } catch (Exception | Error caught) {
                    prepareFailure = caught;
                }
                // prepare 可触发最后一个任务；这里才封闭提交，且先审计原生吞掉的取消。
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java#L247
                try { compactionLifecycle.sealFinalPrepare(); }
                catch (Exception | Error control) {
                    if (prepareFailure != null) { control.addSuppressed(prepareFailure); }
                    throw control;
                }
                if (prepareFailure != null) {
                    if (!PaimonCompactionExecutor.isNativeCompactionFailure(prepareFailure)) {
                        throwIfPresent(prepareFailure);
                    }
                    discarded = true;
                    discardedPhase(progress, nextCommitIdentifier, prepareFailure);
                } else if (hasFinalCompaction) {
                    if (nextCommitIdentifier == Long.MAX_VALUE) {
                        throw new IllegalStateException("Paimon commit identifier is exhausted for " + tableKey);
                    }
                    phase(progress, "FINAL_COMMIT");
                    // 与业务提交共享精确 pending 确认核心；不调用 Service 的 offset 发布路径。
                    // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/StreamTableCommit.java#L52
                    pendingCommits.put(nextCommitIdentifier, messages);
                    commitPrepared(nextCommitIdentifier, messages);
                }
            } catch (Exception | Error caught) {
                failure = caught;
            }
        }
        phase(progress, "WAIT_COMPACTION");
        InterruptedException interruption = compactionLifecycle.shutdownAndAwaitCompletion();
        failure = appendFailure(failure, interruption);
        observe(progress::compactionTerminated);
        try {
            for (Throwable syncFailure : nativeWriteAccess.syncAll()) {
                if (discarded && PaimonCompactionExecutor.isNativeCompactionFailure(syncFailure)) {
                    discardedPhase(progress, nextCommitIdentifier, syncFailure);
                } else {
                    failure = appendFailure(failure, syncFailure);
                }
            }
        } catch (Exception | Error caught) {
            failure = appendFailure(failure, caught);
        }
        try { compactionLifecycle.auditControlFailure(); }
        catch (Exception | Error control) { failure = appendFailure(failure, control); }

        phase(progress, "CLOSE_WRITER");
        boolean writerClosed = false;
        boolean committerClosed = false;
        try { writerStrategy.close(); writerClosed = true; }
        catch (Exception | Error caught) { failure = appendFailure(failure, caught); }
        phase(progress, "CLOSE_COMMITTER");
        try { tableCommitter.close(); committerClosed = true; }
        catch (Exception | Error caught) { failure = appendFailure(failure, caught); }
        // AbstractFileStoreWrite.close 某桶普通 I/O 错误可能截断后续 bucket；不能把 executor
        // 已终止等价为全部 writer 已关闭。未取得资源证明时保留 IOManager 和目录保护。
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L304
        if (writerClosed && committerClosed) {
            boolean deleted = ioManager == null;
            if (ioManager != null) {
                phase(progress, "CLOSE_SPILL");
                try { ioManager.close(); deleted = true; }
                catch (Exception | Error caught) { failure = appendFailure(failure, caught); }
                PaimonSpillDirCleaner.releaseAfterClose(spillDirs, deleted);
                if (deleted) { observe(progress::spillClosed); }
            }
            cleanupComplete = deleted;
        }
        gracefulCloseFailure = failure;
        stopOutcome = failure != null ? StopOutcome.FAILED
                : discarded ? StopOutcome.SUCCESS_COMPACTION_DISCARDED : StopOutcome.SUCCESS;
        gracefulCloseFinished = true;
        closeState = CloseState.CLOSED;
        if (interruption != null) { Thread.currentThread().interrupt(); }
        throwIfPresent(failure);
        return stopOutcome;
    }

    public boolean cleanupComplete() { return cleanupComplete; }

    public enum StopOutcome { SUCCESS, SUCCESS_COMPACTION_DISCARDED, FAILED }

    @FunctionalInterface
    public interface CloseObserver {
        void phase(String phase);
        default void compactionDiscarded(long identifier, Throwable failure) {}
        default void compactionTerminated() {}
        default void spillClosed() {}
    }

    private static void phase(CloseObserver progress, String phase) {
        observe(() -> progress.phase(phase));
    }

    private static void discardedPhase(CloseObserver progress, long identifier, Throwable failure) {
        observe(() -> progress.compactionDiscarded(identifier, failure));
    }

    private static void observe(Runnable event) {
        try { event.run(); }
        catch (RuntimeException ignored) { /* INFO 观察失败不能破坏资源收尾。 */ }
    }

    private static Throwable appendFailure(Throwable first, Throwable next) {
        if (first == null) { return next; }
        if (next != null && next != first) { first.addSuppressed(next); }
        return first;
    }

    private static void throwIfPresent(Throwable failure) throws Exception {
        if (failure instanceof Error) { throw (Error) failure; }
        if (failure != null) { throw (Exception) failure; }
    }

    public void write(InternalRow row) throws Exception {
        ensureWritable();
        try {
            writerStrategy.write(row);
        } catch (Exception | Error e) {
            failed = true;
            throw e;
        }
    }

    /** 创建时缓存的 canonical Spill 路径；日志不得重新触发 IOManager 的懒初始化。 */
    public List<String> spillDirs() { return spillDirs; }

    /** 普通关闭仅清理；STOP 的最终准备和失败豁免必须显式通过 closeForStop 准入。 */
    @Override
    public void close() throws Exception { closeForStop(false, phase -> {}); }

    private void ensureOpen() {
        if (closeState != CloseState.ACTIVE) {
            throw new IllegalStateException(
                    "Paimon table write context is closing or closed: " + tableKey);
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

    private enum CloseState { ACTIVE, CLOSING, CLOSED }

    @FunctionalInterface
    public interface CommitStateStore {
        CommitStateStore NOOP = nextCommitIdentifier -> { };

        void save(long nextCommitIdentifier) throws Exception;
    }
}
