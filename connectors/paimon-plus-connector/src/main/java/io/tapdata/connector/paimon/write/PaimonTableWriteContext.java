package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.util.PaimonFailures;
import io.tapdata.connector.paimon.write.bucket.DefaultPaimonBucketWriterRuntimeFactory;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;

import io.tapdata.connector.paimon.util.PaimonSpillDirCleaner;
import io.tapdata.connector.paimon.service.PaimonStopController;
import io.tapdata.connector.paimon.service.PaimonStopResources;
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
    private PaimonStopResources.Scope stopScope = PaimonStopResources.Scope.standalone("table-context");
    public void attachStopScope(PaimonStopResources.Scope scope) {
        this.stopScope = Objects.requireNonNull(scope);
        if (compactionLifecycle != null) { compactionLifecycle.attachStopController(scope.controller()); }
    }
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
                    stopScope.call("business prepare", () -> writerStrategy.prepareCommit(identifier)),
                    "Paimon prepareCommit messages for " + tableKey);
            compactionLifecycle.auditControlFailure();
            // Publish pending before the first external commit I/O. A direct failure is always an
            // unknown outcome until filterAndCommit confirms the same envelope.
            stopScope.controller().publish("retain pending business commit", () -> {
                pendingCommits.put(identifier, messages); return null;
            });
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
            stopScope.run("snapshot commit", () -> tableCommitter.commit(identifier, messages));
        } catch (RuntimeException directFailure) {
            try {
                return confirmPendingCommit();
            } catch (Exception recoveryFailure) {
                if (recoveryFailure != directFailure) {
                    PaimonFailures.append(recoveryFailure, directFailure);
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
        int committed = stopScope.call("snapshot filterAndCommit", () -> tableCommitter.filterAndCommit(snapshot));
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
        stopScope.check("publish confirmed pending");
        long confirmedNext = Math.max(nextCommitIdentifier, lastIdentifier + 1L);
        try {
            stopScope.run("save commit identity", () -> commitStateStore.save(confirmedNext));
            stopScope.controller().publish("publish confirmed pending", () -> {
                pendingCommits.clear(); nextCommitIdentifier = confirmedNext; return null;
            });
        } catch (Exception | Error e) {
            // 正常状态存储失败：snapshot 已确认，保留旧的失败栅栏语义；
            // 超时/冻结时保留 pending，迟到返回不能推进身份或启动重试。
            if (!stopScope.controller().isRetained() && !stopScope.controller().expired()) {
                pendingCommits.clear();
                nextCommitIdentifier = confirmedNext;
            }
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
            IllegalStateException missing = new IllegalStateException("Paimon spill barrier is missing for table " + tableKey);
            stopScope.retain(missing);
            throw missing;
        }
        PaimonStopController controller = stopScope.controller();
        stopScope.check("close table");
        closeState = CloseState.CLOSING;
        Throwable failure = null;
        boolean discarded = false;
        boolean interrupted = false;
        PaimonStopController.FinalAttempt attempt = null;
        List<CommitMessage> messages = null;
        boolean hasFinalCompaction = false;
        try {
            if (finalizeCompaction) {
                if (failed || !pendingCommits.isEmpty()) {
                    throw new IllegalStateException("Final Compaction requires a confirmed business barrier for " + tableKey);
                }
                if (!controller.isStarted()) { controller.start(); }
                attempt = controller.beginFinal(tableKey, nextCommitIdentifier, compactionLifecycle);
                phase(progress, "FINAL_PREPARE");
                if (attempt.permitsPrepare()) {
                    try {
                        PaimonStopController.FinalAttempt preparing = attempt;
                        messages = Objects.requireNonNull(stopScope.call("final prepare", () -> {
                            try { return writerStrategy.prepareFinalCommit(nextCommitIdentifier); }
                            finally { preparing.markPrepareReturned(); }
                        }));
                        hasFinalCompaction = PaimonNativeWriteAccess.hasFinalCompaction(messages);
                    } catch (Exception | Error caught) {
                        if (compactionLifecycle.isStopCancellation(caught, attempt)
                                || PaimonCompactionExecutor.isNativeCompactionFailure(caught)) {
                            discarded = true;
                            discardedPhase(progress, nextCommitIdentifier, caught);
                        } else { failure = caught; interrupted |= caught instanceof InterruptedException; }
                    }
                } else {
                    PaimonStopController.FinalAttempt cancelled = controller.pollCancellation();
                    if (cancelled != null) { observe(progress::compactionCancellationRequested); cancelled.requestCancellation(); }
                    discarded = true;
                }
                // 取消桥接让 CompactFutureManager 的 get 抛 ExecutionException，防止空结果触发
                // AbstractFileStoreWrite.prepareCommit 内联 writer.close；外层才能先取得真实退出证明。
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L238-L254
                compactionLifecycle.sealFinalPrepare();
            }
        } catch (Exception | Error caught) {
            interrupted |= caught instanceof InterruptedException;
            failure = appendFailure(failure, caught);
        }

        try {
            phase(progress, "WAIT_COMPACTION");
            stopScope.run("shutdown compaction submissions", compactionLifecycle::shutdown);
            while (!compactionLifecycle.isTerminated()) {
                stopScope.check("await compaction termination");
                PaimonStopController.FinalAttempt cancelled = controller.pollCancellation();
                if (cancelled != null) { observe(progress::compactionCancellationRequested); cancelled.requestCancellation(); }
                compactionLifecycle.awaitTermination(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(50));
            }
            stopScope.check("consume terminated compaction");
            if (attempt != null) {
                controller.finalTerminated(attempt); // prepare 已返回 + 实际 executor terminated 两项证明。
                discarded |= attempt.cancelRequested();
            }
            observe(progress::compactionTerminated);
            for (Throwable syncFailure : nativeWriteAccess.syncAll(stopScope)) {
                if (attempt != null && (compactionLifecycle.isStopCancellation(syncFailure, attempt)
                        || (discarded && PaimonCompactionExecutor.isNativeCompactionFailure(syncFailure)))) {
                    discardedPhase(progress, nextCommitIdentifier, syncFailure);
                } else { failure = appendFailure(failure, syncFailure); }
            }
            compactionLifecycle.auditControlFailure();
            if (attempt != null && failure == null && !discarded && hasFinalCompaction) {
                if (controller.admitFinalCommit(attempt)) {
                    if (nextCommitIdentifier == Long.MAX_VALUE) {
                        throw new IllegalStateException("Paimon commit identifier is exhausted for " + tableKey);
                    }
                    phase(progress, "FINAL_COMMIT");
                    final List<CommitMessage> prepared = messages;
                    controller.publish("retain final pending", () -> {
                        pendingCommits.put(nextCommitIdentifier, prepared); return null;
                    });
                    commitPrepared(nextCommitIdentifier, messages);
                } else {
                    observe(progress::compactionCancellationRequested);
                    attempt.requestCancellation();
                    discarded = true;
                }
            }
        } catch (Exception | Error caught) {
            interrupted |= caught instanceof InterruptedException;
            failure = appendFailure(failure, caught);
        }

        // shutdown/cancel/isDone 都不是删除证据；冻结后每个新清理动作都会被短门禁拒绝。
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L304-L317
        try {
            if (!compactionLifecycle.isTerminated()) {
                throw new IllegalStateException("Compaction has not physically terminated for " + tableKey);
            }
            phase(progress, "CLOSE_WRITER");
            stopScope.run("close writer", writerStrategy::close);
            phase(progress, "CLOSE_COMMITTER");
            stopScope.run("close committer", tableCommitter::close);
            if (ioManager != null) {
                phase(progress, "CLOSE_SPILL");
                stopScope.run("close IOManager", ioManager::close);
                PaimonSpillDirCleaner.ReleaseResult release = stopScope.call("release spill owner",
                        () -> PaimonSpillDirCleaner.releaseAfterClose(spillDirs, true, controller));
                throwIfPresent(release.failure());
                if (!release.complete()) { throw new IllegalStateException("Spill owner release incomplete"); }
                observe(progress::spillClosed);
            }
            stopScope.check("publish table cleanup proof");
            cleanupComplete = true;
            stopScope.completed();
        } catch (Exception | Error caught) {
            interrupted |= caught instanceof InterruptedException;
            failure = appendFailure(failure, caught);
        }
        if (!cleanupComplete) {
            if (failure == null) { failure = new IllegalStateException("Missing table cleanup proof " + tableKey); }
            stopScope.retain(failure);
        }
        gracefulCloseFailure = failure;
        stopOutcome = !cleanupComplete ? StopOutcome.FAILED_RETAINED : failure != null ? StopOutcome.FAILED
                : discarded ? StopOutcome.SUCCESS_COMPACTION_DISCARDED : StopOutcome.SUCCESS;
        gracefulCloseFinished = true;
        closeState = CloseState.CLOSED;
        if (interrupted) { Thread.currentThread().interrupt(); }
        throwIfPresent(failure);
        return stopOutcome;
    }

    public boolean cleanupComplete() { return cleanupComplete; }

    public enum StopOutcome { SUCCESS, SUCCESS_COMPACTION_DISCARDED, FAILED, FAILED_RETAINED }

    @FunctionalInterface
    public interface CloseObserver {
        void phase(String phase);
        default void compactionDiscarded(long identifier, Throwable failure) {}
        default void compactionTerminated() {}
        default void compactionCancellationRequested() {}
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
        return PaimonFailures.append(first, next);
    }

    private static void throwIfPresent(Throwable failure) throws Exception {
        if (failure instanceof Error) { throw (Error) failure; }
        if (failure != null) { throw (Exception) failure; }
    }

    public void write(InternalRow row) throws Exception {
        ensureWritable();
        try {
            // 原生 assigner 与 TableWriteImpl.write 各自准入，Context 不重复登记整行。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java#L163
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
        stopScope.check("table access");
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
