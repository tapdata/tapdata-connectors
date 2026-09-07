package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.util.PaimonFailures;
import io.tapdata.connector.paimon.write.PaimonCompactionLifecycle;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** 单次停止的短控制门禁。锁内不调用 Paimon、Lifecycle、executor 或日志后端。 */
public final class PaimonStopController {
    private final Object gate = new Object();
    private final LongSupplier nanoClock;
    private final String owner;
    private final long totalNanos;
    private final long finalNanos;
    private final long graceNanos;
    private final Map<Long, String> actions = new LinkedHashMap<>();
    private long actionSequence;
    private volatile boolean started;
    private volatile boolean frozen;
    private volatile boolean finished;
    private Throwable failure;
    private volatile FinalAttempt finalAttempt;
    private StopTimeoutException timeout;
    volatile boolean workerInterrupted;
    private volatile Object resourceRoot;
    public void attachResourceRoot(Object root) { resourceRoot = Objects.requireNonNull(root); }
    volatile long startedNanos;
    final AtomicLong lastProgressLog = new AtomicLong();
    volatile Progress progress;
    final CountDownLatch completed = new CountDownLatch(1);
    volatile Thread worker;
    volatile int discardedTables;
    volatile boolean startLogged;
    volatile boolean businessDrained;
    final java.util.concurrent.atomic.AtomicBoolean terminalLogged = new java.util.concurrent.atomic.AtomicBoolean();
    private volatile Terminal terminal;
    public Terminal terminal() { return terminal; }

    public PaimonStopController(String owner, long totalSeconds, long finalSeconds, long graceSeconds) {
        this(owner, totalSeconds, finalSeconds, graceSeconds, System::nanoTime);
    }

    PaimonStopController(String owner, long totalSeconds, long finalSeconds, long graceSeconds,
                         LongSupplier nanoClock) {
        this.owner = Objects.requireNonNull(owner);
        this.totalNanos = seconds(totalSeconds);
        this.finalNanos = seconds(finalSeconds);
        this.graceNanos = seconds(graceSeconds);
        this.nanoClock = Objects.requireNonNull(nanoClock);
        this.progress = new Progress("<service>", "DRAIN", nanoClock.getAsLong());
    }

    private static long seconds(long value) {
        if (value <= 0 || value > Long.MAX_VALUE / 4 / 1_000_000_000L) {
            throw new IllegalArgumentException("STOP seconds must be positive and safely convertible to nanos");
        }
        return value * 1_000_000_000L;
    }

    public void start() {
        synchronized (gate) {
            if (started) { return; }
            startedNanos = nanoClock.getAsLong();
            lastProgressLog.set(startedNanos);
            progress = new Progress("<service>", "DRAIN", startedNanos);
            started = true;
        }
    }

    public long now() { return nanoClock.getAsLong(); }
    public boolean isStarted() { return started; }
    public boolean isRetained() { return frozen; }
    public boolean isFinished() { return finished; }
    public String owner() { return owner; }

    public long remainingNanos() {
        return started ? Math.max(0, totalNanos - (now() - startedNanos)) : totalNanos;
    }

    public boolean expired() {
        synchronized (gate) {
            return expiredLocked();
        }
    }

    private boolean expiredLocked() {
        return started && (remainingNanos() == 0 || (finalAttempt != null
                && finalAttempt.cancelRequested() && !finalAttempt.terminated
                && now() - finalAttempt.cancelStarted >= graceNanos));
    }

    /** 仅短准入，已取得许可的外部调用可迟到返回；后续动作必须重新准入。 */
    public void checkAction(String action) {
        // RUNNING 的纯状态检查不登记动作；实际外部调用仍由 call/run 在锁内重新准入。
        if (!started && !frozen && !finished) { return; }
        synchronized (gate) { checkLocked(action); }
    }

    private void checkLocked(String action) {
        if (!frozen && !finished && expiredLocked()) { throw timeoutFailure(); }
        if (frozen || finished) {
            throw new FrozenException("Paimon STOP rejects " + action + " owner=" + owner, failure);
        }
    }

    public <T> T call(String action, CheckedSupplier<T> body) throws Exception {
        long id;
        synchronized (gate) {
            checkLocked(action);
            id = ++actionSequence;
            actions.put(id, action);
        }
        try { return body.get(); }
        finally { synchronized (gate) { actions.remove(id); } }
    }

    public void run(String action, CheckedRunnable body) throws Exception {
        call(action, () -> { body.run(); return null; });
    }

    /** 调用者已持有自己的状态锁；这里只运行不取其他锁、不做 IO 的内存发布。 */
    public <T> T publish(String action, java.util.function.Supplier<T> mutation) {
        synchronized (gate) { checkLocked(action); return mutation.get(); }
    }

    public String inFlightActions() {
        synchronized (gate) { return actions.values().toString(); }
    }

    public FinalAttempt beginFinal(String table, long identifier, PaimonCompactionLifecycle lifecycle) {
        FinalAttempt attempt;
        synchronized (gate) {
            checkLocked("final prepare");
            if (finalAttempt != null && !finalAttempt.terminated) {
                throw new IllegalStateException("Previous final attempt is not terminated");
            }
            long allowance = Math.min(finalNanos, Math.max(0, remainingNanos() - graceNanos));
            attempt = new FinalAttempt(this, table, identifier, now(), allowance, lifecycle);
            finalAttempt = attempt;
        }
        // executor 注册不持有控制锁；ready 发布前 supervisor 不向未绑定 executor 发取消。
        if (lifecycle != null) { lifecycle.beginFinal(attempt); }
        attempt.ready = true;
        return attempt;
    }

    /** 只选择取消胜者；调用者必须在锁外向该 attempt 的 executor 发取消请求。 */
    public FinalAttempt pollCancellation() {
        synchronized (gate) {
            if (frozen || finished || finalAttempt == null || finalAttempt.terminated || !finalAttempt.ready || finalAttempt.decision != Decision.PREPARING) {
                return null;
            }
            if (now() - finalAttempt.started < finalAttempt.allowance) { return null; }
            finalAttempt.cancelStarted = now();
            finalAttempt.decision = Decision.CANCEL_REQUESTED;
            return finalAttempt;
        }
    }

    public boolean admitFinalCommit(FinalAttempt attempt) {
        synchronized (gate) {
            checkLocked("final commit");
            if (failure != null) { throw new IllegalStateException("STOP control failure prevents final commit", failure); }
            requireCurrent(attempt);
            if (attempt.decision == Decision.CANCEL_REQUESTED) { return false; }
            if (attempt.decision != Decision.PREPARING) {
                throw new IllegalStateException("Final decision already made");
            }
            // 截止与提交准入在同一门禁线性化；调用者随后分发取消，不在门禁内执行 executor。
            if (now() - attempt.started >= attempt.allowance) {
                attempt.cancelStarted = now();
                attempt.decision = Decision.CANCEL_REQUESTED;
                return false;
            }
            attempt.decision = Decision.COMMIT_ADMITTED;
            return true;
        }
    }

    public void finalTerminated(FinalAttempt attempt) {
        synchronized (gate) { requireCurrent(attempt); attempt.terminated = true; }
    }

    private void requireCurrent(FinalAttempt attempt) {
        if (attempt == null || finalAttempt != attempt) {
            throw new IllegalArgumentException("Foreign final attempt");
        }
    }

    public void recordFailure(Throwable next) {
        synchronized (gate) {
            if (!finished && next != null) {
                failure = PaimonFailures.append(failure, next);
            }
        }
    }

    public Throwable failure() { synchronized (gate) { return failure; } }

    public StopTimeoutException timeoutFailure() {
        synchronized (gate) {
            if (timeout == null) {
                Progress p = progress;
                timeout = new StopTimeoutException("Paimon STOP timeout; phase=" + p.phase + " table=" + p.tableKey
                        + " owner=" + owner + " elapsedMs=" + TimeUnit.NANOSECONDS.toMillis(now() - startedNanos)
                        + " totalBudgetMs=" + TimeUnit.NANOSECONDS.toMillis(totalNanos)
                        + " finalBudgetMs=" + TimeUnit.NANOSECONDS.toMillis(finalNanos)
                        + " cancelGraceMs=" + TimeUnit.NANOSECONDS.toMillis(graceNanos)
                        + "; resources retained until process exit");
            }
            return timeout;
        }
    }

    public void retain(Throwable cause, Object resourceRoot) {
        refreshExecutorObservation();
        synchronized (gate) {
            if (finished) { return; }
            frozen = true;
            if (failure == null) { failure = Objects.requireNonNull(cause); }
            else if (cause instanceof StopTimeoutException) { failure = PaimonFailures.append(failure, cause); }
        }
        // 强引用登记先于 finished/countDown；ConcurrentHashMap 操作不调用原生 IO。
        PaimonStopResources.retain(this, this.resourceRoot == null ? resourceRoot : this.resourceRoot);
        synchronized (gate) {
            if (!finished) {
                terminal = new Terminal(progress, failure, true, now() - startedNanos, discardedTables, actions.values().toString(), diagnosticsLocked(), timeout != null);
                finished = true;
            }
        }
        completed.countDown();
    }

    public void complete(Throwable cause) {
        refreshExecutorObservation();
        synchronized (gate) {
            if (finished || frozen) { return; }
            if (failure == null) { failure = cause; }
            terminal = new Terminal(progress, failure, false, now() - startedNanos, discardedTables, actions.values().toString(), diagnosticsLocked(), timeout != null);
            finished = true;
        }
        completed.countDown();
    }

    /** 终态字段一次发布；迟到 worker 不能把 FAILED_RETAINED 改写为正常退出。 */
    public static final class Terminal {
        public final String table, phase, inFlight, diagnostics;
        public final boolean timedOut;
        public final Throwable failure;
        public final boolean retained;
        public final long elapsedNanos;
        public final int discardedTables;
        private Terminal(Progress progress, Throwable failure, boolean retained, long elapsedNanos,
                int discardedTables, String inFlight, String diagnostics, boolean timedOut) {
            this.table = progress.tableKey; this.phase = progress.phase; this.failure = failure;
            this.retained = retained; this.elapsedNanos = elapsedNanos;
            this.discardedTables = discardedTables; this.inFlight = inFlight;
            this.diagnostics = diagnostics; this.timedOut = timedOut;
        }
    }

    public boolean finalCancellationRequested() {
        FinalAttempt attempt = finalAttempt;
        return attempt != null && attempt.cancelRequested();
    }

    // 观察实际 executor termination，不用 Future.isDone/cancel 作为资源退出证据。
    // 外部组件调用在控制锁外；诊断字段不参与清理许可判定。
    private void refreshExecutorObservation() {
        FinalAttempt attempt = finalAttempt;
        if (attempt != null && attempt.lifecycle != null && attempt.ready) {
            attempt.executorTerminated = attempt.lifecycle.isTerminated();
        }
    }

    public String diagnostics() {
        refreshExecutorObservation();
        synchronized (gate) { return diagnosticsLocked(); }
    }

    private String diagnosticsLocked() {
        FinalAttempt attempt = finalAttempt;
        return " attempt=" + (attempt == null ? "none" : attempt.identity())
                + " remainingMs=" + TimeUnit.NANOSECONDS.toMillis(remainingNanos())
                + " cancelRequested=" + (attempt != null && attempt.cancelRequested())
                + " executorTerminated=" + (attempt == null || attempt.executorTerminated == null
                        ? "unknown" : attempt.executorTerminated)
                + " prepareReturned=" + (attempt != null && attempt.prepareReturned)
                + " inFlightAction=" + actions.values();
    }

    public static final class FinalAttempt {
        private final PaimonStopController owner;
        private final String table;
        private final long identifier;
        private final long started;
        private final long allowance;
        private final PaimonCompactionLifecycle lifecycle;
        private volatile Decision decision = Decision.PREPARING;
        private volatile long cancelStarted;
        private volatile boolean terminated;
        private volatile boolean ready;
        private volatile boolean prepareReturned;
        private volatile Boolean executorTerminated;
        private FinalAttempt(PaimonStopController owner, String table, long identifier,
                             long started, long allowance, PaimonCompactionLifecycle lifecycle) {
            this.owner = owner; this.table = table; this.identifier = identifier;
            this.started = started; this.allowance = allowance; this.lifecycle = lifecycle;
        }
        public void markPrepareReturned() { prepareReturned = true; }
        public boolean cancelRequested() { return decision == Decision.CANCEL_REQUESTED; }
        public boolean permitsPrepare() { return allowance > 0 && !cancelRequested() && !owner.isRetained(); }
        public boolean belongsTo(PaimonCompactionLifecycle value) { return lifecycle == value; }
        public String identity() { return owner.owner + ':' + table + ':' + identifier; }
        public void requestCancellation() {
            if (!cancelRequested()) { throw new IllegalStateException("Cancellation was not admitted"); }
            if (lifecycle != null) { lifecycle.cancelForStop(this); }
        }
    }

    private enum Decision { PREPARING, COMMIT_ADMITTED, CANCEL_REQUESTED }
    static final class Progress {
        final String tableKey; final String phase; final long startedNanos;
        Progress(String tableKey, String phase, long startedNanos) {
            this.tableKey = tableKey; this.phase = phase; this.startedNanos = startedNanos;
        }
    }
    public static class FrozenException extends IllegalStateException {
        FrozenException(String message, Throwable cause) { super(message, cause); }
    }
    public static final class StopTimeoutException extends FrozenException {
        StopTimeoutException(String message) { super(message, null); }
    }
    @FunctionalInterface public interface CheckedSupplier<T> { T get() throws Exception; }
    @FunctionalInterface public interface CheckedRunnable { void run() throws Exception; }
}
