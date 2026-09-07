package io.tapdata.connector.paimon.write;

import org.apache.paimon.compact.CompactTask;
import io.tapdata.connector.paimon.service.PaimonStopController.FinalAttempt;
import java.util.ArrayList;

import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Paimon 1.3.2 Compaction 的唯一执行器：任务结果由原生 Future 消费，控制失败保存首个证据。 */
public final class PaimonCompactionExecutor extends AbstractExecutorService {
    private static final ThreadGroup THREAD_GROUP = stableThreadGroup();
    private final ThreadPoolExecutor delegate;
    private final Object controlLock = new Object();
    private Throwable controlFailure;
    private boolean sealed;
    private final Set<GuardedFuture<?>> outstanding = Collections.newSetFromMap(new IdentityHashMap<>());
    private volatile FinalAttempt finalAttempt;
    private volatile io.tapdata.connector.paimon.service.PaimonStopController stopController;
    void attachStopController(io.tapdata.connector.paimon.service.PaimonStopController controller) {
        synchronized (controlLock) {
            if (stopController != null && stopController != controller) { throw new IllegalStateException("Compaction controller cannot change"); }
            stopController = controller;
        }
    }

    public void beginFinal(FinalAttempt attempt) {
        synchronized (controlLock) {
            if (finalAttempt != null && finalAttempt != attempt) { throw new IllegalStateException("Foreign final attempt"); }
            finalAttempt = attempt;
        }
    }

    public void cancelForStop(FinalAttempt attempt) {
        synchronized (controlLock) {
            if (finalAttempt != attempt || !attempt.cancelRequested()) {
                throw new IllegalArgumentException("Unauthorised Compaction cancellation");
            }
            sealed = true;
            // 已入队和正在运行的任务都登记在 outstanding；cancel/isDone 不证明实际退场。
            for (GuardedFuture<?> future : new ArrayList<>(outstanding)) { future.cancelForStop(attempt); }
            for (Runnable queued : delegate.shutdownNow()) {
                GuardedFuture<?> future = (GuardedFuture<?>) queued;
                future.cancelForStop(attempt);
                outstanding.remove(future); // 已移出队列，且不会开始 run。
            }
        }
    }

    public boolean isStopCancellation(Throwable error, FinalAttempt attempt) {
        if (error instanceof ExecutionException && error.getCause() instanceof StopCompactionCancelled) {
            return ((StopCompactionCancelled) error.getCause()).attempt == attempt;
        }
        return error instanceof StopSubmissionRejected && ((StopSubmissionRejected) error).attempt == attempt;
    }

    public int outstandingCount() { synchronized (controlLock) { return outstanding.size(); } }


    public PaimonCompactionExecutor(String tableKey) {
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        delegate = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), runnable -> {
                    Thread thread = new Thread(THREAD_GROUP, runnable,
                            "paimon-compaction-" + tableKey.replaceAll("[^A-Za-z0-9._-]", "_"));
                    thread.setContextClassLoader(contextLoader);
                    thread.setDaemon(true);
                    return thread;
                });
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        // Paimon 1.3.2 的 MergeTree / bucketed append 均提交 CompactTask 子类。
        // 仅标记 callable 执行异常，不改变内核选取、调度或应用 Compaction 结果的行为。
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/compact/CompactTask.java#L34
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/append/BucketedAppendCompactManager.java#L115
        synchronized (controlLock) {
            if (stopController != null) { stopController.checkAction("submit compaction task"); }
            if (!(task instanceof CompactTask)) {
                IllegalArgumentException failure = new IllegalArgumentException(
                        "Unverified Paimon Compaction task: " + (task == null ? "null" : task.getClass().getName()));
                recordControlFailure(failure);
                throw failure;
            }
            if (finalAttempt != null && finalAttempt.cancelRequested()) {
                throw new StopSubmissionRejected(finalAttempt);
            }
            if (sealed) {
                RejectedExecutionException failure = new RejectedExecutionException("Paimon final prepare is sealed");
                recordControlFailure(failure);
                throw failure;
            }
            TaskState state = new TaskState();
            GuardedFuture<T> future = new GuardedFuture<>(() -> {
                try {
                    T result = task.call();
                    if (Thread.currentThread().isInterrupted() && state.cancelOwner == null) {
                        throw new InterruptedException("Paimon Compaction worker interrupted");
                    }
                    return result;
                } catch (Exception | Error failure) {
                    if (state.cancelOwner != null) {
                        // FutureTask 取消后会丢弃 callable 的结果；迟到 Error/独立 IO 必须另存审计证据。
                        if (!onlyExpectedInterruption(failure)) { recordControlFailure(failure); }
                        throw failure;
                    }
                    if (controlCause(failure) != null || Thread.currentThread().isInterrupted()) {
                        recordControlFailure(failure);
                        throw failure;
                    }
                    throw new NativeCompactionFailure(failure);
                }
            }, state);
            outstanding.add(future);
            try {
                delegate.execute(future);
            } catch (RejectedExecutionException failure) {
                outstanding.remove(future);
                recordControlFailure(failure);
                throw failure;
            }
            return future;
        }
    }

    public void auditAndSeal() {
        synchronized (controlLock) {
            sealed = true;
            audit();
        }
    }

    public void audit() {
        synchronized (controlLock) {
            if (controlFailure != null) {
                throw new IllegalStateException("Paimon Compaction control failure", controlFailure);
            }
        }
    }

    private void recordControlFailure(Throwable failure) {
        synchronized (controlLock) {
            if (controlFailure == null) { controlFailure = failure; }
        }
    }

    public static boolean isNativeCompactionFailure(Throwable failure) {
        return failure instanceof ExecutionException
                && failure.getCause() instanceof NativeCompactionFailure
                && controlCause(failure) == null;
    }

    private static Throwable controlCause(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        if (failure != null) { pending.add(failure); }
        while (!pending.isEmpty()) {
            Throwable cause = pending.removeFirst();
            if (!seen.add(cause)) { continue; }
            if (cause instanceof Error || cause instanceof InterruptedException
                    || cause instanceof InterruptedIOException || cause instanceof ClosedByInterruptException
                    || cause instanceof CancellationException || cause instanceof RejectedExecutionException) {
                return cause;
            }
            if (cause.getCause() != null) { pending.add(cause.getCause()); }
            Collections.addAll(pending, cause.getSuppressed());
        }
        return null;
    }

    private static boolean onlyExpectedInterruption(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        ArrayDeque<Throwable> pending = new ArrayDeque<>(); pending.add(failure);
        while (!pending.isEmpty()) {
            Throwable next = pending.removeFirst();
            if (!seen.add(next)) { continue; }
            if (!(next instanceof InterruptedException || next instanceof InterruptedIOException
                    || next instanceof ClosedByInterruptException)) { return false; }
            if (next.getCause() != null) { pending.add(next.getCause()); }
            Collections.addAll(pending, next.getSuppressed());
        }
        return true;
    }

    private static final class TaskState {
        volatile FinalAttempt cancelOwner;
        volatile Thread runner;
    }

    private final class GuardedFuture<T> extends FutureTask<T> {
        private final TaskState state;
        private GuardedFuture(Callable<T> callable, TaskState state) { super(callable); this.state = state; }
        @Override public void run() {
            state.runner = Thread.currentThread();
            try { super.run(); }
            finally {
                state.runner = null;
                synchronized (controlLock) { outstanding.remove(this); }
            }
        }
        private void cancelForStop(FinalAttempt attempt) {
            if (isDone()) { return; }
            if (state.runner != null && state.runner.isInterrupted()) {
                recordControlFailure(new InterruptedException("Worker was interrupted before controlled cancellation"));
            }
            state.cancelOwner = attempt;
            if (!super.cancel(true)) { state.cancelOwner = null; }
        }
        @Override public boolean cancel(boolean mayInterruptIfRunning) {
            synchronized (controlLock) {
                if (sealed && !isDone()) {
                    recordControlFailure(new IllegalStateException("Unexpected cancellation after final prepare"));
                    return false;
                }
                boolean cancelled = super.cancel(mayInterruptIfRunning);
                if (cancelled) { recordControlFailure(new CancellationException("Paimon Compaction cancelled externally")); }
                return cancelled;
            }
        }
        private ExecutionException cancelled(CancellationException original) {
            synchronized (controlLock) {
                // Paimon 1.3.2 只吞 CancellationException；空结果可能触发 prepare 内联 writer.close。
                // 此 Future 仅供已验证 CompactTask 使用，刻意转换通用 Future.get 的取消异常。
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java#L47
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L238
                return new ExecutionException(state.cancelOwner == null
                        ? new IllegalStateException("Uncontrolled native cancellation", original)
                        : new StopCompactionCancelled(state.cancelOwner));
            }
        }
        @Override public T get() throws InterruptedException, ExecutionException {
            try { return super.get(); }
            catch (CancellationException failure) { throw cancelled(failure); }
            catch (InterruptedException failure) { recordControlFailure(failure); throw failure; }
        }
        @Override public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            try { return super.get(timeout, unit); }
            catch (CancellationException failure) { throw cancelled(failure); }
            catch (InterruptedException failure) { recordControlFailure(failure); throw failure; }
        }
    }

    private static final class StopCompactionCancelled extends Exception {
        final FinalAttempt attempt;
        StopCompactionCancelled(FinalAttempt attempt) { super("STOP cancelled " + attempt.identity()); this.attempt = attempt; }
    }
    private static final class StopSubmissionRejected extends RejectedExecutionException {
        final FinalAttempt attempt;
        StopSubmissionRejected(FinalAttempt attempt) { super("STOP sealed " + attempt.identity()); this.attempt = attempt; }
    }

    private static final class NativeCompactionFailure extends Exception {
        private NativeCompactionFailure(Throwable cause) { super("Native Paimon Compaction failed", cause); }
    }

    @Override
    public void execute(Runnable command) {
        IllegalArgumentException failure = new IllegalArgumentException("Only verified CompactTask submission is supported");
        recordControlFailure(failure);
        throw failure;
    }

    @Override
    public void shutdown() { delegate.shutdown(); }

    @Override
    public List<Runnable> shutdownNow() {
        // 公共强制终止入口没有 STOP token，保存硬失败；仅 cancelForStop 能受控取消任务。
        recordControlFailure(new IllegalStateException("Compaction requires graceful shutdown"));
        shutdown();
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() { return delegate.isShutdown(); }
    @Override
    public boolean isTerminated() { return delegate.isTerminated(); }
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    private static ThreadGroup stableThreadGroup() {
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        while (root.getParent() != null) { root = root.getParent(); }
        return new ThreadGroup(root, "paimon-compaction-workers");
    }
}
