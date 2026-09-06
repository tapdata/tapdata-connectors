package io.tapdata.connector.paimon.write;

import org.apache.paimon.compact.CompactTask;

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
            if (!(task instanceof CompactTask)) {
                IllegalArgumentException failure = new IllegalArgumentException(
                        "Unverified Paimon Compaction task: " + (task == null ? "null" : task.getClass().getName()));
                recordControlFailure(failure);
                throw failure;
            }
            if (sealed) {
                RejectedExecutionException failure = new RejectedExecutionException("Paimon final prepare is sealed");
                recordControlFailure(failure);
                throw failure;
            }
            FutureTask<T> future = new GuardedFuture<>(() -> {
                try {
                    T result = task.call();
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("Paimon Compaction worker interrupted");
                    }
                    return result;
                } catch (Exception | Error failure) {
                    if (controlCause(failure) != null || Thread.currentThread().isInterrupted()) {
                        recordControlFailure(failure);
                        throw failure;
                    }
                    throw new NativeCompactionFailure(failure);
                }
            });
            try {
                delegate.execute(future);
            } catch (RejectedExecutionException failure) {
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

    private final class GuardedFuture<T> extends FutureTask<T> {
        private GuardedFuture(Callable<T> callable) { super(callable); }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            // CompactFutureManager.innerGetCompactionResult 吞掉 CancellationException，且 finally
            // 清空 Future；取消成功证据必须保留。super.cancel 会先唤醒 get，因此 cancel + 记录
            // 与 auditAndSeal 共用短锁，避免 audit 在取消记录之前通过。锁不覆盖任何等待/提交 IO。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java#L47
            synchronized (controlLock) {
                if (sealed && !isDone()) {
                    recordControlFailure(new IllegalStateException("Unexpected cancellation after final prepare"));
                    return false;
                }
                boolean cancelled = super.cancel(mayInterruptIfRunning);
                if (cancelled) { recordControlFailure(new CancellationException("Paimon Compaction cancelled")); }
                return cancelled;
            }
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            try { return super.get(); }
            catch (InterruptedException failure) { recordControlFailure(failure); throw failure; }
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            try { return super.get(timeout, unit); }
            catch (InterruptedException failure) { recordControlFailure(failure); throw failure; }
        }
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
        // 生命周期没有强制终止入口。出现意外调用时保存硬失败，已接收的任务仍完整执行。
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
