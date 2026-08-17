package io.tapdata.connector.paimon.commit;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Lazily creates a bounded daemon pool and schedules only the nearest eligible table deadline.
 *
 * <p>Different physical tables may commit concurrently because each table owns an independent
 * writer and committer. The same table must remain serial: Paimon's writer and committer contain
 * mutable state and do not define a thread-safe concurrent-entry contract. The connector therefore
 * combines {@link #inFlightTables} with the service's per-table {@code commitLocks}.
 *
 * <p>Paimon 1.3.2 kernel basis:
 *
 * <ul>
 *   <li>{@code AbstractFileStore#newCommit} creates a new {@code FileStoreCommitImpl} for a table
 *       committer. Source: {@code paimon-core/src/main/java/org/apache/paimon/AbstractFileStore.java},
 *       lines 264-304.
 *   <li>Paimon's concurrent-commit test gives every concurrent thread a distinct commit user,
 *       writer, and committer. Source: {@code paimon-core/src/test/java/org/apache/paimon/operation/
 *       FileStoreCommitTest.java#testRandomConcurrent}, lines 287-327, and
 *       {@code TestCommitThread.java#TestCommitThread}, lines 72-105.
 *   <li>The writer and committer hold mutable per-instance fields. Source:
 *       {@code paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java}, lines
 *       60-90, and {@code paimon-core/src/main/java/org/apache/paimon/operation/
 *       FileStoreCommitImpl.java}, lines 121-145.
 * </ul>
 *
 * <p>Baseline for every source location above: {@code apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e}
 * (Paimon 1.3.2).
 */
public final class PaimonAsyncCommitScheduler {

    private static final int MIN_CONCURRENCY = 1;
    private static final int MAX_CONCURRENCY = 16;

    private final Object lock = new Object();
    private final boolean enabled;
    private final int maxConcurrency;
    private final PaimonMicroBatchCoordinator coordinator;
    private final Clock clock;
    private final ExecutorFactory executorFactory;
    private final FlushAction flushAction;
    private final FailureHandler failureHandler;
    private final Set<String> inFlightTables = new HashSet<>();

    private ScheduledExecutorService executor;
    private ScheduledFuture<?> scheduledFuture;
    private long scheduledDeadlineMs = Long.MIN_VALUE;
    private long scheduleVersion;
    private boolean shutdown;
    private boolean failed;
    private Throwable failureCause;

    public PaimonAsyncCommitScheduler(
            boolean enabled,
            int maxConcurrency,
            PaimonMicroBatchCoordinator coordinator,
            Clock clock,
            ExecutorFactory executorFactory,
            FlushAction flushAction,
            FailureHandler failureHandler) {
        if (coordinator == null
                || clock == null
                || executorFactory == null
                || flushAction == null
                || failureHandler == null) {
            throw new IllegalArgumentException("Scheduler dependencies must not be null");
        }
        if (maxConcurrency < MIN_CONCURRENCY || maxConcurrency > MAX_CONCURRENCY) {
            throw new IllegalArgumentException(
                    "Async commit concurrency must be between "
                            + MIN_CONCURRENCY
                            + " and "
                            + MAX_CONCURRENCY);
        }
        this.enabled = enabled;
        this.maxConcurrency = maxConcurrency;
        this.coordinator = coordinator;
        this.clock = clock;
        this.executorFactory = executorFactory;
        this.flushAction = flushAction;
        this.failureHandler = failureHandler;
    }

    public void stateChanged() {
        Throwable schedulingFailure = null;
        synchronized (lock) {
            try {
                stateChangedLocked();
            } catch (Throwable failure) {
                if (transitionToFailureLocked(failure)) {
                    schedulingFailure = failure;
                }
            }
        }
        if (schedulingFailure != null) {
            notifyFailure(schedulingFailure);
            rethrowUnchecked(schedulingFailure);
        }
    }

    private void stateChangedLocked() {
        if (!enabled || shutdown || failed) {
            return;
        }

        int availableSlots = maxConcurrency - inFlightTables.size();
        if (availableSlots <= 0) {
            cancelScheduledLocked();
            return;
        }

        OptionalLong nextDeadline = coordinator.nextDeadlineMs(inFlightTables);
        if (!nextDeadline.isPresent()) {
            cancelScheduledLocked();
            return;
        }

        long deadlineMs = nextDeadline.getAsLong();
        if (scheduledFuture != null
                && !scheduledFuture.isCancelled()
                && !scheduledFuture.isDone()
                && scheduledDeadlineMs == deadlineMs) {
            return;
        }

        cancelScheduledLocked();
        ensureWorkerLocked();
        long version = ++scheduleVersion;
        scheduledDeadlineMs = deadlineMs;
        long delayMs = Math.max(0L, deadlineMs - clock.currentTimeMillis());
        scheduledFuture =
                executor.schedule(
                        () -> dispatchDueTables(version), delayMs, TimeUnit.MILLISECONDS);
        if (scheduledFuture == null) {
            throw new IllegalStateException("Scheduler executor returned a null future");
        }
    }

    private void dispatchDueTables(long version) {
        List<String> reservedTables = Collections.emptyList();
        ScheduledExecutorService worker = null;
        synchronized (lock) {
            if (shutdown || failed || version != scheduleVersion) {
                return;
            }
            scheduledFuture = null;
            scheduledDeadlineMs = Long.MIN_VALUE;
            int availableSlots = maxConcurrency - inFlightTables.size();
            if (availableSlots > 0) {
                reservedTables =
                        coordinator.dueTables(
                                clock.currentTimeMillis(), inFlightTables, availableSlots);
                inFlightTables.addAll(reservedTables);
                worker = executor;
            }
        }

        submitReservedTables(worker, reservedTables);
    }

    private void submitReservedTables(
            ScheduledExecutorService worker, List<String> reservedTables) {
        for (int index = 0; index < reservedTables.size(); index++) {
            String tableKey = reservedTables.get(index);
            synchronized (lock) {
                if (shutdown || failed || !inFlightTables.contains(tableKey)) {
                    releaseReservationsLocked(reservedTables, index);
                    lock.notifyAll();
                    return;
                }
            }

            try {
                worker.execute(() -> runTableTask(tableKey));
            } catch (Throwable submissionFailure) {
                boolean reportFailure = false;
                synchronized (lock) {
                    releaseReservationsLocked(reservedTables, index);
                    if (!shutdown && !failed) {
                        reportFailure = transitionToFailureLocked(submissionFailure);
                    }
                    lock.notifyAll();
                }
                if (reportFailure) {
                    notifyFailure(submissionFailure);
                }
                return;
            }
        }

        // A dispatch can reserve fewer tables than the configured concurrency. Replan immediately
        // so a later deadline is still observed while an already-started table remains slow.
        stateChanged();
    }

    private void runTableTask(String tableKey) {
        boolean shouldRun;
        synchronized (lock) {
            shouldRun = !shutdown && !failed && inFlightTables.contains(tableKey);
        }

        Throwable taskFailure = null;
        if (shouldRun) {
            try {
                flushAction.flush(tableKey);
            } catch (Throwable failure) {
                taskFailure = failure;
            }
        }

        boolean reportFailure = false;
        boolean shouldReplan = false;
        synchronized (lock) {
            inFlightTables.remove(tableKey);
            if (taskFailure != null) {
                reportFailure = transitionToFailureLocked(taskFailure);
            } else {
                shouldReplan = !shutdown && !failed;
            }
            lock.notifyAll();
        }

        if (reportFailure) {
            notifyFailure(taskFailure);
        } else if (shouldReplan) {
            stateChanged();
        }
    }

    private void releaseReservationsLocked(List<String> reservedTables, int firstIndex) {
        for (int index = firstIndex; index < reservedTables.size(); index++) {
            inFlightTables.remove(reservedTables.get(index));
        }
    }

    private boolean transitionToFailureLocked(Throwable failure) {
        if (failed) {
            if (failureCause != null && failureCause != failure) {
                failureCause.addSuppressed(failure);
            }
            return false;
        }
        failed = true;
        shutdown = true;
        failureCause = failure;
        cancelScheduledLocked();
        if (executor != null) {
            // Do not interrupt commits which have already entered Paimon's synchronous commit I/O.
            // Queued table tasks observe the sticky fence in runTableTask and skip their flush.
            executor.shutdown();
        }
        lock.notifyAll();
        return true;
    }

    private void notifyFailure(Throwable failure) {
        try {
            failureHandler.onFailure(failure);
        } catch (Throwable handlerFailure) {
            if (handlerFailure != failure) {
                failure.addSuppressed(handlerFailure);
            }
        }
    }

    private static void rethrowUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IllegalStateException("Paimon scheduler planning failed", failure);
    }

    public boolean shutdownAndAwait(long timeout, TimeUnit unit) throws InterruptedException {
        if (unit == null) {
            throw new IllegalArgumentException("TimeUnit must not be null");
        }
        ScheduledExecutorService worker;
        synchronized (lock) {
            shutdown = true;
            cancelScheduledLocked();
            worker = executor;
            if (worker != null) {
                worker.shutdown();
            }
            lock.notifyAll();
        }
        return worker == null || worker.awaitTermination(timeout, unit);
    }

    public boolean isWorkerCreated() {
        synchronized (lock) {
            return executor != null;
        }
    }

    public boolean isFailed() {
        synchronized (lock) {
            return failed;
        }
    }

    private void ensureWorkerLocked() {
        if (executor == null) {
            executor = executorFactory.create(maxConcurrency);
            if (executor == null) {
                throw new IllegalStateException("Scheduler executor factory returned null");
            }
        }
    }

    private void cancelScheduledLocked() {
        boolean hadSchedule = scheduledFuture != null || scheduledDeadlineMs != Long.MIN_VALUE;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        scheduledFuture = null;
        scheduledDeadlineMs = Long.MIN_VALUE;
        if (hadSchedule) {
            scheduleVersion++;
        }
    }

    public static ScheduledExecutorService newDaemonExecutor(int concurrency) {
        if (concurrency < MIN_CONCURRENCY || concurrency > MAX_CONCURRENCY) {
            throw new IllegalArgumentException(
                    "Async commit concurrency must be between "
                            + MIN_CONCURRENCY
                            + " and "
                            + MAX_CONCURRENCY);
        }
        AtomicInteger threadNumber = new AtomicInteger();
        ThreadFactory threadFactory =
                runnable -> {
                    Thread thread =
                            new Thread(
                                    runnable,
                                    "paimon-async-commit-" + threadNumber.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                };
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(concurrency, threadFactory);
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        // This is intentionally separate from Paimon's file-operation.thread-num. Paimon 1.3.2
        // uses a bounded-size pool backed by an unbounded LinkedBlockingQueue for internal FileIO.
        // Source: paimon-common/src/main/java/org/apache/paimon/utils/
        // FileOperationThreadPool.java#getExecutorService, lines 32-43, and
        // paimon-api/src/main/java/org/apache/paimon/utils/
        // ThreadPoolUtils.java#createCachedThreadPool, lines 48-74.
        // Baseline: apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e.
        // The connector bounds table-level work with inFlightTables instead of relying on the
        // ScheduledThreadPoolExecutor's own unbounded delayed-work queue.
        return executor;
    }

    @FunctionalInterface
    public interface Clock {
        long currentTimeMillis();
    }

    @FunctionalInterface
    public interface ExecutorFactory {
        ScheduledExecutorService create(int concurrency);
    }

    @FunctionalInterface
    public interface FlushAction {
        void flush(String tableKey) throws Exception;
    }

    @FunctionalInterface
    public interface FailureHandler {
        void onFailure(Throwable failure);
    }
}
