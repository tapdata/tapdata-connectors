package io.tapdata.connector.paimon.service;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Lazily creates one daemon worker and schedules only the nearest table deadline.
 *
 * <p>The adapter never owns commit state. Each task asks the coordinator which tables are due and
 * delegates their I/O to the service-provided flush action. One-shot scheduling avoids the fixed
 * phase delay of {@code scheduleAtFixedRate} and lets every state transition recalculate the nearest
 * deadline.
 */
final class PaimonAsyncCommitScheduler {

    private final Object lock = new Object();
    private final boolean enabled;
    private final PaimonMicroBatchCoordinator coordinator;
    private final Clock clock;
    private final ExecutorFactory executorFactory;
    private final FlushAction flushAction;
    private final FailureHandler failureHandler;

    private ScheduledExecutorService executor;
    private ScheduledFuture<?> scheduledFuture;
    private long scheduledDeadlineMs = Long.MIN_VALUE;
    private long scheduleVersion;
    private boolean taskRunning;
    private boolean shutdown;
    private boolean failed;

    PaimonAsyncCommitScheduler(
            boolean enabled,
            PaimonMicroBatchCoordinator coordinator,
            FlushAction flushAction,
            FailureHandler failureHandler) {
        this(
                enabled,
                coordinator,
                System::currentTimeMillis,
                PaimonAsyncCommitScheduler::newDaemonExecutor,
                flushAction,
                failureHandler);
    }

    PaimonAsyncCommitScheduler(
            boolean enabled,
            PaimonMicroBatchCoordinator coordinator,
            Clock clock,
            FlushAction flushAction,
            FailureHandler failureHandler) {
        this(
                enabled,
                coordinator,
                clock,
                PaimonAsyncCommitScheduler::newDaemonExecutor,
                flushAction,
                failureHandler);
    }

    PaimonAsyncCommitScheduler(
            boolean enabled,
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
        this.enabled = enabled;
        this.coordinator = coordinator;
        this.clock = clock;
        this.executorFactory = executorFactory;
        this.flushAction = flushAction;
        this.failureHandler = failureHandler;
    }

    void stateChanged() {
        Throwable schedulingFailure = null;
        synchronized (lock) {
            try {
                stateChangedLocked();
            } catch (Throwable failure) {
                failed = true;
                shutdown = true;
                cancelScheduledLocked();
                if (executor != null) {
                    executor.shutdown();
                }
                schedulingFailure = failure;
            }
        }
        if (schedulingFailure != null) {
            failureHandler.onFailure(schedulingFailure);
            rethrowUnchecked(schedulingFailure);
        }
    }

    private void stateChangedLocked() {
        if (!enabled || shutdown || failed || taskRunning) {
            return;
        }

        OptionalLong nextDeadline = coordinator.nextDeadlineMs();
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
                        () -> runScheduledTask(version), delayMs, TimeUnit.MILLISECONDS);
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

    boolean shutdownAndAwait(long timeout, TimeUnit unit) throws InterruptedException {
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
        }
        return worker == null || worker.awaitTermination(timeout, unit);
    }

    boolean isWorkerCreated() {
        synchronized (lock) {
            return executor != null;
        }
    }

    boolean isFailed() {
        synchronized (lock) {
            return failed;
        }
    }

    private void runScheduledTask(long version) {
        synchronized (lock) {
            if (shutdown || failed || version != scheduleVersion) {
                return;
            }
            scheduledFuture = null;
            scheduledDeadlineMs = Long.MIN_VALUE;
            taskRunning = true;
        }

        Throwable taskFailure = null;
        try {
            List<String> dueTables = coordinator.dueTables(clock.currentTimeMillis());
            for (String tableKey : dueTables) {
                flushAction.flush(tableKey);
            }
        } catch (Throwable failure) {
            taskFailure = failure;
        }

        boolean shouldReplan;
        synchronized (lock) {
            taskRunning = false;
            if (taskFailure != null) {
                failed = true;
                shutdown = true;
                if (executor != null) {
                    executor.shutdown();
                }
                shouldReplan = false;
            } else {
                shouldReplan = !shutdown && !failed;
            }
            lock.notifyAll();
        }

        if (taskFailure != null) {
            failureHandler.onFailure(taskFailure);
        } else if (shouldReplan) {
            stateChanged();
        }
    }

    private void ensureWorkerLocked() {
        if (executor == null) {
            executor = executorFactory.create();
            if (executor == null) {
                throw new IllegalStateException("Scheduler executor factory returned null");
            }
        }
    }

    private void cancelScheduledLocked() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            scheduledFuture = null;
            scheduledDeadlineMs = Long.MIN_VALUE;
            scheduleVersion++;
        }
    }

    static ScheduledExecutorService newDaemonExecutor() {
        ThreadFactory threadFactory =
                runnable -> {
                    Thread thread = new Thread(runnable, "paimon-async-commit");
                    thread.setDaemon(true);
                    return thread;
                };
        ScheduledThreadPoolExecutor executor =
                new ScheduledThreadPoolExecutor(1, threadFactory);
        executor.setRemoveOnCancelPolicy(true);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        return executor;
    }

    @FunctionalInterface
    interface Clock {
        long currentTimeMillis();
    }

    @FunctionalInterface
    interface ExecutorFactory {
        ScheduledExecutorService create();
    }

    @FunctionalInterface
    interface FlushAction {
        void flush(String tableKey) throws Exception;
    }

    @FunctionalInterface
    interface FailureHandler {
        void onFailure(Throwable failure);
    }
}
