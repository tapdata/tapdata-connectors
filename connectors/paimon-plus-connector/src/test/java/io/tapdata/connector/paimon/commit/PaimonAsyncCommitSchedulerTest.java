package io.tapdata.connector.paimon.commit;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonAsyncCommitSchedulerTest {

    @Test
    void slowTableMustNotBlockAnotherDueTable() throws Exception {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1L);
        AtomicLong clock = new AtomicLong(0L);
        CountDownLatch slowTableStarted = new CountDownLatch(1);
        CountDownLatch releaseSlowTable = new CountDownLatch(1);
        CountDownLatch fastTableFinished = new CountDownLatch(1);
        ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(2);
        PaimonAsyncCommitScheduler scheduler =
                new PaimonAsyncCommitScheduler(
                        true,
                        2,
                        coordinator,
                        clock::get,
                        ignored -> executor,
                        tableKey -> {
                            if ("default.a".equals(tableKey)) {
                                slowTableStarted.countDown();
                                if (!releaseSlowTable.await(10, TimeUnit.SECONDS)) {
                                    throw new AssertionError("Timed out waiting to release slow table");
                                }
                            }
                            coordinator.publishCommit(
                                    coordinator.captureCommitTarget(tableKey), clock.get());
                            if ("default.b".equals(tableKey)) {
                                fastTableFinished.countDown();
                            }
                        },
                        failure -> { });

        try {
            coordinator.acceptCdc(
                    "default.a", 1, Collections.singleton("source-a"), 0L);
            coordinator.acceptCdc(
                    "default.b", 1, Collections.singleton("source-a"), 0L);
            clock.set(1L);
            scheduler.stateChanged();

            assertTrue(slowTableStarted.await(5, TimeUnit.SECONDS));
            assertTrue(
                    fastTableFinished.await(5, TimeUnit.SECONDS),
                    "a blocked table must not delay another physical table");
        } finally {
            releaseSlowTable.countDown();
            scheduler.shutdownAndAwait(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void configuredConcurrencyMustBoundDispatchedTableTasks() throws Exception {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1L);
        AtomicLong clock = new AtomicLong(1L);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        AtomicReference<Runnable> dispatcher = new AtomicReference<>();
        List<Runnable> tableTasks = new ArrayList<>();
        when(executor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenAnswer(
                        invocation -> {
                            dispatcher.set(invocation.getArgument(0));
                            ScheduledFuture<?> future = mock(ScheduledFuture.class);
                            when(future.isDone()).thenReturn(false);
                            when(future.isCancelled()).thenReturn(false);
                            return future;
                        });
        doAnswer(
                        invocation -> {
                            tableTasks.add(invocation.getArgument(0));
                            return null;
                        })
                .when(executor)
                .execute(any(Runnable.class));
        PaimonAsyncCommitScheduler scheduler =
                new PaimonAsyncCommitScheduler(
                        true,
                        2,
                        coordinator,
                        clock::get,
                        ignored -> executor,
                        tableKey ->
                                coordinator.publishCommit(
                                        coordinator.captureCommitTarget(tableKey), clock.get()),
                        failure -> { });

        for (int index = 0; index < 10; index++) {
            coordinator.acceptCdc(
                    "default.t" + index,
                    1,
                    Collections.singleton("source-a"),
                    0L);
        }
        scheduler.stateChanged();
        dispatcher.get().run();

        assertEquals(2, tableTasks.size(), "only free slots may be submitted");
        for (int index = 0; index < 20; index++) {
            scheduler.stateChanged();
        }
        assertEquals(2, tableTasks.size(), "full slots must not grow the executor queue");
        verify(executor, times(1))
                .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));

        tableTasks.get(0).run();
        verify(executor, times(2))
                .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
        dispatcher.get().run();
        assertEquals(3, tableTasks.size(), "one completed table releases exactly one slot");
    }

    @Test
    void activeTableCommitsMustNeverExceedConfiguredConcurrency() throws Exception {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1L);
        AtomicLong clock = new AtomicLong(1L);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maxActive = new AtomicInteger();
        CountDownLatch firstTwoStarted = new CountDownLatch(2);
        CountDownLatch releaseCommits = new CountDownLatch(1);
        CountDownLatch allFinished = new CountDownLatch(4);
        PaimonAsyncCommitScheduler scheduler =
                new PaimonAsyncCommitScheduler(
                        true,
                        2,
                        coordinator,
                        clock::get,
                        PaimonAsyncCommitScheduler::newDaemonExecutor,
                        tableKey -> {
                            int current = active.incrementAndGet();
                            maxActive.updateAndGet(previous -> Math.max(previous, current));
                            firstTwoStarted.countDown();
                            try {
                                if (!releaseCommits.await(5, TimeUnit.SECONDS)) {
                                    throw new AssertionError("Timed out waiting to release commits");
                                }
                                coordinator.publishCommit(
                                        coordinator.captureCommitTarget(tableKey), clock.get());
                            } finally {
                                active.decrementAndGet();
                                allFinished.countDown();
                            }
                        },
                        failure -> { });

        try {
            for (int index = 0; index < 4; index++) {
                coordinator.acceptCdc(
                        "default.t" + index,
                        1,
                        Collections.singleton("source-a"),
                        0L);
            }
            scheduler.stateChanged();

            assertTrue(firstTwoStarted.await(5, TimeUnit.SECONDS));
            assertEquals(2, active.get());
            assertEquals(2, maxActive.get());
            releaseCommits.countDown();
            assertTrue(allFinished.await(5, TimeUnit.SECONDS));
            assertEquals(2, maxActive.get());
        } finally {
            releaseCommits.countDown();
            scheduler.shutdownAndAwait(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void sameTableMustHaveOnlyOneInFlightTaskWithoutBusyRescheduling() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1L);
        AtomicLong clock = new AtomicLong(1L);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        AtomicReference<Runnable> dispatcher = new AtomicReference<>();
        AtomicReference<Runnable> tableTask = new AtomicReference<>();
        when(executor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenAnswer(
                        invocation -> {
                            dispatcher.set(invocation.getArgument(0));
                            ScheduledFuture<?> future = mock(ScheduledFuture.class);
                            when(future.isDone()).thenReturn(false);
                            when(future.isCancelled()).thenReturn(false);
                            return future;
                        });
        doAnswer(
                        invocation -> {
                            if (!tableTask.compareAndSet(null, invocation.getArgument(0))) {
                                throw new AssertionError("same table was dispatched twice");
                            }
                            return null;
                        })
                .when(executor)
                .execute(any(Runnable.class));
        PaimonAsyncCommitScheduler scheduler =
                new PaimonAsyncCommitScheduler(
                        true,
                        4,
                        coordinator,
                        clock::get,
                        ignored -> executor,
                        tableKey ->
                                coordinator.publishCommit(
                                        coordinator.captureCommitTarget(tableKey), clock.get()),
                        failure -> { });

        coordinator.acceptCdc(
                "default.orders", 1, Collections.singleton("source-a"), 0L);
        scheduler.stateChanged();
        dispatcher.get().run();
        for (int index = 0; index < 20; index++) {
            scheduler.stateChanged();
        }

        verify(executor, times(1)).execute(any(Runnable.class));
        verify(executor, times(1))
                .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
        tableTask.get().run();
        assertFalse(coordinator.nextDeadlineMs().isPresent());
    }

    @Test
    void concurrencyOneMustPreserveSerialTableScheduling() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1L);
        AtomicLong clock = new AtomicLong(1L);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        AtomicReference<Runnable> dispatcher = new AtomicReference<>();
        List<Runnable> tableTasks = new ArrayList<>();
        when(executor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenAnswer(
                        invocation -> {
                            dispatcher.set(invocation.getArgument(0));
                            ScheduledFuture<?> future = mock(ScheduledFuture.class);
                            when(future.isDone()).thenReturn(false);
                            when(future.isCancelled()).thenReturn(false);
                            return future;
                        });
        doAnswer(
                        invocation -> {
                            tableTasks.add(invocation.getArgument(0));
                            return null;
                        })
                .when(executor)
                .execute(any(Runnable.class));
        PaimonAsyncCommitScheduler scheduler =
                new PaimonAsyncCommitScheduler(
                        true,
                        1,
                        coordinator,
                        clock::get,
                        ignored -> executor,
                        tableKey ->
                                coordinator.publishCommit(
                                        coordinator.captureCommitTarget(tableKey), clock.get()),
                        failure -> { });

        coordinator.acceptCdc(
                "default.a", 1, Collections.singleton("source-a"), 0L);
        coordinator.acceptCdc(
                "default.b", 1, Collections.singleton("source-a"), 0L);
        scheduler.stateChanged();
        dispatcher.get().run();

        assertEquals(1, tableTasks.size());
        tableTasks.get(0).run();
        dispatcher.get().run();
        assertEquals(2, tableTasks.size(), "second table starts only after the first releases its slot");
    }

    @Test
    void firstTaskFailureMustFenceAndSkipTasksWhichHaveNotStarted() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1L);
        AtomicLong clock = new AtomicLong(1L);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        AtomicReference<Runnable> dispatcher = new AtomicReference<>();
        List<Runnable> tableTasks = new ArrayList<>();
        AtomicInteger flushCalls = new AtomicInteger();
        AtomicInteger failureCalls = new AtomicInteger();
        when(executor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenAnswer(
                        invocation -> {
                            dispatcher.set(invocation.getArgument(0));
                            ScheduledFuture<?> future = mock(ScheduledFuture.class);
                            when(future.isDone()).thenReturn(false);
                            when(future.isCancelled()).thenReturn(false);
                            return future;
                        });
        doAnswer(
                        invocation -> {
                            tableTasks.add(invocation.getArgument(0));
                            return null;
                        })
                .when(executor)
                .execute(any(Runnable.class));
        PaimonAsyncCommitScheduler scheduler =
                new PaimonAsyncCommitScheduler(
                        true,
                        2,
                        coordinator,
                        clock::get,
                        ignored -> executor,
                        tableKey -> {
                            flushCalls.incrementAndGet();
                            throw new IllegalStateException("first-table-failure");
                        },
                        failure -> failureCalls.incrementAndGet());

        coordinator.acceptCdc(
                "default.a", 1, Collections.singleton("source-a"), 0L);
        coordinator.acceptCdc(
                "default.b", 1, Collections.singleton("source-a"), 0L);
        scheduler.stateChanged();
        dispatcher.get().run();
        assertEquals(2, tableTasks.size());

        tableTasks.get(0).run();
        tableTasks.get(1).run();

        assertTrue(scheduler.isFailed());
        assertEquals(1, flushCalls.get());
        assertEquals(1, failureCalls.get());
        verify(executor).shutdown();
        verify(executor, never()).shutdownNow();
    }

    @Test
    void workerMustBeCreatedLazilyOnlyForUncommittedCdcDeadline() {
        Fixture fixture = fixture();

        fixture.scheduler.stateChanged();
        fixture.coordinator.acceptInitial("default.orders", 1);
        fixture.scheduler.stateChanged();
        assertEquals(0, fixture.createdWorkers.get());

        fixture.coordinator.acceptCdc(
                "default.orders", 1, Collections.singleton("source-a"), 100L);
        fixture.clock.set(100L);
        fixture.scheduler.stateChanged();

        assertEquals(1, fixture.createdWorkers.get());
        assertTrue(fixture.scheduler.isWorkerCreated());
        verify(fixture.executor, times(1))
                .schedule(any(Runnable.class), eq(1000L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void immediateCommitMustNotCreateAWorker() {
        Fixture fixture = fixture(1, 1000L);
        PaimonMicroBatchCoordinator.BatchDecision decision =
                fixture.coordinator.acceptCdc(
                        "default.orders", 1, Collections.singleton("source-a"), 100L);
        assertTrue(decision.shouldCommitBySize());
        fixture.coordinator.publishCommit(
                fixture.coordinator.captureCommitTarget("default.orders"), 110L);

        fixture.scheduler.stateChanged();

        assertEquals(0, fixture.createdWorkers.get());
        verify(fixture.executor, never())
                .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void earlierDeadlineMustCancelAndReplaceTheOneShotTask() {
        Fixture fixture = fixture();
        fixture.coordinator.acceptCdc(
                "default.a", 1, Collections.singleton("source-a"), 100L);
        fixture.clock.set(100L);
        fixture.scheduler.stateChanged();
        ScheduledFuture<?> firstFuture = fixture.lastFuture.get();

        fixture.coordinator.acceptInitial("default.b", 1);
        fixture.coordinator.publishCommit(
                fixture.coordinator.captureCommitTarget("default.b"), 0L);
        fixture.coordinator.acceptCdc(
                "default.b", 1, Collections.singleton("source-a"), 200L);
        fixture.clock.set(200L);
        fixture.scheduler.stateChanged();

        verify(firstFuture).cancel(false);
        verify(fixture.executor, times(2))
                .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
        assertEquals(800L, fixture.lastDelayMs.get());
    }

    @Test
    void dueTaskMustFlushAndBecomeIdleWhenCommitClearsTheDeadline() {
        Fixture fixture = fixture();
        fixture.coordinator.acceptCdc(
                "default.orders", 1, Collections.singleton("source-a"), 100L);
        fixture.clock.set(100L);
        fixture.scheduler.stateChanged();
        Runnable task = fixture.lastTask.get();

        fixture.clock.set(1100L);
        task.run();

        assertEquals(1, fixture.flushCount.get());
        assertEquals("default.orders", fixture.lastFlushedTable.get());
        assertFalse(fixture.coordinator.nextDeadlineMs().isPresent());
        assertNull(fixture.failure.get());
    }

    @Test
    void taskFailureMustBeReportedOnceAndStopRescheduling() {
        Fixture fixture = fixture();
        fixture.failFlush.set(true);
        fixture.coordinator.acceptCdc(
                "default.orders", 1, Collections.singleton("source-a"), 100L);
        fixture.clock.set(100L);
        fixture.scheduler.stateChanged();
        Runnable task = fixture.lastTask.get();

        fixture.clock.set(1100L);
        task.run();

        assertEquals("scheduler-failure", fixture.failure.get().getMessage());
        assertTrue(fixture.scheduler.isFailed());
        verify(fixture.executor, times(1))
                .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void schedulingFailureMustBeReportedAndFenceFurtherPlanning() {
        Fixture fixture = fixture();
        RejectedExecutionException rejection =
                new RejectedExecutionException("scheduler rejected task");
        when(fixture.executor.schedule(
                        any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenThrow(rejection);
        fixture.coordinator.acceptCdc(
                "default.orders", 1, Collections.singleton("source-a"), 100L);

        RejectedExecutionException thrown =
                assertThrows(RejectedExecutionException.class, fixture.scheduler::stateChanged);

        assertSame(rejection, thrown);
        assertSame(rejection, fixture.failure.get());
        assertTrue(fixture.scheduler.isFailed());
        fixture.scheduler.stateChanged();
        verify(fixture.executor, times(1))
                .schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void shutdownMustBeOrderlyAndNeverInterruptRunningCommit() throws Exception {
        Fixture fixture = fixture();
        fixture.coordinator.acceptCdc(
                "default.orders", 1, Collections.singleton("source-a"), 100L);
        fixture.scheduler.stateChanged();
        when(fixture.executor.awaitTermination(5L, TimeUnit.SECONDS)).thenReturn(true);

        assertTrue(fixture.scheduler.shutdownAndAwait(5L, TimeUnit.SECONDS));

        verify(fixture.executor).shutdown();
        verify(fixture.executor, never()).shutdownNow();
    }

    private static Fixture fixture() {
        return fixture(100, 1000L);
    }

    @SuppressWarnings("unchecked")
    private static Fixture fixture(int batchSize, long intervalMs) {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(batchSize, intervalMs);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        AtomicInteger createdWorkers = new AtomicInteger();
        AtomicReference<Runnable> lastTask = new AtomicReference<>();
        AtomicLong lastDelayMs = new AtomicLong(-1L);
        AtomicReference<ScheduledFuture<?>> lastFuture = new AtomicReference<>();
        when(executor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenAnswer(
                        invocation -> {
                            lastTask.set(invocation.getArgument(0));
                            lastDelayMs.set(invocation.getArgument(1));
                            ScheduledFuture<?> future = mock(ScheduledFuture.class);
                            when(future.isDone()).thenReturn(false);
                            when(future.isCancelled()).thenReturn(false);
                            lastFuture.set(future);
                            return future;
                        });
        doAnswer(
                        invocation -> {
                            ((Runnable) invocation.getArgument(0)).run();
                            return null;
                        })
                .when(executor)
                .execute(any(Runnable.class));
        AtomicLong clock = new AtomicLong();
        AtomicInteger flushCount = new AtomicInteger();
        AtomicReference<String> lastFlushedTable = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> failFlush = new AtomicReference<>(false);

        PaimonAsyncCommitScheduler scheduler =
                new PaimonAsyncCommitScheduler(
                        true,
                        4,
                        coordinator,
                        clock::get,
                        ignored -> {
                            createdWorkers.incrementAndGet();
                            return executor;
                        },
                        tableKey -> {
                            flushCount.incrementAndGet();
                            lastFlushedTable.set(tableKey);
                            if (failFlush.get()) {
                                throw new IllegalStateException("scheduler-failure");
                            }
                            coordinator.publishCommit(
                                    coordinator.captureCommitTarget(tableKey), clock.get());
                        },
                        error -> failure.compareAndSet(null, error));
        return new Fixture(
                coordinator,
                scheduler,
                executor,
                createdWorkers,
                lastTask,
                lastDelayMs,
                lastFuture,
                clock,
                flushCount,
                lastFlushedTable,
                failure,
                failFlush);
    }

    private static final class Fixture {
        private final PaimonMicroBatchCoordinator coordinator;
        private final PaimonAsyncCommitScheduler scheduler;
        private final ScheduledExecutorService executor;
        private final AtomicInteger createdWorkers;
        private final AtomicReference<Runnable> lastTask;
        private final AtomicLong lastDelayMs;
        private final AtomicReference<ScheduledFuture<?>> lastFuture;
        private final AtomicLong clock;
        private final AtomicInteger flushCount;
        private final AtomicReference<String> lastFlushedTable;
        private final AtomicReference<Throwable> failure;
        private final AtomicReference<Boolean> failFlush;

        private Fixture(
                PaimonMicroBatchCoordinator coordinator,
                PaimonAsyncCommitScheduler scheduler,
                ScheduledExecutorService executor,
                AtomicInteger createdWorkers,
                AtomicReference<Runnable> lastTask,
                AtomicLong lastDelayMs,
                AtomicReference<ScheduledFuture<?>> lastFuture,
                AtomicLong clock,
                AtomicInteger flushCount,
                AtomicReference<String> lastFlushedTable,
                AtomicReference<Throwable> failure,
                AtomicReference<Boolean> failFlush) {
            this.coordinator = coordinator;
            this.scheduler = scheduler;
            this.executor = executor;
            this.createdWorkers = createdWorkers;
            this.lastTask = lastTask;
            this.lastDelayMs = lastDelayMs;
            this.lastFuture = lastFuture;
            this.clock = clock;
            this.flushCount = flushCount;
            this.lastFlushedTable = lastFlushedTable;
            this.failure = failure;
            this.failFlush = failFlush;
        }
    }
}
