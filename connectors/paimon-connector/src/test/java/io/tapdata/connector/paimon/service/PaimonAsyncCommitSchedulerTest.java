package io.tapdata.connector.paimon.service;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonAsyncCommitSchedulerTest {

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
        AtomicLong clock = new AtomicLong();
        AtomicInteger flushCount = new AtomicInteger();
        AtomicReference<String> lastFlushedTable = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<Boolean> failFlush = new AtomicReference<>(false);

        PaimonAsyncCommitScheduler scheduler =
                new PaimonAsyncCommitScheduler(
                        true,
                        coordinator,
                        clock::get,
                        () -> {
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
