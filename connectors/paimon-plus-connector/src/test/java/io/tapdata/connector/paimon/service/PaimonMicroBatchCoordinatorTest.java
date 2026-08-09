package io.tapdata.connector.paimon.service;

import io.tapdata.entity.event.TapCallbackOffset;
import org.junit.jupiter.api.Test;

import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonMicroBatchCoordinatorTest {

    @Test
    void initialAndCdcMustUseIndependentCountersAndGenerations() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(3, 1000L);

        coordinator.acceptInitial("default.orders", 2);
        PaimonMicroBatchCoordinator.TableSnapshot initial =
                coordinator.tableSnapshot("default.orders");
        assertEquals(2L, initial.bufferedRecordCount());
        assertEquals(0L, initial.accumulatedRecordCount());
        assertEquals(0L, initial.acceptedGeneration());
        assertFalse(initial.cdcEligible());
        assertFalse(coordinator.nextDeadlineMs().isPresent());

        PaimonMicroBatchCoordinator.BatchDecision decision =
                coordinator.acceptCdc(
                        "default.orders", 2, Collections.singleton("source-a"), 100L);

        assertEquals(1L, decision.acceptedGeneration());
        assertEquals(2L, decision.accumulatedRecordCount());
        assertFalse(decision.shouldCommit());
        PaimonMicroBatchCoordinator.TableSnapshot cdc =
                coordinator.tableSnapshot("default.orders");
        assertEquals(4L, cdc.bufferedRecordCount());
        assertEquals(2L, cdc.accumulatedRecordCount());
        assertEquals(1L, cdc.lastAcceptedGeneration("source-a"));
        assertTrue(cdc.cdcEligible());
        assertEquals(1100L, coordinator.nextDeadlineMs().getAsLong());
    }

    @Test
    void sizeAndCallTimeDecisionsMustUseTheTableDeadline() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(3, 1000L);

        assertFalse(
                coordinator.acceptCdc(
                                "default.orders", 1, Collections.singleton("source-a"), 100L)
                        .shouldCommit());
        assertTrue(
                coordinator.acceptCdc(
                                "default.orders", 2, Collections.singleton("source-a"), 200L)
                        .shouldCommitBySize());

        PaimonMicroBatchCoordinator.CommitTarget target =
                coordinator.captureCommitTarget("default.orders");
        coordinator.publishCommit(target, 250L);

        assertEquals(0L, coordinator.tableSnapshot("default.orders").accumulatedRecordCount());
        assertFalse(
                coordinator.acceptCdc(
                                "default.orders", 1, Collections.singleton("source-a"), 1249L)
                        .shouldCommitByTime());
        assertTrue(
                coordinator.acceptCdc(
                                "default.orders", 1, Collections.singleton("source-a"), 1250L)
                        .shouldCommitByTime());
    }

    @Test
    void commitMustPublishOnlyTheCapturedGenerationAndRetainLaterData() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1000L);
        coordinator.acceptCdc(
                "default.orders", 2, Collections.singleton("source-a"), 100L);
        PaimonMicroBatchCoordinator.CommitTarget target =
                coordinator.captureCommitTarget("default.orders");
        coordinator.markPendingCommit(target);
        coordinator.acceptCdc(
                "default.orders", 1, Collections.singleton("source-a"), 200L);

        coordinator.publishCommit(target, 300L);

        PaimonMicroBatchCoordinator.TableSnapshot snapshot =
                coordinator.tableSnapshot("default.orders");
        assertEquals(1L, snapshot.committedGeneration());
        assertEquals(1L, snapshot.accumulatedRecordCount());
        assertEquals(1L, snapshot.bufferedRecordCount());
        assertFalse(snapshot.hasPendingCommit());
        assertEquals(1300L, coordinator.nextDeadlineMs().getAsLong());
    }

    @Test
    void disabledIntervalMustNeverExposeADeadline() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 0L);
        coordinator.acceptCdc(
                "default.orders", 1, Collections.singleton("source-a"), 100L);

        OptionalLong deadline = coordinator.nextDeadlineMs();

        assertFalse(deadline.isPresent());
        assertTrue(coordinator.dueTables(10000L).isEmpty());
    }

    @Test
    void tableKeyValidationMustPreserveMessageForNullAndBlankValues() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1000L);

        IllegalArgumentException nullFailure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> coordinator.tableSnapshot(null));
        IllegalArgumentException blankFailure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> coordinator.tableSnapshot(" \t"));

        assertEquals("Table key must not be blank", nullFailure.getMessage());
        assertEquals("Table key must not be blank", blankFailure.getMessage());
    }

    @Test
    void sourceLaneValidationMustPreserveMessageForNullAndBlankValues() {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1000L);

        IllegalArgumentException nullFailure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> coordinator.registerHeartbeat(null, new TapCallbackOffset()));
        IllegalArgumentException blankFailure =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> coordinator.registerHeartbeat(" \t", new TapCallbackOffset()));

        assertEquals("Source lane must not be blank", nullFailure.getMessage());
        assertEquals("Source lane must not be blank", blankFailure.getMessage());
    }

    @Test
    void crossTableCdcMutationsMustStaySerializedForHeartbeatSnapshots() throws Exception {
        PaimonMicroBatchCoordinator coordinator =
                new PaimonMicroBatchCoordinator(100, 1000L);
        CountDownLatch firstMutationEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstMutation = new CountDownLatch(1);
        Set<String> blockingSourceLanes =
                blockOnSecondIteration(firstMutationEntered, releaseFirstMutation);
        AtomicReference<Thread> secondMutationThread = new AtomicReference<>();
        AtomicReference<Thread> heartbeatThread = new AtomicReference<>();
        ExecutorService executor = Executors.newFixedThreadPool(3);

        try {
            Future<PaimonMicroBatchCoordinator.BatchDecision> first =
                    executor.submit(
                            () ->
                                    coordinator.acceptCdc(
                                            "default.orders_a",
                                            1,
                                            blockingSourceLanes,
                                            100L));
            assertTrue(firstMutationEntered.await(5, TimeUnit.SECONDS));

            Future<PaimonMicroBatchCoordinator.BatchDecision> second =
                    executor.submit(
                            () -> {
                                secondMutationThread.set(Thread.currentThread());
                                return coordinator.acceptCdc(
                                        "default.orders_b",
                                        1,
                                        Collections.singleton("source-a"),
                                        100L);
                            });
            awaitBlocked(secondMutationThread, "second table CDC mutation");

            Future<PaimonMicroBatchCoordinator.CallbackReservation> heartbeat =
                    executor.submit(
                            () -> {
                                heartbeatThread.set(Thread.currentThread());
                                return coordinator.registerHeartbeat(
                                        "source-a", new TapCallbackOffset());
                            });
            awaitBlocked(heartbeatThread, "Heartbeat registration");

            releaseFirstMutation.countDown();
            assertEquals(1L, first.get(5, TimeUnit.SECONDS).acceptedGeneration());
            assertEquals(1L, second.get(5, TimeUnit.SECONDS).acceptedGeneration());
            heartbeat.get(5, TimeUnit.SECONDS);
            assertEquals(
                    1L,
                    coordinator
                            .tableSnapshot("default.orders_a")
                            .lastAcceptedGeneration("source-a"));
            assertEquals(
                    1L,
                    coordinator
                            .tableSnapshot("default.orders_b")
                            .lastAcceptedGeneration("source-a"));
        } finally {
            releaseFirstMutation.countDown();
            executor.shutdownNow();
        }
    }

    private static Set<String> blockOnSecondIteration(
            CountDownLatch entered, CountDownLatch release) {
        return new AbstractSet<String>() {
            private final AtomicInteger iteratorCount = new AtomicInteger();

            @Override
            public Iterator<String> iterator() {
                if (iteratorCount.incrementAndGet() == 2) {
                    entered.countDown();
                    try {
                        if (!release.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("Timed out waiting to release CDC mutation");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("Interrupted while blocking CDC mutation", e);
                    }
                }
                return Collections.singleton("source-a").iterator();
            }

            @Override
            public int size() {
                return 1;
            }
        };
    }

    private static void awaitBlocked(
            AtomicReference<Thread> threadReference, String operation)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            Thread thread = threadReference.get();
            if (thread != null && thread.getState() == Thread.State.BLOCKED) {
                return;
            }
            if (thread != null && !thread.isAlive()) {
                throw new AssertionError(operation + " completed before reaching the global lock");
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("Timed out waiting for " + operation + " to block");
    }
}
