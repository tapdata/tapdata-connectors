package io.tapdata.connector.paimon.service;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
