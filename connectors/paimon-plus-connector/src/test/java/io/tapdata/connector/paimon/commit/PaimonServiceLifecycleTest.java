package io.tapdata.connector.paimon.commit;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonServiceLifecycleTest {

    @Test
    void constructionMustRemainNewUntilInitializationPublishesRunning() throws Exception {
        PaimonServiceLifecycle lifecycle = new PaimonServiceLifecycle();

        assertEquals(PaimonServiceLifecycle.State.NEW, lifecycle.state());
        assertThrows(IllegalStateException.class, () -> lifecycle.enter("writeRecords"));

        lifecycle.publishRunning();
        try (PaimonServiceLifecycle.Ingress ignored = lifecycle.enter("writeRecords")) {
            assertEquals(PaimonServiceLifecycle.State.RUNNING, lifecycle.state());
            assertEquals(1, lifecycle.activeIngressCount());
        }
        assertEquals(0, lifecycle.activeIngressCount());
    }

    @Test
    void stoppingNewLifecycleMustPreventConcurrentInitialization() {
        PaimonServiceLifecycle lifecycle = new PaimonServiceLifecycle();

        assertTrue(lifecycle.beginStopping());
        assertEquals(PaimonServiceLifecycle.State.STOPPING, lifecycle.state());
        assertThrows(IllegalStateException.class, lifecycle::publishRunning);
    }

    @Test
    void firstFailureMustBeStickyAndRejectEveryNewIngress() {
        PaimonServiceLifecycle lifecycle = runningLifecycle();
        IllegalStateException first = new IllegalStateException("first");

        assertSame(first, lifecycle.fail(first));
        assertSame(first, lifecycle.fail(new IllegalArgumentException("second")));

        Exception thrown = assertThrows(Exception.class, () -> lifecycle.enter("heartbeat"));
        assertSame(first, thrown);
        assertEquals(PaimonServiceLifecycle.State.FAILED, lifecycle.state());
    }

    @Test
    void stoppingMustRejectNewIngressAndWaitForAnAlreadyAdmittedOperation() throws Exception {
        PaimonServiceLifecycle lifecycle = runningLifecycle();
        PaimonServiceLifecycle.Ingress ingress = lifecycle.enter("scheduler");
        CountDownLatch waiterStarted = new CountDownLatch(1);
        AtomicBoolean waiterFinished = new AtomicBoolean();
        AtomicReference<Throwable> waiterFailure = new AtomicReference<>();

        assertTrue(lifecycle.beginStopping());
        Thread waiter = new Thread(() -> {
            waiterStarted.countDown();
            try {
                lifecycle.awaitQuiescence();
                waiterFinished.set(true);
            } catch (Throwable failure) {
                waiterFailure.set(failure);
            }
        });
        waiter.start();
        assertTrue(waiterStarted.await(5, TimeUnit.SECONDS));
        assertThrows(IllegalStateException.class, () -> lifecycle.enter("ddl"));
        assertFalse(waiterFinished.get());

        ingress.close();
        waiter.join(5000L);

        assertFalse(waiter.isAlive());
        assertTrue(waiterFinished.get());
        assertNull(waiterFailure.get());
    }

    @Test
    void normalConsumerMustNotStartAfterStoppingWinsTheGate() {
        PaimonServiceLifecycle lifecycle = runningLifecycle();
        AtomicBoolean marked = new AtomicBoolean();

        lifecycle.beginStopping();
        PaimonServiceLifecycle.ConsumerPermit permit =
                lifecycle.tryStartConsumer(false, () -> marked.set(true));

        assertNull(permit);
        assertFalse(marked.get());
    }

    @Test
    void consumerThatStartsBeforeStoppingMustRemainVisibleUntilItCompletes() throws Exception {
        PaimonServiceLifecycle lifecycle = runningLifecycle();
        AtomicBoolean marked = new AtomicBoolean();
        PaimonServiceLifecycle.ConsumerPermit permit =
                lifecycle.tryStartConsumer(false, () -> marked.set(true));

        assertTrue(marked.get());
        assertEquals(1, lifecycle.activeConsumerCount());
        lifecycle.beginStopping();
        assertFalse(lifecycle.isQuiescent());

        permit.close();

        assertTrue(lifecycle.isQuiescent());
    }

    @Test
    void explicitStopDrainMayStartOnlyWhileStoppingAndNoFailureExists() {
        PaimonServiceLifecycle lifecycle = runningLifecycle();
        lifecycle.beginStopping();
        AtomicBoolean marked = new AtomicBoolean();

        PaimonServiceLifecycle.ConsumerPermit permit =
                lifecycle.tryStartConsumer(true, () -> marked.set(true));

        assertTrue(marked.get());
        permit.close();
        lifecycle.fail(new IllegalStateException("drain-failure"));
        assertNull(lifecycle.tryStartConsumer(true, () -> { }));
    }

    @Test
    void stopDrainAdmissionGuardMustBeCheckedBeforeMarkingConsumerStarted() {
        PaimonServiceLifecycle lifecycle = runningLifecycle();
        lifecycle.beginStopping();
        AtomicBoolean marked = new AtomicBoolean();

        PaimonServiceLifecycle.ConsumerPermit permit =
                lifecycle.tryStartConsumer(true, () -> false, () -> marked.set(true));

        assertNull(permit);
        assertFalse(marked.get());
        assertEquals(0, lifecycle.activeConsumerCount());
    }

    @Test
    void interruptedWaitMustBeObservableAndCallerCanFinishCleanupThenRestoreInterrupt()
            throws Exception {
        PaimonServiceLifecycle lifecycle = runningLifecycle();
        PaimonServiceLifecycle.Ingress ingress = lifecycle.enter("writeRecords");
        lifecycle.beginStopping();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptedAfterCleanup = new AtomicBoolean();

        Thread waiter = new Thread(() -> {
            boolean interrupted = false;
            try {
                lifecycle.awaitQuiescence();
            } catch (InterruptedException expected) {
                interrupted = true;
                try {
                    lifecycle.awaitQuiescence();
                } catch (Throwable unexpected) {
                    failure.set(unexpected);
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                interruptedAfterCleanup.set(Thread.currentThread().isInterrupted());
            }
        });
        waiter.start();
        waiter.interrupt();
        ingress.close();
        waiter.join(5000L);

        assertNull(failure.get());
        assertTrue(interruptedAfterCleanup.get());
    }

    @Test
    void closePublicationMustBeIdempotentAndKeepTheSameTerminalOutcome() {
        PaimonServiceLifecycle lifecycle = runningLifecycle();
        IllegalStateException outcome = new IllegalStateException("close-failure");

        assertSame(outcome, lifecycle.publishClosed(outcome));
        assertSame(outcome, lifecycle.publishClosed(new IllegalStateException("ignored")));
        assertEquals(PaimonServiceLifecycle.State.CLOSED, lifecycle.state());
        assertSame(outcome, lifecycle.terminalOutcome());
    }

    private static PaimonServiceLifecycle runningLifecycle() {
        PaimonServiceLifecycle lifecycle = new PaimonServiceLifecycle();
        lifecycle.publishRunning();
        return lifecycle;
    }
}
