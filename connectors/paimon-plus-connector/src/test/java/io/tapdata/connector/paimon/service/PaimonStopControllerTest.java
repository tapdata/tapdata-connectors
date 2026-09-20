package io.tapdata.connector.paimon.service;

import org.junit.jupiter.api.Test;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class PaimonStopControllerTest {
    @Test void workerDeadlineMustReportSameTimeoutWithBudgetsAndDeduplicateObservations() {
        AtomicLong now = new AtomicLong();
        PaimonStopController stop = new PaimonStopController("deadline", 1, 1, 1, now::get);
        stop.start(); now.set(TimeUnit.SECONDS.toNanos(1));
        PaimonStopController.StopTimeoutException timeout = assertThrows(
                PaimonStopController.StopTimeoutException.class, () -> stop.checkAction("worker poll"));
        stop.recordFailure(timeout); stop.recordFailure(timeout);
        stop.retain(stop.timeoutFailure(), new Object());
        assertSame(timeout, stop.failure()); assertEquals(0, timeout.getSuppressed().length);
        assertTrue(timeout.getMessage().contains("totalBudgetMs=1000"));
        assertTrue(stop.terminal().timedOut);
        assertTrue(stop.terminal().diagnostics.contains("remainingMs=0"));
    }

    @Test void repeatedSecondaryFailureMustBeSuppressedOnceAndPreserveBusinessCause() {
        PaimonStopController stop = new PaimonStopController("failure", 1, 1, 1);
        Exception business = new Exception("business"), cleanup = new Exception("cleanup");
        stop.recordFailure(business); stop.recordFailure(cleanup); stop.recordFailure(cleanup);
        assertSame(business, stop.failure());
        assertArrayEquals(new Throwable[] {cleanup}, business.getSuppressed());
    }

    @Test void diagnosticsMustDistinguishPrepareReturnFromExecutorTermination() {
        PaimonStopController stop = new PaimonStopController("observed", 180, 120, 30);
        io.tapdata.connector.paimon.write.PaimonCompactionLifecycle lifecycle = org.mockito.Mockito.mock(
                io.tapdata.connector.paimon.write.PaimonCompactionLifecycle.class);
        stop.start(); PaimonStopController.FinalAttempt attempt = stop.beginFinal("t", 7, lifecycle);
        attempt.markPrepareReturned();
        assertTrue(stop.diagnostics().contains("attempt=observed:t:7"));
        assertTrue(stop.diagnostics().contains("prepareReturned=true"));
        assertTrue(stop.diagnostics().contains("executorTerminated=false"));
        org.mockito.Mockito.when(lifecycle.isTerminated()).thenReturn(true);
        assertTrue(stop.diagnostics().contains("executorTerminated=true"));
    }

    @Test void totalBudgetIsNotResetAndRetainedOutcomeCannotBecomeSuccess() {
        AtomicLong now = new AtomicLong();
        PaimonStopController stop = new PaimonStopController("owner", 180, 120, 30, now::get);
        stop.start();
        now.set(TimeUnit.SECONDS.toNanos(100));
        stop.start();
        assertEquals(80, TimeUnit.NANOSECONDS.toSeconds(stop.remainingNanos()));
        now.set(TimeUnit.SECONDS.toNanos(180));
        Object resources = new Object();
        stop.retain(stop.timeoutFailure(), resources);
        Throwable terminal = stop.failure();
        stop.complete(null);
        assertSame(terminal, stop.failure());
        assertTrue(stop.isFinished());
        assertTrue(stop.isRetained());
        assertThrows(PaimonStopController.FrozenException.class, () -> stop.checkAction("late commit"));
        assertSame(resources, PaimonStopResources.retainedRoot(stop));
    }

    @Test void finalDecisionHasOnlyOneWinnerAndLeavesCancellationReserve() {
        AtomicLong now = new AtomicLong();
        PaimonStopController stop = new PaimonStopController("owner", 180, 120, 30, now::get);
        stop.start();
        now.set(TimeUnit.SECONDS.toNanos(140));
        PaimonStopController.FinalAttempt attempt = stop.beginFinal("table", 7, null);
        now.set(TimeUnit.SECONDS.toNanos(150));
        assertSame(attempt, stop.pollCancellation());
        assertFalse(stop.admitFinalCommit(attempt));
        assertTrue(attempt.cancelRequested());
        assertNull(stop.pollCancellation());
        now.set(TimeUnit.SECONDS.toNanos(180));
        assertTrue(stop.expired());
    }

    @Test void admittedCommitDisablesFinalTimerButNotTotalTimer() {
        AtomicLong now = new AtomicLong();
        PaimonStopController stop = new PaimonStopController("owner", 180, 120, 30, now::get);
        stop.start();
        PaimonStopController.FinalAttempt attempt = stop.beginFinal("t", 7, null);
        assertTrue(stop.admitFinalCommit(attempt));
        now.set(TimeUnit.SECONDS.toNanos(140));
        assertNull(stop.pollCancellation());
        assertFalse(stop.expired());
        now.set(TimeUnit.SECONDS.toNanos(180));
        assertTrue(stop.expired());
    }
    @Test void commitAndCancellationMustHaveOneWinnerUnderConcurrentThreads() throws Exception {
        for (int round = 0; round < 50; round++) {
            AtomicLong now = new AtomicLong();
            PaimonStopController stop = new PaimonStopController("race", 180, 120, 30, now::get);
            stop.start();
            PaimonStopController.FinalAttempt attempt = stop.beginFinal("t", 0, null);
            boolean cancelWins = round % 2 == 0;
            now.set(TimeUnit.SECONDS.toNanos(cancelWins ? 120 : 119));
            java.util.concurrent.CyclicBarrier start = new java.util.concurrent.CyclicBarrier(2);
            java.util.concurrent.atomic.AtomicReference<Boolean> commit = new java.util.concurrent.atomic.AtomicReference<>();
            java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
            Thread submitter = new Thread(() -> { try { start.await(); commit.set(stop.admitFinalCommit(attempt)); } catch (Throwable e) { failure.set(e); } });
            Thread timer = new Thread(() -> { try { start.await(); stop.pollCancellation(); } catch (Throwable e) { failure.set(e); } });
            submitter.start(); timer.start(); submitter.join(3000); timer.join(3000);
            assertFalse(submitter.isAlive()); assertFalse(timer.isAlive()); assertNull(failure.get());
            assertEquals(!cancelWins, commit.get()); assertEquals(cancelWins, attempt.cancelRequested());
            assertNull(stop.pollCancellation());
        }
    }

    @Test void frozenStatePublicationMustBeRejectedAfterWaitingForApplicationLock() throws Exception {
        AtomicLong now = new AtomicLong();
        PaimonStopController stop = new PaimonStopController("publish", 1, 1, 1, now::get);
        Object stateLock = new Object();
        java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger published = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<Throwable> error = new java.util.concurrent.atomic.AtomicReference<>();
        Thread worker;
        synchronized (stateLock) {
            worker = new Thread(() -> { entered.countDown(); synchronized (stateLock) {
                try { stop.publish("publish pending", published::incrementAndGet); } catch (Throwable e) { error.set(e); }
            } });
            worker.start(); assertTrue(entered.await(3, TimeUnit.SECONDS));
            stop.start(); now.set(TimeUnit.SECONDS.toNanos(1));
            stop.retain(stop.timeoutFailure(), stateLock);
            assertTrue(stop.isFinished(), "终态不能等待应用持有的长锁");
        }
        worker.join(3000); assertEquals(0, published.get());
        assertInstanceOf(PaimonStopController.FrozenException.class, error.get());
    }

    @Test void retainedRootMustKeepLateBoundScopeAliveAcrossGcAndRepeatedCompletion() throws Exception {
        AtomicLong now = new AtomicLong();
        PaimonStopController stop = new PaimonStopController("gc", 1, 1, 1, now::get);
        PaimonStopResources ledger = new PaimonStopResources(); stop.attachResourceRoot(ledger);
        PaimonStopResources.Scope scope = ledger.scope("IOManager allocation", stop);
        PaimonStopResources.Slot slot = scope.reserve("raw IOManager");
        stop.start(); now.set(TimeUnit.SECONDS.toNanos(1)); stop.retain(stop.timeoutFailure(), ledger);
        Object resource = new byte[1024 * 1024];
        java.lang.ref.WeakReference<Object> reference = new java.lang.ref.WeakReference<>(resource);
        slot.bind(resource); resource = null; slot = null; scope = null; ledger = null;
        PaimonStopController.Terminal terminal = stop.terminal();
        for (int i = 0; i < 3; i++) { System.gc(); Thread.sleep(10); }
        assertNotNull(reference.get()); assertNotNull(PaimonStopResources.retainedRoot(stop));
        stop.complete(null); stop.retain(new Exception("late"), new Object());
        assertSame(terminal, stop.terminal());
    }

    @Test void finalRegistrationWindowsMustNotBlockTimeoutOrPermitLatePrepare() throws Exception {
        for (boolean bindBeforeBlocking : new boolean[] {false, true}) {
            AtomicLong now = new AtomicLong();
            PaimonStopController stop = new PaimonStopController("register-window", 1, 1, 1, now::get);
            io.tapdata.connector.paimon.write.PaimonCompactionLifecycle lifecycle = org.mockito.Mockito.spy(
                    io.tapdata.connector.paimon.write.PaimonCompactionLifecycle.forTable("register-window"));
            lifecycle.attachStopController(stop);
            PaimonStopResources ledger = new PaimonStopResources(); stop.attachResourceRoot(ledger);
            ledger.scope("executor", stop).reserve("executor").bind(lifecycle);
            java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
            java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
            org.mockito.Mockito.doAnswer(invocation -> {
                if (bindBeforeBlocking) { invocation.callRealMethod(); }
                entered.countDown(); assertTrue(release.await(3, TimeUnit.SECONDS));
                if (!bindBeforeBlocking) { invocation.callRealMethod(); }
                return null;
            }).when(lifecycle).beginFinal(org.mockito.ArgumentMatchers.any());
            java.util.concurrent.atomic.AtomicReference<PaimonStopController.FinalAttempt> attempt = new java.util.concurrent.atomic.AtomicReference<>();
            Thread worker = new Thread(() -> attempt.set(stop.beginFinal("t", 0, lifecycle)));
            stop.start(); worker.start();
            try {
                assertTrue(entered.await(3, TimeUnit.SECONDS)); now.set(TimeUnit.SECONDS.toNanos(1));
                assertNull(stop.pollCancellation(), "executor 注册未完成前不能发送取消");
                stop.retain(stop.timeoutFailure(), ledger); assertTrue(stop.isFinished());
                assertSame(ledger, PaimonStopResources.retainedRoot(stop));
                release.countDown(); worker.join(3000);
                assertFalse(worker.isAlive()); assertFalse(attempt.get().permitsPrepare());
                assertThrows(PaimonStopController.FrozenException.class, () -> stop.run("late prepare", () -> fail("frozen")));
            } finally {
                release.countDown(); worker.join(3000); lifecycle.shutdown();
                assertTrue(lifecycle.awaitTermination(TimeUnit.SECONDS.toNanos(3)));
            }
        }
    }

}
