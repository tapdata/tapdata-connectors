package io.tapdata.connector.paimon.service;

import io.tapdata.entity.logger.Log;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class PaimonStopLogTest {
    @Test
    void terminalEventsAreDeliveredAcrossIdleWorkerExitAndRestart() throws Exception {
        ThreadPoolExecutor dispatcher = dispatcher();
        long keepAlive = dispatcher.getKeepAliveTime(TimeUnit.NANOSECONDS);
        dispatcher.setKeepAliveTime(1, TimeUnit.MILLISECONDS);
        try {
            for (int i = 0; i < 100; i++) {
                CountDownLatch delivered = new CountDownLatch(1);
                Log log = mock(Log.class);
                doAnswer(call -> { delivered.countDown(); return null; }).when(log).error(anyString());
                PaimonStopLog.offerError(log, "terminal-" + i, null);
                assertTrue(delivered.await(5, TimeUnit.SECONDS), "Last event must not need another offer");
                await(() -> dispatcher.getPoolSize() == 0);
                assertEquals(0, PaimonStopLog.queuedCount());
            }
        } finally {
            dispatcher.setKeepAliveTime(keepAlive, TimeUnit.NANOSECONDS);
        }
    }

    @Test
    @SuppressWarnings("removal")
    void blockedBackendDoesNotPreventTaskGroupDestructionOrBoundedOffers() throws Exception {
        ThreadGroup taskGroup = new ThreadGroup("stop-log-old-task");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<ThreadGroup> workerGroup = new AtomicReference<>();
        Log log = mock(Log.class);
        doAnswer(call -> {
            workerGroup.set(Thread.currentThread().getThreadGroup());
            entered.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return null;
        }).when(log).info("blocked");
        Thread caller = new Thread(taskGroup, () -> PaimonStopLog.offer(log, "blocked", null));
        try {
            caller.start();
            caller.join(5000);
            assertFalse(caller.isAlive());
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertNotSame(taskGroup, workerGroup.get());
            taskGroup.interrupt();
            taskGroup.destroy();
            assertTrue(taskGroup.isDestroyed());

            long dropped = PaimonStopLog.droppedCount();
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(2), () -> {
                for (int i = 0; i < 257; i++) PaimonStopLog.offer(log, "queued", null);
            });
            assertEquals(256, PaimonStopLog.queuedCount());
            assertEquals(dropped + 1, PaimonStopLog.droppedCount());
        } finally {
            release.countDown();
            await(() -> dispatcher().getActiveCount() == 0 && PaimonStopLog.queuedCount() == 0);
            if (!taskGroup.isDestroyed()) taskGroup.destroy();
        }
    }

    @Test
    void backendFailureIsCountedAndNextEventStillDelivered() throws Exception {
        Log log = mock(Log.class);
        doThrow(new IllegalStateException("backend unavailable")).when(log).error("failed");
        long dropped = PaimonStopLog.droppedCount();
        PaimonStopLog.offerError(log, "failed", null);
        PaimonStopLog.offer(log, "next", null);
        verify(log, timeout(5000)).info("next");
        assertEquals(dropped + 1, PaimonStopLog.droppedCount());
    }

    private static ThreadPoolExecutor dispatcher() {
        try {
            Field field = PaimonStopLog.class.getDeclaredField("DISPATCHER");
            field.setAccessible(true);
            return (ThreadPoolExecutor) field.get(null);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError(failure);
        }
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(1);
        assertTrue(condition.getAsBoolean());
    }
}
