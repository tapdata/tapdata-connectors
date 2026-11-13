package io.tapdata.connector.paimon.util;

import io.tapdata.entity.logger.Log;
import org.junit.jupiter.api.Test;
import java.util.Collections;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PaimonStartupReclaimTest {
    @Test
    void blockedScanDoesNotBlockStartupOrGetCancelled() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        Log log = mock(Log.class);
        worker.submit(() -> {
            started.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        });
        try {
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertTimeoutPreemptively(java.time.Duration.ofSeconds(2), () ->
                    PaimonSpillDirCleaner.reclaimForStartup(Collections.emptyList(), log, worker, 20));
            verify(log, timeout(5000)).info(contains("continues in background"));
            release.countDown();
            worker.shutdown();
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            verify(log, timeout(5000)).info(contains("reclaim finished"));
        } finally {
            release.countDown();
            worker.shutdownNow();
        }
    }

    @Test
    void backgroundFailureIsReportedAfterCallerStopsWaiting() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Log log = mock(Log.class);
        try {
            // A malformed root fails outside the protocol event sink.
            PaimonSpillDirCleaner.reclaimForStartup(Collections.singletonList(null), log, worker, 0);
            verify(log, timeout(5000)).error(contains("background reclaim failed"));
        } finally {
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void shutDownWorkerIsReportedAsUnavailable() throws Exception {
        ExecutorService rejected = Executors.newSingleThreadExecutor();
        rejected.shutdown();
        Log log = mock(Log.class);
        assertDoesNotThrow(() -> PaimonSpillDirCleaner.reclaimForStartup(
                Collections.emptyList(), log, rejected, 20));
        verify(log, timeout(5000)).warn(contains("reclaim worker unavailable"));
    }

    @Test
    void busyWorkerDefersAsInfoWithoutQueueing() throws Exception {
        // 真正的 busy：core=0 + SynchronousQueue + AbortPolicy，与生产池同构。
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        ThreadPoolExecutor worker = new ThreadPoolExecutor(0, 1, 1, TimeUnit.SECONDS,
                new SynchronousQueue<>(), new ThreadPoolExecutor.AbortPolicy());
        Log log = mock(Log.class);
        try {
            worker.submit(() -> {
                started.countDown();
                try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            assertDoesNotThrow(() -> PaimonSpillDirCleaner.reclaimForStartup(
                    Collections.emptyList(), log, worker, 20));
            verify(log, timeout(5000)).info(contains("another scan is running"));
            Thread.sleep(200); // PaimonStopLog 是异步派发，给误记的 WARN 一个到达窗口
            verify(log, never()).warn(anyString());
            assertEquals(0, worker.getQueue().size(), "被拒的扫描不能排队");
        } finally {
            release.countDown();
            worker.shutdownNow();
            worker.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void reclaimWorkerThreadKeepsCallerTcclAndGetsUniqueName() throws Exception {
        ClassLoader caller = new java.net.URLClassLoader(new java.net.URL[0], getClass().getClassLoader());
        ClassLoader other = new java.net.URLClassLoader(new java.net.URL[0], getClass().getClassLoader());
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread first;
        Thread second;
        try {
            Thread.currentThread().setContextClassLoader(caller);
            first = PaimonSpillDirCleaner.newReclaimThread(() -> { });
            Thread.currentThread().setContextClassLoader(other);
            second = PaimonSpillDirCleaner.newReclaimThread(() -> { });
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
        try {
            assertSame(caller, first.getContextClassLoader(), "扫描线程必须沿用提交方的 TCCL");
            assertSame(other, second.getContextClassLoader());
            assertTrue(first.isDaemon());
            assertTrue(first.getName().matches("paimon-spill-reclaim-[0-9a-f]+-\\d+"), first.getName());
            assertNotEquals(first.getName(), second.getName(), "不同 ClassLoader 实例的线程名必须可区分");
        } finally {
            ((java.net.URLClassLoader) caller).close();
            ((java.net.URLClassLoader) other).close();
        }
    }

    @Test
    void taskAuditDoesNotDependOnEngineAppender() {
        Log log = mock(Log.class);
        RuntimeException denied = new RuntimeException("permission denied");
        PaimonSpillAudit.withTaskLog(log, (event, fields, failure) -> {
            throw new IllegalStateException("engine logger is unavailable");
        }).record(PaimonSpillAudit.Event.RECLAIM_FAILED,
                PaimonSpillAudit.details("lease", "/scratch/lease"), denied);
        verify(log, timeout(5000)).error(contains("permission denied"));
        PaimonSpillAudit.withTaskLog(log).record(PaimonSpillAudit.Event.ADMISSION_DENIED,
                PaimonSpillAudit.details("fileStoreType", "nfs"));
        verify(log, timeout(5000)).warn(contains("fileStoreType=nfs"));
    }
}
