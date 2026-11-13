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
            verify(log, timeout(5000)).warn(contains("continues in background"));
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
    void busyWorkerDefersWithoutQueueing() throws Exception {
        ExecutorService rejected = Executors.newSingleThreadExecutor();
        rejected.shutdown();
        Log log = mock(Log.class);
        assertDoesNotThrow(() -> PaimonSpillDirCleaner.reclaimForStartup(
                Collections.emptyList(), log, rejected, 20));
        verify(log, timeout(5000)).warn(contains("reclaim deferred"));
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
