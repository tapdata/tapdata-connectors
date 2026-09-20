package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.service.PaimonStopController;
import io.tapdata.connector.paimon.service.PaimonStopResources;
import org.apache.paimon.CoreOptions.ExpireExecutionMode;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** 真实 TableCommitImpl 维护入口与连接器退出适配器的边界验证。 */
class PaimonNativeCommitterCloseTest {
    @ParameterizedTest
    @EnumSource(ExpireExecutionMode.class)
    void acceptedNativeModeMustCloseOnceWithoutExtraMaintenance(ExpireExecutionMode mode) throws Exception {
        FileStoreCommit raw = mock(FileStoreCommit.class);
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        TableCommitImpl commit = nativeCommit(raw, calls::incrementAndGet, mode);
        PaimonNativeCommitterClose close = new PaimonNativeCommitterClose(commit,
                PaimonStopResources.Scope.standalone("native-mode"));
        try {
            commit.commitMultiple(Collections.singletonList(new ManifestCommittable(1L)), false);
            close.close();
            close.close();
            assertEquals(1, calls.get());
            assertTrue(commit.getMaintainExecutor().isTerminated());
            verify(raw).close();
        } finally { commit.getMaintainExecutor().shutdownNow(); }
    }

    @Test
    void closeMustWaitForActualMaintenanceReturnBeforeRawClose() throws Exception {
        try (Fixture f = new Fixture(false)) {
            f.startClose();
            assertFalse(f.returned.await(100, TimeUnit.MILLISECONDS));
            verify(f.raw, never()).close();
            assertFalse(f.interrupted.get());
            f.release.countDown();
            assertTrue(f.returned.await(5, TimeUnit.SECONDS));
            assertNull(f.failure.get());
            assertTrue(f.commit.getMaintainExecutor().isTerminated());
            verify(f.raw).close();
            assertFalse(f.scope.controller().isStarted(), "普通关闭不得启动全局 STOP");
        }
    }

    @Test
    void startedStopBudgetMustBoundWaitAndKeepFailureStickyAfterLateTermination() throws Exception {
        try (Fixture f = new Fixture(true)) {
            f.startClose();
            assertTrue(f.returned.await(5, TimeUnit.SECONDS));
            assertInstanceOf(PaimonStopController.StopTimeoutException.class, f.failure.get());
            verify(f.raw, never()).close();
            assertFalse(f.interrupted.get(), "超时不是强杀维护线程的授权");
            f.release.countDown();
            assertTrue(f.commit.getMaintainExecutor().awaitTermination(5, TimeUnit.SECONDS));
            assertSame(f.failure.get(), assertThrows(Exception.class, f.close::close));
            verify(f.raw, never()).close();
        }
    }

    @Test
    void interruptedCloserMustNotCloseRawOrInterruptMaintenance() throws Exception {
        try (Fixture f = new Fixture(false)) {
            f.startClose();
            assertFalse(f.returned.await(100, TimeUnit.MILLISECONDS));
            f.closer.interrupt();
            assertTrue(f.returned.await(5, TimeUnit.SECONDS));
            assertInstanceOf(InterruptedException.class, f.failure.get());
            assertTrue(f.closerInterrupted.get());
            assertFalse(f.interrupted.get());
            verify(f.raw, never()).close();
        }
    }

    @Test
    void rawCloseFailureMustNeverBeRetriedOrReportedAsSuccess() throws Exception {
        FileStoreCommit raw = mock(FileStoreCommit.class);
        Exception failure = new Exception("原生 close 失败");
        doThrow(failure).when(raw).close();
        TableCommitImpl commit = nativeCommit(raw, () -> {}, ExpireExecutionMode.ASYNC);
        PaimonNativeCommitterClose close = new PaimonNativeCommitterClose(commit,
                PaimonStopResources.Scope.standalone("sticky-close"));
        try {
            assertSame(failure, assertThrows(Exception.class, close::close));
            assertSame(failure, assertThrows(Exception.class, close::close));
            verify(raw).close();
            assertTrue(commit.getMaintainExecutor().isTerminated());
        } finally { commit.getMaintainExecutor().shutdownNow(); }
    }

    private static TableCommitImpl nativeCommit(FileStoreCommit raw, Runnable expiration,
            ExpireExecutionMode mode) {
        return new TableCommitImpl(raw, expiration, null, null, null, null, mode,
                "native-close-test", false, 1);
    }

    private static final class Fixture implements AutoCloseable {
        final FileStoreCommit raw = mock(FileStoreCommit.class);
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), returned = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean(), closerInterrupted = new AtomicBoolean();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final PaimonStopResources.Scope scope;
        final TableCommitImpl commit;
        final PaimonNativeCommitterClose close;
        Thread closer;

        Fixture(boolean startStop) throws Exception {
            PaimonStopController controller = new PaimonStopController("native-close", 1, 1, 1);
            scope = new PaimonStopResources().scope("native-close", controller);
            commit = nativeCommit(raw, () -> {
                entered.countDown();
                try { if (!release.await(15, TimeUnit.SECONDS)) { throw new AssertionError("维护屏障超时"); } }
                catch (InterruptedException e) { interrupted.set(true); Thread.currentThread().interrupt(); }
            }, ExpireExecutionMode.ASYNC);
            close = new PaimonNativeCommitterClose(commit, scope);
            commit.commitMultiple(Collections.singletonList(new ManifestCommittable(1L)), false);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            if (startStop) { controller.start(); }
        }

        void startClose() {
            closer = new Thread(() -> {
                try { close.close(); } catch (Throwable t) { failure.set(t); }
                finally { closerInterrupted.set(Thread.currentThread().isInterrupted()); returned.countDown(); }
            }, "native-close-test");
            closer.start();
        }

        @Override public void close() throws Exception {
            release.countDown();
            if (closer != null) { closer.join(5000); assertFalse(closer.isAlive()); }
            commit.getMaintainExecutor().shutdownNow();
            assertTrue(commit.getMaintainExecutor().awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
