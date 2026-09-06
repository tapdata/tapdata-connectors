package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.BucketMode;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** 同步关闭只接受实际 termination；业务终态与资源清理证明分别校验。 */
class PaimonCompactionLifecycleTest {

    @Test
    void gracefulShutdownMustWaitForActualTaskWithoutInterruptingIt() throws Exception {
        try (BlockedLifecycle fixture = new BlockedLifecycle("graceful")) {
            AtomicReference<InterruptedException> interruption = new AtomicReference<>();
            fixture.startWaiter(() -> interruption.set(fixture.lifecycle.shutdownAndAwaitCompletion()));
            fixture.awaitShutdown();
            assertFalse(fixture.returned.await(100L, TimeUnit.MILLISECONDS));
            assertFalse(fixture.lifecycle.compactionExecutor().isTerminated());
            assertFalse(fixture.workerInterrupted.get());
            fixture.release.countDown();
            assertTrue(fixture.returned.await(5L, TimeUnit.SECONDS));
            assertNull(interruption.get());
            assertNull(fixture.waiterFailure.get());
            assertTrue(fixture.lifecycle.compactionExecutor().isTerminated());
            assertDoesNotThrow(fixture.lifecycle::auditControlFailure);
        }
    }

    @Test
    void interruptedTerminationWaitMustContinueUntilActualTaskExit() throws Exception {
        try (BlockedLifecycle fixture = new BlockedLifecycle("interrupt")) {
            AtomicReference<InterruptedException> interruption = new AtomicReference<>();
            fixture.startWaiter(() -> interruption.set(fixture.lifecycle.shutdownAndAwaitCompletion()));
            fixture.awaitShutdown();
            fixture.waiter.interrupt();
            assertFalse(fixture.returned.await(100L, TimeUnit.MILLISECONDS),
                    "等待者中断不得使运行中的 CompactTask 提前逃逸");
            assertFalse(fixture.workerInterrupted.get());
            fixture.release.countDown();
            assertTrue(fixture.returned.await(5L, TimeUnit.SECONDS));
            assertNotNull(interruption.get(), "把首次中断交还 Context 作为硬错误");
            assertNull(fixture.waiterFailure.get());
            assertTrue(fixture.lifecycle.compactionExecutor().isTerminated());
        }
    }

    @Test
    void contextCloseMustKeepAllResourcesOpenUntilCompactionTerminates() throws Exception {
        try (BlockedLifecycle fixture = new BlockedLifecycle("context")) {
            PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
            PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
            IOManager io = mock(IOManager.class);
            PaimonTableWriteContext context = context(fixture.lifecycle, strategy, committer, io);
            fixture.startWaiter(context::close);
            fixture.awaitShutdown();
            assertFalse(fixture.returned.await(100L, TimeUnit.MILLISECONDS));
            assertFalse(context.cleanupComplete());
            verify(strategy, never()).close();
            verify(committer, never()).close();
            verify(io, never()).close();
            fixture.release.countDown();
            assertTrue(fixture.returned.await(5L, TimeUnit.SECONDS));
            assertNull(fixture.waiterFailure.get());
            assertTrue(context.cleanupComplete());
            assertFalse(fixture.workerInterrupted.get());
            context.close();
            verify(io, times(1)).close();
        }
    }

    @Test
    void contextCloseMustRunStrategyThenCommitterThenIoExactlyOnceAfterProof() throws Exception {
        PaimonCompactionLifecycle lifecycle = PaimonCompactionLifecycle.forTable("default.order");
        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        IOManager io = mock(IOManager.class);
        PaimonTableWriteContext context = context(lifecycle, strategy, committer, io);
        try {
            context.close();
            context.close();
            InOrder order = inOrder(strategy, committer, io);
            order.verify(strategy).close();
            order.verify(committer).close();
            order.verify(io).close();
            order.verifyNoMoreInteractions();
            assertTrue(context.cleanupComplete());
            assertTrue(lifecycle.compactionExecutor().isTerminated());
        } finally {
            shutdownAndAssertTerminated(lifecycle);
        }
    }

    @Test
    void writerCloseFailureMustKeepIoOpenAndResourceProofFalse() throws Exception {
        PaimonCompactionLifecycle lifecycle = PaimonCompactionLifecycle.forTable("default.writer-error");
        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        IOManager io = mock(IOManager.class);
        IOException writerError = new IOException("writer close failed");
        doThrow(writerError).when(strategy).close();
        PaimonTableWriteContext context = context(lifecycle, strategy, committer, io);
        try {
            // 原生 close 可能在首个失败 bucket 短路；executor termination 不能代替全部
            // writer 的资源关闭证明，此时 IOManager 仍不得删除 Spill 目录。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L304
            assertSame(writerError, assertThrows(IOException.class, context::close));
            assertSame(writerError, assertThrows(IOException.class, context::close));
            verify(strategy, times(1)).close();
            verify(committer, times(1)).close();
            verify(io, never()).close();
            assertTrue(lifecycle.compactionExecutor().isTerminated());
            assertFalse(context.cleanupComplete());
        } finally {
            shutdownAndAssertTerminated(lifecycle);
        }
    }

    @Test
    void committerCloseFailureMustKeepIoOpenAndResourceProofFalse() throws Exception {
        PaimonCompactionLifecycle lifecycle = PaimonCompactionLifecycle.forTable("default.committer-error");
        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        IOManager io = mock(IOManager.class);
        IOException error = new IOException("committer close failed");
        doThrow(error).when(committer).close();
        PaimonTableWriteContext context = context(lifecycle, strategy, committer, io);
        try {
            assertSame(error, assertThrows(IOException.class, context::close));
            verify(strategy).close();
            verify(io, never()).close();
            assertFalse(context.cleanupComplete());
        } finally {
            shutdownAndAssertTerminated(lifecycle);
        }
    }

    @Test
    void ioCloseFailureMustRemainTerminalWithoutPublishingCompleteCleanup() throws Exception {
        PaimonCompactionLifecycle lifecycle = PaimonCompactionLifecycle.forTable("default.io-error");
        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        IOManager io = mock(IOManager.class);
        IOException error = new IOException("IOManager directory deletion failed");
        doThrow(error).when(io).close();
        PaimonTableWriteContext context = context(lifecycle, strategy, committer, io);
        try {
            assertSame(error, assertThrows(IOException.class, context::close));
            assertSame(error, assertThrows(IOException.class, context::close));
            verify(strategy, times(1)).close();
            verify(committer, times(1)).close();
            verify(io, times(1)).close();
            assertFalse(context.cleanupComplete());
            assertTrue(lifecycle.compactionExecutor().isTerminated());
        } finally {
            shutdownAndAssertTerminated(lifecycle);
        }
    }

    @Test
    void interruptedContextCloseMustFinishCleanupThenRestoreCallerInterruptAndFailure() throws Exception {
        try (BlockedLifecycle fixture = new BlockedLifecycle("context-interrupt")) {
            PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
            PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
            IOManager io = mock(IOManager.class);
            PaimonTableWriteContext context = context(fixture.lifecycle, strategy, committer, io);
            fixture.startWaiter(context::close);
            fixture.awaitShutdown();
            fixture.waiter.interrupt();
            assertFalse(fixture.returned.await(100L, TimeUnit.MILLISECONDS));
            verify(io, never()).close();
            fixture.release.countDown();
            assertTrue(fixture.returned.await(5L, TimeUnit.SECONDS));
            assertInstanceOf(InterruptedException.class, fixture.waiterFailure.get());
            assertTrue(fixture.waiterInterruptRestored.get());
            assertTrue(context.cleanupComplete(), "硬错误与完整资源证明彼此独立");
            assertSame(fixture.waiterFailure.get(), assertThrows(InterruptedException.class, context::close));
            verify(io, times(1)).close();
        }
    }

    @Test
    void missingLifecycleWithIoManagerMustFailClosed() throws Exception {
        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        IOManager io = mock(IOManager.class);
        PaimonTableWriteContext context = context(null, strategy, committer, io);
        IllegalStateException failure = assertThrows(IllegalStateException.class, context::close);
        assertTrue(failure.getMessage().contains("spill barrier is missing"));
        assertFalse(context.cleanupComplete());
        verify(strategy, never()).close();
        verify(committer, never()).close();
        verify(io, never()).close();
    }

    @Test
    void progressMustBeEmittedOnceAndObserverFailureCannotBreakCleanup() throws Exception {
        PaimonCompactionLifecycle lifecycle = PaimonCompactionLifecycle.forTable("default.progress");
        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        IOManager io = mock(IOManager.class);
        PaimonTableWriteContext context = context(lifecycle, strategy, committer, io);
        List<String> phases = new ArrayList<>();
        try {
            assertEquals(PaimonTableWriteContext.StopOutcome.SUCCESS,
                    context.closeForStop(false, phase -> {
                        phases.add(phase);
                        throw new IllegalStateException("INFO observer failure");
                    }));
            context.closeForStop(false, phases::add);
            assertEquals(Arrays.asList("WAIT_COMPACTION", "CLOSE_WRITER",
                    "CLOSE_COMMITTER", "CLOSE_SPILL"), phases);
            assertTrue(context.cleanupComplete());
            verify(io, times(1)).close();
        } finally {
            shutdownAndAssertTerminated(lifecycle);
        }
    }

    @Test
    void explicitNoAsyncTestLifecycleCompletesWithoutCreatingAnExecutor() {
        PaimonCompactionLifecycle lifecycle = PaimonCompactionLifecycle.withoutCompactionExecutor();
        assertNull(lifecycle.shutdownAndAwaitCompletion());
        assertDoesNotThrow(lifecycle::sealFinalPrepare);
        assertDoesNotThrow(lifecycle::auditControlFailure);
    }

    @FunctionalInterface
    private interface CloseAction { void run() throws Exception; }

    private static final class BlockedLifecycle implements AutoCloseable {
        final PaimonCompactionLifecycle lifecycle;
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch returned = new CountDownLatch(1);
        final AtomicBoolean workerInterrupted = new AtomicBoolean();
        final AtomicBoolean waiterInterruptRestored = new AtomicBoolean();
        final AtomicReference<Throwable> waiterFailure = new AtomicReference<>();
        Thread waiter;

        BlockedLifecycle(String name) throws Exception {
            lifecycle = PaimonCompactionLifecycle.forTable("default." + name);
            lifecycle.compactionExecutor().submit(new CompactTask(null) {
                @Override
                protected CompactResult doCompact() {
                    entered.countDown();
                    while (true) {
                        try { release.await(); break; }
                        catch (InterruptedException ignored) { workerInterrupted.set(true); }
                    }
                    return new CompactResult();
                }
            });
            if (!entered.await(5L, TimeUnit.SECONDS)) {
                close();
                throw new AssertionError("真实 CompactTask 未开始");
            }
        }

        void startWaiter(CloseAction action) {
            waiter = new Thread(() -> {
                try { action.run(); }
                catch (Throwable failure) { waiterFailure.set(failure); }
                finally {
                    waiterInterruptRestored.set(Thread.currentThread().isInterrupted());
                    returned.countDown();
                }
            }, "lifecycle-test-close");
            waiter.setDaemon(true);
            waiter.start();
        }

        void awaitShutdown() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (!lifecycle.compactionExecutor().isShutdown() && System.nanoTime() < deadline) {
                Thread.sleep(1L);
            }
            assertTrue(lifecycle.compactionExecutor().isShutdown());
        }

        @Override
        public void close() throws Exception {
            release.countDown();
            try {
                if (waiter != null) {
                    waiter.join(TimeUnit.SECONDS.toMillis(5L));
                    assertFalse(waiter.isAlive(), "放行 latch 后关闭线程必须退出");
                }
            } finally {
                shutdownAndAssertTerminated(lifecycle);
            }
        }
    }

    private static void shutdownAndAssertTerminated(PaimonCompactionLifecycle lifecycle)
            throws InterruptedException {
        lifecycle.compactionExecutor().shutdown();
        assertTrue(lifecycle.compactionExecutor().awaitTermination(5L, TimeUnit.SECONDS));
    }

    private static PaimonTableWriteContext context(PaimonCompactionLifecycle lifecycle,
            PaimonBucketWriterStrategy strategy, PaimonTableCommitter committer, IOManager io) {
        when(strategy.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        return new PaimonTableWriteContext("default.t", "t", "user", strategy, committer, io,
                Collections.emptyList(), 0L, PaimonTableWriteContext.CommitStateStore.NOOP,
                lifecycle, PaimonNativeWriteAccess.withoutNativeWriters());
    }
}
