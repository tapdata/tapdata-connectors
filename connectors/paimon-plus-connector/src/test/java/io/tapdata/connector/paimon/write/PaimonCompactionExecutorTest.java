package io.tapdata.connector.paimon.write;

import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class PaimonCompactionExecutorTest {
    @Test
    void workerInterruptMustRemainHardEvenWhenTaskThrowsAnOrdinaryException() throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("interrupt-with-io");
        try {
            Future<?> future = executor.submit(task(() -> {
                Thread.currentThread().interrupt();
                throw new IOException("ordinary I/O after interruption");
            }));
            assertThrows(ExecutionException.class, future::get);
            assertThrows(IllegalStateException.class, executor::audit);
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void suppressedFatalErrorAndUnthrownWorkerInterruptionMustRemainHardFailures() throws Exception {
        PaimonCompactionExecutor suppressed = new PaimonCompactionExecutor("suppressed-fatal");
        try {
            Future<?> future = suppressed.submit(task(() -> {
                IOException ordinary = new IOException("primary I/O");
                ordinary.addSuppressed(new OutOfMemoryError("fatal cleanup"));
                throw ordinary;
            }));
            ExecutionException failure = assertThrows(ExecutionException.class, future::get);
            assertFalse(PaimonCompactionExecutor.isNativeCompactionFailure(failure));
            assertThrows(IllegalStateException.class, suppressed::audit);
        } finally {
            suppressed.shutdown();
            assertTrue(suppressed.awaitTermination(5, TimeUnit.SECONDS));
        }
        PaimonCompactionExecutor interrupted = new PaimonCompactionExecutor("self-interrupt");
        try {
            Future<?> future = interrupted.submit(task(() -> {
                Thread.currentThread().interrupt();
                return emptyResult();
            }));
            assertThrows(ExecutionException.class, future::get);
            assertThrows(IllegalStateException.class, interrupted::audit);
        } finally {
            interrupted.shutdown();
            assertTrue(interrupted.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
    @Test
    void ordinaryNativeFailureMustKeepCauseAndRemainEligibleOnlyThroughExecutionException() throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("ordinary");
        IOException cause = new IOException("spill failed");
        try {
            Future<CompactResult> future = executor.submit(task(() -> { throw cause; }));
            ExecutionException failure = assertThrows(ExecutionException.class, future::get);
            assertTrue(PaimonCompactionExecutor.isNativeCompactionFailure(failure));
            assertSame(cause, failure.getCause().getCause());
            assertFalse(PaimonCompactionExecutor.isNativeCompactionFailure(cause));
            executor.auditAndSeal();
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void cancellationMustSurviveNativeFutureConsumptionWithoutProvingWorkerExit() throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("cancel");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean exited = new AtomicBoolean();
        try {
            Future<CompactResult> future = executor.submit(task(() -> {
                entered.countDown();
                awaitIgnoringInterrupt(release);
                exited.set(true);
                return emptyResult();
            }));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertTrue(future.cancel(true));
            assertThrows(CancellationException.class, future::get);
            assertThrows(IllegalStateException.class, executor::auditAndSeal);
            executor.shutdown();
            assertFalse(executor.awaitTermination(50, TimeUnit.MILLISECONDS));
            assertFalse(exited.get());
        } finally {
            release.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
        assertTrue(exited.get());
        assertThrows(IllegalStateException.class, executor::audit);
    }

    @Test
    void completedCancelMustBeHarmlessButSealedLiveCancelMustFailClosed() throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("seal");
        CountDownLatch release = new CountDownLatch(1);
        try {
            Future<CompactResult> completed = executor.submit(task(PaimonCompactionExecutorTest::emptyResult));
            completed.get();
            assertFalse(completed.cancel(true));
            Future<CompactResult> running = executor.submit(task(() -> {
                release.await();
                return emptyResult();
            }));
            executor.auditAndSeal();
            assertFalse(running.cancel(true));
            assertFalse(running.isCancelled());
            assertThrows(IllegalStateException.class, executor::audit);
        } finally {
            release.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void fatalWrappedCauseAndUnknownTasksMustNeverQualifyAsOrdinaryCompactionFailure() throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("fatal");
        try {
            Future<CompactResult> future = executor.submit(task(() -> {
                throw new IOException("wrapped interruption", new InterruptedException("stop"));
            }));
            ExecutionException failure = assertThrows(ExecutionException.class, future::get);
            assertFalse(PaimonCompactionExecutor.isNativeCompactionFailure(failure));
            assertThrows(IllegalStateException.class, executor::auditAndSeal);
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
        PaimonCompactionExecutor unknown = new PaimonCompactionExecutor("unknown");
        try {
            assertThrows(IllegalArgumentException.class, () -> unknown.submit(() -> "unverified"));
            assertThrows(IllegalStateException.class, unknown::audit);
        } finally {
            unknown.shutdown();
            assertTrue(unknown.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void errorHiddenInCheckedExceptionCauseMustRemainAHardControlFailure() throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("wrapped-error");
        AssertionError fatalCause = new AssertionError("SS04 内核不可恢复错误");
        IOException wrapper = new IOException("SS04 包装后的任务错误", fatalCause);
        try {
            Future<CompactResult> future = executor.submit(task(() -> { throw wrapper; }));
            ExecutionException failure = assertThrows(ExecutionException.class, future::get);
            assertSame(wrapper, failure.getCause(), "控制错误不能被重新标记成普通 Compaction 错误");
            assertSame(fatalCause, failure.getCause().getCause());
            assertFalse(PaimonCompactionExecutor.isNativeCompactionFailure(failure));
            IllegalStateException auditFailure = assertThrows(
                    IllegalStateException.class, executor::auditAndSeal);
            assertSame(wrapper, auditFailure.getCause());
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void directErrorMustRemainAHardControlFailureAfterFutureConsumption() throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("direct-error");
        AssertionError fatal = new AssertionError("SS04 直接任务 Error");
        try {
            Future<CompactResult> future = executor.submit(task(() -> { throw fatal; }));
            ExecutionException failure = assertThrows(ExecutionException.class, future::get);
            assertSame(fatal, failure.getCause());
            assertFalse(PaimonCompactionExecutor.isNativeCompactionFailure(failure));
            assertSame(fatal, assertThrows(IllegalStateException.class, executor::audit).getCause());
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void rejectedSubmissionAfterShutdownMustBeRecordedBeforeItEscapes() throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("shutdown-rejection");
        AtomicBoolean taskRan = new AtomicBoolean();
        try {
            executor.shutdown();
            RejectedExecutionException rejection = assertThrows(
                    RejectedExecutionException.class,
                    () -> executor.submit(task(() -> {
                        taskRan.set(true);
                        return emptyResult();
                    })));
            assertFalse(taskRan.get());
            assertSame(rejection,
                    assertThrows(IllegalStateException.class, executor::auditAndSeal).getCause());
            assertFalse(PaimonCompactionExecutor.isNativeCompactionFailure(
                    new ExecutionException(rejection)));
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void rejectedSubmissionAfterFinalPrepareSealMustRemainObservable() throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("sealed-rejection");
        try {
            executor.auditAndSeal();
            RejectedExecutionException rejection = assertThrows(
                    RejectedExecutionException.class,
                    () -> executor.submit(task(PaimonCompactionExecutorTest::emptyResult)));
            assertSame(rejection,
                    assertThrows(IllegalStateException.class, executor::audit).getCause());
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void interruptedBlockingGetCallerMustRecordFailureWithoutCancellingWorker() throws Exception {
        assertGetCallerInterruption(false);
    }

    @Test
    void interruptedTimedGetCallerMustRecordFailureWithoutCancellingWorker() throws Exception {
        assertGetCallerInterruption(true);
    }

    @Test
    void completedFutureCancellationReturningFalseMustRemainHarmlessBeforeAndAfterSeal()
            throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("completed-cancel");
        try {
            Future<CompactResult> future = executor.submit(task(PaimonCompactionExecutorTest::emptyResult));
            future.get(5, TimeUnit.SECONDS);
            assertFalse(future.cancel(true));
            assertFalse(future.cancel(false));
            assertDoesNotThrow(executor::auditAndSeal);
            assertFalse(future.cancel(true));
            assertFalse(future.cancel(false));
            assertDoesNotThrow(executor::audit,
                    "原生 writer.close 对已完成 Future 的取消请求不能把成功停止改成失败");
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void cancellationMustAcquireControlLockBeforeChangingFutureState() throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("cancel-lock");
        CountDownLatch taskEntered = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch cancelEntered = new CountDownLatch(1);
        AtomicReference<Boolean> cancelled = new AtomicReference<>();
        Thread cancelling = null;
        try {
            Future<CompactResult> future = executor.submit(task(() -> {
                taskEntered.countDown();
                awaitIgnoringInterrupt(releaseTask);
                return emptyResult();
            }));
            assertTrue(taskEntered.await(5, TimeUnit.SECONDS));
            cancelling = new Thread(() -> {
                cancelEntered.countDown();
                cancelled.set(future.cancel(false));
            }, "SS04-cancel-await-control-lock");
            synchronized (controlLock(executor)) {
                cancelling.start();
                assertTrue(cancelEntered.await(5, TimeUnit.SECONDS));
                awaitThreadState(cancelling, Thread.State.BLOCKED);
                assertFalse(future.isCancelled(), "未取得短锁前，不得先取消 Future 再补记控制失败");
                assertFalse(future.isDone());
            }
            joinAndAssertStopped(cancelling);
            assertEquals(Boolean.TRUE, cancelled.get());
            assertThrows(CancellationException.class, future::get);
            assertInstanceOf(CancellationException.class,
                    assertThrows(IllegalStateException.class, executor::auditAndSeal).getCause());
        } finally {
            releaseTask.countDown();
            try {
                joinAndAssertStopped(cancelling);
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void auditAndSealMustWaitForCancellationCriticalSectionAndObserveItsFailure() throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("cancel-audit-lock");
        CountDownLatch taskEntered = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch cancellationVisible = new CountDownLatch(1);
        CountDownLatch releaseCancellationLock = new CountDownLatch(1);
        CountDownLatch auditEntered = new CountDownLatch(1);
        CountDownLatch auditFinished = new CountDownLatch(1);
        AtomicReference<Boolean> cancelled = new AtomicReference<>();
        AtomicReference<Throwable> auditFailure = new AtomicReference<>();
        Thread cancelling = null;
        Thread auditing = null;
        try {
            Future<CompactResult> future = executor.submit(task(() -> {
                taskEntered.countDown();
                awaitIgnoringInterrupt(releaseTask);
                return emptyResult();
            }));
            assertTrue(taskEntered.await(5, TimeUnit.SECONDS));
            Object lock = controlLock(executor);
            cancelling = new Thread(() -> {
                synchronized (lock) {
                    cancelled.set(future.cancel(false));
                    cancellationVisible.countDown();
                    awaitIgnoringInterrupt(releaseCancellationLock);
                }
            }, "SS04-cancel-critical-section");
            cancelling.start();
            assertTrue(cancellationVisible.await(5, TimeUnit.SECONDS));
            assertEquals(Boolean.TRUE, cancelled.get());
            assertThrows(CancellationException.class, future::get);

            // Paimon 1.3.2 会吞掉 get 的 CancellationException；这里让取消已经对 get
            // 可见，同时保持真实 controlLock，以确定性证明提交前审计不能越过临界区。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java#L47-L61
            auditing = new Thread(() -> {
                auditEntered.countDown();
                try {
                    executor.auditAndSeal();
                } catch (Throwable failure) {
                    auditFailure.set(failure);
                } finally {
                    auditFinished.countDown();
                }
            }, "SS04-audit-after-cancellation");
            auditing.start();
            assertTrue(auditEntered.await(5, TimeUnit.SECONDS));
            awaitThreadState(auditing, Thread.State.BLOCKED);
            assertEquals(1L, auditFinished.getCount(), "同一短锁尚未释放，审计不得提前通过");

            releaseCancellationLock.countDown();
            joinAndAssertStopped(cancelling);
            assertTrue(auditFinished.await(5, TimeUnit.SECONDS));
            joinAndAssertStopped(auditing);
            assertInstanceOf(IllegalStateException.class, auditFailure.get());
            assertInstanceOf(CancellationException.class, auditFailure.get().getCause());
        } finally {
            releaseCancellationLock.countDown();
            releaseTask.countDown();
            try {
                try {
                    joinAndAssertStopped(cancelling);
                } finally {
                    joinAndAssertStopped(auditing);
                }
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    private static void assertGetCallerInterruption(boolean timed) throws Exception {
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("get-interrupted-" + timed);
        CountDownLatch taskEntered = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CountDownLatch waiterEntered = new CountDownLatch(1);
        CountDownLatch waiterFinished = new CountDownLatch(1);
        AtomicReference<Throwable> observed = new AtomicReference<>();
        Thread waiting = null;
        try {
            Future<CompactResult> future = executor.submit(task(() -> {
                taskEntered.countDown();
                awaitIgnoringInterrupt(releaseTask);
                return emptyResult();
            }));
            assertTrue(taskEntered.await(5, TimeUnit.SECONDS));
            waiting = new Thread(() -> {
                waiterEntered.countDown();
                try {
                    if (timed) {
                        future.get(1, TimeUnit.MINUTES);
                    } else {
                        future.get();
                    }
                } catch (Throwable failure) {
                    observed.set(failure);
                } finally {
                    waiterFinished.countDown();
                }
            }, "SS04-interrupted-future-get-" + timed);
            waiting.start();
            assertTrue(waiterEntered.await(5, TimeUnit.SECONDS));
            awaitThreadState(waiting, timed ? Thread.State.TIMED_WAITING : Thread.State.WAITING);
            waiting.interrupt();
            assertTrue(waiterFinished.await(5, TimeUnit.SECONDS));
            assertInstanceOf(InterruptedException.class, observed.get());
            assertFalse(future.isCancelled(), "等待调用者中断不能取消尚在执行的 Compaction");
            assertFalse(future.isDone(), "真实任务仍由 latch 阻塞");
            assertSame(observed.get(),
                    assertThrows(IllegalStateException.class, executor::auditAndSeal).getCause());
            releaseTask.countDown();
            assertNotNull(future.get(5, TimeUnit.SECONDS));
            assertSame(observed.get(),
                    assertThrows(IllegalStateException.class, executor::audit).getCause(),
                    "任务后来成功也不能覆盖等待调用者的首次控制失败");
        } finally {
            releaseTask.countDown();
            try {
                if (waiting != null) {
                    waiting.interrupt();
                    joinAndAssertStopped(waiting);
                }
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    private static Object controlLock(PaimonCompactionExecutor executor) throws Exception {
        // 仅测试读取短锁以固定线程交错；生产适配器不依赖反射，也不增加测试专用 hook。
        Field lock = PaimonCompactionExecutor.class.getDeclaredField("controlLock");
        lock.setAccessible(true);
        return lock.get(executor);
    }

    private static void awaitThreadState(Thread thread, Thread.State expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (thread.getState() != expected && thread.isAlive() && System.nanoTime() < deadline) {
            Thread.sleep(1L);
        }
        assertEquals(expected, thread.getState(), "线程没有停在预期同步边界：" + thread.getName());
    }

    private static void joinAndAssertStopped(Thread thread) throws InterruptedException {
        if (thread != null) {
            thread.join(TimeUnit.SECONDS.toMillis(5L));
            assertFalse(thread.isAlive(), "测试线程必须在 finally 释放后退出：" + thread.getName());
        }
    }

    static CompactTask task(java.util.concurrent.Callable<CompactResult> body) {
        return new CompactTask(null) {
            @Override
            protected CompactResult doCompact() throws Exception { return body.call(); }
        };
    }

    static CompactResult emptyResult() {
        return new CompactResult(java.util.Collections.emptyList(), java.util.Collections.emptyList());
    }

    static void awaitIgnoringInterrupt(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try { latch.await(); break; }
            catch (InterruptedException e) { interrupted = true; }
        }
        if (interrupted) { Thread.currentThread().interrupt(); }
    }
}
