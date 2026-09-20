package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.commit.PaimonAsyncCommitScheduler;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.write.*;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;
import io.tapdata.entity.logger.Log;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.BucketMode;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class PaimonBoundedStopTest {
    @Test void schedulerInterruptionMustBeRestoredOnlyOnCloseWorker() throws Exception {
        Fixture f = new Fixture(180, 120, 30);
        PaimonAsyncCommitScheduler scheduler = mock(PaimonAsyncCommitScheduler.class);
        InterruptedException interruption = new InterruptedException("scheduler interrupted");
        when(scheduler.shutdownAndAwait(anyLong(), any())).thenThrow(interruption).thenReturn(true);
        set(f.service, "asyncCommitScheduler", scheduler);
        assertSame(interruption, assertThrows(InterruptedException.class, f.service::close));
        f.worker().join(3000); assertFalse(f.worker().isAlive());
        assertTrue(f.worker().isInterrupted()); assertFalse(Thread.currentThread().isInterrupted());
        assertFalse(f.control().isRetained()); verify(scheduler, times(2)).shutdownAndAwait(anyLong(), any());
    }

    @Test void timeoutMustLogImmutableDiagnosticsAndProcessExitInstructionOnce() throws Exception {
        java.util.List<String> events = new java.util.concurrent.CopyOnWriteArrayList<>();
        Log log = mock(Log.class, invocation -> {
            if (invocation.getMethod().getName().equals("info")) { events.add(invocation.getArgument(0)); }
            return org.mockito.Mockito.RETURNS_DEFAULTS.answer(invocation);
        });
        Fixture f = new Fixture(1, 1, 1, log);
        PaimonAsyncCommitScheduler scheduler = mock(PaimonAsyncCommitScheduler.class);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        when(scheduler.shutdownAndAwait(anyLong(), any())).thenAnswer(i -> { entered.countDown(); await(release); return true; });
        set(f.service, "asyncCommitScheduler", scheduler); f.start();
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS)); f.advance(1); f.joinFailedRetained();
            assertThrows(Exception.class, f.service::close);
            awaitCondition(() -> events.stream().anyMatch(e -> e.contains("event=finished")));
            assertEquals(1, events.stream().filter(e -> e.contains("event=finished")).count());
            assertEquals(1, events.stream().filter(e -> e.contains("event=timeout-retained")).count());
            String retained = events.stream().filter(e -> e.contains("event=timeout-retained")).findFirst().get();
            for (String field : new String[] {"attempt=none", "remainingMs=0", "cancelRequested=false",
                    "executorTerminated=unknown", "prepareReturned=false", "inFlightAction="}) {
                assertTrue(retained.contains(field), retained);
            }
            assertTrue(retained.contains("需要确认旧进程退出"));
            assertFalse(events.stream().anyMatch(e -> e.contains("正常退出")));
        } finally { release.countDown(); f.worker().join(3000); }
    }

    @Test void blockedSchedulerMustConsumeTotalBudgetWithoutEnteringResourceCleanup() throws Exception {
        Fixture f = new Fixture(1, 1, 1); Table t = f.table();
        PaimonAsyncCommitScheduler scheduler = mock(PaimonAsyncCommitScheduler.class);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        doAnswer(i -> { entered.countDown(); await(release); return true; })
                .when(scheduler).shutdownAndAwait(anyLong(), any());
        set(f.service, "asyncCommitScheduler", scheduler);
        f.start();
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS)); f.advance(1); f.joinFailedRetained();
            release.countDown(); f.worker().join(3000);
            verify(t.writer, never()).prepareCommit(anyLong()); verify(t.io, never()).close();
            assertSame(f.service, PaimonStopResources.retainedRoot(f.control()));
        } finally { release.countDown(); t.lifecycle.shutdown(); assertTrue(t.lifecycle.awaitTermination(TimeUnit.SECONDS.toNanos(3))); }
    }

    @Test void blockedBusinessPrepareMustNotStartCommitOrFinalPrepareAfterDeadline() throws Exception {
        Fixture f = new Fixture(1, 1, 1); Table t = f.table();
        ((io.tapdata.connector.paimon.commit.PaimonMicroBatchCoordinator) get(f.service, "microBatchCoordinator"))
                .acceptInitial("default.a", 1);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        when(t.writer.prepareCommit(anyLong())).thenAnswer(i -> {
            entered.countDown(); await(release); return Collections.singletonList(compactionMessage());
        });
        f.start();
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS)); f.advance(1); f.joinFailedRetained();
            release.countDown(); f.worker().join(3000);
            verify(t.committer, never()).commit(anyLong(), anyList());
            verify(t.writer, never()).prepareFinalCommit(anyLong()); verify(t.io, never()).close();
        } finally { release.countDown(); t.lifecycle.shutdown(); assertTrue(t.lifecycle.awaitTermination(TimeUnit.SECONDS.toNanos(3))); }
    }

    @Test void admittedCallbackMustRemainUnconfirmedAfterTotalDeadlineAndLateReturn() throws Exception {
        Fixture f = new Fixture(1, 1, 1);
        Catalog catalog = mock(Catalog.class); set(f.service, "catalog", catalog);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        set(f.service, "flushOffsetCallback", (java.util.function.Consumer<Object>)
                value -> { entered.countDown(); await(release); });
        io.tapdata.entity.event.control.HeartbeatEvent heartbeat = new io.tapdata.entity.event.control.HeartbeatEvent().init().referenceTime(100L);
        heartbeat.addInfo("syncStage", "CDC"); heartbeat.addInfo("streamOffset", "offset");
        heartbeat.addInfo("sourceTime", 100L); heartbeat.addInfo("nodeIds", Collections.singletonList("source"));
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        Thread callback = new Thread(() -> { try { f.service.processHeartbeat(heartbeat); } catch (Throwable e) { callbackFailure.set(e); } });
        callback.setDaemon(true); callback.start();
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS)); f.start();
            awaitCondition(() -> { try { return f.control().isStarted(); } catch (Exception e) { throw new RuntimeException(e); } });
            f.advance(1); f.joinFailedRetained();
            release.countDown(); callback.join(3000); f.worker().join(3000);
            assertFalse(callback.isAlive()); assertNotNull(callbackFailure.get());
            io.tapdata.connector.paimon.commit.PaimonMicroBatchCoordinator coordinator =
                    (io.tapdata.connector.paimon.commit.PaimonMicroBatchCoordinator) get(f.service, "microBatchCoordinator");
            assertTrue(coordinator.hasInFlight("source"), "迟到 callback 不得发布确认或清除 reservation");
            verify(catalog, never()).close();
        } finally { release.countDown(); }
    }

    @Test void connectorStopFailureMustNotInvokeBlockingWarnAndMustKeepRetainedServiceReachable() throws Exception {
        Fixture f = new Fixture(1, 1, 1); Catalog catalog = mock(Catalog.class);
        set(f.service, "catalog", catalog);
        RuntimeException original = new RuntimeException("catalog close failed");
        doThrow(original).when(catalog).close();
        io.tapdata.connector.paimon.PaimonConnector connector = new io.tapdata.connector.paimon.PaimonConnector();
        set(connector, "paimonService", f.service);
        Log log = mock(Log.class);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(i -> { await(release); return null; }).when(log).warn(anyString());
        doAnswer(i -> { await(release); return null; }).when(log).info(anyString());
        io.tapdata.pdk.apis.context.TapConnectionContext connection = mock(io.tapdata.pdk.apis.context.TapConnectionContext.class);
        when(connection.getLog()).thenReturn(log);
        AtomicReference<Throwable> outcome = new AtomicReference<>();
        Thread caller = new Thread(() -> { try { connector.onStop(connection); } catch (Throwable e) { outcome.set(e); } });
        caller.setDaemon(true); caller.start();
        try {
            caller.join(3000); assertFalse(caller.isAlive()); assertSame(original, outcome.get());
            assertNull(get(connector, "paimonService"));
            assertSame(f.service, PaimonStopResources.retainedRoot(f.control()));
            verify(log, never()).warn(anyString());
        } finally { release.countDown(); }
    }

    @Test void catalogCloseTimeoutMustReturnRetainAndRefuseLatePublication() throws Exception {
        Fixture f = new Fixture(10, 5, 2);
        Catalog catalog = mock(Catalog.class);
        set(f.service, "catalog", catalog);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        doAnswer(i -> { entered.countDown(); await(release); return null; }).when(catalog).close();
        f.start();
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            f.advance(10);
            f.joinFailedRetained();
            assertSame(f.service, PaimonStopResources.retainedRoot(f.control()));
            Throwable terminal = f.error.get();
            assertSame(terminal, assertThrows(Exception.class, f.service::close));
            release.countDown();
            f.worker().join(3000);
            assertSame(catalog, get(f.service, "catalog"));
            assertSame(terminal, f.control().terminal().failure);
            verify(catalog, times(1)).close();
        } finally { release.countDown(); }
    }

    @Test void ignoredInterruptMustKeepSpillAndNeverCloseAfterLatePhysicalExit() throws Exception {
        Fixture f = new Fixture(10, 1, 2);
        Table t = f.table();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        Future<CompactResult> future = t.lifecycle.compactionExecutor().submit(task(entered, release, false));
        when(t.writer.prepareFinalCommit(anyLong())).thenAnswer(i -> { future.get(); return Collections.emptyList(); });
        assertTrue(entered.await(3, TimeUnit.SECONDS));
        f.start();
        try {
            f.awaitPhase("FINAL_PREPARE");
            f.advance(1);
            awaitCondition(future::isCancelled);
            assertFalse(t.lifecycle.isTerminated());
            f.advance(2);
            f.joinFailedRetained();
            verify(t.writer, never()).close();
            verify(t.io, never()).close();
            release.countDown();
            assertTrue(t.lifecycle.awaitTermination(TimeUnit.SECONDS.toNanos(3)));
            f.worker().join(3000);
            verify(t.writer, never()).close();
            verify(t.committer, never()).commit(anyLong(), anyList());
            verify(t.io, never()).close();
        } finally { release.countDown(); }
    }

    @Test void cooperativeCancellationMustDiscardFinalAttemptAndCloseOnlyAfterExit() throws Exception {
        Fixture f = new Fixture(10, 1, 2);
        Table t = f.table();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        Future<CompactResult> future = t.lifecycle.compactionExecutor().submit(task(entered, release, true));
        when(t.writer.prepareFinalCommit(anyLong())).thenAnswer(i -> { future.get(); return Collections.emptyList(); });
        doAnswer(i -> { assertTrue(t.lifecycle.isTerminated()); return null; }).when(t.io).close();
        assertTrue(entered.await(3, TimeUnit.SECONDS));
        f.start();
        try {
            f.awaitPhase("FINAL_PREPARE");
            f.advance(1);
            f.caller.join(3000);
            assertFalse(f.caller.isAlive());
            assertNull(f.error.get());
            assertEquals(1, f.control().terminal().discardedTables);
            verify(t.committer, never()).commit(anyLong(), anyList());
            verify(t.writer).close();
            verify(t.io).close();
            assertTrue(t.context.cleanupComplete());
        } finally { release.countDown(); }
    }

    @Test void insufficientFinalBudgetMustSkipPrepareAndCancelExistingTask() throws Exception {
        Fixture f = new Fixture(1, 1, 2);
        Table t = f.table();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        t.lifecycle.compactionExecutor().submit(task(entered, release, true));
        assertTrue(entered.await(3, TimeUnit.SECONDS));
        f.start();
        try {
            f.caller.join(3000);
            assertFalse(f.caller.isAlive());
            assertNull(f.error.get());
            verify(t.writer, never()).prepareFinalCommit(anyLong());
            verify(t.io).close();
            assertEquals(1, f.control().terminal().discardedTables);
        } finally { release.countDown(); }
    }

    @Test void allocationReturningAfterTimeoutMustBindToRetainedLedgerAndRejectNextAction() throws Exception {
        Fixture f = new Fixture(1, 1, 1);
        PaimonStopResources.Scope scope = f.resources().scope("late constructor", f.control());
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        Object allocated = new Object();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread constructor = new Thread(() -> {
            try { scope.create("blocked allocate", () -> { entered.countDown(); await(release); return allocated; }); }
            catch (Throwable failure) { error.set(failure); }
        });
        constructor.setDaemon(true); constructor.start();
        assertTrue(entered.await(3, TimeUnit.SECONDS));
        f.start();
        try {
            f.caller.join(3000); // worker sees reserved resource without close proof and fails closed immediately
            assertTrue(f.control().isRetained());
            release.countDown(); constructor.join(3000);
            assertInstanceOf(PaimonStopController.FrozenException.class, error.get());
            assertTrue(scope.hasResources());
            assertThrows(PaimonStopController.FrozenException.class, () -> scope.run("late close", () -> fail("must not execute")));
        } finally { release.countDown(); }
    }

    @Test void blockedLogBackendAndFullQueueMustNotBlockStopOrSpawnPerServiceThreads() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        Log blocked = mock(Log.class);
        doAnswer(i -> { entered.countDown(); await(release); return null; }).when(blocked).info(anyString());
        PaimonStopLog.offer(blocked, "block backend", null);
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            long before = PaimonStopLog.droppedCount();
            for (int i = 0; i < 300; i++) { PaimonStopLog.offer(mock(Log.class), "queued", null); }
            Fixture f = new Fixture(1, 1, 1);
            f.start(); f.caller.join(1000);
            assertFalse(f.caller.isAlive()); assertNull(f.error.get());
            assertTrue(PaimonStopLog.droppedCount() > before);
            assertTrue(PaimonStopLog.queuedCount() <= 256);
            assertNotNull(f.control().terminal());
        } finally { release.countDown(); }
    }

    @Test void admittedFinalCommitMustKeepExactPendingAndRefuseLateStateSaveOrRetry() throws Exception {
        Fixture f = new Fixture(10, 1, 2);
        Table t = f.table();
        org.apache.paimon.table.sink.CommitMessage message = compactionMessage();
        java.util.List<org.apache.paimon.table.sink.CommitMessage> messages = Collections.singletonList(message);
        when(t.writer.prepareFinalCommit(anyLong())).thenReturn(messages);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        doAnswer(i -> { entered.countDown(); await(release); throw new RuntimeException("late response lost"); })
                .when(t.committer).commit(0, messages);
        f.start();
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS));
            f.advance(2); Thread.sleep(80);
            assertFalse(f.control().isRetained(), "final commit 已准入，normal timer 不能再取消");
            assertNull(f.control().pollCancellation());
            f.advance(8); f.joinFailedRetained();
            release.countDown(); f.worker().join(3000);
            assertTrue(t.context.hasPendingCommit());
            verify(t.committer, times(1)).commit(0, messages);
            verify(t.committer, never()).filterAndCommit(anyMap());
            verify(t.writer, never()).close(); verify(t.io, never()).close();
            assertThrows(Exception.class, t.context::retryPendingCommit);
        } finally { release.countDown(); }
    }

    @Test void cancellationWinningAfterNonEmptyPrepareMustNeverCommitMessages() throws Exception {
        Fixture f = new Fixture(10, 1, 2); Table t = f.table();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        when(t.writer.prepareFinalCommit(anyLong())).thenAnswer(i -> {
            entered.countDown(); await(release); return Collections.singletonList(compactionMessage());
        });
        f.start();
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS)); f.advance(1);
            awaitCondition(() -> t.lifecycle.compactionExecutor().isShutdown());
            release.countDown(); f.caller.join(3000);
            assertFalse(f.caller.isAlive()); assertNull(f.error.get());
            verify(t.committer, never()).commit(anyLong(), anyList());
            assertEquals(1, f.control().terminal().discardedTables);
        } finally { release.countDown(); }
    }

    private static org.apache.paimon.table.sink.CommitMessage compactionMessage() {
        return new org.apache.paimon.table.sink.CommitMessageImpl(org.apache.paimon.data.BinaryRow.EMPTY_ROW,
                0, 1, org.apache.paimon.io.DataIncrement.emptyIncrement(),
                new org.apache.paimon.io.CompactIncrement(Collections.singletonList(mock(org.apache.paimon.io.DataFileMeta.class)),
                        Collections.singletonList(mock(org.apache.paimon.io.DataFileMeta.class)), Collections.emptyList()));
    }

    private static CompactTask task(CountDownLatch entered, CountDownLatch release, boolean cooperative) {
        return new CompactTask(null) {
            @Override protected CompactResult doCompact() throws Exception {
                entered.countDown();
                if (cooperative) { release.await(); } else { await(release); }
                return new CompactResult(Collections.emptyList(), Collections.emptyList());
            }
        };
    }
    private static void await(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) { try { latch.await(); break; } catch (InterruptedException e) { interrupted = true; } }
        if (interrupted) { Thread.currentThread().interrupt(); }
    }
    private static void awaitCondition(java.util.function.BooleanSupplier condition) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < end) { Thread.sleep(5); }
        assertTrue(condition.getAsBoolean());
    }
    private static Object get(Object owner, String name) throws Exception {
        Field f = owner.getClass().getDeclaredField(name); f.setAccessible(true); return f.get(owner);
    }
    private static void set(Object owner, String name, Object value) throws Exception {
        Field f = owner.getClass().getDeclaredField(name); f.setAccessible(true); f.set(owner, value);
    }
    static class Fixture {
        final AtomicLong nanos = new AtomicLong();
        final PaimonService service;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        Thread caller;
        Fixture(int total, int normal, int grace) { this(total, normal, grace, mock(Log.class)); }
        Fixture(int total, int normal, int grace, Log log) {
            PaimonConfig config = new PaimonConfig(); config.setDatabase("default");
            config.setEnableAsyncCommit(false); config.setStopTimeoutSeconds(total);
            config.setFinalCompactionTimeoutSeconds(normal); config.setCompactionCancelGraceSeconds(grace);
            service = new PaimonService(config, log, () -> 100L, () -> {},
                    PaimonAsyncCommitScheduler::newDaemonExecutor, nanos::get);
            service.startForTest();
        }
        void start() { caller = new Thread(() -> { try { service.close(); } catch (Throwable failure) { error.set(failure); } }); caller.setDaemon(true); caller.start(); }
        void advance(long seconds) { nanos.addAndGet(TimeUnit.SECONDS.toNanos(seconds)); }
        PaimonStopController control() throws Exception { return (PaimonStopController) get(service, "stopController"); }
        PaimonStopResources resources() throws Exception { return (PaimonStopResources) get(service, "stopResources"); }
        Thread worker() throws Exception { return control().worker; }
        void awaitPhase(String phase) throws Exception { PaimonStopController c = control(); awaitCondition(() -> c.progress.phase.equals(phase)); }
        void joinFailedRetained() throws Exception {
            caller.join(3000); assertFalse(caller.isAlive()); assertNotNull(error.get()); assertTrue(control().isRetained());
        }
        @SuppressWarnings("unchecked") Table table() throws Exception {
            Table t = new Table();
            PaimonStopResources.Scope scope = resources().scope("test writer", control());
            scope.reserve("test context").bind(t.context); t.context.attachStopScope(scope);
            ((Map<String, PaimonTableWriteContext>) get(service, "tableWriteContexts")).put("default.a", t.context);
            return t;
        }
    }
    private static class Table {
        final PaimonBucketWriterStrategy writer = mock(PaimonBucketWriterStrategy.class);
        final PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        final IOManager io = mock(IOManager.class);
        final PaimonCompactionLifecycle lifecycle = PaimonCompactionLifecycle.forTable("default.a");
        final PaimonTableWriteContext context;
        Table() throws Exception {
            when(writer.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
            when(writer.prepareCommit(anyLong())).thenReturn(Collections.emptyList());
            when(writer.prepareFinalCommit(anyLong())).thenReturn(Collections.emptyList());
            context = new PaimonTableWriteContext("default.a", "a", "user", writer, committer, io,
                    Collections.emptyList(), 0, PaimonTableWriteContext.CommitStateStore.NOOP, lifecycle);
        }
    }
}
