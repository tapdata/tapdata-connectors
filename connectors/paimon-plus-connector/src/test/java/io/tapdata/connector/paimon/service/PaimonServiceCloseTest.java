package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.commit.PaimonAsyncCommitScheduler;
import io.tapdata.connector.paimon.commit.PaimonMicroBatchCoordinator;
import io.tapdata.connector.paimon.commit.PaimonServiceLifecycle;

import io.tapdata.connector.paimon.write.PaimonCompactionLifecycle;
import io.tapdata.connector.paimon.write.PaimonTableCommitter;
import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractTestFactory;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.TapCallbackOffset;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonServiceCloseTest {

    @Test
    void queryMustHoldIngressUntilCatalogAccessCompletesAndRejectAfterStop() throws Exception {
        PaimonService service = service();
        Catalog catalog = mock(Catalog.class);
        setCatalog(service, catalog);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> queryFailure = new AtomicReference<>();
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        org.apache.paimon.catalog.Identifier id = org.apache.paimon.catalog.Identifier.create("default", "orders");
        when(catalog.getTable(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
            entered.countDown();
            assertTrue(release.await(10L, TimeUnit.SECONDS));
            throw new Catalog.TableNotExistException(id);
        });
        Thread query = new Thread(() -> {
            try { service.queryByAdvanceFilter(new TapTable("orders"), null, rows -> { }, mock(Log.class)); }
            catch (Throwable e) { queryFailure.set(e); }
        });
        Thread closer = new Thread(() -> {
            try { service.close(); } catch (Throwable e) { closeFailure.set(e); }
        });
        query.start();
        try {
            assertTrue(entered.await(5L, TimeUnit.SECONDS));
            assertEquals(1, lifecycle(service).activeIngressCount());
            closer.start();
            awaitCloseWaitingForIngress(lifecycle(service), closer);
            verify(catalog, never()).close();
            assertThrows(IllegalStateException.class, () -> service.queryByAdvanceFilter(
                    new TapTable("orders"), null, rows -> { }, mock(Log.class)));
        } finally {
            release.countDown();
            query.join(5000L);
            if (closer.getState() == Thread.State.NEW) { closer.start(); }
            closer.join(5000L);
        }
        assertFalse(query.isAlive());
        assertFalse(closer.isAlive());
        assertNull(queryFailure.get());
        assertNull(closeFailure.get());
        verify(catalog).close();
        assertThrows(IllegalStateException.class, () -> service.queryByAdvanceFilter(
                new TapTable("orders"), null, rows -> { }, mock(Log.class)));
    }

    @Test
    void closeMustNotBlockIngressThatIsBindingTaskState() throws Exception {
        PaimonService service = service();
        PaimonServiceLifecycle lifecycle = lifecycle(service);
        TapConnectorContext connectorContext = connectorContext();
        CountDownLatch ingressEntered = new CountDownLatch(1);
        CountDownLatch attemptBind = new CountDownLatch(1);
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();

        Thread writer =
                new Thread(
                        () -> {
                            try (PaimonServiceLifecycle.Ingress ignored =
                                    lifecycle.enter("bind-task-state-test")) {
                                ingressEntered.countDown();
                                if (!attemptBind.await(5L, TimeUnit.SECONDS)) {
                                    throw new AssertionError("Timed out waiting to bind task state");
                                }
                                invokeBindTaskState(service, connectorContext);
                            } catch (Throwable failure) {
                                writerFailure.set(failure);
                            }
                        },
                        "paimon-bind-task-state-test");
        writer.setDaemon(true);
        writer.start();
        assertTrue(ingressEntered.await(5L, TimeUnit.SECONDS));

        Thread closer =
                new Thread(
                        () -> {
                            try {
                                service.close();
                            } catch (Throwable failure) {
                                closeFailure.set(failure);
                            }
                        },
                        "paimon-close-bind-task-state-test");
        closer.setDaemon(true);
        closer.start();
        awaitCloseWaitingForIngress(lifecycle, closer);

        attemptBind.countDown();
        writer.join(2_000L);
        closer.join(2_000L);

        assertFalse(writer.isAlive(), "Task-state binding remained blocked by close");
        assertFalse(closer.isAlive(), "Close remained blocked waiting for task-state binding");
        assertNull(writerFailure.get());
        assertNull(closeFailure.get());
    }

    @Test
    void catalogCloseFailureMustBeMaterialAndRemainIdempotent() throws Exception {
        PaimonService service = service();
        Catalog catalog = mock(Catalog.class);
        IOException failure = new IOException("catalog close failed");
        doThrow(failure).when(catalog).close();
        setCatalog(service, catalog);

        Exception thrown = assertThrows(Exception.class, service::close);
        Exception repeated = assertThrows(Exception.class, service::close);

        assertSame(failure, thrown);
        assertSame(failure, repeated);
        verify(catalog, times(1)).close();
    }

    @Test
    void closingNewServiceMustPreventLaterInitialization() throws Exception {
        PaimonService service =
                new PaimonService(config(), mock(Log.class), () -> 100L, () -> { });

        service.close();
        IllegalStateException rejected =
                assertThrows(IllegalStateException.class, service::init);

        assertTrue(rejected.getMessage().contains("cannot initialize"));
        assertEquals(PaimonServiceLifecycle.State.CLOSED, lifecycle(service).state());
    }

    @Test
    void normalCloseMustDrainInitialBufferAndRemainIdempotent() throws Exception {
        PaimonService service = service();
        TableFixture table = table(service, "a");
        coordinator(service).acceptInitial("default.a", 1);

        service.close();
        service.close();

        verify(table.strategy, times(1)).prepareCommit(0L);
        verify(table.strategy, times(1)).prepareFinalCommit(1L);
        verify(table.committer, times(1)).commit(anyLong(), anyList());
        verify(table.committer, never()).filterAndCommit(anyMap());
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
        assertEquals(PaimonServiceLifecycle.State.CLOSED, lifecycle(service).state());
    }

    @Test
    void oneDrainFailureMustStillDrainOtherTablesAndSuppressAllCallbacks() throws Exception {
        PaimonService service =
                new PaimonService(config(), mock(Log.class), () -> 100L, () -> { });
        AtomicInteger callbackCount = new AtomicInteger();
        service.setFlushOffsetCallback(ignored -> callbackCount.incrementAndGet());
        service.startForTest();
        TableFixture failing = table(service, "a");
        TableFixture healthy = table(service, "b");
        coordinator(service).acceptInitial("default.a", 1);
        coordinator(service).acceptInitial("default.b", 1);
        IOException first = new IOException("a-drain-failure");
        when(failing.strategy.prepareCommit(0L)).thenThrow(first);

        Exception thrown = assertThrows(Exception.class, service::close);
        Exception repeated = assertThrows(Exception.class, service::close);

        assertSame(first, thrown);
        assertSame(first, repeated);
        verify(healthy.strategy, times(1)).prepareCommit(0L);
        verify(healthy.committer, times(1)).commit(anyLong(), anyList());
        verify(healthy.committer, never()).filterAndCommit(anyMap());
        verify(failing.strategy, times(1)).close();
        verify(healthy.strategy, times(1)).close();
        verify(failing.strategy, never()).prepareFinalCommit(anyLong());
        verify(healthy.strategy, never()).prepareFinalCommit(anyLong());
        assertEquals(0, callbackCount.get());
    }

    @Test
    void allTableDrainMustReleaseOneHeartbeatOnlyAfterEveryDependencyCommits()
            throws Exception {
        PaimonConfig config = config();
        PaimonService service =
                new PaimonService(config, mock(Log.class), () -> 100L, () -> { });
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicReference<TapCallbackOffset> callbackPayload = new AtomicReference<>();
        service.setFlushOffsetCallback(
                payload -> {
                    callbackCount.incrementAndGet();
                    callbackPayload.set((TapCallbackOffset) payload);
                });
        service.startForTest();
        TableFixture tableA = table(service, "a");
        TableFixture tableB = table(service, "b");
        TapConnectorContext context = connectorContext();
        service.writeRecords(
                Collections.singletonList(cdcEvent("a", 1)), tapTable("a"), context);
        service.writeRecords(
                Collections.singletonList(cdcEvent("b", 2)), tapTable("b"), context);
        HeartbeatEvent heartbeat = heartbeat("offset-7", 123L, 456L);

        service.processHeartbeat(heartbeat);
        assertEquals(0, callbackCount.get());

        service.close();

        verify(tableA.strategy, times(1)).prepareCommit(0L);
        verify(tableB.strategy, times(1)).prepareCommit(0L);
        assertEquals(1, callbackCount.get());
        TapCallbackOffset payload = callbackPayload.get();
        assertEquals("offset-7", payload.get(TapCallbackOffset.KEY_STREAM_OFFSET));
        assertEquals(123L, payload.get(TapCallbackOffset.KEY_SOURCE_TIME));
        assertEquals(456L, payload.get(TapCallbackOffset.KEY_EVENT_TIME));
        assertEquals("CDC", payload.get(TapCallbackOffset.KEY_SYNC_STAGE));
        assertEquals(
                Collections.singletonList("source-a"),
                payload.get(TapCallbackOffset.KEY_NODE_IDS));
    }

    @Test
    void interruptedRetryOnCloseWorkerMustFinishCleanupWithoutInterruptingCaller()
            throws Exception {
        AtomicInteger waits = new AtomicInteger();
        PaimonService service =
                new PaimonService(
                        config(),
                        mock(Log.class),
                        () -> 100L,
                        () -> {
                            if (waits.getAndIncrement() == 0) {
                                throw new InterruptedException("close interrupted");
                            }
                        },
                        PaimonAsyncCommitScheduler::newDaemonExecutor);
        service.startForTest();
        TableFixture table = table(service, "a");
        coordinator(service).acceptInitial("default.a", 1);
        RuntimeException ambiguous = new RuntimeException("ambiguous commit");
        doThrow(new RuntimeException("direct outcome unknown"))
                .when(table.committer)
                .commit(anyLong(), anyList());
        when(table.committer.filterAndCommit(anyMap()))
                .thenThrow(ambiguous)
                .thenReturn(0);

        try {
            InterruptedException thrown =
                    assertThrows(InterruptedException.class, service::close);

            assertEquals("close interrupted", thrown.getMessage());
            java.lang.reflect.Field operationField = PaimonService.class.getDeclaredField("closeOperation");
            operationField.setAccessible(true);
            Thread worker = ((PaimonStopController) operationField.get(service)).worker;
            worker.join(3000);
            assertFalse(worker.isAlive());
            assertTrue(worker.isInterrupted(), "中断只恢复在实际 close worker 上");
            assertFalse(Thread.currentThread().isInterrupted());
            assertEquals(2, waits.get());
            verify(table.strategy, times(1)).prepareCommit(0L);
            verify(table.committer, times(1)).commit(anyLong(), anyList());
            verify(table.committer, times(2)).filterAndCommit(anyMap());
            verify(table.strategy, times(1)).close();
            verify(table.committer, times(1)).close();
            assertEquals(PaimonServiceLifecycle.State.CLOSED, lifecycle(service).state());
        } finally {
            // Do not leak the deliberately restored interrupt flag into the JUnit worker.
            Thread.interrupted();
        }
    }

    @Test
    void closeMustNotReturnWhileForegroundIngressIsBlocked() throws Exception {
        PaimonService service = service();
        TableFixture table = table(service, "a");
        PaimonServiceLifecycle lifecycle = lifecycle(service);
        PaimonServiceLifecycle.Ingress blockedIngress =
                lifecycle.enter("blocked-close-regression");
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer =
                closeInDaemonThread(service, closeFailure, "paimon-close-foreground-blocked");

        try {
            // closeInDaemonThread starts the caller itself.
            awaitCloseWaitingForIngress(lifecycle, closer);
            // Spec I3：准入仍未归零时调用者必须等待，不能关闭任何资源。
            closer.join(2_000L);
            assertTrue(
                    closer.isAlive(),
                    "close() must not return while a foreground ingress is still running");
            verify(table.strategy, never()).close();
            verify(table.committer, never()).close();
        } finally {
            blockedIngress.close();
        }

        closer.join(3_000L);
        assertFalse(closer.isAlive());
        assertNull(closeFailure.get());
        awaitLifecycleState(lifecycle, PaimonServiceLifecycle.State.CLOSED);
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
    }

    @Test
    void closeMustNotReturnWhileStopDrainCommitIsBlockedAndAckAfterwards() throws Exception {
        PaimonService service = newService(config());
        AtomicInteger callbackCount = new AtomicInteger();
        service.setFlushOffsetCallback(ignored -> callbackCount.incrementAndGet());
        service.startForTest();
        TableFixture table = table(service, "a");
        CountDownLatch commitEntered = new CountDownLatch(1);
        CountDownLatch releaseCommit = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            commitEntered.countDown();
                            assertTrue(releaseCommit.await(10L, TimeUnit.SECONDS));
                            return null;
                        })
                .when(table.committer)
                .commit(anyLong(), anyList());
        TapConnectorContext context = connectorContext();
        service.writeRecords(
                Collections.singletonList(cdcEvent("a", 1)), tapTable("a"), context);
        service.processHeartbeat(heartbeat("offset-blocked-commit", 123L, 456L));

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer = closeInDaemonThread(service, closeFailure, "paimon-stop-drain-blocked");
        try {
            assertTrue(commitEntered.await(5L, TimeUnit.SECONDS));
            closer.join(800L);
            assertTrue(
                    closer.isAlive(),
                    "close() must not return while the stop-drain commit is still running");
            assertEquals(0, callbackCount.get());
            verify(table.strategy, never()).close();
        } finally {
            releaseCommit.countDown();
        }

        closer.join(3_000L);
        assertFalse(closer.isAlive());
        assertNull(closeFailure.get());
        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        verify(table.committer, times(1)).commit(anyLong(), anyList());
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
        // No failure occurred, so the reserved offset callback is acknowledged after the commit.
        assertEquals(1, callbackCount.get());
    }

    @Test
    void finalPrepareWithoutBusinessRowsMustRunOnceWithoutPublishingOffset() throws Exception {
        PaimonService service = newService(config());
        AtomicInteger callbacks = new AtomicInteger();
        service.setFlushOffsetCallback(ignored -> callbacks.incrementAndGet());
        service.startForTest();
        TableFixture table = table(service, "a");

        service.close();
        service.close();

        verify(table.strategy, never()).prepareCommit(anyLong());
        verify(table.strategy, times(1)).prepareFinalCommit(0L);
        verify(table.committer, never()).commit(anyLong(), anyList());
        assertEquals(0, callbacks.get());
        assertTrue(table.context.cleanupComplete());
    }

    @Test
    void closeWorkerMustRunInRootThreadGroupIndependentOfCallerGroup() throws Exception {
        PaimonService service = service();
        TableFixture table = table(service, "a");
        PaimonServiceLifecycle.Ingress blockedIngress =
                lifecycle(service).enter("task-thread-group-close");
        ThreadGroup taskGroup = new ThreadGroup("pdk-task-close-test");
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer =
                new Thread(
                        taskGroup,
                        () -> {
                            try {
                                service.close();
                            } catch (Throwable failure) {
                                closeFailure.set(failure);
                            }
                        },
                        "pdk-task-close-caller");
        closer.setDaemon(true);

        try {
            closer.start();
            awaitCloseWaitingForIngress(lifecycle(service), closer);
            Thread worker = closeWorker(service);
            assertTrue(worker.isDaemon());
            // The worker lives in the root-level close-worker group, not the caller's task
            // group, so a task-group teardown can never destroy it.
            assertFalse(worker.getThreadGroup() == taskGroup);
            assertEquals(
                    "paimon-service-close-workers", worker.getThreadGroup().getName());
        } finally {
            blockedIngress.close();
        }

        closer.join(3_000L);
        assertFalse(closer.isAlive());
        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
    }

    @Test
    void closeMustNotReturnWhileOffsetCallbackIsInProgress() throws Exception {
        PaimonService service = newService(config());
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger();
        service.setFlushOffsetCallback(
                ignored -> {
                    callbackCount.incrementAndGet();
                    callbackEntered.countDown();
                    try {
                        assertTrue(releaseCallback.await(10L, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                });
        service.startForTest();
        AtomicReference<Throwable> heartbeatFailure = new AtomicReference<>();
        Thread heartbeatThread =
                new Thread(
                        () -> {
                            try {
                                service.processHeartbeat(heartbeat("offset-running", 123L, 456L));
                            } catch (Throwable failure) {
                                heartbeatFailure.set(failure);
                            }
                        },
                        "paimon-blocked-callback");
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
        assertTrue(callbackEntered.await(5L, TimeUnit.SECONDS));

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer = closeInDaemonThread(service, closeFailure, "paimon-callback-in-progress");
        try {
            closer.join(800L);
            assertTrue(
                    closer.isAlive(),
                    "close() must not return while an offset callback is still running");
            assertEquals(1, callbackCount.get());
        } finally {
            releaseCallback.countDown();
        }

        heartbeatThread.join(2_000L);
        assertFalse(heartbeatThread.isAlive());
        assertNull(heartbeatFailure.get());
        closer.join(3_000L);
        assertFalse(closer.isAlive());
        assertNull(closeFailure.get());
        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        assertEquals(1, callbackCount.get());
    }

    @Test
    void stopDrainCallbackMayReenterCloseWithoutWaitingForItsOwnWorker() throws Exception {
        PaimonService service = newService(config());
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        service.setFlushOffsetCallback(
                ignored -> {
                    callbackCount.incrementAndGet();
                    try {
                        service.close();
                        assertEquals(PaimonServiceLifecycle.State.STOPPING, lifecycle(service).state());
                        assertFalse(tableContexts(service).get("default.a").cleanupComplete(),
                                "close worker 重入返回不能发布终态或提前清理资源");
                    } catch (Throwable failure) {
                        callbackFailure.set(failure);
                    }
                });
        service.startForTest();
        table(service, "a");
        service.writeRecords(
                Collections.singletonList(cdcEvent("a", 1)), tapTable("a"), connectorContext());
        service.processHeartbeat(heartbeat("offset-reentrant", 123L, 456L));

        service.close();

        assertEquals(1, callbackCount.get());
        assertNull(callbackFailure.get());
        assertEquals(PaimonServiceLifecycle.State.CLOSED, lifecycle(service).state());
    }

    @Test
    void closeMustWaitForCatalogCleanupToFinish() throws Exception {
        PaimonService service = service();
        Catalog catalog = mock(Catalog.class);
        CountDownLatch catalogCloseEntered = new CountDownLatch(1);
        CountDownLatch releaseCatalogClose = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            catalogCloseEntered.countDown();
                            assertTrue(releaseCatalogClose.await(10L, TimeUnit.SECONDS));
                            return null;
                        })
                .when(catalog)
                .close();
        setCatalog(service, catalog);
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer = closeInDaemonThread(service, closeFailure, "paimon-catalog-close-blocked");

        try {
            assertTrue(catalogCloseEntered.await(5L, TimeUnit.SECONDS));
            closer.join(2_000L);
            assertTrue(closer.isAlive(), "catalog.close 尚未完成时不能发布停止终态");
            assertNull(closeFailure.get());
            assertEquals(PaimonServiceLifecycle.State.STOPPING, lifecycle(service).state());
        } finally {
            releaseCatalogClose.countDown();
        }

        closer.join(3_000L);
        assertFalse(closer.isAlive());
        assertNull(closeFailure.get());
        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        verify(catalog, times(1)).close();
    }

    @Test
    void closeMustNotReturnWhileSynchronizedServiceOperationHoldsMonitor() throws Exception {
        PaimonService service = service();
        TableFixture table = table(service, "a");
        CountDownLatch monitorEntered = new CountDownLatch(1);
        CountDownLatch releaseMonitor = new CountDownLatch(1);
        AtomicReference<Throwable> monitorFailure = new AtomicReference<>();
        Thread synchronizedOperation =
                new Thread(
                        () -> {
                            synchronized (service) {
                                monitorEntered.countDown();
                                try {
                                    assertTrue(releaseMonitor.await(10L, TimeUnit.SECONDS));
                                } catch (Throwable failure) {
                                    monitorFailure.set(failure);
                                }
                            }
                        },
                        "paimon-synchronized-operation");
        synchronizedOperation.setDaemon(true);
        synchronizedOperation.start();
        assertTrue(monitorEntered.await(5L, TimeUnit.SECONDS));

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer =
                closeInDaemonThread(service, closeFailure, "paimon-monitor-held-close");
        try {
            closer.join(800L);
            assertTrue(
                    closer.isAlive(),
                    "close() must not return while a synchronized service operation blocks"
                            + " the foreground drain");
            verify(table.strategy, never()).close();
            verify(table.committer, never()).close();
        } finally {
            releaseMonitor.countDown();
        }

        synchronizedOperation.join(2_000L);
        assertFalse(synchronizedOperation.isAlive());
        assertNull(monitorFailure.get());
        closer.join(3_000L);
        assertFalse(closer.isAlive());
        assertNull(closeFailure.get());
        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
    }

    @Test
    void concurrentCloseCallersMustBothWaitForForegroundAndShareOutcome() throws Exception {
        PaimonService service = service();
        TableFixture table = table(service, "a");
        PaimonServiceLifecycle.Ingress blockedIngress = lifecycle(service).enter("concurrent-close");
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> first = new AtomicReference<>();
        AtomicReference<Throwable> second = new AtomicReference<>();
        Thread firstCloser = concurrentCloser(service, start, first, "paimon-close-first");
        Thread secondCloser = concurrentCloser(service, start, second, "paimon-close-second");

        try {
            firstCloser.start();
            secondCloser.start();
            start.countDown();
            firstCloser.join(800L);
            secondCloser.join(800L);
            assertTrue(firstCloser.isAlive(), "First caller must wait for the foreground drain");
            assertTrue(secondCloser.isAlive(), "Second caller must wait for the foreground drain");
            verify(table.strategy, never()).close();
        } finally {
            blockedIngress.close();
        }

        firstCloser.join(3_000L);
        secondCloser.join(3_000L);
        assertFalse(firstCloser.isAlive());
        assertFalse(secondCloser.isAlive());
        assertNull(first.get());
        assertNull(second.get());
        // Both callers observed one shared close: resources are closed exactly once and a
        // repeated close returns the terminal outcome immediately.
        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
        long repeatedStartedAt = System.nanoTime();
        assertDoesNotThrow(service::close);
        assertTrue(
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - repeatedStartedAt) < 1_000L,
                "Repeated close must not restart the shutdown");
    }

    @Test
    void interruptedCloseCallerMustKeepWaitingAndRestoreItsInterruptFlag() throws Exception {
        PaimonService service = service();
        PaimonServiceLifecycle.Ingress blockedIngress = lifecycle(service).enter("interrupt-close");
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        AtomicReference<Boolean> interruptedAfterClose = new AtomicReference<>(false);
        Thread closer =
                new Thread(
                        () -> {
                            try {
                                service.close();
                            } catch (Throwable failure) {
                                closeFailure.set(failure);
                            } finally {
                                interruptedAfterClose.set(Thread.currentThread().isInterrupted());
                            }
                        },
                        "paimon-interrupted-close-caller");
        closer.setDaemon(true);

        try {
            closer.start();
            awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.STOPPING);
            closer.interrupt();
            // Interruption is recorded but never releases the foreground wait.
            closer.join(500L);
            assertTrue(closer.isAlive(), "Interrupted caller must keep waiting for the drain");
            blockedIngress.close();
            closer.join(3_000L);

            assertFalse(closer.isAlive());
            assertTrue(closeFailure.get() instanceof InterruptedException);
            assertTrue(interruptedAfterClose.get());
        } finally {
            blockedIngress.close();
        }

        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        assertSame(closeFailure.get(), assertThrows(InterruptedException.class, service::close));
    }

    @Test
    void closeMustNotReturnWhileAsyncSchedulerCommitIsBlocked() throws Exception {
        PaimonConfig config = configWithAsyncCommit();
        config.setCommitIntervalMs(20);
        PaimonService service =
                new PaimonService(
                        config,
                        mock(Log.class),
                        System::currentTimeMillis,
                        () -> { },
                        PaimonAsyncCommitScheduler::newDaemonExecutor);
        service.startForTest();
        TableFixture table = table(service, "a");
        CountDownLatch commitEntered = new CountDownLatch(1);
        CountDownLatch releaseCommit = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            commitEntered.countDown();
                            assertTrue(releaseCommit.await(10L, TimeUnit.SECONDS));
                            return null;
                        })
                .when(table.committer)
                .commit(anyLong(), anyList());
        service.writeRecords(
                Collections.singletonList(cdcEvent("a", 1)), tapTable("a"), connectorContext());
        assertTrue(commitEntered.await(5L, TimeUnit.SECONDS));

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer =
                closeInDaemonThread(service, closeFailure, "paimon-scheduler-commit-blocked");
        try {
            closer.join(800L);
            assertTrue(
                    closer.isAlive(),
                    "close() must not return while a scheduler commit is still running");
            verify(table.strategy, never()).close();
            verify(table.committer, never()).close();
        } finally {
            releaseCommit.countDown();
        }

        closer.join(3_000L);
        assertFalse(closer.isAlive());
        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
    }

    @Test
    void blockedCompactionMustKeepEveryCallerWaitingAndCloseResourcesOnce() throws Exception {
        PaimonService service = service();
        PaimonCompactionLifecycle compaction = PaimonCompactionLifecycle.forTable("default.a");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        compaction.compactionExecutor().submit(blockingCompaction(started, release));
        assertTrue(started.await(5L, TimeUnit.SECONDS));
        TableFixture table = tableWithLifecycle(service, "a", compaction);
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        AtomicReference<Throwable> secondFailure = new AtomicReference<>();
        Thread first = closeInDaemonThread(service, firstFailure, "paimon-stop-compaction-first");
        Thread second = closeInDaemonThread(service, secondFailure, "paimon-stop-compaction-second");
        try {
            first.join(400L);
            second.join(400L);
            assertTrue(first.isAlive());
            assertTrue(second.isAlive());
            assertFalse(table.context.cleanupComplete());
            verify(table.strategy, never()).close();
            verify(table.committer, never()).close();
        } finally {
            release.countDown();
        }
        first.join(3_000L);
        second.join(3_000L);
        assertFalse(first.isAlive());
        assertFalse(second.isAlive());
        assertNull(firstFailure.get());
        assertNull(secondFailure.get());
        assertTrue(compaction.compactionExecutor().isTerminated());
        assertTrue(table.context.cleanupComplete());
        verify(table.strategy, times(1)).prepareFinalCommit(0L);
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
    }

    @Test
    void closeMustFinishAllTablesOnOneWorkerAfterBlockedResourceIsReleased() throws Exception {
        PaimonService service = service();
        TableFixture firstTable = table(service, "a");
        TableFixture secondTable = table(service, "b");
        CountDownLatch firstCloseEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstClose = new CountDownLatch(1);
        AtomicInteger closeCount = new AtomicInteger();
        List<String> closingThreads = Collections.synchronizedList(new ArrayList<>());
        org.mockito.stubbing.Answer<Void> close = invocation -> {
            closingThreads.add(Thread.currentThread().getName());
            if (closeCount.incrementAndGet() == 1) {
                firstCloseEntered.countDown();
                awaitUninterruptibly(releaseFirstClose);
            }
            return null;
        };
        doAnswer(close).when(firstTable.strategy).close();
        doAnswer(close).when(secondTable.strategy).close();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller = closeInDaemonThread(service, failure, "paimon-stop-all-resources");
        try {
            assertTrue(firstCloseEntered.await(5L, TimeUnit.SECONDS));
            caller.join(400L);
            assertTrue(caller.isAlive());
            assertEquals(1, closeCount.get());
        } finally {
            releaseFirstClose.countDown();
        }
        caller.join(3_000L);
        assertFalse(caller.isAlive());
        assertNull(failure.get());
        assertEquals(2, closingThreads.size());
        assertEquals(closingThreads.get(0), closingThreads.get(1));
        assertEquals(closeWorker(service).getName(), closingThreads.get(0));
        assertTrue(firstTable.context.cleanupComplete());
        assertTrue(secondTable.context.cleanupComplete());
        verify(firstTable.committer, times(1)).close();
        verify(secondTable.committer, times(1)).close();
    }

    @Test
    void closeMustEmitInfoProgressWhileResourceCloseBlocksAndPublishOneTerminalEvent() throws Exception {
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        PaimonService service = new PaimonService(config(), recordingStopLog(events), () -> 100L, () -> { });
        service.startForTest();
        TableFixture table = table(service, "a");
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch allowFinalPrepare = new CountDownLatch(1);
        when(table.strategy.prepareFinalCommit(anyLong())).thenAnswer(invocation -> {
            awaitUninterruptibly(allowFinalPrepare);
            return Collections.emptyList();
        });
        doAnswer(invocation -> {
            entered.countDown();
            awaitUninterruptibly(release);
            return null;
        }).when(table.strategy).close();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller = closeInDaemonThread(service, failure, "paimon-stop-info-progress");
        try {
            long awaitingDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (caller.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < awaitingDeadline) {
                Thread.yield();
            }
            assertEquals(Thread.State.TIMED_WAITING, caller.getState());
            // 阶段切换刻意晚于 caller 首次 await；固定 5 秒轮询会把心跳漏到第二轮。
            Thread.sleep(250L);
            allowFinalPrepare.countDown();
            assertTrue(entered.await(5L, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8L);
            while (countEvents(events, "event=waiting") == 0 && System.nanoTime() < deadline) {
                Thread.sleep(20L);
            }
            assertTrue(caller.isAlive());
            assertTrue(countEvents(events, "event=waiting") > 0, events.toString());
            assertEquals(0, countEvents(events, "event=finished"));
            String waiting;
            synchronized (events) {
                waiting = events.stream().filter(event -> event.contains("event=waiting")).findFirst().get();
            }
            assertTrue(waiting.contains("table=default.a"), waiting);
            assertTrue(waiting.contains("phase=CLOSE_WRITER"), waiting);
            assertTrue(waiting.contains("owner="), waiting);
            assertTrue(waiting.contains("elapsedMs="), waiting);
            assertTrue(waiting.contains("phaseElapsedMs="), waiting);
            for (String field : new String[] {"attempt=", "remainingMs=", "cancelRequested=",
                    "executorTerminated=true", "prepareReturned=true", "inFlightAction="}) {
                assertTrue(waiting.contains(field), waiting);
            }
        } finally {
            allowFinalPrepare.countDown();
            release.countDown();
        }
        caller.join(3_000L);
        assertFalse(caller.isAlive());
        assertNull(failure.get());
        service.close();
        awaitEvent(events, "event=start");
        assertEquals(1, countEvents(events, "event=start"));
        awaitEvent(events, "event=finished");
        assertEquals(1, countEvents(events, "event=finished"));
        awaitEvent(events, "正常退出");
        assertEquals(1, countEvents(events, "正常退出"));
    }

    @Test
    void loggingRuntimeFailureMustNotChangeCommitOrCleanupOutcome() throws Exception {
        Log log = mock(Log.class, invocation -> {
            if ("info".equals(invocation.getMethod().getName())) {
                throw new IllegalStateException("INFO 后端不可用");
            }
            return org.mockito.Mockito.RETURNS_DEFAULTS.answer(invocation);
        });
        PaimonService service = new PaimonService(config(), log, () -> 100L, () -> { });
        service.startForTest();
        TableFixture table = table(service, "a");
        coordinator(service).acceptInitial("default.a", 1);

        assertDoesNotThrow(service::close);
        assertDoesNotThrow(service::close);

        verify(table.committer, times(1)).commit(anyLong(), anyList());
        verify(table.strategy, times(1)).close();
        assertTrue(table.context.cleanupComplete());
    }

    @Test
    void resourceCloseFailureMustRemainSharedAndMustNeverLogNormalExit() throws Exception {
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        PaimonService service = new PaimonService(config(), recordingStopLog(events), () -> 100L, () -> { });
        service.startForTest();
        TableFixture table = table(service, "a");
        IOException expected = new IOException("Writer close 失败");
        doThrow(expected).when(table.strategy).close();

        assertSame(expected, assertThrows(Exception.class, service::close));
        assertSame(expected, assertThrows(Exception.class, service::close));

        assertFalse(table.context.cleanupComplete());
        verify(table.strategy, times(1)).close();
        verify(table.committer, never()).close();
        assertEquals(0, countEvents(events, "正常退出"));
        awaitEvent(events, "event=finished");
        assertEquals(1, countEvents(events, "event=finished"));
    }

    @Test
    void interruptionAfterTerminalPublicationMustNotMutateCachedCloseResult() throws Exception {
        PaimonService service = service();
        TableFixture table = table(service, "a");
        service.close();
        AtomicReference<Throwable> repeatedFailure = new AtomicReference<>();
        AtomicReference<Boolean> interruptRestored = new AtomicReference<>(false);
        Thread lateCaller = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                service.close();
            } catch (Throwable failure) {
                repeatedFailure.set(failure);
            } finally {
                interruptRestored.set(Thread.currentThread().isInterrupted());
            }
        }, "paimon-stop-late-interrupt");
        lateCaller.start();
        lateCaller.join(3_000L);

        assertFalse(lateCaller.isAlive());
        assertNull(repeatedFailure.get());
        assertTrue(interruptRestored.get());
        assertDoesNotThrow(service::close);
        verify(table.strategy, times(1)).close();
    }

    @Test
    void finalNativeCompactionFailureMustLogCauseThenNormalDiscardedOutcome() throws Exception {
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        PaimonService service = new PaimonService(config(), recordingStopLog(events), () -> 100L, () -> { });
        service.startForTest();
        PaimonCompactionLifecycle executor = PaimonCompactionLifecycle.forTable("default.a");
        TableFixture table = tableWithLifecycle(service, "a", executor);
        IOException cause = new IOException("真实 task 内部的 spill channel 故障");
        java.util.concurrent.Future<CompactResult> result = executor.compactionExecutor().submit(
                failingCompaction(cause));
        when(table.strategy.prepareFinalCommit(0L)).thenAnswer(invocation -> {
            result.get();
            return Collections.emptyList();
        });

        assertDoesNotThrow(service::close);
        assertDoesNotThrow(service::close);

        assertTrue(executor.compactionExecutor().isTerminated());
        assertTrue(table.context.cleanupComplete());
        verify(table.committer, never()).commit(anyLong(), anyList());
        verify(table.committer, never()).filterAndCommit(anyMap());
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
        awaitEvent(events, "event=compaction-discarded");
        assertEquals(1, countEvents(events, "event=compaction-discarded"));
        awaitEvent(events, "event=finished");
        assertEquals(1, countEvents(events, "event=finished"));
        String discarded = events.stream().filter(event -> event.contains("event=compaction-discarded"))
                .findFirst().get();
        assertTrue(discarded.contains("java.util.concurrent.ExecutionException"), discarded);
        assertTrue(discarded.contains("NativeCompactionFailure"), discarded);
        assertTrue(discarded.contains("Caused by: java.io.IOException: " + cause.getMessage()), discarded);
        assertTrue(discarded.contains("at "), "必须保留异常堆栈而非只打印 message");
        assertTrue(discarded.contains("identifier=0"), discarded);
        String terminal = events.stream().filter(event -> event.contains("event=finished")).findFirst().get();
        assertTrue(terminal.contains("outcome=SUCCESS_COMPACTION_DISCARDED"), terminal);
        assertTrue(terminal.contains("discardedTables=1"), terminal);
        assertTrue(terminal.contains("正常退出"), terminal);
        assertTrue(events.indexOf(discarded) < events.indexOf(terminal));
    }

    @Test
    void nativeCompactionFailureDuringBusinessDrainMustRemainHardFailure() throws Exception {
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        PaimonService service = new PaimonService(config(), recordingStopLog(events), () -> 100L, () -> { });
        service.startForTest();
        PaimonCompactionLifecycle executor = PaimonCompactionLifecycle.forTable("default.a");
        TableFixture table = tableWithLifecycle(service, "a", executor);
        IOException cause = new IOException("真实 task 内部的 spill channel 故障");
        java.util.concurrent.Future<CompactResult> result = executor.compactionExecutor().submit(
                failingCompaction(cause));
        when(table.strategy.prepareCommit(0L)).thenAnswer(invocation -> {
            result.get();
            return Collections.emptyList();
        });
        coordinator(service).acceptInitial("default.a", 1);

        java.util.concurrent.ExecutionException failure =
                assertThrows(java.util.concurrent.ExecutionException.class, service::close);

        assertSame(cause, failure.getCause().getCause());
        assertSame(failure, assertThrows(java.util.concurrent.ExecutionException.class, service::close));
        verify(table.strategy, never()).prepareFinalCommit(anyLong());
        verify(table.committer, never()).commit(anyLong(), anyList());
        assertTrue(table.context.cleanupComplete());
        assertEquals(0, countEvents(events, "event=compaction-discarded"));
        assertEquals(0, countEvents(events, "正常退出"));
        awaitEvent(events, "outcome=FAILED");
        assertEquals(1, countEvents(events, "outcome=FAILED"));
    }

    @Test
    void businessWriteFailureMustNotEnterFinalCompactionWaiver() throws Exception {
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        PaimonService service = new PaimonService(config(), recordingStopLog(events), () -> 100L, () -> { });
        service.startForTest();
        TableFixture table = table(service, "a");
        IOException failure = new IOException("业务 writer 写入失败");
        doThrow(failure).when(table.strategy).write(org.mockito.ArgumentMatchers.any());

        assertSame(failure, assertThrows(Exception.class, () -> service.writeRecords(
                Collections.singletonList(cdcEvent("a", 1)), tapTable("a"), connectorContext())));
        assertSame(failure, assertThrows(Exception.class, service::close));

        verify(table.strategy, never()).prepareFinalCommit(anyLong());
        verify(table.committer, never()).commit(anyLong(), anyList());
        assertTrue(table.context.cleanupComplete());
        assertEquals(0, countEvents(events, "event=compaction-discarded"));
        assertEquals(0, countEvents(events, "正常退出"));
        awaitEvent(events, "outcome=FAILED");
        assertEquals(1, countEvents(events, "outcome=FAILED"));
    }

    @org.junit.jupiter.params.ParameterizedTest(name = "公开业务入口 {0} 的 Error 必须锁存并阻止最终豁免")
    @org.junit.jupiter.params.provider.EnumSource(HardErrorStage.class)
    void publicBusinessErrorMustFenceStopUntilSafeCleanupAndReleaseOwner(HardErrorStage stage)
            throws Exception {
        PaimonConfig config = config();
        config.setBatchAccumulationSize(1);
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        PaimonService service = new PaimonService(config, recordingStopLog(events), () -> 100L, () -> { });
        PaimonService nextService = newService(config());
        AssertionError original = new AssertionError("业务入口 Error: " + stage);
        AtomicInteger callbacks = new AtomicInteger();
        service.setFlushOffsetCallback(offset -> {
            callbacks.incrementAndGet();
            if (stage == HardErrorStage.OFFSET_CALLBACK) { throw original; }
        });
        service.startForTest();
        TableFixture table = table(service, "a");
        org.apache.paimon.table.FileStoreTable physical = mock(org.apache.paimon.table.FileStoreTable.class);
        when(physical.location()).thenReturn(new org.apache.paimon.fs.Path(
                "file:///tmp/paimon-hard-error-owner-" + java.util.UUID.randomUUID()));
        Method register = PaimonService.class.getDeclaredMethod("registerPhysicalTableOwner",
                String.class, org.apache.paimon.table.FileStoreTable.class);
        Method unregister = PaimonService.class.getDeclaredMethod("unregisterPhysicalTableOwner", String.class);
        register.setAccessible(true);
        unregister.setAccessible(true);
        register.invoke(service, "default.a", physical);
        if (stage == HardErrorStage.WRITE) {
            doThrow(original).when(table.strategy).write(org.mockito.ArgumentMatchers.any());
        } else if (stage == HardErrorStage.BATCH_PREPARE || stage == HardErrorStage.INITIAL_END_PREPARE) {
            when(table.strategy.prepareCommit(0L)).thenThrow(original);
        } else if (stage == HardErrorStage.COMMIT) {
            doThrow(original).when(table.committer).commit(anyLong(), anyList());
        }
        try {
            AssertionError propagated = assertThrows(AssertionError.class, () -> {
                if (stage == HardErrorStage.OFFSET_CALLBACK) {
                    service.processHeartbeat(heartbeat("offset-error", 123L, 456L));
                } else if (stage == HardErrorStage.INITIAL_END_PREPARE) {
                    service.afterInitialSync(connectorContext(), tapTable("a"));
                } else {
                    service.writeRecords(Collections.singletonList(cdcEvent("a", 1)),
                            tapTable("a"), connectorContext());
                }
            });
            assertSame(original, propagated);
            assertEquals(PaimonServiceLifecycle.State.FAILED, lifecycle(service).state());

            assertSame(original, assertThrows(AssertionError.class, service::close));
            assertSame(original, assertThrows(AssertionError.class, service::close));

            verify(table.strategy, never()).prepareFinalCommit(anyLong());
            verify(table.strategy, times(1)).close();
            verify(table.committer, times(1)).close();
            assertTrue(table.context.cleanupComplete());
            assertEquals(stage == HardErrorStage.OFFSET_CALLBACK ? 1 : 0, callbacks.get());
            assertEquals(0, countEvents(events, "event=compaction-discarded"));
            assertEquals(0, countEvents(events, "正常退出"));
            awaitEvent(events, "outcome=FAILED");
            assertEquals(1, countEvents(events, "outcome=FAILED"));
            assertDoesNotThrow(() -> register.invoke(nextService, "default.a", physical));
        } finally {
            try { service.close(); } catch (Throwable expectedFailure) { /* 保留测试注入的硬失败。 */ }
            unregister.invoke(service, "default.a");
            unregister.invoke(nextService, "default.a");
            nextService.close();
        }
    }

    private enum HardErrorStage { WRITE, BATCH_PREPARE, INITIAL_END_PREPARE, COMMIT, OFFSET_CALLBACK }

    private static CompactTask failingCompaction(Exception failure) {
        return new CompactTask(null) {
            @Override
            protected CompactResult doCompact() throws Exception {
                throw failure;
            }
        };
    }

    private static CompactTask blockingCompaction(CountDownLatch started, CountDownLatch release) {
        return new CompactTask(null) {
            @Override
            protected CompactResult doCompact() {
                started.countDown();
                awaitUninterruptibly(release);
                return new CompactResult();
            }
        };
    }

    private static void awaitEvent(List<String> events, String token) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (countEvents(events, token) == 0 && System.nanoTime() < deadline) { Thread.sleep(5); }
    }

    private static Log recordingStopLog(List<String> events) {
        return mock(Log.class, invocation -> {
            Object[] arguments = invocation.getArguments();
            if ("info".equals(invocation.getMethod().getName()) && arguments.length > 0
                    && arguments[0] instanceof String && ((String) arguments[0]).startsWith("[paimon-stop]")) {
                events.add((String) arguments[0]);
            }
            return org.mockito.Mockito.RETURNS_DEFAULTS.answer(invocation);
        });
    }

    private static long countEvents(List<String> events, String text) {
        synchronized (events) {
            return events.stream().filter(event -> event.contains(text)).count();
        }
    }

    private static PaimonConfig configWithAsyncCommit() {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(100);
        config.setCommitIntervalMs(30_000);
        config.setEnableAsyncCommit(true);
        return config;
    }

    private static PaimonService service() {
        PaimonService service =
                new PaimonService(config(), mock(Log.class), () -> 100L, () -> { });
        service.startForTest();
        return service;
    }

    private static PaimonService newService(PaimonConfig config) {
        return new PaimonService(config, mock(Log.class), () -> 100L, () -> { },
                PaimonAsyncCommitScheduler::newDaemonExecutor);
    }

    private static Thread closeWorker(PaimonService service) throws Exception {
        Field operationField = PaimonService.class.getDeclaredField("closeOperation");
        operationField.setAccessible(true);
        Object operation = operationField.get(service);
        Field workerField = operation.getClass().getDeclaredField("worker");
        workerField.setAccessible(true);
        return (Thread) workerField.get(operation);
    }

    private static PaimonConfig config() {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(100);
        config.setCommitIntervalMs(30_000);
        config.setEnableAsyncCommit(false);
        return config;
    }

    private static TableFixture table(PaimonService service, String tableName) throws Exception {
        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        return registerTable(service, tableName, strategy, committer, null, null,
                Collections.emptyList());
    }

    private static TableFixture tableWithLifecycle(
            PaimonService service,
            String tableName,
            PaimonCompactionLifecycle compactionLifecycle)
            throws Exception {
        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        return registerTable(service, tableName, strategy, committer, compactionLifecycle,
                null, Collections.emptyList());
    }

    private static TableFixture registerTable(
            PaimonService service,
            String tableName,
            PaimonBucketWriterStrategy strategy,
            PaimonTableCommitter committer,
            PaimonCompactionLifecycle compactionLifecycle,
            org.apache.paimon.disk.IOManager ioManager,
            List<String> spillDirs)
            throws Exception {
        when(strategy.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        when(strategy.writeSemanticContract())
                .thenReturn(PaimonWriteSemanticContractTestFactory.forMode(BucketMode.HASH_FIXED));
        when(strategy.prepareCommit(anyLong())).thenReturn(Collections.emptyList());
        when(strategy.prepareFinalCommit(anyLong())).thenReturn(Collections.emptyList());
        when(committer.filterAndCommit(anyMap())).thenReturn(0);
        String tableKey = "default." + tableName;
        tableContexts(service)
                .put(
                        tableKey,
                        new PaimonTableWriteContext(
                                tableKey,
                                tableName,
                                "stable-" + tableName,
                                strategy,
                                committer,
                                ioManager,
                                spillDirs,
                                0L,
                                PaimonTableWriteContext.CommitStateStore.NOOP,
                                compactionLifecycle != null
                                        ? compactionLifecycle
                                        : PaimonCompactionLifecycle.withoutCompactionExecutor()));
        Field controllerField = PaimonService.class.getDeclaredField("stopController");
        controllerField.setAccessible(true);
        Field resourcesField = PaimonService.class.getDeclaredField("stopResources");
        resourcesField.setAccessible(true);
        PaimonStopResources.Scope scope = ((PaimonStopResources) resourcesField.get(service)).scope(
                tableKey, (PaimonStopController) controllerField.get(service));
        scope.reserve("test context").bind(tableContexts(service).get(tableKey));
        tableContexts(service).get(tableKey).attachStopScope(scope);
        fieldCache(service).put(
                tableKey, Collections.singletonList(new DataField(0, "id", DataTypes.INT())));
        return new TableFixture(strategy, committer, tableContexts(service).get(tableKey));
    }

    private static TapTable tapTable(String tableName) {
        TapTable table = mock(TapTable.class);
        when(table.getName()).thenReturn(tableName);
        when(table.primaryKeys(true)).thenReturn(Collections.emptyList());
        return table;
    }

    private static TapConnectorContext connectorContext() {
        TapConnectorContext context = mock(TapConnectorContext.class);
        when(context.getStateMap()).thenReturn(mock(KVMap.class));
        when(context.getLog()).thenReturn(mock(Log.class));
        return context;
    }

    private static TapInsertRecordEvent cdcEvent(String tableName, int id) {
        TapInsertRecordEvent event =
                new TapInsertRecordEvent()
                        .init()
                        .table(tableName)
                        .after(Collections.singletonMap("id", id));
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        event.addInfo("nodeIds", Collections.singletonList("source-a"));
        return event;
    }

    private static HeartbeatEvent heartbeat(Object offset, Long sourceTime, Long eventTime) {
        HeartbeatEvent heartbeat = new HeartbeatEvent().init().referenceTime(eventTime);
        heartbeat.addInfo("syncStage", "CDC");
        heartbeat.addInfo("streamOffset", offset);
        heartbeat.addInfo("sourceTime", sourceTime);
        heartbeat.addInfo("nodeIds", Collections.singletonList("source-a"));
        return heartbeat;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, PaimonTableWriteContext> tableContexts(PaimonService service)
            throws Exception {
        Field field = PaimonService.class.getDeclaredField("tableWriteContexts");
        field.setAccessible(true);
        return (Map<String, PaimonTableWriteContext>) field.get(service);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<DataField>> fieldCache(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("paimonFieldCache");
        field.setAccessible(true);
        return (Map<String, List<DataField>>) field.get(service);
    }

    private static PaimonMicroBatchCoordinator coordinator(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("microBatchCoordinator");
        field.setAccessible(true);
        return (PaimonMicroBatchCoordinator) field.get(service);
    }

    private static PaimonServiceLifecycle lifecycle(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("lifecycle");
        field.setAccessible(true);
        return (PaimonServiceLifecycle) field.get(service);
    }

    private static void setCatalog(PaimonService service, Catalog catalog) throws Exception {
        Field field = PaimonService.class.getDeclaredField("catalog");
        field.setAccessible(true);
        field.set(service, catalog);
    }

    private static void invokeBindTaskState(
            PaimonService service, TapConnectorContext connectorContext) throws Exception {
        Method method =
                PaimonService.class.getDeclaredMethod(
                        "bindTaskState", TapConnectorContext.class);
        method.setAccessible(true);
        try {
            method.invoke(service, connectorContext);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new IllegalStateException(cause);
        }
    }

    private static void awaitCloseWaitingForIngress(
            PaimonServiceLifecycle lifecycle, Thread closer) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            if (lifecycle.state() == PaimonServiceLifecycle.State.STOPPING
                    && (closer.getState() == Thread.State.WAITING
                            || closer.getState() == Thread.State.TIMED_WAITING)) {
                return;
            }
            if (!closer.isAlive()) {
                throw new AssertionError("Close completed before waiting for active ingress");
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("Timed out waiting for close to await active ingress");
    }

    private static void awaitLifecycleState(
            PaimonServiceLifecycle lifecycle, PaimonServiceLifecycle.State expected)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            if (lifecycle.state() == expected) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError(
                "Timed out waiting for lifecycle state "
                        + expected
                        + "; current state="
                        + lifecycle.state());
    }

    private static Thread closeInDaemonThread(
            PaimonService service, AtomicReference<Throwable> failure, String threadName) {
        Thread closer =
                new Thread(
                        () -> {
                            try {
                                service.close();
                            } catch (Throwable closeFailure) {
                                failure.set(closeFailure);
                            }
                        },
                        threadName);
        closer.setDaemon(true);
        closer.start();
        return closer;
    }

    private static Thread concurrentCloser(
            PaimonService service,
            CountDownLatch start,
            AtomicReference<Throwable> failure,
            String threadName) {
        Thread closer =
                new Thread(
                        () -> {
                            try {
                                assertTrue(start.await(5L, TimeUnit.SECONDS));
                                service.close();
                            } catch (Throwable closeFailure) {
                                failure.set(closeFailure);
                            }
                        },
                        threadName);
        closer.setDaemon(true);
        return closer;
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    latch.await();
                    return;
                } catch (InterruptedException interruption) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final class TableFixture {
        private final PaimonBucketWriterStrategy strategy;
        private final PaimonTableCommitter committer;
        private final PaimonTableWriteContext context;

        private TableFixture(
                PaimonBucketWriterStrategy strategy, PaimonTableCommitter committer,
                PaimonTableWriteContext context) {
            this.strategy = strategy;
            this.committer = committer;
            this.context = context;
        }
    }
}
