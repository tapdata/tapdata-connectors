package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.commit.PaimonAsyncCommitScheduler;
import io.tapdata.connector.paimon.commit.PaimonMicroBatchCoordinator;
import io.tapdata.connector.paimon.commit.PaimonServiceLifecycle;

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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
    void closeTimeoutMustCoverActiveIngressAndDeferResourceCleanup() throws Exception {
        PaimonService service = serviceWithCloseTimeout(1L, TimeUnit.SECONDS);
        TableFixture table = table(service, "a");
        PaimonServiceLifecycle lifecycle = lifecycle(service);
        PaimonServiceLifecycle.Ingress blockedIngress =
                lifecycle.enter("blocked-close-regression");
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer =
                new Thread(
                        () -> {
                            try {
                                service.close();
                            } catch (Throwable failure) {
                                closeFailure.set(failure);
                            }
                        },
                        "paimon-close-timeout-regression");
        closer.setDaemon(true);

        try {
            closer.start();
            closer.join(3_000L);

            assertFalse(closer.isAlive(), "close() exceeded its total timeout budget");
            assertTrue(closeFailure.get() instanceof IllegalStateException);
            assertTrue(closeFailure.get().getMessage().contains("cleanup continues asynchronously"));
            assertEquals(PaimonServiceLifecycle.State.FAILED, lifecycle.state());
            verify(table.strategy, never()).close();
            verify(table.committer, never()).close();
        } finally {
            blockedIngress.close();
        }

        awaitLifecycleState(lifecycle, PaimonServiceLifecycle.State.CLOSED);
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
        assertSame(closeFailure.get(), assertThrows(Exception.class, service::close));
    }

    @Test
    void timeoutDuringStopDrainMustCommitWithoutAcknowledgingOffset() throws Exception {
        PaimonService service = newServiceWithCloseTimeout(config(), 200L, TimeUnit.MILLISECONDS);
        AtomicInteger callbackCount = new AtomicInteger();
        service.setFlushOffsetCallback(ignored -> callbackCount.incrementAndGet());
        service.startForTest();
        TableFixture table = table(service, "a");
        CountDownLatch commitEntered = new CountDownLatch(1);
        CountDownLatch releaseCommit = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            commitEntered.countDown();
                            assertTrue(releaseCommit.await(5L, TimeUnit.SECONDS));
                            return null;
                        })
                .when(table.committer)
                .commit(anyLong(), anyList());
        TapConnectorContext context = connectorContext();
        service.writeRecords(
                Collections.singletonList(cdcEvent("a", 1)), tapTable("a"), context);
        service.processHeartbeat(heartbeat("offset-timeout", 123L, 456L));

        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer = closeInDaemonThread(service, closeFailure, "paimon-stop-drain-timeout");
        try {
            assertTrue(commitEntered.await(5L, TimeUnit.SECONDS));
            closer.join(2_000L);
            assertFalse(closer.isAlive(), "close() exceeded stop-drain timeout");
            assertTrue(closeFailure.get() instanceof IllegalStateException);
            assertEquals(0, callbackCount.get());
            verify(table.strategy, never()).close();
        } finally {
            releaseCommit.countDown();
        }

        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        verify(table.committer, times(1)).commit(anyLong(), anyList());
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
        assertEquals(0, callbackCount.get());
    }

    @Test
    void deadlineMustFenceStopDrainCallbackBeforeTimeoutCallerIsScheduled() throws Exception {
        AtomicLong nanoClock = new AtomicLong();
        PaimonService service =
                newServiceWithCloseTimeout(config(), 10L, TimeUnit.SECONDS, nanoClock::get);
        AtomicInteger callbackCount = new AtomicInteger();
        service.setFlushOffsetCallback(ignored -> callbackCount.incrementAndGet());
        service.startForTest();
        TableFixture table = table(service, "a");
        doAnswer(
                        invocation -> {
                            nanoClock.set(TimeUnit.SECONDS.toNanos(11L));
                            return null;
                        })
                .when(table.committer)
                .commit(anyLong(), anyList());
        service.writeRecords(
                Collections.singletonList(cdcEvent("a", 1)), tapTable("a"), connectorContext());
        service.processHeartbeat(heartbeat("offset-deadline", 123L, 456L));

        IllegalStateException timeout =
                assertThrows(IllegalStateException.class, service::close);

        assertTrue(timeout.getMessage().contains("cleanup continues asynchronously"));
        assertEquals(0, callbackCount.get());
        verify(table.committer, times(1)).commit(anyLong(), anyList());
        assertEquals(PaimonServiceLifecycle.State.CLOSED, lifecycle(service).state());
    }

    @Test
    void deferredCloseWorkerMustOutliveCallingTaskThreadGroup() throws Exception {
        PaimonService service = serviceWithCloseTimeout(200L, TimeUnit.MILLISECONDS);
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
            closer.join(2_000L);
            assertFalse(closer.isAlive());
            assertTrue(closeFailure.get() instanceof IllegalStateException);
            Thread worker = closeWorker(service);
            assertTrue(worker.isDaemon());
            assertFalse(worker.getThreadGroup() == taskGroup);

            // Invoke reflectively because ThreadGroup destruction is deprecated in newer JDKs,
            // while Java 11 PDK runtimes still use it to retire per-task groups.
            ThreadGroup.class.getMethod("destroy").invoke(taskGroup);
        } finally {
            blockedIngress.close();
        }

        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
    }

    @Test
    void closeTimeoutMustCoverCallbackAlreadyInProgress() throws Exception {
        PaimonService service = newServiceWithCloseTimeout(config(), 200L, TimeUnit.MILLISECONDS);
        CountDownLatch callbackEntered = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger();
        service.setFlushOffsetCallback(
                ignored -> {
                    callbackCount.incrementAndGet();
                    callbackEntered.countDown();
                    try {
                        assertTrue(releaseCallback.await(5L, TimeUnit.SECONDS));
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

        try {
            IllegalStateException timeout =
                    assertThrows(IllegalStateException.class, service::close);
            assertTrue(timeout.getMessage().contains("cleanup continues asynchronously"));
            assertEquals(1, callbackCount.get());
            assertEquals(PaimonServiceLifecycle.State.FAILED, lifecycle(service).state());
        } finally {
            releaseCallback.countDown();
        }

        heartbeatThread.join(2_000L);
        assertFalse(heartbeatThread.isAlive());
        assertNull(heartbeatFailure.get());
        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        assertEquals(1, callbackCount.get());
    }

    @Test
    void stopDrainCallbackMayReenterCloseWithoutWaitingForItsOwnWorker() throws Exception {
        PaimonService service = newServiceWithCloseTimeout(config(), 1L, TimeUnit.SECONDS);
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        service.setFlushOffsetCallback(
                ignored -> {
                    callbackCount.incrementAndGet();
                    try {
                        service.close();
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
    void closeTimeoutMustCoverBlockingCatalogClose() throws Exception {
        PaimonService service = serviceWithCloseTimeout(200L, TimeUnit.MILLISECONDS);
        Catalog catalog = mock(Catalog.class);
        CountDownLatch catalogCloseEntered = new CountDownLatch(1);
        CountDownLatch releaseCatalogClose = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            catalogCloseEntered.countDown();
                            assertTrue(releaseCatalogClose.await(5L, TimeUnit.SECONDS));
                            return null;
                        })
                .when(catalog)
                .close();
        setCatalog(service, catalog);
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer = closeInDaemonThread(service, closeFailure, "paimon-catalog-close-timeout");

        try {
            assertTrue(catalogCloseEntered.await(5L, TimeUnit.SECONDS));
            closer.join(2_000L);
            assertFalse(closer.isAlive(), "close() exceeded catalog-close timeout");
            assertTrue(closeFailure.get() instanceof IllegalStateException);
            assertEquals(PaimonServiceLifecycle.State.FAILED, lifecycle(service).state());
        } finally {
            releaseCatalogClose.countDown();
        }

        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        verify(catalog, times(1)).close();
    }

    @Test
    void closeTimeoutMustNotRaceCleanupWithSynchronizedServiceOperation() throws Exception {
        PaimonService service = serviceWithCloseTimeout(200L, TimeUnit.MILLISECONDS);
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
                                    assertTrue(releaseMonitor.await(5L, TimeUnit.SECONDS));
                                } catch (Throwable failure) {
                                    monitorFailure.set(failure);
                                }
                            }
                        },
                        "paimon-synchronized-operation");
        synchronizedOperation.setDaemon(true);
        synchronizedOperation.start();
        assertTrue(monitorEntered.await(5L, TimeUnit.SECONDS));

        try {
            IllegalStateException timeout =
                    assertThrows(IllegalStateException.class, service::close);
            assertTrue(timeout.getMessage().contains("cleanup continues asynchronously"));
            verify(table.strategy, never()).close();
            verify(table.committer, never()).close();
        } finally {
            releaseMonitor.countDown();
        }

        synchronizedOperation.join(2_000L);
        assertFalse(synchronizedOperation.isAlive());
        assertNull(monitorFailure.get());
        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
    }

    @Test
    void concurrentCloseCallersMustShareDeadlineFailureAndCleanup() throws Exception {
        PaimonService service = serviceWithCloseTimeout(200L, TimeUnit.MILLISECONDS);
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
            firstCloser.join(2_000L);
            secondCloser.join(2_000L);

            assertFalse(firstCloser.isAlive());
            assertFalse(secondCloser.isAlive());
            assertTrue(first.get() instanceof IllegalStateException);
            assertSame(first.get(), second.get());
            long repeatedStartedAt = System.nanoTime();
            assertSame(first.get(), assertThrows(Exception.class, service::close));
            assertTrue(
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - repeatedStartedAt) < 100L,
                    "Repeated close reset the total deadline");
            verify(table.strategy, never()).close();
        } finally {
            blockedIngress.close();
        }

        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
    }

    @Test
    void interruptedCloseCallerMustKeepDeadlineAndRestoreItsInterruptFlag() throws Exception {
        PaimonService service = serviceWithCloseTimeout(1L, TimeUnit.SECONDS);
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
            blockedIngress.close();
            closer.join(2_000L);

            assertFalse(closer.isAlive());
            assertTrue(closeFailure.get() instanceof InterruptedException);
            assertTrue(interruptedAfterClose.get());
        } finally {
            blockedIngress.close();
        }

        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
    }

    @Test
    void closeTimeoutMustCoverRealSchedulerCommitAndDeferResourceCleanup() throws Exception {
        PaimonConfig config = configWithAsyncCommit();
        config.setCommitIntervalMs(20);
        PaimonService service =
                new PaimonService(
                        config,
                        mock(Log.class),
                        System::currentTimeMillis,
                        () -> { },
                        PaimonAsyncCommitScheduler::newDaemonExecutor,
                        200L,
                        TimeUnit.MILLISECONDS);
        service.startForTest();
        TableFixture table = table(service, "a");
        CountDownLatch commitEntered = new CountDownLatch(1);
        CountDownLatch releaseCommit = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            commitEntered.countDown();
                            assertTrue(releaseCommit.await(5L, TimeUnit.SECONDS));
                            return null;
                        })
                .when(table.committer)
                .commit(anyLong(), anyList());
        service.writeRecords(
                Collections.singletonList(cdcEvent("a", 1)), tapTable("a"), connectorContext());
        assertTrue(commitEntered.await(5L, TimeUnit.SECONDS));

        long start = System.nanoTime();
        Throwable timeout;
        try {
            timeout = assertThrows(IllegalStateException.class, service::close);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertTrue(elapsedMs < 2_000L, "close() took " + elapsedMs + "ms");
            assertTrue(timeout.getMessage().contains("cleanup continues asynchronously"));
            assertEquals(PaimonServiceLifecycle.State.FAILED, lifecycle(service).state());
            verify(table.strategy, never()).close();
            verify(table.committer, never()).close();
        } finally {
            releaseCommit.countDown();
        }

        awaitLifecycleState(lifecycle(service), PaimonServiceLifecycle.State.CLOSED);
        verify(table.strategy, times(1)).close();
        verify(table.committer, times(1)).close();
        assertSame(timeout, assertThrows(Exception.class, service::close));
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

    private static PaimonService serviceWithCloseTimeout(long timeout, TimeUnit unit) {
        PaimonService service = newServiceWithCloseTimeout(config(), timeout, unit);
        service.startForTest();
        return service;
    }

    private static PaimonService newServiceWithCloseTimeout(
            PaimonConfig config, long timeout, TimeUnit unit) {
        return newServiceWithCloseTimeout(config, timeout, unit, System::nanoTime);
    }

    private static PaimonService newServiceWithCloseTimeout(
            PaimonConfig config, long timeout, TimeUnit unit, java.util.function.LongSupplier nanoClock) {
        return new PaimonService(
                config,
                mock(Log.class),
                () -> 100L,
                () -> { },
                PaimonAsyncCommitScheduler::newDaemonExecutor,
                timeout,
                unit,
                nanoClock);
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
        when(strategy.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        when(strategy.writeSemanticContract())
                .thenReturn(PaimonWriteSemanticContractTestFactory.forMode(BucketMode.HASH_FIXED));
        when(strategy.prepareCommit(anyLong())).thenReturn(Collections.emptyList());
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
                                null,
                                Collections.emptyList(),
                                0L));
        fieldCache(service).put(
                tableKey, Collections.singletonList(new DataField(0, "id", DataTypes.INT())));
        return new TableFixture(strategy, committer);
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

    private static final class TableFixture {
        private final PaimonBucketWriterStrategy strategy;
        private final PaimonTableCommitter committer;

        private TableFixture(
                PaimonBucketWriterStrategy strategy, PaimonTableCommitter committer) {
            this.strategy = strategy;
            this.committer = committer;
        }
    }
}
