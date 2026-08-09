package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonServiceAsyncCommitIntegrationTest {

    @Test
    void lowTrafficCdcMustCommitAtItsDeadlineWithoutAnotherWrite() throws Exception {
        Fixture fixture = fixture();
        fixture.clock.set(100L);

        fixture.write(1);

        verify(fixture.strategy, never()).prepareCommit(anyLong());
        assertEquals(1_000L, fixture.scheduledDelay.get());
        assertTrue(fixture.scheduledTask.get() != null);

        fixture.clock.set(1_100L);
        fixture.scheduledTask.get().run();

        verify(fixture.strategy, times(1)).prepareCommit(0L);
        verify(fixture.committer, times(1)).filterAndCommit(anyMap());
        assertEquals(0L, fixture.state().accumulatedRecordCount());
        assertEquals(1L, fixture.state().committedGeneration());
    }

    @Test
    void writeThatCommitsBeforeStaleScheduledTaskMustNotCreateASecondSnapshot()
            throws Exception {
        Fixture fixture = fixture();
        fixture.clock.set(100L);
        fixture.write(1);
        Runnable staleTask = fixture.scheduledTask.get();

        fixture.clock.set(1_100L);
        fixture.write(2);
        staleTask.run();

        verify(fixture.strategy, times(1)).prepareCommit(0L);
        verify(fixture.committer, times(1)).filterAndCommit(anyMap());
        assertEquals(2L, fixture.state().committedGeneration());
    }

    @Test
    void staleSchedulerBlockedByDdlMustRecheckTheNewContextDeadline() throws Exception {
        Fixture fixture = fixture();
        Catalog catalog = mock(Catalog.class);
        Table table = mock(Table.class, RETURNS_DEEP_STUBS);
        when(catalog.getTable(Identifier.create("default", "t"))).thenReturn(table);
        setField(fixture.service, "catalog", catalog);

        fixture.clock.set(100L);
        fixture.write(1);
        Runnable staleTask = fixture.scheduledTask.get();

        PaimonBucketWriterStrategy replacementStrategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter replacementCommitter = mock(PaimonTableCommitter.class);
        when(replacementStrategy.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        when(replacementStrategy.writeSemanticContract())
                .thenReturn(PaimonWriteSemanticContractTestFactory.forMode(BucketMode.HASH_FIXED));
        when(replacementStrategy.prepareCommit(anyLong())).thenReturn(Collections.emptyList());
        when(replacementCommitter.filterAndCommit(anyMap())).thenReturn(0);
        PaimonTableWriteContext replacementContext =
                new PaimonTableWriteContext(
                        "default.t",
                        "t",
                        "replacement-user",
                        replacementStrategy,
                        replacementCommitter,
                        null,
                        Collections.emptyList(),
                        0L);

        AtomicReference<Thread> schedulerThread = new AtomicReference<>();
        ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "blocked-stale-scheduler-test");
            schedulerThread.set(thread);
            return thread;
        });
        Future<?> staleFuture = null;
        try {
            fixture.clock.set(1_100L);
            Object tableLock = commitLocks(fixture.service).get("default.t");
            synchronized (tableLock) {
                staleFuture = executor.submit(staleTask);
                awaitBlocked(schedulerThread);

                fixture.service.clearTable("t");
                tableContexts(fixture.service).put("default.t", replacementContext);
                fieldCache(fixture.service).put(
                        "default.t",
                        Collections.singletonList(new DataField(0, "id", DataTypes.INT())));
                fixture.write(2);
            }
            staleFuture.get(5, TimeUnit.SECONDS);

            assertNull(stickyFailure(fixture.service).get());
            verify(fixture.strategy, times(1)).prepareCommit(0L);
            verify(fixture.committer, times(1)).filterAndCommit(anyMap());
            verify(replacementStrategy, never()).prepareCommit(anyLong());
            assertEquals(1L, fixture.state().accumulatedRecordCount());

            fixture.clock.set(2_099L);
            fixture.scheduledTask.get().run();
            verify(replacementStrategy, never()).prepareCommit(anyLong());
            assertEquals(1L, fixture.state().accumulatedRecordCount());

            fixture.clock.set(2_100L);
            fixture.scheduledTask.get().run();

            verify(replacementStrategy, times(1)).prepareCommit(0L);
            verify(replacementCommitter, times(1)).filterAndCommit(anyMap());
            assertEquals(0L, fixture.state().accumulatedRecordCount());
        } finally {
            executor.shutdownNow();
            replacementContext.close();
        }
    }

    @Test
    void scheduledCommitMustWaitForInFlightWriteOnSameTable() throws Exception {
        Fixture fixture = fixture();
        fixture.clock.set(100L);
        fixture.write(1);
        Runnable scheduledTask = fixture.scheduledTask.get();
        CountDownLatch secondWriteEntered = new CountDownLatch(1);
        CountDownLatch releaseSecondWrite = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            secondWriteEntered.countDown();
                            assertTrue(releaseSecondWrite.await(5, TimeUnit.SECONDS));
                            return null;
                        })
                .when(fixture.strategy)
                .write(any());

        fixture.clock.set(1_100L);
        AtomicReference<Thread> schedulerThread = new AtomicReference<>();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> writeFuture = null;
        Future<?> schedulerFuture = null;
        try {
            writeFuture =
                    executor.submit(
                            () -> {
                                fixture.write(2);
                                return null;
                            });
            assertTrue(secondWriteEntered.await(5, TimeUnit.SECONDS));

            schedulerFuture =
                    executor.submit(
                            () -> {
                                schedulerThread.set(Thread.currentThread());
                                scheduledTask.run();
                            });
            awaitBlocked(schedulerThread);
            verify(fixture.strategy, never()).prepareCommit(anyLong());

            releaseSecondWrite.countDown();
            writeFuture.get(5, TimeUnit.SECONDS);
            schedulerFuture.get(5, TimeUnit.SECONDS);

            verify(fixture.strategy, times(1)).prepareCommit(0L);
            verify(fixture.committer, times(1)).filterAndCommit(anyMap());
            assertNull(stickyFailure(fixture.service).get());
            assertEquals(0L, fixture.state().accumulatedRecordCount());
        } finally {
            releaseSecondWrite.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void exhaustedSchedulerConfirmationMustBecomeStickyAndVisibleAtNextIngress()
            throws Exception {
        Fixture fixture = fixture();
        RuntimeException first = new RuntimeException("scheduler-ambiguous");
        when(fixture.committer.filterAndCommit(anyMap()))
                .thenThrow(
                        first,
                        new RuntimeException("retry-1"),
                        new RuntimeException("retry-2"),
                        new RuntimeException("retry-3"));
        fixture.clock.set(100L);
        fixture.write(1);

        fixture.clock.set(1_100L);
        fixture.scheduledTask.get().run();

        Exception visible = assertThrows(Exception.class, () -> fixture.write(2));
        assertSame(first, visible);
        verify(fixture.committer, times(4)).filterAndCommit(anyMap());
        assertTrue(fixture.state().hasPendingCommit());
        assertEquals(0L, fixture.state().committedGeneration());
    }

    private static Fixture fixture() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(100);
        config.setCommitIntervalMs(1_000);
        config.setEnableAsyncCommit(true);
        AtomicLong clock = new AtomicLong();
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        AtomicLong scheduledDelay = new AtomicLong(-1L);
        when(executor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenAnswer(
                        invocation -> {
                            scheduledTask.set(invocation.getArgument(0));
                            scheduledDelay.set(invocation.getArgument(1));
                            ScheduledFuture<?> future = mock(ScheduledFuture.class);
                            when(future.isDone()).thenReturn(false);
                            when(future.isCancelled()).thenReturn(false);
                            return future;
                        });

        PaimonService service =
                new PaimonService(
                        config,
                        mock(Log.class),
                        clock::get,
                        () -> { },
                        () -> executor);
        service.setFlushOffsetCallback(ignored -> { });
        service.startForTest();

        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        when(strategy.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        when(strategy.writeSemanticContract())
                .thenReturn(PaimonWriteSemanticContractTestFactory.forMode(BucketMode.HASH_FIXED));
        when(strategy.prepareCommit(anyLong())).thenReturn(Collections.emptyList());
        when(committer.filterAndCommit(anyMap())).thenReturn(0);
        tableContexts(service)
                .put(
                        "default.t",
                        new PaimonTableWriteContext(
                                "default.t",
                                "t",
                                "stable-user",
                                strategy,
                                committer,
                                null,
                                Collections.emptyList(),
                                0L));
        fieldCache(service).put(
                "default.t", Collections.singletonList(new DataField(0, "id", DataTypes.INT())));
        TapTable tapTable = mock(TapTable.class);
        when(tapTable.getName()).thenReturn("t");
        when(tapTable.primaryKeys(true)).thenReturn(Collections.emptyList());
        TapConnectorContext connectorContext = mock(TapConnectorContext.class);
        when(connectorContext.getStateMap()).thenReturn(mock(KVMap.class));
        when(connectorContext.getLog()).thenReturn(mock(Log.class));
        return new Fixture(
                service,
                strategy,
                committer,
                tapTable,
                connectorContext,
                clock,
                scheduledTask,
                scheduledDelay);
    }

    private static TapInsertRecordEvent event(int id) {
        TapInsertRecordEvent event =
                new TapInsertRecordEvent()
                        .init()
                        .table("t")
                        .after(Collections.singletonMap("id", id));
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        event.addInfo("nodeIds", Collections.singletonList("source-a"));
        return event;
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

    @SuppressWarnings("unchecked")
    private static Map<String, Object> commitLocks(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("commitLocks");
        field.setAccessible(true);
        return (Map<String, Object>) field.get(service);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<Throwable> stickyFailure(PaimonService service)
            throws Exception {
        Field field = PaimonService.class.getDeclaredField("stickyWriteFailure");
        field.setAccessible(true);
        return (AtomicReference<Throwable>) field.get(service);
    }

    private static void setField(PaimonService service, String name, Object value)
            throws Exception {
        Field field = PaimonService.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(service, value);
    }

    private static void awaitBlocked(AtomicReference<Thread> threadReference)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            Thread thread = threadReference.get();
            if (thread != null && thread.getState() == Thread.State.BLOCKED) {
                return;
            }
            if (thread != null && !thread.isAlive()) {
                throw new AssertionError("Scheduler task terminated before reaching the table lock");
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("Timed out waiting for scheduler task to block on the table lock");
    }

    private static final class Fixture {
        private final PaimonService service;
        private final PaimonBucketWriterStrategy strategy;
        private final PaimonTableCommitter committer;
        private final TapTable tapTable;
        private final TapConnectorContext connectorContext;
        private final AtomicLong clock;
        private final AtomicReference<Runnable> scheduledTask;
        private final AtomicLong scheduledDelay;

        private Fixture(
                PaimonService service,
                PaimonBucketWriterStrategy strategy,
                PaimonTableCommitter committer,
                TapTable tapTable,
                TapConnectorContext connectorContext,
                AtomicLong clock,
                AtomicReference<Runnable> scheduledTask,
                AtomicLong scheduledDelay) {
            this.service = service;
            this.strategy = strategy;
            this.committer = committer;
            this.tapTable = tapTable;
            this.connectorContext = connectorContext;
            this.clock = clock;
            this.scheduledTask = scheduledTask;
            this.scheduledDelay = scheduledDelay;
        }

        private void write(int id) throws Exception {
            service.writeRecords(Collections.singletonList(event(id)), tapTable, connectorContext);
        }

        private PaimonMicroBatchCoordinator.TableSnapshot state() throws Exception {
            return coordinator(service).tableSnapshot("default.t");
        }
    }
}
