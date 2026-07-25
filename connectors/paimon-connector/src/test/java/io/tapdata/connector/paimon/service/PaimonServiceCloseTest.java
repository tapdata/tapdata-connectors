package io.tapdata.connector.paimon.service;

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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonServiceCloseTest {

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
        verify(table.committer, times(1)).filterAndCommit(anyMap());
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
        verify(healthy.committer, times(1)).filterAndCommit(anyMap());
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
    void interruptedRetryDuringCloseMustFinishCleanupAndRestoreInterruptStatus()
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
        when(table.committer.filterAndCommit(anyMap()))
                .thenThrow(ambiguous)
                .thenReturn(0);

        try {
            InterruptedException thrown =
                    assertThrows(InterruptedException.class, service::close);

            assertEquals("close interrupted", thrown.getMessage());
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(2, waits.get());
            verify(table.strategy, times(1)).prepareCommit(0L);
            verify(table.committer, times(2)).filterAndCommit(anyMap());
            verify(table.strategy, times(1)).close();
            verify(table.committer, times(1)).close();
            assertEquals(PaimonServiceLifecycle.State.CLOSED, lifecycle(service).state());
        } finally {
            // Do not leak the deliberately restored interrupt flag into the JUnit worker.
            Thread.interrupted();
        }
    }

    private static PaimonService service() {
        PaimonService service =
                new PaimonService(config(), mock(Log.class), () -> 100L, () -> { });
        service.startForTest();
        return service;
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
