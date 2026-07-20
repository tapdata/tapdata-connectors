package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
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

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonServiceInitialSyncPendingTest {

    @Test
    void concurrentSourceIngressMustFenceInsteadOfUsingThreadOrder() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(0);
        PaimonService service = new PaimonService(config, mock(Log.class));

        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        CountDownLatch firstWriteEntered = new CountDownLatch(1);
        CountDownLatch allowFirstWriteToFinish = new CountDownLatch(1);
        doAnswer(invocation -> {
            firstWriteEntered.countDown();
            assertTrue(allowFirstWriteToFinish.await(5, TimeUnit.SECONDS));
            return null;
        }).when(strategy).write(any());
        when(strategy.bucketMode()).thenReturn(BucketMode.KEY_DYNAMIC);
        when(strategy.requiredRoutingFields()).thenReturn(Collections.emptySet());
        PaimonTableWriteContext context =
                new PaimonTableWriteContext(
                        "default.t",
                        "t",
                        "stable-user",
                        strategy,
                        committer,
                        null,
                        Collections.emptyList(),
                        0L);
        tableContexts(service).put("default.t", context);
        fieldCache(service).put(
                "default.t", Collections.singletonList(new DataField(0, "id", DataTypes.INT())));

        TapTable tapTable = mock(TapTable.class);
        when(tapTable.getName()).thenReturn("t");
        when(tapTable.primaryKeys(true)).thenReturn(Collections.emptyList());
        TapConnectorContext connectorContext = mock(TapConnectorContext.class);
        when(connectorContext.getStateMap()).thenReturn(mock(KVMap.class));
        when(connectorContext.getLog()).thenReturn(mock(Log.class));
        TapInsertRecordEvent event =
                new TapInsertRecordEvent()
                        .init()
                        .table("t")
                        .after(Collections.singletonMap("id", 1));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> first = executor.submit(() -> service.writeRecords(
                    Collections.singletonList(event), tapTable, connectorContext));
            assertTrue(firstWriteEntered.await(5, TimeUnit.SECONDS));

            IllegalStateException overlap = assertThrows(
                    IllegalStateException.class,
                    () -> service.writeRecords(
                            Collections.singletonList(event), tapTable, connectorContext));
            assertTrue(overlap.getMessage().contains("Concurrent Paimon source ingress"));

            allowFirstWriteToFinish.countDown();
            ExecutionException firstFailure = assertThrows(
                    ExecutionException.class,
                    () -> first.get(5, TimeUnit.SECONDS));
            assertTrue(firstFailure.getCause() instanceof IllegalStateException);
            assertTrue(firstFailure.getCause().getMessage().contains("fenced after an ingress failure"));
            verify(strategy, times(1)).write(any());

            IllegalStateException fenced = assertThrows(
                    IllegalStateException.class,
                    () -> service.writeRecords(
                            Collections.singletonList(event), tapTable, connectorContext));
            assertTrue(fenced.getMessage().contains("fenced after an ingress failure"));
        } finally {
            allowFirstWriteToFinish.countDown();
            executor.shutdownNow();
            context.close();
        }
    }

    @Test
    void differentDynamicTablesMustKeepIndependentIngressConcurrency() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(0);
        PaimonService service = new PaimonService(config, mock(Log.class));

        PaimonBucketWriterStrategy strategy1 = mock(PaimonBucketWriterStrategy.class);
        PaimonBucketWriterStrategy strategy2 = mock(PaimonBucketWriterStrategy.class);
        CountDownLatch table1Entered = new CountDownLatch(1);
        CountDownLatch releaseTable1 = new CountDownLatch(1);
        doAnswer(invocation -> {
            table1Entered.countDown();
            assertTrue(releaseTable1.await(5, TimeUnit.SECONDS));
            return null;
        }).when(strategy1).write(any());
        when(strategy1.bucketMode()).thenReturn(BucketMode.HASH_DYNAMIC);
        when(strategy2.bucketMode()).thenReturn(BucketMode.KEY_DYNAMIC);
        when(strategy1.requiredRoutingFields()).thenReturn(Collections.emptySet());
        when(strategy2.requiredRoutingFields()).thenReturn(Collections.emptySet());
        PaimonTableWriteContext context1 = new PaimonTableWriteContext(
                "default.t1", "t1", "user-1", strategy1,
                mock(PaimonTableCommitter.class), null, Collections.emptyList(), 0L);
        PaimonTableWriteContext context2 = new PaimonTableWriteContext(
                "default.t2", "t2", "user-2", strategy2,
                mock(PaimonTableCommitter.class), null, Collections.emptyList(), 0L);
        tableContexts(service).put("default.t1", context1);
        tableContexts(service).put("default.t2", context2);
        fieldCache(service).put(
                "default.t1", Collections.singletonList(new DataField(0, "id", DataTypes.INT())));
        fieldCache(service).put(
                "default.t2", Collections.singletonList(new DataField(0, "id", DataTypes.INT())));

        TapTable tapTable1 = mock(TapTable.class);
        TapTable tapTable2 = mock(TapTable.class);
        when(tapTable1.getName()).thenReturn("t1");
        when(tapTable2.getName()).thenReturn("t2");
        when(tapTable1.primaryKeys(true)).thenReturn(Collections.emptyList());
        when(tapTable2.primaryKeys(true)).thenReturn(Collections.emptyList());
        KVMap<Object> stateMap = mock(KVMap.class);
        TapConnectorContext connectorContext = mock(TapConnectorContext.class);
        when(connectorContext.getStateMap()).thenReturn(stateMap);
        when(connectorContext.getLog()).thenReturn(mock(Log.class));
        TapInsertRecordEvent event1 = new TapInsertRecordEvent().init().table("t1")
                .after(Collections.singletonMap("id", 1));
        TapInsertRecordEvent event2 = new TapInsertRecordEvent().init().table("t2")
                .after(Collections.singletonMap("id", 2));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> first = executor.submit(() -> service.writeRecords(
                    Collections.singletonList(event1), tapTable1, connectorContext));
            assertTrue(table1Entered.await(5, TimeUnit.SECONDS));
            service.writeRecords(Collections.singletonList(event2), tapTable2, connectorContext);
            releaseTable1.countDown();
            first.get(5, TimeUnit.SECONDS);
            verify(strategy1, times(1)).write(any());
            verify(strategy2, times(1)).write(any());
        } finally {
            releaseTable1.countDown();
            executor.shutdownNow();
            context1.close();
            context2.close();
        }
    }

    @Test
    void historicalPendingCommitMustNotReplayCurrentInitialSyncBatch() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(0);
        PaimonService service = new PaimonService(config, mock(Log.class));

        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        when(strategy.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        when(strategy.requiredRoutingFields()).thenReturn(Collections.emptySet());
        PaimonTableWriteContext context =
                new PaimonTableWriteContext(
                        "default.t",
                        "t",
                        "stable-user",
                        strategy,
                        committer,
                        null,
                        Collections.emptyList(),
                        0L);
        when(strategy.prepareCommit(0L)).thenReturn(Collections.emptyList());
        when(committer.filterAndCommit(anyMap()))
                .thenThrow(new RuntimeException("ambiguous"))
                .thenReturn(0);
        assertThrows(RuntimeException.class, context::commit);

        tableContexts(service).put("default.t", context);
        fieldCache(service).put(
                "default.t", Collections.singletonList(new DataField(0, "id", DataTypes.INT())));

        TapTable tapTable = mock(TapTable.class);
        when(tapTable.getName()).thenReturn("t");
        when(tapTable.primaryKeys(true)).thenReturn(Collections.emptyList());
        TapConnectorContext connectorContext = mock(TapConnectorContext.class);
        when(connectorContext.getStateMap()).thenReturn(mock(KVMap.class));
        when(connectorContext.getLog()).thenReturn(mock(Log.class));
        TapInsertRecordEvent event =
                new TapInsertRecordEvent()
                        .init()
                        .table("t")
                        .after(Collections.singletonMap("id", 1));

        service.writeRecords(Collections.singletonList(event), tapTable, connectorContext);

        verify(strategy, times(1)).write(any());
        assertFalse(context.hasPendingCommit());
        assertEquals(1, accumulatedCounts(service).get("default.t").get());
        context.close();
    }

    @Test
    void historicalPendingAndCurrentAmbiguousCdcCommitMustNotReplayCurrentBatch() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(0);
        PaimonService service = new PaimonService(config, mock(Log.class));

        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        when(strategy.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        when(strategy.requiredRoutingFields()).thenReturn(Collections.emptySet());
        PaimonTableWriteContext context =
                new PaimonTableWriteContext(
                        "default.t",
                        "t",
                        "stable-user",
                        strategy,
                        committer,
                        null,
                        Collections.emptyList(),
                        0L);
        when(strategy.prepareCommit(0L)).thenReturn(Collections.emptyList());
        when(strategy.prepareCommit(1L)).thenReturn(Collections.emptyList());
        when(committer.filterAndCommit(anyMap()))
                .thenThrow(new RuntimeException("historical commit ambiguous"))
                .thenReturn(0)
                .thenThrow(new RuntimeException("current commit ambiguous"))
                .thenReturn(0);
        assertThrows(RuntimeException.class, context::commit);

        tableContexts(service).put("default.t", context);
        fieldCache(service).put(
                "default.t", Collections.singletonList(new DataField(0, "id", DataTypes.INT())));

        TapTable tapTable = mock(TapTable.class);
        when(tapTable.getName()).thenReturn("t");
        when(tapTable.primaryKeys(true)).thenReturn(Collections.emptyList());
        TapConnectorContext connectorContext = mock(TapConnectorContext.class);
        when(connectorContext.getStateMap()).thenReturn(mock(KVMap.class));
        when(connectorContext.getLog()).thenReturn(mock(Log.class));
        TapInsertRecordEvent event =
                new TapInsertRecordEvent()
                        .init()
                        .table("t")
                        .after(Collections.singletonMap("id", 1));
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");

        service.writeRecords(Collections.singletonList(event), tapTable, connectorContext);

        verify(strategy, times(1)).write(any());
        verify(strategy, times(1)).prepareCommit(1L);
        assertFalse(context.hasPendingCommit());
        assertEquals(0, accumulatedCounts(service).get("default.t").get());
        context.close();
    }

    @Test
    void pollutedDynamicTableCreationFailureMustNotBecomeRetryable() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        PaimonService service = new PaimonService(config, mock(Log.class));
        Catalog catalog = mock(Catalog.class);
        PaimonDynamicBucketPollutedException polluted =
                new PaimonDynamicBucketPollutedException(
                        "default.t", new IllegalStateException("data contains duplicates"));
        when(catalog.getTable(any())).thenThrow(polluted);
        setCatalog(service, catalog);

        TapTable tapTable = mock(TapTable.class);
        when(tapTable.getName()).thenReturn("t");
        when(tapTable.primaryKeys(true)).thenReturn(Collections.emptyList());
        TapConnectorContext connectorContext = mock(TapConnectorContext.class);
        when(connectorContext.getStateMap()).thenReturn(mock(KVMap.class));
        when(connectorContext.getLog()).thenReturn(mock(Log.class));
        TapInsertRecordEvent event =
                new TapInsertRecordEvent()
                        .init()
                        .table("t")
                        .after(Collections.singletonMap("id", 1));

        PaimonDynamicBucketPollutedException thrown = assertThrows(
                PaimonDynamicBucketPollutedException.class,
                () -> service.writeRecords(
                        Collections.singletonList(event), tapTable, connectorContext));
        assertEquals(polluted, thrown);
    }

    @Test
    void missingDynamicRoutingFieldMustBeFatalWithoutWritingOrRetrying() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        PaimonService service = new PaimonService(config, mock(Log.class));

        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        when(strategy.bucketMode()).thenReturn(BucketMode.KEY_DYNAMIC);
        when(strategy.requiredRoutingFields())
                .thenReturn(new java.util.LinkedHashSet<>(java.util.Arrays.asList("id", "pt")));
        PaimonTableWriteContext context =
                new PaimonTableWriteContext(
                        "default.t",
                        "t",
                        "stable-user",
                        strategy,
                        mock(PaimonTableCommitter.class),
                        null,
                        Collections.emptyList(),
                        0L);
        tableContexts(service).put("default.t", context);
        fieldCache(service).put(
                "default.t",
                java.util.Arrays.asList(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "pt", DataTypes.INT())));

        TapTable tapTable = mock(TapTable.class);
        when(tapTable.getName()).thenReturn("t");
        when(tapTable.primaryKeys(true)).thenReturn(Collections.singletonList("id"));
        TapConnectorContext connectorContext = mock(TapConnectorContext.class);
        when(connectorContext.getStateMap()).thenReturn(mock(KVMap.class));
        when(connectorContext.getLog()).thenReturn(mock(Log.class));
        TapInsertRecordEvent event =
                new TapInsertRecordEvent()
                        .init()
                        .table("t")
                        .after(Collections.singletonMap("id", 1));

        PaimonFatalWriteException thrown = assertThrows(
                PaimonFatalWriteException.class,
                () -> service.writeRecords(
                        Collections.singletonList(event), tapTable, connectorContext));
        assertTrue(thrown.getMessage().contains("routing field 'pt'"));
        verify(strategy, times(0)).write(any());
        context.close();
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

    @SuppressWarnings("unchecked")
    private static Map<String, AtomicInteger> accumulatedCounts(PaimonService service)
            throws Exception {
        Field field = PaimonService.class.getDeclaredField("accumulatedRecordCount");
        field.setAccessible(true);
        return (Map<String, AtomicInteger>) field.get(service);
    }

    private static void setCatalog(PaimonService service, Catalog catalog) throws Exception {
        Field field = PaimonService.class.getDeclaredField("catalog");
        field.setAccessible(true);
        field.set(service, catalog);
    }
}
