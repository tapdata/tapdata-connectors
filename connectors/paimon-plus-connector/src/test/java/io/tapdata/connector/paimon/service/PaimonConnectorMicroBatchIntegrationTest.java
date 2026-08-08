package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.commit.PaimonServiceLifecycle;

import io.tapdata.connector.paimon.PaimonConnector;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.TapCallbackOffset;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonConnectorMicroBatchIntegrationTest {

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void twoTablesAndTwoSourceLanesMustReleaseOffsetsOnlyAfterRealSnapshots()
            throws Throwable {
        PaimonConfig config = config();
        AtomicLong clock = new AtomicLong(100L);
        AtomicReference<Runnable> scheduledTask = new AtomicReference<>();
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        when(executor.schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS)))
                .thenAnswer(
                        invocation -> {
                            scheduledTask.set(invocation.getArgument(0));
                            ScheduledFuture<?> future = mock(ScheduledFuture.class);
                            when(future.isDone()).thenReturn(false);
                            when(future.isCancelled()).thenReturn(false);
                            return future;
                        });
        doAnswer(
                        invocation -> {
                            ((Runnable) invocation.getArgument(0)).run();
                            return null;
                        })
                .when(executor)
                .execute(any(Runnable.class));
        when(executor.awaitTermination(5L, TimeUnit.SECONDS)).thenReturn(true);
        PaimonService service =
                new PaimonService(
                        config,
                        mock(Log.class),
                        clock::get,
                        () -> { },
                        ignored -> executor);
        List<TapCallbackOffset> callbacks = new ArrayList<>();
        AtomicBoolean callbackRunning = new AtomicBoolean();
        service.setFlushOffsetCallback(
                payload -> {
                    if (!callbackRunning.compareAndSet(false, true)) {
                        throw new AssertionError("offset callback executed concurrently");
                    }
                    try {
                        callbacks.add((TapCallbackOffset) payload);
                    } finally {
                        callbackRunning.set(false);
                    }
                });
        service.init();
        TestConnector connector = new TestConnector();
        setService(connector, service);
        TapConnectorContext context = connectorContext();
        TapTable tableA = table("orders_a");
        TapTable tableB = table("orders_b");

        try {
            service.createTable(tableA);
            service.createTable(tableB);

            service.writeRecords(
                    Collections.singletonList(event("orders_a", 1, "source-a")),
                    tableA,
                    context);
            service.writeRecords(
                    Collections.singletonList(event("orders_b", 1, "source-a")),
                    tableB,
                    context);
            connector.forward(context, heartbeat("offset-a", "source-a", 10L));

            assertEquals(0, callbacks.size());
            assertNull(table(service, "orders_a").snapshotManager().latestSnapshotIdFromFileSystem());
            assertNull(table(service, "orders_b").snapshotManager().latestSnapshotIdFromFileSystem());

            service.writeRecords(
                    Collections.singletonList(event("orders_a", 2, "source-b")),
                    tableA,
                    context);
            assertEquals(0, callbacks.size());
            assertNull(table(service, "orders_a").snapshotManager().latestSnapshotIdFromFileSystem());
            assertNull(table(service, "orders_b").snapshotManager().latestSnapshotIdFromFileSystem());

            service.writeRecords(
                    Collections.singletonList(event("orders_b", 2, "source-b")),
                    tableB,
                    context);
            connector.forward(context, heartbeat("offset-b", "source-b", 20L));

            assertEquals(0, callbacks.size());
            clock.set(1_100L);
            scheduledTask.get().run();

            assertEquals(2, callbacks.size());
            assertEquals(
                    "offset-a",
                    callbacks.get(0).get(TapCallbackOffset.KEY_STREAM_OFFSET));
            assertEquals(
                    "offset-b",
                    callbacks.get(1).get(TapCallbackOffset.KEY_STREAM_OFFSET));
            assertEquals(10L, callbacks.get(0).get(TapCallbackOffset.KEY_EVENT_TIME));
            assertEquals(20L, callbacks.get(1).get(TapCallbackOffset.KEY_EVENT_TIME));
            assertEquals("CDC", callbacks.get(0).get(TapCallbackOffset.KEY_SYNC_STAGE));
            assertEquals("CDC", callbacks.get(1).get(TapCallbackOffset.KEY_SYNC_STAGE));
            assertEquals(10L, callbacks.get(0).get(TapCallbackOffset.KEY_SOURCE_TIME));
            assertEquals(20L, callbacks.get(1).get(TapCallbackOffset.KEY_SOURCE_TIME));
            assertEquals(
                    Collections.singletonList("source-a"),
                    callbacks.get(0).get(TapCallbackOffset.KEY_NODE_IDS));
            assertEquals(
                    Collections.singletonList("source-b"),
                    callbacks.get(1).get(TapCallbackOffset.KEY_NODE_IDS));
            assertEquals(2, rowCount(table(service, "orders_a")));
            assertEquals(2, rowCount(table(service, "orders_b")));

            TapConnectionContext stopContext = mock(TapConnectionContext.class);
            when(stopContext.getLog()).thenReturn(mock(Log.class));
            connector.onStop(stopContext);
        } finally {
            if (lifecycle(service).state() != PaimonServiceLifecycle.State.CLOSED) {
                service.close();
            }
        }
    }

    private PaimonConfig config() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse(Files.createDirectories(tempDir.resolve("warehouse")).toString());
        config.setDiskTmpDir(Files.createDirectories(tempDir.resolve("spill")).toString());
        config.setStorageType("local");
        config.setDatabase("default");
        config.setBucketMode("fixed");
        config.setBucketCount(2);
        config.setEnableAutoCompaction(false);
        config.setBatchAccumulationSize(100);
        config.setCommitIntervalMs(1_000);
        config.setEnableAsyncCommit(true);
        config.setWriteBufferSize(8);
        return config;
    }

    private static TapTable table(String name) {
        return new TapTable(name)
                .add(new TapField("id", "INT").primaryKeyPos(1))
                .add(new TapField("value", "STRING"));
    }

    private static TapInsertRecordEvent event(String table, int id, String sourceLane) {
        Map<String, Object> after = new java.util.LinkedHashMap<>();
        after.put("id", id);
        after.put("value", "v" + id);
        TapInsertRecordEvent event =
                new TapInsertRecordEvent()
                        .init()
                        .table(table)
                        .after(after);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        event.addInfo(TapCallbackOffset.KEY_NODE_IDS, Collections.singletonList(sourceLane));
        event.addInfo(TapCallbackOffset.KEY_STREAM_OFFSET, table + '-' + id);
        event.addInfo(TapCallbackOffset.KEY_SOURCE_TIME, (long) id);
        return event;
    }

    private static HeartbeatEvent heartbeat(Object offset, String sourceLane, Long eventTime) {
        HeartbeatEvent heartbeat = new HeartbeatEvent().init().referenceTime(eventTime);
        heartbeat.addInfo(TapCallbackOffset.KEY_SYNC_STAGE, "CDC");
        heartbeat.addInfo(TapCallbackOffset.KEY_STREAM_OFFSET, offset);
        heartbeat.addInfo(TapCallbackOffset.KEY_SOURCE_TIME, eventTime);
        heartbeat.addInfo(
                TapCallbackOffset.KEY_NODE_IDS, Collections.singletonList(sourceLane));
        return heartbeat;
    }

    @SuppressWarnings("unchecked")
    private static TapConnectorContext connectorContext() {
        Map<String, Object> values = new ConcurrentHashMap<>();
        KVMap<Object> stateMap = mock(KVMap.class);
        when(stateMap.get(anyString()))
                .thenAnswer(invocation -> values.get(invocation.getArgument(0)));
        when(stateMap.putIfAbsent(anyString(), any()))
                .thenAnswer(
                        invocation ->
                                values.putIfAbsent(
                                        invocation.getArgument(0),
                                        invocation.getArgument(1)));
        doAnswer(
                        invocation -> {
                            values.put(invocation.getArgument(0), invocation.getArgument(1));
                            return null;
                        })
                .when(stateMap)
                .put(anyString(), any());
        TapConnectorContext context = mock(TapConnectorContext.class);
        when(context.getStateMap()).thenReturn(stateMap);
        when(context.getLog()).thenReturn(mock(Log.class));
        return context;
    }

    private static FileStoreTable table(PaimonService service, String tableName)
            throws Exception {
        return (FileStoreTable)
                catalog(service).getTable(Identifier.create("default", tableName));
    }

    private static int rowCount(FileStoreTable table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        int count = 0;
        try (RecordReader<InternalRow> reader =
                readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    while (batch.next() != null) {
                        count++;
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        return count;
    }

    private static Catalog catalog(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("catalog");
        field.setAccessible(true);
        return (Catalog) field.get(service);
    }

    private static PaimonServiceLifecycle lifecycle(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("lifecycle");
        field.setAccessible(true);
        return (PaimonServiceLifecycle) field.get(service);
    }

    private static void setService(PaimonConnector connector, PaimonService service)
            throws Exception {
        Field field = PaimonConnector.class.getDeclaredField("paimonService");
        field.setAccessible(true);
        field.set(connector, service);
    }

    private static final class TestConnector extends PaimonConnector {
        private void forward(TapConnectorContext context, HeartbeatEvent heartbeat)
                throws Throwable {
            processControl(context, heartbeat);
        }
    }
}
