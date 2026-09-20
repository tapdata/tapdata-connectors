package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.config.PaimonWriteOptions;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PaimonSpillOptionsIntegrationTest {
    @TempDir Path temp;
    private static final Identifier ID = Identifier.create("default", "spill_options");
    private static final String SPILL = CoreOptions.WRITE_BUFFER_SPILLABLE.key();

    @ParameterizedTest
    @ValueSource(strings = {"scan.snapshot-id", "scan.tag-name", "scan.timestamp-millis"})
    void historicalReadOptionMustNotMoveRuntimeWriterBackToOldSchema(String scanKey) throws Exception {
        try (Catalog catalog = catalog()) {
            catalog.createTable(ID, Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id")
                    .option("bucket", "1").option("snapshot.expire.execution-mode", "SYNC").build(), false);
            FileStoreTable original = (FileStoreTable) catalog.getTable(ID);
            StreamWriteBuilder builder = original.newStreamWriteBuilder();
            try (StreamTableWrite write = builder.newWrite(); StreamTableCommit commit = builder.newCommit()) {
                write.write(GenericRow.of(1));
                commit.commit(1L, write.prepareCommit(false, 1L));
            }
            long snapshotId = original.snapshotManager().latestSnapshotId();
            long timestamp = original.snapshotManager().snapshot(snapshotId).timeMillis();
            original.createTag("old", snapshotId);
            String scanValue = scanKey.equals("scan.tag-name") ? "old" :
                    Long.toString(scanKey.equals("scan.snapshot-id") ? snapshotId : timestamp);
            catalog.alterTable(ID, java.util.Arrays.asList(
                    SchemaChange.addColumn("new_column", DataTypes.STRING()),
                    SchemaChange.setOption(scanKey, scanValue)), false);
            catalog.invalidateTable(ID);
            FileStoreTable catalogResolved = (FileStoreTable) catalog.getTable(ID);
            FileStoreTable preserved = PaimonWriteOptions.runtimeWriteTable(catalogResolved, config(false));
            assertEquals(catalogResolved.schema().id(), preserved.schema().id());
            assertEquals(catalogResolved.rowType(), preserved.rowType());
            // Catalog 自身可能已解释读取选项；显式取得带这些选项的当前 schema，
            // 模拟传入写边界的预解析 Table。Spill 覆盖必须保留这个输入 schema。
            FileStoreTable current = ((FileStoreTable) catalog.getTable(ID)).copyWithLatestSchema();
            Map<String, String> before = new HashMap<>(current.options());
            assertEquals(2, current.rowType().getFieldCount());
            // 原版 copy 会切回旧 schema；写入覆盖必须使用不解释时间旅行的公开入口。
            assertEquals(1, current.copy(Collections.singletonMap(SPILL, "false")).rowType().getFieldCount());
            FileStoreTable runtime = PaimonWriteOptions.runtimeWriteTable(current, config(false));
            assertEquals(current.schema().id(), runtime.schema().id());
            assertEquals(current.rowType(), runtime.rowType());
            assertFalse(runtime.coreOptions().writeBufferSpillable());
            assertEquals(scanValue, runtime.options().get(scanKey));
            catalog.invalidateTable(ID);
            assertEquals(before, catalog.getTable(ID).options());
        }
    }

    @ParameterizedTest
    @MethodSource("bucketSchemas")
    void runtimeCopyPreservesNativeAsyncModeAndAllBucketContracts(Schema schema) throws Exception {
        try (Catalog catalog = catalog()) {
            catalog.createTable(ID, schema, false);
            FileStoreTable original = (FileStoreTable) catalog.getTable(ID);
            Map<String, String> before = new HashMap<>(original.options());
            FileStoreTable runtime = PaimonWriteOptions.runtimeWriteTable(original, config(false));
            Map<String, String> normalized = new HashMap<>(original.copyWithoutTimeTravel(Collections.emptyMap()).options());
            normalized.put(SPILL, "false");
            assertEquals(normalized, runtime.options());
            assertFalse(runtime.coreOptions().writeBufferSpillable());
            assertEquals(CoreOptions.ExpireExecutionMode.ASYNC, runtime.coreOptions().snapshotExpireExecutionMode());
            assertEquals(original.rowType(), runtime.rowType());
            assertEquals(original.primaryKeys(), runtime.primaryKeys());
            assertEquals(original.partitionKeys(), runtime.partitionKeys());
            assertEquals(original.bucketMode(), runtime.bucketMode());
            assertEquals(original.location(), runtime.location());
            assertEquals(original.schema().id(), runtime.schema().id());
            assertEquals(before, original.options());
            catalog.invalidateTable(ID);
            assertEquals(before, catalog.getTable(ID).options());
        }
    }

    private static java.util.stream.Stream<Schema> bucketSchemas() {
        return java.util.stream.Stream.of("fixed", "hash_dynamic", "key_dynamic", "append", "postpone")
                .map(mode -> {
                    Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT())
                            .column("pt", DataTypes.INT()).partitionKeys("pt")
                            .option(SPILL, "true").option("snapshot.expire.execution-mode", "ASYNC");
                    if (!mode.equals("append")) {
                        if (mode.equals("key_dynamic")) {
                            schema.primaryKey("id");
                        } else {
                            schema.primaryKey("id", "pt");
                        }
                    }
                    return schema.option("bucket", mode.equals("fixed") ? "1" :
                            mode.equals("postpone") ? "-2" : "-1").build();
                });
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(booleans = {false, true})
    void newTableSwitchOverridesConflictingPropertiesAndCatalogDefault(Boolean enabled)
            throws Exception {
        boolean expected = Boolean.TRUE.equals(enabled);
        Catalog catalog = catalog();
        PaimonConfig config = config(enabled);
        LinkedHashMap<String, String> property = new LinkedHashMap<>();
        property.put("propKey", SPILL);
        property.put("propValue", Boolean.toString(!expected));
        config.setTableProperties(Collections.singletonList(property));
        PaimonService service = service(config, catalog);
        try {
            assertTrue(service.createTable(tapTable()));
            catalog.invalidateTable(ID);
            FileStoreTable table = (FileStoreTable) catalog.getTable(ID);
            assertEquals(expected, table.coreOptions().writeBufferSpillable());
            assertEquals(Boolean.toString(expected), table.options().get(SPILL));
            assertEquals(Boolean.toString(!expected), property.get("propValue"));
        } finally {
            service.close();
        }
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(booleans = {false, true})
    void existingTableUsesRuntimeCopyBeforeCreatingWriterWithoutChangingMetadata(Boolean enabled)
            throws Exception {
        boolean expected = Boolean.TRUE.equals(enabled);
        Catalog actual = catalog();
        actual.createTable(ID, Schema.newBuilder().column("id", DataTypes.INT()).primaryKey("id")
                .option("bucket", "1").option(SPILL, Boolean.toString(!expected))
                .option("write-buffer-spill.max-disk-size", "7 mb")
                .option("snapshot.expire.execution-mode", "ASYNC").build(), false);
        FileStoreTable original = spy((FileStoreTable) actual.getTable(ID));
        Map<String, String> before = new HashMap<>(original.options());
        AtomicReference<FileStoreTable> runtime = new AtomicReference<>();
        doAnswer(call -> {
            FileStoreTable copy = spy((FileStoreTable) call.callRealMethod());
            runtime.set(copy);
            return copy;
        }).when(original).copyWithoutTimeTravel(anyMap());
        Catalog catalog = spy(actual);
        doReturn(original).when(catalog).getTable(ID);
        PaimonService service = service(config(enabled), catalog);
        try {
            TapInsertRecordEvent event = new TapInsertRecordEvent().init().table(ID.getObjectName())
                    .after(Collections.singletonMap("id", 1));
            event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
            event.addInfo("nodeIds", Collections.singletonList("source"));
            service.writeRecords(Collections.singletonList(event), tapTable(), context());
            assertNotNull(runtime.get(), "已有表必须在创建 Writer 前生成 runtime Table");
            verify(runtime.get()).newStreamWriteBuilder();
            verify(original, never()).newStreamWriteBuilder();
            verify(original).copyWithoutTimeTravel(Collections.singletonMap(SPILL, Boolean.toString(expected)));
            assertEquals(expected, runtime.get().coreOptions().writeBufferSpillable());
            assertEquals(original.schema().id(), runtime.get().schema().id());
            assertEquals(original.location(), runtime.get().location());
            assertEquals(original.rowType(), runtime.get().rowType());
            assertEquals(original.bucketMode(), runtime.get().bucketMode());
            assertEquals(7 * 1024 * 1024L, runtime.get().coreOptions().writeBufferSpillDiskSize().getBytes());
            assertEquals(before, original.options());
            actual.invalidateTable(ID);
            assertEquals(before, actual.getTable(ID).options());
            assertNotNull(((FileStoreTable) actual.getTable(ID)).snapshotManager().latestSnapshotId());
        } finally {
            service.close();
        }
    }

    private Catalog catalog() throws Exception {
        Options options = new Options();
        options.set("warehouse", temp.resolve("warehouse").toString());
        options.set("table-default." + SPILL, "true");
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase(ID.getDatabaseName(), true);
        return catalog;
    }

    private PaimonConfig config(Boolean enabled) {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase(ID.getDatabaseName());
        config.setWarehouse(temp.resolve("warehouse").toString());
        config.setStorageType("local");
        config.setBucketMode("fixed");
        config.setBucketCount(1);
        config.setHashKey(false);
        config.setEnableAutoCompaction(null);
        config.setDiskOverflowWrite(enabled);
        config.setDiskTmpDir(temp.resolve("spill").toString());
        config.setEnableAsyncCommit(false);
        config.setBatchAccumulationSize(0);
        config.setCommitIntervalMs(0);
        return config;
    }

    private PaimonService service(PaimonConfig config, Catalog catalog) throws Exception {
        PaimonService service = new PaimonService(config, mock(Log.class));
        Field field = PaimonService.class.getDeclaredField("catalog");
        field.setAccessible(true);
        field.set(service, catalog);
        service.startForTest();
        return service;
    }

    private TapTable tapTable() {
        return new TapTable(ID.getObjectName()).add(new TapField("id", "INT").primaryKeyPos(1));
    }

    @SuppressWarnings("unchecked")
    private TapConnectorContext context() {
        TapConnectorContext context = mock(TapConnectorContext.class);
        Map<String, Object> values = new java.util.concurrent.ConcurrentHashMap<>();
        KVMap<Object> state = mock(KVMap.class);
        when(state.get(anyString())).thenAnswer(call -> values.get(call.getArgument(0)));
        when(state.putIfAbsent(anyString(), any())).thenAnswer(
                call -> values.putIfAbsent(call.getArgument(0), call.getArgument(1)));
        doAnswer(call -> {
            values.put(call.getArgument(0), call.getArgument(1));
            return null;
        }).when(state).put(anyString(), any());
        when(context.getStateMap()).thenReturn(state);
        when(context.getLog()).thenReturn(mock(Log.class));
        return context;
    }
}
