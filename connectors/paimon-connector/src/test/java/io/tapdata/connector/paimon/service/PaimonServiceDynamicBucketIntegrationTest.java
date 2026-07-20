package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonServiceDynamicBucketIntegrationTest {

    private static final String DATABASE = "default";

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void syntheticHashKeyMustWriteThroughHashDynamicWithoutMutatingSource() throws Exception {
        PaimonConfig config = config("synthetic-hash-dynamic");
        config.setHashKey(true);
        KVMap<Object> stateMap = stateMap();
        TapTable tapTable = syntheticHashTapTable("synthetic_hash_t", false);
        Map<String, Object> sourceData = syntheticHashData("value");
        Map<String, Object> originalSourceData = new LinkedHashMap<>(sourceData);

        PaimonService service = service(config);
        try {
            createSyntheticHashDynamicTable(catalog(service), "synthetic_hash_t");
            service.writeRecords(
                    Collections.singletonList(cdcInsert("synthetic_hash_t", sourceData)),
                    tapTable,
                    context(stateMap));

            List<InternalRow> rows =
                    readRows(
                            catalog(service)
                                    .getTable(
                                            Identifier.create(
                                                    DATABASE, "synthetic_hash_t")));
            assertEquals(1, rows.size());
            assertEquals(
                    service.toHash(tapTable.primaryKeys(true), sourceData),
                    rows.get(0).getString(1).toString());
            assertEquals(originalSourceData, sourceData);
            assertFalse(sourceData.containsKey("_hash_key"));
        } finally {
            service.close();
        }
    }

    @Test
    void syntheticHashKeyInitialSyncMustCommitOnlyAfterInitialSyncCallback() throws Exception {
        PaimonConfig config = config("synthetic-hash-initial");
        config.setHashKey(true);
        KVMap<Object> stateMap = stateMap();
        TapTable tapTable = syntheticHashTapTable("synthetic_initial_t", false);
        Map<String, Object> sourceData = syntheticHashData("initial");

        PaimonService service = service(config);
        try {
            createSyntheticHashDynamicTable(catalog(service), "synthetic_initial_t");
            TapConnectorContext connectorContext = context(stateMap);
            TapInsertRecordEvent event =
                    new TapInsertRecordEvent()
                            .init()
                            .table("synthetic_initial_t")
                            .after(sourceData);

            service.writeRecords(
                    Collections.singletonList(event), tapTable, connectorContext);
            FileStoreTable targetTable =
                    (FileStoreTable)
                            catalog(service)
                                    .getTable(
                                            Identifier.create(
                                                    DATABASE, "synthetic_initial_t"));
            assertNull(targetTable.snapshotManager().latestSnapshotIdFromFileSystem());

            service.afterInitialSync(connectorContext, tapTable);

            assertEquals(1L, targetTable.snapshotManager().latestSnapshotIdFromFileSystem());
            List<InternalRow> rows = readRows(targetTable);
            assertEquals(1, rows.size());
            assertEquals(
                    service.toHash(tapTable.primaryKeys(true), sourceData),
                    rows.get(0).getString(1).toString());
        } finally {
            service.close();
        }
    }

    @Test
    void syntheticHashKeyMustSupportKeyDynamicUpdateDeleteAndNullPartition()
            throws Exception {
        PaimonConfig config = config("synthetic-key-dynamic");
        config.setHashKey(true);
        KVMap<Object> stateMap = stateMap();
        TapTable tapTable = syntheticHashTapTable("synthetic_key_t", true);

        PaimonService service = service(config);
        try {
            createSyntheticKeyDynamicTable(catalog(service), "synthetic_key_t");
            TapConnectorContext connectorContext = context(stateMap);
            Map<String, Object> inserted = syntheticHashData("old", 1);
            Map<String, Object> moved = syntheticHashData("moved", 2);
            service.writeRecords(
                    Collections.singletonList(cdcInsert("synthetic_key_t", inserted)),
                    tapTable,
                    connectorContext);
            service.writeRecords(
                    Collections.singletonList(
                            cdcUpdate("synthetic_key_t", inserted, moved)),
                    tapTable,
                    connectorContext);

            List<InternalRow> movedRows =
                    readRows(
                            catalog(service)
                                    .getTable(
                                            Identifier.create(
                                                    DATABASE, "synthetic_key_t")));
            assertEquals(1, movedRows.size());
            assertEquals(2, movedRows.get(0).getInt(0));
            assertEquals("moved", movedRows.get(0).getString(1).toString());
            assertEquals(
                    service.toHash(tapTable.primaryKeys(true), moved),
                    movedRows.get(0).getString(2).toString());

            service.writeRecords(
                    Collections.singletonList(cdcDelete("synthetic_key_t", moved)),
                    tapTable,
                    connectorContext);
            assertEquals(
                    0,
                    readRows(
                                    catalog(service)
                                            .getTable(
                                                    Identifier.create(
                                                            DATABASE,
                                                            "synthetic_key_t")))
                            .size());

            Map<String, Object> nullPartition = syntheticHashData("null-partition", null);
            service.writeRecords(
                    Collections.singletonList(
                            cdcInsert("synthetic_key_t", nullPartition)),
                    tapTable,
                    connectorContext);
            List<InternalRow> nullPartitionRows =
                    readRows(
                            catalog(service)
                                    .getTable(
                                            Identifier.create(
                                                    DATABASE, "synthetic_key_t")));
            assertEquals(1, nullPartitionRows.size());
            assertTrue(nullPartitionRows.get(0).isNullAt(0));
            assertEquals(
                    service.toHash(tapTable.primaryKeys(true), nullPartition),
                    nullPartitionRows.get(0).getString(2).toString());
        } finally {
            service.close();
        }
    }

    @Test
    void hashDynamicShouldRemainUniqueAcrossRealServiceRestart() throws Exception {
        PaimonConfig config = config("hash-service");
        KVMap<Object> stateMap = stateMap();
        TapTable tapTable = new TapTable("hash_t")
                // Deliberately differ from the target Paimon RowType order.
                .add(new TapField("value", "STRING"))
                .add(new TapField("id", "INT").primaryKeyPos(1));

        PaimonService first = service(config);
        try {
            createHashDynamicTable(catalog(first), "hash_t");
            first.writeRecords(
                    Collections.singletonList(cdcInsert("hash_t", map("value", "v1", "id", 1))),
                    tapTable,
                    context(stateMap));
        } finally {
            first.close();
        }

        PaimonService restarted = service(config);
        try {
            restarted.writeRecords(
                    Collections.singletonList(cdcUpdateAfter(
                            "hash_t", map("value", "v2", "id", 1))),
                    tapTable,
                    context(stateMap));

            List<InternalRow> rows = readRows(catalog(restarted).getTable(
                    Identifier.create(DATABASE, "hash_t")));
            assertEquals(1, rows.size());
            assertEquals(1, rows.get(0).getInt(0));
            assertEquals("v2", rows.get(0).getString(1).toString());
        } finally {
            restarted.close();
        }
    }

    @Test
    void keyDynamicAfterOnlyUpdateShouldMovePartitionThroughRealService() throws Exception {
        PaimonConfig config = config("key-service");
        KVMap<Object> stateMap = stateMap();
        TapTable tapTable = new TapTable("key_t")
                // Target RowType is pt,id,value; event metadata order must not affect conversion.
                .add(new TapField("value", "STRING"))
                .add(new TapField("id", "INT").primaryKeyPos(1))
                .add(new TapField("pt", "INT"));

        PaimonService first = service(config);
        try {
            createKeyDynamicTable(catalog(first), "key_t");
            first.writeRecords(
                    Collections.singletonList(cdcInsert(
                            "key_t", map("value", "old", "id", 10, "pt", 1))),
                    tapTable,
                    context(stateMap));
        } finally {
            first.close();
        }

        PaimonService restarted = service(config);
        try {
            restarted.writeRecords(
                    Collections.singletonList(cdcUpdateAfter(
                            "key_t", map("value", "new", "id", 10, "pt", 2))),
                    tapTable,
                    context(stateMap));

            List<InternalRow> rows = readRows(catalog(restarted).getTable(
                    Identifier.create(DATABASE, "key_t")));
            assertEquals(1, rows.size());
            assertEquals(2, rows.get(0).getInt(0));
            assertEquals(10, rows.get(0).getInt(1));
            assertEquals("new", rows.get(0).getString(2).toString());
        } finally {
            restarted.close();
        }
    }

    @Test
    void keyDynamicCompleteCdcDeleteAndReinsertShouldRemainUnique() throws Exception {
        PaimonConfig config = config("key-complete-cdc-service");
        KVMap<Object> stateMap = stateMap();
        TapTable tapTable =
                new TapTable("key_complete_t")
                        .add(new TapField("value", "STRING"))
                        .add(new TapField("id", "INT").primaryKeyPos(1))
                        .add(new TapField("pt", "INT"));

        PaimonService service = service(config);
        try {
            createKeyDynamicTable(catalog(service), "key_complete_t");
            TapConnectorContext connectorContext = context(stateMap);
            service.writeRecords(
                    Collections.singletonList(
                            cdcInsert(
                                    "key_complete_t",
                                    map("value", "old", "id", 10, "pt", 1))),
                    tapTable,
                    connectorContext);
            service.writeRecords(
                    Collections.singletonList(
                            cdcUpdate(
                                    "key_complete_t",
                                    map("value", "old", "id", 10, "pt", 1),
                                    map("value", "moved", "id", 10, "pt", 2))),
                    tapTable,
                    connectorContext);

            List<InternalRow> moved =
                    readRows(
                            catalog(service)
                                    .getTable(
                                            Identifier.create(DATABASE, "key_complete_t")));
            assertEquals(1, moved.size());
            assertEquals(2, moved.get(0).getInt(0));
            assertEquals("moved", moved.get(0).getString(2).toString());

            service.writeRecords(
                    Collections.singletonList(
                            cdcDelete(
                                    "key_complete_t",
                                    map("value", "moved", "id", 10, "pt", 2))),
                    tapTable,
                    connectorContext);
            assertEquals(
                    0,
                    readRows(
                                    catalog(service)
                                            .getTable(
                                                    Identifier.create(
                                                            DATABASE, "key_complete_t")))
                            .size());

            service.writeRecords(
                    Collections.singletonList(
                            cdcInsert(
                                    "key_complete_t",
                                    map("value", "reinserted", "id", 10, "pt", 3))),
                    tapTable,
                    connectorContext);
            List<InternalRow> reinserted =
                    readRows(
                            catalog(service)
                                    .getTable(
                                            Identifier.create(DATABASE, "key_complete_t")));
            assertEquals(1, reinserted.size());
            assertEquals(3, reinserted.get(0).getInt(0));
            assertEquals(10, reinserted.get(0).getInt(1));
            assertEquals("reinserted", reinserted.get(0).getString(2).toString());
        } finally {
            service.close();
        }
    }

    @Test
    void bucketUnawareAfterOnlyUpdateShouldAppendThroughRealService() throws Exception {
        PaimonConfig config = config("append-after-only-service");
        KVMap<Object> stateMap = stateMap();
        TapTable tapTable =
                new TapTable("append_t")
                        .add(new TapField("value", "STRING"))
                        .add(new TapField("id", "INT"));

        PaimonService service = service(config);
        try {
            createAppendOnlyTable(catalog(service), "append_t", false);
            service.writeRecords(
                    Collections.singletonList(
                            cdcUpdateAfter("append_t", map("value", "after", "id", 1))),
                    tapTable,
                    context(stateMap));

            List<InternalRow> rows =
                    readRows(
                            catalog(service)
                                    .getTable(Identifier.create(DATABASE, "append_t")));
            assertEquals(1, rows.size());
            assertEquals(1, rows.get(0).getInt(0));
            assertEquals("after", rows.get(0).getString(1).toString());
        } finally {
            service.close();
        }
    }

    @Test
    void bucketUnawareRetractsShouldRemainControlledByPaimonOptions() throws Exception {
        PaimonConfig config = config("append-retract-service");
        KVMap<Object> stateMap = stateMap();
        TapTable defaultTable =
                new TapTable("append_default")
                        .add(new TapField("id", "INT"))
                        .add(new TapField("value", "STRING"));
        TapTable ignoreTable =
                new TapTable("append_ignore")
                        .add(new TapField("id", "INT"))
                        .add(new TapField("value", "STRING"));

        PaimonService defaultService = service(config);
        try {
            createAppendOnlyTable(catalog(defaultService), "append_default", false);
            Exception rejected =
                    assertThrows(
                            Exception.class,
                            () ->
                                    defaultService.writeRecords(
                                            Collections.singletonList(
                                                    cdcDelete(
                                                            "append_default",
                                                            map("id", 1, "value", "delete"))),
                                            defaultTable,
                                            context(stateMap)));
            assertTrue(containsMessage(rejected, "Append only writer can not accept"));
        } finally {
            IllegalStateException closeFailure =
                    assertThrows(IllegalStateException.class, defaultService::close);
            assertTrue(containsMessage(closeFailure, "fenced after an ingress failure"));
        }

        // An ingress write error intentionally fences a service instance. A restarted instance
        // must still honor Paimon's table-level retract filters rather than connector rewriting.
        PaimonService ignoreService = service(config);
        try {
            createAppendOnlyTable(catalog(ignoreService), "append_ignore", true);
            ignoreService.writeRecords(
                    Collections.singletonList(
                            cdcInsert("append_ignore", map("id", 1, "value", "v1"))),
                    ignoreTable,
                    context(stateMap));
            ignoreService.writeRecords(
                    Collections.singletonList(
                            cdcUpdate(
                                    "append_ignore",
                                    map("id", 1, "value", "v1"),
                                    map("id", 1, "value", "v2"))),
                    ignoreTable,
                    context(stateMap));
            ignoreService.writeRecords(
                    Collections.singletonList(
                            cdcDelete("append_ignore", map("id", 1, "value", "v2"))),
                    ignoreTable,
                    context(stateMap));

            List<InternalRow> rows =
                    readRows(
                            catalog(ignoreService)
                                    .getTable(Identifier.create(DATABASE, "append_ignore")));
            assertEquals(2, rows.size());
            assertTrue(
                    rows.stream()
                            .anyMatch(row -> "v1".equals(row.getString(1).toString())));
            assertTrue(
                    rows.stream()
                            .anyMatch(row -> "v2".equals(row.getString(1).toString())));
        } finally {
            ignoreService.close();
        }
    }

    private PaimonConfig config(String name) throws Exception {
        java.nio.file.Path warehouse = Files.createDirectories(tempDir.resolve(name).resolve("warehouse"));
        java.nio.file.Path spill = Files.createDirectories(tempDir.resolve(name).resolve("spill"));
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse(warehouse.toString());
        config.setStorageType("local");
        config.setDatabase(DATABASE);
        config.setDiskTmpDir(spill.toString());
        config.setBatchAccumulationSize(0);
        config.setCommitIntervalMs(0);
        config.setEnableAsyncCommit(false);
        config.setWriteBufferSize(8);
        return config;
    }

    private PaimonService service(PaimonConfig config) throws Exception {
        PaimonService service = new PaimonService(config, mock(Log.class));
        service.init();
        catalog(service).createDatabase(DATABASE, true);
        return service;
    }

    private void createHashDynamicTable(Catalog catalog, String tableName) throws Exception {
        catalog.createTable(
                Identifier.create(DATABASE, tableName),
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "-1")
                        .option("dynamic-bucket.target-row-num", "1")
                        .option("write-buffer-size", "8mb")
                        .build(),
                false);
    }

    private void createKeyDynamicTable(Catalog catalog, String tableName) throws Exception {
        catalog.createTable(
                Identifier.create(DATABASE, tableName),
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .partitionKeys("pt")
                        .primaryKey("id")
                        .option("bucket", "-1")
                        .option("dynamic-bucket.target-row-num", "1")
                        .option("write-buffer-size", "8mb")
                        .build(),
                false);
    }

    private void createSyntheticHashDynamicTable(Catalog catalog, String tableName)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("value", DataTypes.STRING())
                        // Keep the synthetic key away from index 0 to verify name-based resolution.
                        .column("_hash_key", DataTypes.VARCHAR(32));
        for (int i = 1; i <= 6; i++) {
            builder.column("pk" + i, DataTypes.INT());
        }
        catalog.createTable(
                Identifier.create(DATABASE, tableName),
                builder.primaryKey("_hash_key")
                        .option("bucket", "-1")
                        .option("dynamic-bucket.target-row-num", "1")
                        .option("write-buffer-size", "8mb")
                        .build(),
                false);
    }

    private void createSyntheticKeyDynamicTable(Catalog catalog, String tableName)
            throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .column("_hash_key", DataTypes.VARCHAR(32));
        for (int i = 1; i <= 6; i++) {
            builder.column("pk" + i, DataTypes.INT());
        }
        catalog.createTable(
                Identifier.create(DATABASE, tableName),
                builder.partitionKeys("pt")
                        .primaryKey("_hash_key")
                        .option("bucket", "-1")
                        .option("dynamic-bucket.target-row-num", "1")
                        .option("write-buffer-size", "8mb")
                        .build(),
                false);
    }

    private TapTable syntheticHashTapTable(String tableName, boolean partitioned) {
        TapTable table = new TapTable(tableName).add(new TapField("value", "STRING"));
        for (int i = 1; i <= 6; i++) {
            table.add(new TapField("pk" + i, "INT").primaryKeyPos(i));
        }
        if (partitioned) {
            table.add(new TapField("pt", "INT"));
        }
        return table;
    }

    private Map<String, Object> syntheticHashData(String value) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("value", value);
        for (int i = 1; i <= 6; i++) {
            data.put("pk" + i, i);
        }
        return data;
    }

    private Map<String, Object> syntheticHashData(String value, Integer partition) {
        Map<String, Object> data = syntheticHashData(value);
        data.put("pt", partition);
        return data;
    }

    private void createAppendOnlyTable(
            Catalog catalog, String tableName, boolean ignoreRetracts) throws Exception {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .option("bucket", "-1")
                        .option("write-buffer-size", "8mb");
        if (ignoreRetracts) {
            builder.option("ignore-delete", "true").option("ignore-update-before", "true");
        }
        catalog.createTable(
                Identifier.create(DATABASE, tableName), builder.build(), false);
    }

    private TapInsertRecordEvent cdcInsert(String table, Map<String, Object> after) {
        TapInsertRecordEvent event = new TapInsertRecordEvent().init().table(table).after(after);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        return event;
    }

    private TapUpdateRecordEvent cdcUpdateAfter(String table, Map<String, Object> after) {
        TapUpdateRecordEvent event = new TapUpdateRecordEvent().init().table(table).after(after);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        return event;
    }

    private TapUpdateRecordEvent cdcUpdate(
            String table, Map<String, Object> before, Map<String, Object> after) {
        TapUpdateRecordEvent event =
                new TapUpdateRecordEvent()
                        .init()
                        .table(table)
                        .before(before)
                        .after(after);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        return event;
    }

    private TapDeleteRecordEvent cdcDelete(String table, Map<String, Object> before) {
        TapDeleteRecordEvent event =
                new TapDeleteRecordEvent().init().table(table).before(before);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        return event;
    }

    private TapConnectorContext context(KVMap<Object> stateMap) {
        TapConnectorContext context = mock(TapConnectorContext.class);
        when(context.getStateMap()).thenReturn(stateMap);
        when(context.getLog()).thenReturn(mock(Log.class));
        return context;
    }

    private Map<String, Object> map(Object... values) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put((String) values[i], values[i + 1]);
        }
        return map;
    }

    private List<InternalRow> readRows(Table table) throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        List<InternalRow> rows = new ArrayList<>();
        try (RecordReader<InternalRow> reader =
                     readBuilder.newRead().createReader(readBuilder.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        rows.add(serializer.copy(row));
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private KVMap<Object> stateMap() {
        Map<String, Object> values = new ConcurrentHashMap<>();
        KVMap<Object> stateMap = mock(KVMap.class);
        when(stateMap.get(anyString())).thenAnswer(
                invocation -> values.get(invocation.getArgument(0)));
        when(stateMap.putIfAbsent(anyString(), org.mockito.ArgumentMatchers.any()))
                .thenAnswer(invocation -> values.putIfAbsent(
                        invocation.getArgument(0), invocation.getArgument(1)));
        doAnswer(invocation -> {
            values.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(stateMap).put(anyString(), org.mockito.ArgumentMatchers.any());
        return stateMap;
    }

    private Catalog catalog(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("catalog");
        field.setAccessible(true);
        return (Catalog) field.get(service);
    }

    private boolean containsMessage(Throwable error, String text) {
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(text)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
