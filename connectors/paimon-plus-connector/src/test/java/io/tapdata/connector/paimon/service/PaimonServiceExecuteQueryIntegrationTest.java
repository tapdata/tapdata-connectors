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
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonServiceExecuteQueryIntegrationTest {

    private static final String DATABASE = "default";
    private static final String TABLE = "execute_query_orders";

    @TempDir
    Path tempDir;

    @Test
    void executesFilteredRowsAndCountWithoutAllowingMutatingSql() throws Exception {
        PaimonConfig config = config();
        PaimonService service = new PaimonService(config, mock(Log.class));
        try {
            service.init();
            Catalog catalog = catalog(service);
            catalog.createDatabase(DATABASE, true);
            catalog.createTable(
                    Identifier.create(DATABASE, TABLE),
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("category", DataTypes.STRING())
                            .primaryKey("id")
                            .option("bucket", "1")
                            .option("write-buffer-size", "8mb")
                            .build(),
                    false);

            TapTable tapTable = new TapTable(TABLE)
                    .add(new TapField("id", "INT").primaryKeyPos(1))
                    .add(new TapField("category", "STRING"));
            TapConnectorContext context = context();
            service.writeRecords(Arrays.asList(
                    cdcInsert(map("id", 1, "category", "A")),
                    cdcInsert(map("id", 2, "category", "B")),
                    cdcInsert(map("id", 3, "category", "A"))), tapTable, context);

            List<Map<String, Object>> rows = castRows(service.executeQuery(
                    "SELECT * FROM " + TABLE + " WHERE category = 'A'", mock(Log.class)));
            assertEquals(2, rows.size());
            assertEquals(1, rows.stream().filter(row -> Integer.valueOf(1).equals(row.get("id"))).count());
            assertEquals(1, rows.stream().filter(row -> Integer.valueOf(3).equals(row.get("id"))).count());

            Object count = service.executeQuery(
                    "SELECT COUNT(1) FROM (SELECT * FROM " + TABLE
                            + " WHERE category = 'A') AS COUNT", mock(Log.class));
            assertEquals(2L, count);

            service.writeRecords(Arrays.asList(
                    cdcUpdate(map("id", 1, "category", "A"), map("id", 1, "category", "C")),
                    cdcDelete(map("id", 2, "category", "B"))), tapTable, context);
            List<Map<String, Object>> finalRows = castRows(service.executeQuery(
                    "SELECT * FROM " + TABLE + " WHERE category = 'A'", mock(Log.class)));
            assertEquals(1, finalRows.size());
            assertEquals(3, finalRows.get(0).get("id"));
            assertEquals(1L, service.executeQuery(
                    "SELECT COUNT(1) FROM (SELECT * FROM " + TABLE
                            + " WHERE category = 'A') AS COUNT", mock(Log.class)));

            assertThrows(IllegalArgumentException.class, () -> service.executeQuery(
                    "DELETE FROM " + TABLE + " WHERE id = 1", mock(Log.class)));
        } finally {
            service.close();
        }
    }

    private PaimonConfig config() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse(Files.createDirectories(tempDir.resolve("warehouse")).toString());
        config.setStorageType("local");
        config.setDatabase(DATABASE);
        config.setDiskTmpDir(Files.createDirectories(tempDir.resolve("spill")).toString());
        config.setBatchAccumulationSize(0);
        config.setCommitIntervalMs(0);
        config.setEnableAsyncCommit(false);
        config.setWriteBufferSize(8);
        return config;
    }

    private TapInsertRecordEvent cdcInsert(Map<String, Object> after) {
        TapInsertRecordEvent event = new TapInsertRecordEvent().init().table(TABLE).after(after);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        event.addInfo("nodeIds", Collections.singletonList("test-source"));
        return event;
    }

    private TapUpdateRecordEvent cdcUpdate(Map<String, Object> before, Map<String, Object> after) {
        TapUpdateRecordEvent event = new TapUpdateRecordEvent().init().table(TABLE)
                .before(before).after(after);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        event.addInfo("nodeIds", Collections.singletonList("test-source"));
        return event;
    }

    private TapDeleteRecordEvent cdcDelete(Map<String, Object> before) {
        TapDeleteRecordEvent event = new TapDeleteRecordEvent().init().table(TABLE).before(before);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        event.addInfo("nodeIds", Collections.singletonList("test-source"));
        return event;
    }

    @SuppressWarnings("unchecked")
    private TapConnectorContext context() {
        Map<String, Object> values = new ConcurrentHashMap<>();
        KVMap<Object> stateMap = mock(KVMap.class);
        when(stateMap.get(anyString())).thenAnswer(invocation -> values.get(invocation.getArgument(0)));
        when(stateMap.putIfAbsent(anyString(), any())).thenAnswer(
                invocation -> values.putIfAbsent(invocation.getArgument(0), invocation.getArgument(1)));
        doAnswer(invocation -> {
            values.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(stateMap).put(anyString(), any());

        TapConnectorContext context = mock(TapConnectorContext.class);
        when(context.getStateMap()).thenReturn(stateMap);
        when(context.getLog()).thenReturn(mock(Log.class));
        return context;
    }

    private Catalog catalog(PaimonService service) throws Exception {
        java.lang.reflect.Field field = PaimonService.class.getDeclaredField("catalog");
        field.setAccessible(true);
        return (Catalog) field.get(service);
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> castRows(Object result) {
        return (List<Map<String, Object>>) result;
    }

    private Map<String, Object> map(Object... values) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            result.put((String) values[i], values[i + 1]);
        }
        return result;
    }
}
