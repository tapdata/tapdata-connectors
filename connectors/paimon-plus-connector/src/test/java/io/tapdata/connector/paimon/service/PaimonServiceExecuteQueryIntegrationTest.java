package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.PaimonConnector;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
    private static final String TABLE = "DRSRedemptions";

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
                            .column("created", DataTypes.TIMESTAMP(3))
                            .primaryKey("id")
                            .option("bucket", "1")
                            .option("write-buffer-size", "8mb")
                            .build(),
                    false);

            TapTable tapTable = new TapTable(TABLE)
                    .add(new TapField("id", "INT").primaryKeyPos(1))
                    .add(new TapField("category", "STRING"))
                    .add(new TapField("created", "TIMESTAMP"));
            TapConnectorContext context = context();
            service.writeRecords(Arrays.asList(
                    cdcInsert(map("id", 1, "category", "A", "created", timestamp("2026-03-31 00:00:00"))),
                    cdcInsert(map("id", 2, "category", "B", "created", timestamp("2026-04-02 00:00:00"))),
                    cdcInsert(map("id", 3, "category", "A", "created", timestamp("2026-04-03 00:00:00")))),
                    tapTable, context);

            List<Map<String, Object>> rows = castRows(service.executeQuery(
                    "SELECT * FROM " + TABLE + " WHERE category = 'A'", mock(Log.class)));
            assertEquals(2, rows.size());
            assertEquals(1, rows.stream().filter(row -> Integer.valueOf(1).equals(row.get("id"))).count());
            assertEquals(1, rows.stream().filter(row -> Integer.valueOf(3).equals(row.get("id"))).count());

            Object count = service.executeQuery(
                    "SELECT COUNT(1) FROM (SELECT * FROM " + TABLE
                            + " WHERE category = 'A') AS COUNT", mock(Log.class));
            assertEquals(2L, count);
            assertEquals(3L, service.executeQuery(
                    "SELECT COUNT(1) FROM " + TABLE, mock(Log.class)));
            assertEquals(2L, service.executeQuery(
                    "SELECT COUNT(1) FROM " + TABLE
                            + " WHERE created > CAST('2026-04-01' AS TIMESTAMP)", mock(Log.class)));
            assertEquals(3, castRows(service.executeQuery(
                    "SELECT * FROM " + TABLE, mock(Log.class))).size());

            service.writeRecords(Arrays.asList(
                    cdcUpdate(map("id", 1, "category", "A", "created", timestamp("2026-03-31 00:00:00")),
                            map("id", 1, "category", "C", "created", timestamp("2026-03-31 00:00:00"))),
                    cdcDelete(map("id", 2, "category", "B", "created", timestamp("2026-04-02 00:00:00")))),
                    tapTable, context);
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

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles"})
    void executesCustomerDateAggregationOnRealPaimonWithProjectionAndFiltering(String zone) throws Throwable {
        java.util.TimeZone previous = java.util.TimeZone.getDefault();
        try {
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone(zone));
            executeCustomerDateAggregation();
        } finally {
            java.util.TimeZone.setDefault(previous);
        }
    }

    private void executeCustomerDateAggregation() throws Throwable {
        PaimonConfig config = config();
        PaimonService service = new PaimonService(config, mock(Log.class));
        try {
            service.init();
            Catalog catalog = catalog(service);
            catalog.createDatabase(DATABASE, true);
            catalog.createTable(Identifier.create(DATABASE, TABLE), Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("CreationTime", DataTypes.TIMESTAMP(3))
                    .column("TransactionDatetime", DataTypes.TIMESTAMP(3))
                    .column("TotalValues", DataTypes.DECIMAL(20, 2))
                    .column("PointsAfterRedemption", DataTypes.DECIMAL(20, 2))
                    .column("op", DataTypes.STRING())
                    .column("unused_payload", DataTypes.STRING())
                    .primaryKey("id")
                    .option("bucket", "1").option("write-buffer-size", "8mb").build(), false);
            TapTable tapTable = new TapTable(TABLE)
                    .add(new TapField("id", "INT").primaryKeyPos(1))
                    .add(new TapField("CreationTime", "TIMESTAMP"))
                    .add(new TapField("TransactionDatetime", "TIMESTAMP"))
                    .add(new TapField("TotalValues", "DECIMAL(20,2)"))
                    .add(new TapField("PointsAfterRedemption", "DECIMAL(20,2)"))
                    .add(new TapField("op", "STRING"))
                    .add(new TapField("unused_payload", "STRING"));
            service.writeRecords(Arrays.asList(
                    redemption(1, "2025-01-02 23:59:59", "2025-01-01 00:00:00", "0.10", "1.10", "i"),
                    redemption(2, "2025-01-02 00:00:01", "2025-02-01 00:00:00", "0.20", null, "u"),
                    redemption(3, "2025-01-01 12:00:00", "2025-07-01 00:00:00", "3.00", "4.00", "i"),
                    redemption(4, "2025-01-01 12:00:00", "2025-07-01 00:00:00.001", "900.00", "900.00", "i"),
                    redemption(5, "2025-01-02 12:00:00", "2025-03-01 00:00:00", "900.00", "900.00", "d"),
                    redemption(6, null, "2025-03-01 00:00:00", null, null, "i"),
                    redemption(7, "2025-01-02 12:00:00", "2025-03-01 00:00:00", "900.00", "900.00", null)),
                    tapTable, context());
            String sql = "SELECT CAST(CreationTime AS DATE), COUNT(*), SUM(TotalValues), SUM(PointsAfterRedemption) "
                    + "FROM DRSRedemptions "
                    + "WHERE TransactionDatetime BETWEEN '2025-01-01 00:00:00' AND '2025-07-01 00:00:00' "
                    + "AND op <> 'd' GROUP BY CAST(CreationTime AS DATE) ORDER BY CAST(CreationTime AS DATE)";
            // The customer's filter is not on this test table's PK/partition. Require explicit opt-in.
            assertThrows(IllegalArgumentException.class, () -> service.executeQuery(sql, mock(Log.class)));
            Map<String, Object> params = new LinkedHashMap<>();
            params.put("allowFullScan", true);
            List<Map<String, Object>> rows = castRows(service.executeQuery(sql, params, mock(Log.class)));
            assertEquals(3, rows.size());
            assertEquals(null, rows.get(0).get("CAST(CreationTime AS DATE)"));
            assertEquals(1L, rows.get(0).get("COUNT(*)"));
            assertEquals(null, rows.get(0).get("SUM(TotalValues)"));
            assertEquals(java.time.LocalDate.of(2025, 1, 1), rows.get(1).get("CAST(CreationTime AS DATE)"));
            assertEquals(1L, rows.get(1).get("COUNT(*)"));
            assertEquals(new java.math.BigDecimal("3.00"), rows.get(1).get("SUM(TotalValues)"));
            assertEquals(java.time.LocalDate.of(2025, 1, 2), rows.get(2).get("CAST(CreationTime AS DATE)"));
            assertEquals(2L, rows.get(2).get("COUNT(*)"));
            assertEquals(new java.math.BigDecimal("0.30"), rows.get(2).get("SUM(TotalValues)"));
            assertEquals(new java.math.BigDecimal("1.10"), rows.get(2).get("SUM(PointsAfterRedemption)"));

            PaimonConnector connector = new PaimonConnector();
            java.lang.reflect.Field serviceField = PaimonConnector.class.getDeclaredField("paimonService");
            serviceField.setAccessible(true);
            serviceField.set(connector, service);
            ConnectorFunctions functions = new ConnectorFunctions();
            connector.registerCapabilities(functions, new TapCodecsRegistry());
            Map<String, Object> commandParams = new LinkedHashMap<>(params);
            commandParams.put("sql", sql);
            java.util.List<ExecuteResult> callbacks = new java.util.ArrayList<>();
            functions.getExecuteCommandFunction().execute(context(),
                    TapExecuteCommand.create().command("executeQuery").params(commandParams), callbacks::add);
            assertEquals(1, callbacks.size());
            assertEquals(null, callbacks.get(0).getError());
            assertEquals(rows, callbacks.get(0).getResult());
            callbacks.clear();
            commandParams.put("maxScanRows", 1);
            functions.getExecuteCommandFunction().execute(context(),
                    TapExecuteCommand.create().command("executeQuery").params(commandParams), callbacks::add);
            assertEquals(1, callbacks.size());
            org.junit.jupiter.api.Assertions.assertTrue(callbacks.get(0).getError() instanceof IllegalStateException);
            assertEquals(null, callbacks.get(0).getResult());

            // Predicate column id is absent from the output projection.
            List<Map<String, Object>> grouped = castRows(service.executeQuery(
                    "SELECT op, COUNT(*) AS n, COUNT(TotalValues) AS present, AVG(TotalValues) AS mean "
                            + "FROM " + TABLE + " WHERE id BETWEEN 1 AND 3 GROUP BY op ORDER BY op DESC", mock(Log.class)));
            assertEquals(2, grouped.size());
            assertEquals("u", grouped.get(0).get("op"));
            assertEquals(1L, grouped.get(0).get("present"));
            assertEquals(0, new java.math.BigDecimal("0.20").compareTo((java.math.BigDecimal) grouped.get(0).get("mean")));
            assertEquals(2L, grouped.get(1).get("n"));
            List<Map<String, Object>> empty = castRows(service.executeQuery(
                    "SELECT COUNT(*) AS n, SUM(TotalValues) AS total FROM " + TABLE + " WHERE id < 0", mock(Log.class)));
            assertEquals(0L, empty.get(0).get("n"));
            assertEquals(null, empty.get(0).get("total"));

            params.put("maxScanRows", 1);
            assertThrows(IllegalStateException.class, () -> service.executeQuery(sql, params, mock(Log.class)));
            params.remove("maxScanRows");
            params.put("maxGroups", 1);
            assertThrows(IllegalStateException.class, () -> service.executeQuery(sql, params, mock(Log.class)));
            params.remove("maxGroups");
            params.put("maxResultRows", 1);
            assertThrows(IllegalStateException.class, () -> service.executeQuery(sql, params, mock(Log.class)));
            // Limit failures release reader/batch resources and allow subsequent commands.
            params.remove("maxResultRows");
            assertEquals(rows, service.executeQuery(sql, params, mock(Log.class)));
            config.load(Collections.singletonMap("queryAllowFullScan", true));
            assertEquals(rows, service.executeQuery(sql, mock(Log.class)));
        } finally { service.close(); }
    }

    @Test
    void legacyTwoArgumentQueryReturnsMoreThanOneThousandRowsAndExplicitLimitsStillApply() throws Exception {
        PaimonService service = new PaimonService(config(), mock(Log.class));
        try {
            service.init();
            Catalog catalog = catalog(service);
            catalog.createDatabase(DATABASE, true);
            catalog.createTable(Identifier.create(DATABASE, TABLE), Schema.newBuilder()
                    .column("id", DataTypes.INT()).primaryKey("id").option("bucket", "1")
                    .option("write-buffer-size", "8mb").build(), false);
            TapTable table = new TapTable(TABLE).add(new TapField("id", "INT").primaryKeyPos(1));
            List<TapRecordEvent> input = new java.util.ArrayList<>();
            for (int i = 0; i < 1001; i++) { input.add(cdcInsert(map("id", i))); }
            service.writeRecords(input, table, context());
            assertEquals(1001, castRows(service.executeQuery("SELECT * FROM " + TABLE, mock(Log.class))).size());
            assertEquals(1001L, service.executeQuery("SELECT COUNT(1) FROM " + TABLE, mock(Log.class)));
            assertThrows(IllegalStateException.class, () -> service.executeQuery("SELECT * FROM " + TABLE,
                    Collections.singletonMap("maxResultRows", 1000), mock(Log.class)));
        } finally { service.close(); }
    }

    private TapInsertRecordEvent redemption(int id, String created, String transaction, String total, String points, String op) {
        return cdcInsert(map("id", id, "CreationTime", created == null ? null : timestamp(created),
                "TransactionDatetime", timestamp(transaction),
                "TotalValues", total == null ? null : new java.math.BigDecimal(total),
                "PointsAfterRedemption", points == null ? null : new java.math.BigDecimal(points),
                "op", op, "unused_payload", "not projected"));
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

    private java.sql.Timestamp timestamp(String value) {
        // writeRecords receives codec output encoded as epoch time. Build that same
        // representation explicitly; valueOf would shift fixtures with the JVM timezone.
        return java.sql.Timestamp.from(java.time.LocalDateTime.parse(value.replace(' ', 'T'))
                .toInstant(java.time.ZoneOffset.UTC));
    }
}
