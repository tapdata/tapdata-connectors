package io.tapdata.connector.mysql;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.ConnectorTestContext;
import io.tapdata.it.UnderTest;
import io.tapdata.it.performance.PerformanceAdapter;
import io.tapdata.it.schema.TestDataType;
import io.tapdata.it.schema.TestFieldSpec;
import io.tapdata.it.schema.TestTableSpec;
import io.tapdata.it.support.TestStateMap;
import io.tapdata.it.dbforge.DbForgeLeaseProvider;
import io.tapdata.it.tpcc.TpccAdapter;
import io.tapdata.it.tpcc.TpccConnectorIT;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MySQLConnectorIT extends TpccConnectorIT {

    private DbForgeLeaseProvider dbForgeLeaseProvider;
    private DataMap dbForgeConnectionConfig;

    @Override
    protected PerformanceAdapter createPerformanceAdapter() {
        return new MySQLPerformanceAdapter(context.getConfig());
    }

    @Override
    protected TpccAdapter createTpccAdapter() {
        return new MySQLTpccAdapter(context.getConfig());
    }

    @Override
    protected TestTableSpec createTestTableSpec() {
        return TestTableSpec.builder()
                .tableName(TestTableSpec.randomTableName("TAP_MYSQL_IT_"))
                .addField(TestFieldSpec.builder().name("id").dataType("BIGINT").testDataType(TestDataType.BIGINT).primaryKey(true).build())
                .addField(TestFieldSpec.builder().name("c_int").dataType("INT").testDataType(TestDataType.INT).build())
                .addField(TestFieldSpec.builder().name("c_bigint").dataType("BIGINT").testDataType(TestDataType.BIGINT).build())
                .addField(TestFieldSpec.builder().name("c_varchar").dataType("VARCHAR(255)").testDataType(TestDataType.VARCHAR).build())
                .addField(TestFieldSpec.builder().name("c_decimal").dataType("DECIMAL(18,4)").testDataType(TestDataType.DECIMAL).build())
                .addField(TestFieldSpec.builder().name("c_double").dataType("DOUBLE").testDataType(TestDataType.DOUBLE).build())
                .build();
    }

    @Override
    protected long streamReadTimeoutSeconds() {
        return 90L;
    }

    @Override
    protected boolean waitForStreamReadCatchUp() {
        return false;
    }

    @Override
    protected void prepareStreamReadTable() throws Exception {
        Thread.sleep(20000L);
    }

    @Test
    @UnderTest(value = "discoverSchema", requiresVerifier = true)
    @UnderTest(value = "batchRead", requiresVerifier = true)
    @UnderTest(value = "writeRecord", requiresVerifier = true)
    void should_discover_and_copy_mysql_view_snapshot() throws Throwable {
        String tableName = spec.getTableName();
        String viewName = tableName + "_view";
        String targetName = tableName + "_target";
        try {
            execute("CREATE TABLE " + qualified(tableName) + " (id BIGINT PRIMARY KEY, value VARCHAR(64) NOT NULL)");
            execute("INSERT INTO " + qualified(tableName) + " VALUES (1, 'from-view')");
            execute("CREATE VIEW " + qualified(viewName) + " AS SELECT id, value FROM " + qualified(tableName));
            execute("CREATE TABLE " + qualified(targetName) + " (id BIGINT PRIMARY KEY, value VARCHAR(64) NOT NULL)");

            List<TapTable> discovered = new ArrayList<>();
            ((MysqlConnector) context.getConnector()).discoverSchema(connectionContext(),
                    Collections.singletonList(viewName), 100, discovered::addAll);
            TapTable view = discovered.stream().filter(table -> viewName.equals(table.getId())).findFirst()
                    .orElseThrow(() -> new AssertionError("MySQL view was not discovered: " + viewName));
            assertEquals("view", view.getType());

            registerTable(view);
            List<Map<String, Object>> snapshot = batchReadAll(view);
            assertEquals("from-view", findRow(snapshot, 1L).get("value"));

            TapTable target = new TapTable(targetName)
                    .add(new TapField("id", "BIGINT").tapType(TapSimplify.tapNumber().bit(64)).isPrimaryKey(true).primaryKeyPos(1))
                    .add(new TapField("value", "VARCHAR(64)").tapType(TapSimplify.tapString()));
            registerTable(target);
            assertEquals(1L, writeInsertEventsViaEngineCodec(snapshot, target));
            assertEquals("from-view", findRow(batchReadAll(target), 1L).get("value"));
        } finally {
            execute("DROP VIEW IF EXISTS " + qualified(viewName));
            execute("DROP TABLE IF EXISTS " + qualified(targetName));
        }
    }

    @Test
    @UnderTest(value = "batchRead", requiresVerifier = true)
    @UnderTest(value = "writeRecord", requiresVerifier = true)
    @UnderTest(value = "streamRead", requiresVerifier = true)
    void should_round_trip_full_types_and_one_megabyte_fields() throws Throwable {
        String tableName = spec.getTableName();
        String text = String.join("", Collections.nCopies(131072, "mysql"));
        byte[] binary = binaryPayload();
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + qualified(tableName) + " ("
                    + "id BIGINT PRIMARY KEY, c_char CHAR(10), c_varchar VARCHAR(255), c_text LONGTEXT, "
                    + "c_small SMALLINT, c_int INT, c_unsigned BIGINT UNSIGNED, c_decimal DECIMAL(18,4), "
                    + "c_double DOUBLE, c_bool BOOLEAN, c_date DATE, c_time TIME(6), c_datetime DATETIME(6), "
                    + "c_timestamp TIMESTAMP(6) NULL, c_blob LONGBLOB, c_json JSON)");
        }
        TapTable table = fullTypeTable(tableName);
        registerTable(table);
        insertFullTypeRow(tableName, 1L, text, binary);

        Map<String, Object> fullRow = findRow(batchReadAll(table), 1L);
        assertEquals(text, fullRow.get("c_text"));
        assertArrayEquals(binary, (byte[]) fullRow.get("c_blob"));
        assertEquals(new BigDecimal("18446744073709551615"), new BigDecimal(String.valueOf(fullRow.get("c_unsigned"))));

        Map<String, Object> target = new LinkedHashMap<>(fullRow);
        target.put("id", 2L);
        assertEquals(1L, writeInsertEventsViaEngineCodec(Collections.singletonList(target), table));
        Map<String, Object> written = findRow(batchReadAll(table), 2L);
        assertEquals(text, written.get("c_text"));
        assertArrayEquals(binary, (byte[]) written.get("c_blob"));

        Capture capture = startCapture(table, currentOffset());
        try {
            insertFullTypeRow(tableName, 3L, text, binary);
            waitForIds(capture, 3L);
            Map<String, Object> streamed = capture.rows.stream().filter(row -> id(row) == 3L).findFirst()
                    .orElseThrow(() -> new AssertionError("missing MySQL full-type CDC row"));
            assertEquals(text, textValue(streamed.get("c_text")));
            assertArrayEquals(binary, (byte[]) streamed.get("c_blob"));
        } finally {
            capture.stop();
        }
    }

    @Test
    @UnderTest(value = "streamRead", requiresVerifier = true)
    @UnderTest("getStreamOffset")
    void should_resume_from_saved_offset() throws Throwable {
        String tableName = spec.getTableName();
        createTransactionTable(tableName);
        TapTable table = transactionTable(tableName);
        registerTable(table);
        Object savedOffset = currentOffset();
        execute("INSERT INTO " + qualified(tableName) + " VALUES (91, 'written-while-stopped')");
        Capture capture = startCapture(table, savedOffset);
        try {
            waitForIds(capture, 91L);
            assertNotNull(capture.lastOffset.get(), "stream callback should provide a resumable offset");
        } finally {
            capture.stop();
        }
    }

    @Test
    @UnderTest(value = "streamRead", requiresVerifier = true)
    void should_stream_insert_update_and_delete() throws Throwable {
        String tableName = spec.getTableName();
        createTransactionTable(tableName);
        TapTable table = transactionTable(tableName);
        registerTable(table);
        Capture capture = startCapture(table, currentOffset());
        try {
            execute("INSERT INTO " + qualified(tableName) + " VALUES (81, 'inserted')");
            waitForIds(capture, 81L);
            execute("UPDATE " + qualified(tableName) + " SET value='updated' WHERE id=81");
            execute("DELETE FROM " + qualified(tableName) + " WHERE id=81");
            waitForMutationEvents(capture, 81L);
        } finally {
            capture.stop();
        }
    }

    @Test
    @UnderTest(value = "streamRead", requiresVerifier = true)
    @DisplayName("Layer3 no-primary-key rows preserve nulls across CDC")
    void should_stream_no_primary_key_rows_with_null_fields() throws Throwable {
        String tableName = spec.getTableName();
        execute("CREATE TABLE " + qualified(tableName)
                + " (id BIGINT NULL, name VARCHAR(64) NULL, note TEXT NULL)");
        TapTable table = new TapTable(tableName)
                .add(new TapField("id", "BIGINT").tapType(TapSimplify.tapNumber().bit(64)))
                .add(new TapField("name", "VARCHAR(64)").tapType(TapSimplify.tapString()))
                .add(new TapField("note", "TEXT").tapType(TapSimplify.tapString()));
        registerTable(table);
        Capture capture = startCapture(table, currentOffset());
        try {
            execute("INSERT INTO " + qualified(tableName) + " VALUES (101, NULL, 'created')");
            execute("UPDATE " + qualified(tableName) + " SET name='updated', note=NULL WHERE id=101");
            execute("DELETE FROM " + qualified(tableName) + " WHERE id=101");
            waitForNoPrimaryKeyMutations(capture, 101L);
        } finally {
            capture.stop();
        }
    }

    @Test
    @UnderTest(value = "streamRead", requiresVerifier = true)
    @DisplayName("Layer3 source DDL emits add, alter, rename, and drop events")
    void should_stream_supported_source_ddl_events() throws Throwable {
        String tableName = spec.getTableName();
        execute("CREATE TABLE " + qualified(tableName)
                + " (id BIGINT PRIMARY KEY, value VARCHAR(32) NULL, obsolete_col VARCHAR(32) NULL)");
        TapTable table = new TapTable(tableName)
                .add(new TapField("id", "BIGINT").tapType(TapSimplify.tapNumber().bit(64)).isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("value", "VARCHAR(32)").tapType(TapSimplify.tapString()))
                .add(new TapField("obsolete_col", "VARCHAR(32)").tapType(TapSimplify.tapString()));
        registerTable(table);
        Capture capture = startCapture(table, currentOffset());
        try {
            execute("ALTER TABLE " + qualified(tableName) + " ADD added_col VARCHAR(32) DEFAULT 'layer3'");
            execute("ALTER TABLE " + qualified(tableName) + " MODIFY value VARCHAR(128) NOT NULL");
            execute("ALTER TABLE " + qualified(tableName) + " CHANGE added_col renamed_col VARCHAR(64) NULL");
            execute("ALTER TABLE " + qualified(tableName) + " DROP COLUMN obsolete_col");
            waitForSourceDdlEvents(capture);
        } finally {
            capture.stop();
        }
    }

    @Test
    @UnderTest(value = "streamRead", requiresVerifier = true)
    void should_apply_commit_rollback_savepoint_and_uncommitted_rules() throws Throwable {
        String tableName = spec.getTableName();
        createTransactionTable(tableName);
        TapTable table = transactionTable(tableName);
        registerTable(table);
        Capture capture = startCapture(table, currentOffset());
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO " + qualified(tableName) + " VALUES (?, ?)")) {
            connection.setAutoCommit(false);
            insert(statement, 1L, "committed");
            connection.commit();
            insert(statement, 2L, "rolled-back");
            connection.rollback();
            insert(statement, 3L, "before-savepoint");
            java.sql.Savepoint savepoint = connection.setSavepoint("SP_PARTIAL");
            insert(statement, 4L, "after-savepoint");
            connection.rollback(savepoint);
            connection.commit();
            insert(statement, 5L, "uncommitted");
            waitForIds(capture, 1L, 3L);
            Thread.sleep(3000L);
            assertFalse(capture.ids.contains(2L));
            assertFalse(capture.ids.contains(4L));
            assertFalse(capture.ids.contains(5L));
            connection.rollback();
        } finally {
            capture.stop();
        }
    }

    @Test
    @Tag("long-transaction")
    @UnderTest(value = "streamRead", requiresVerifier = true)
    void should_stream_large_committed_transaction() throws Throwable {
        int rows = Integer.getInteger("transaction.long.rows", 10000);
        String tableName = spec.getTableName();
        createTransactionTable(tableName);
        TapTable table = transactionTable(tableName);
        registerTable(table);
        Capture capture = startCapture(table, currentOffset());
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO " + qualified(tableName) + " VALUES (?, ?)")) {
            connection.setAutoCommit(false);
            for (int row = 1; row <= rows; row++) {
                statement.setLong(1, row);
                statement.setString(2, "long-" + row);
                statement.addBatch();
                if (row % 1000 == 0) statement.executeBatch();
            }
            statement.executeBatch();
            connection.commit();
            waitForCount(capture, rows, 600);
            assertEquals(rows, capture.ids.size());
        } finally {
            capture.stop();
        }
    }

    private TapTable fullTypeTable(String tableName) {
        return new TapTable(tableName)
                .add(new TapField("id", "BIGINT").tapType(TapSimplify.tapNumber().bit(64)).isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("c_char", "CHAR(10)").tapType(TapSimplify.tapString()))
                .add(new TapField("c_varchar", "VARCHAR(255)").tapType(TapSimplify.tapString()))
                .add(new TapField("c_text", "LONGTEXT").tapType(TapSimplify.tapString()))
                .add(new TapField("c_small", "SMALLINT").tapType(TapSimplify.tapNumber().bit(16)))
                .add(new TapField("c_int", "INT").tapType(TapSimplify.tapNumber().bit(32)))
                .add(new TapField("c_unsigned", "BIGINT UNSIGNED").tapType(TapSimplify.tapNumber().fixed(true).precision(20).scale(0)))
                .add(new TapField("c_decimal", "DECIMAL(18,4)").tapType(TapSimplify.tapNumber().fixed(true).precision(18).scale(4)))
                .add(new TapField("c_double", "DOUBLE").tapType(TapSimplify.tapNumber().bit(64)))
                .add(new TapField("c_bool", "BOOLEAN").tapType(TapSimplify.tapBoolean()))
                .add(new TapField("c_date", "DATE").tapType(TapSimplify.tapDate()))
                .add(new TapField("c_time", "TIME(6)").tapType(TapSimplify.tapTime()))
                .add(new TapField("c_datetime", "DATETIME(6)").tapType(TapSimplify.tapDateTime()))
                .add(new TapField("c_timestamp", "TIMESTAMP(6)").tapType(TapSimplify.tapDateTime()))
                .add(new TapField("c_blob", "LONGBLOB").tapType(TapSimplify.tapBinary()))
                .add(new TapField("c_json", "JSON").tapType(TapSimplify.tapString()));
    }

    private void insertFullTypeRow(String tableName, long rowId, String text, byte[] binary) throws Exception {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO " + qualified(tableName) + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
            statement.setLong(1, rowId);
            statement.setString(2, "char-value");
            statement.setString(3, "varchar-value");
            statement.setString(4, text);
            statement.setShort(5, (short) 12);
            statement.setInt(6, 123456);
            statement.setBigDecimal(7, new BigDecimal("18446744073709551615"));
            statement.setBigDecimal(8, new BigDecimal("1234567890.1234"));
            statement.setDouble(9, 2.7182818D);
            statement.setBoolean(10, true);
            statement.setDate(11, java.sql.Date.valueOf("2026-09-13"));
            statement.setTime(12, java.sql.Time.valueOf("13:45:00"));
            statement.setTimestamp(13, java.sql.Timestamp.valueOf("2026-09-13 13:45:00.123456"));
            statement.setTimestamp(14, java.sql.Timestamp.valueOf("2026-09-13 13:45:00.123456"));
            statement.setBytes(15, binary);
            statement.setString(16, "{\"user\":\"tapdata\",\"active\":true}");
            statement.executeUpdate();
        }
    }

    private Map<String, Object> findRow(List<Map<String, Object>> rows, long rowId) {
        return rows.stream().filter(row -> id(row) == rowId).findFirst()
                .orElseThrow(() -> new AssertionError("missing row " + rowId));
    }

    private long id(Map<String, Object> row) {
        Object value = row.get("id");
        if (value == null) value = row.get("ID");
        return value instanceof Number ? ((Number) value).longValue() : Long.parseLong(String.valueOf(value));
    }

    private byte[] binaryPayload() {
        byte[] bytes = new byte[1024 * 1024];
        for (int index = 0; index < bytes.length; index++) bytes[index] = (byte) (index % 251);
        return bytes;
    }

    private String textValue(Object value) {
        return value instanceof byte[] ? new String((byte[]) value, StandardCharsets.UTF_8) : String.valueOf(value);
    }

    private void createTransactionTable(String tableName) throws Exception {
        execute("CREATE TABLE " + qualified(tableName) + " (id BIGINT PRIMARY KEY, value VARCHAR(100))");
    }

    private TapTable transactionTable(String tableName) {
        return new TapTable(tableName)
                .add(new TapField("id", "BIGINT").tapType(TapSimplify.tapNumber().bit(64)).isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("value", "VARCHAR(100)").tapType(TapSimplify.tapString()));
    }

    private Capture startCapture(TapTable table, Object offset) throws InterruptedException {
        Capture capture = new Capture();
        capture.consumer = StreamReadConsumer.create((events, callbackOffset) -> {
            capture.events.addAll(events);
            for (TapEvent event : events) {
                if (event instanceof TapInsertRecordEvent) {
                    Map<String, Object> row = ((TapInsertRecordEvent) event).getAfter();
                    capture.rows.add(row);
                    capture.ids.add(id(row));
                }
            }
            capture.lastOffset.set(callbackOffset);
        });
        capture.thread = new Thread(() -> {
            try {
                functions().getStreamReadFunction().streamRead(nodeContext(), Collections.singletonList(table.getId()), offset, 100, capture.consumer);
            } catch (Throwable throwable) {
                capture.error.set(throwable);
            }
        }, "tap-it-mysql-special-stream");
        capture.thread.setDaemon(true);
        capture.thread.start();
        Thread.sleep(20000L);
        return capture;
    }

    private void waitForMutationEvents(Capture capture, long rowId) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (capture.error.get() == null && System.nanoTime() < deadline) {
            TapUpdateRecordEvent update = capture.events.stream()
                    .filter(TapUpdateRecordEvent.class::isInstance)
                    .map(TapUpdateRecordEvent.class::cast)
                    .filter(event -> id(event.getAfter()) == rowId)
                    .findFirst().orElse(null);
            TapDeleteRecordEvent delete = capture.events.stream()
                    .filter(TapDeleteRecordEvent.class::isInstance)
                    .map(TapDeleteRecordEvent.class::cast)
                    .filter(event -> id(event.getBefore()) == rowId)
                    .findFirst().orElse(null);
            if (update != null && delete != null) {
                assertEquals("updated", update.getAfter().get("value"));
                assertEquals("updated", delete.getBefore().get("value"));
                return;
            }
            Thread.sleep(250L);
        }
        assertNull(capture.error.get(), () -> "MySQL stream failed: " + capture.error.get());
        throw new AssertionError("missing MySQL UPDATE or DELETE event for row " + rowId);
    }

    private void waitForNoPrimaryKeyMutations(Capture capture, long rowId) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (capture.error.get() == null && System.nanoTime() < deadline) {
            TapInsertRecordEvent insert = eventForRow(capture, TapInsertRecordEvent.class, rowId);
            TapUpdateRecordEvent update = eventForRow(capture, TapUpdateRecordEvent.class, rowId);
            TapDeleteRecordEvent delete = eventForRow(capture, TapDeleteRecordEvent.class, rowId);
            if (insert != null && update != null && delete != null) {
                assertNull(value(insert.getAfter(), "name"));
                assertEquals("updated", value(update.getAfter(), "name"));
                assertNull(value(update.getAfter(), "note"));
                assertEquals("updated", value(delete.getBefore(), "name"));
                return;
            }
            Thread.sleep(250L);
        }
        assertNull(capture.error.get(), () -> "MySQL stream failed: " + capture.error.get());
        throw new AssertionError("missing no-primary-key CDC mutations for row " + rowId);
    }

    private void waitForSourceDdlEvents(Capture capture) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (capture.error.get() == null && System.nanoTime() < deadline) {
            boolean add = capture.events.stream().anyMatch(TapNewFieldEvent.class::isInstance);
            boolean alter = capture.events.stream().anyMatch(TapAlterFieldAttributesEvent.class::isInstance);
            boolean rename = capture.events.stream().anyMatch(TapAlterFieldNameEvent.class::isInstance);
            boolean drop = capture.events.stream().anyMatch(TapDropFieldEvent.class::isInstance);
            if (add && alter && rename && drop) {
                return;
            }
            Thread.sleep(250L);
        }
        assertNull(capture.error.get(), () -> "MySQL stream failed: " + capture.error.get());
        assertTrue(capture.events.stream().anyMatch(TapNewFieldEvent.class::isInstance), "missing add-field DDL event");
        assertTrue(capture.events.stream().anyMatch(TapAlterFieldAttributesEvent.class::isInstance), "missing alter-field DDL event");
        assertTrue(capture.events.stream().anyMatch(TapAlterFieldNameEvent.class::isInstance), "missing rename-field DDL event");
        assertTrue(capture.events.stream().anyMatch(TapDropFieldEvent.class::isInstance), "missing drop-field DDL event");
    }

    private <T extends TapEvent> T eventForRow(Capture capture, Class<T> eventType, long rowId) {
        return capture.events.stream()
                .filter(eventType::isInstance)
                .map(eventType::cast)
                .filter(event -> id(eventRow(event)) == rowId)
                .findFirst()
                .orElse(null);
    }

    private Map<String, Object> eventRow(TapEvent event) {
        if (event instanceof TapInsertRecordEvent) {
            return ((TapInsertRecordEvent) event).getAfter();
        }
        if (event instanceof TapUpdateRecordEvent) {
            return ((TapUpdateRecordEvent) event).getAfter();
        }
        return ((TapDeleteRecordEvent) event).getBefore();
    }

    private Object value(Map<String, Object> row, String fieldName) {
        Object result = row.get(fieldName);
        return result != null || row.containsKey(fieldName) ? result : row.get(fieldName.toUpperCase());
    }

    private void waitForIds(Capture capture, long... ids) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (capture.error.get() == null && System.nanoTime() < deadline) {
            boolean found = true;
            for (long id : ids) found &= capture.ids.contains(id);
            if (found) return;
            Thread.sleep(250L);
        }
        assertNull(capture.error.get(), () -> "MySQL stream failed: " + capture.error.get());
        for (long id : ids) assertTrue(capture.ids.contains(id), "missing committed MySQL row " + id);
    }

    private void waitForCount(Capture capture, int rows, int seconds) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (capture.ids.size() < rows && capture.error.get() == null && System.nanoTime() < deadline) Thread.sleep(500L);
        assertNull(capture.error.get(), () -> "MySQL stream failed: " + capture.error.get());
    }

    private void insert(PreparedStatement statement, long id, String value) throws Exception {
        statement.setLong(1, id);
        statement.setString(2, value);
        statement.executeUpdate();
    }

    private Object currentOffset() throws Throwable {
        return functions().getTimestampToStreamOffsetFunction().timestampToStreamOffset(nodeContext(), System.currentTimeMillis());
    }

    private Connection connection() throws Exception {
        DataMap config = context.getConfig();
        return DriverManager.getConnection("jdbc:mysql://" + config.getString("host") + ":" + config.getInteger("port")
                + "/" + config.getString("database"), config.getString("user"), config.getString("password"));
    }

    private String qualified(String tableName) {
        return "`" + context.getConfig().getString("database") + "`.`" + tableName + "`";
    }

    private void execute(String sql) throws Exception {
        try (Connection connection = connection(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    @Override
    protected ConnectorTestContext createContext() throws Throwable {
        MysqlConnector connector = new MysqlConnector();
        DataMap config = loadConnectionConfig();
        config.put("port", Integer.parseInt(String.valueOf(config.get("port"))));
        TapLog log = new TapLog();
        TapConnectorContext nodeContext = new TapConnectorContext(loadSpecification("mysql-spec.json"), config,
                DataMap.create().kv("enableTransaction", true), log);
        nodeContext.setStateMap(new TestStateMap());
        ConnectorFunctions functions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = TapCodecsRegistry.create();
        connector.registerCapabilities(functions, codecRegistry);
        return ConnectorTestContext.builder().connector(connector).nodeContext(nodeContext)
                .connectorFunctions(functions).codecRegistry(codecRegistry).config(config).log(log).build();
    }

    private DataMap loadConnectionConfig() throws Exception {
        if (!DbForgeLeaseProvider.isDbForgeSelected()) {
            return readConnectionConfig("config/mysql-connection.json");
        }
        if (dbForgeConnectionConfig == null) {
            dbForgeLeaseProvider = DbForgeLeaseProvider.fromEnvironment("tapdata-mysql-connector-it");
            DbForgeLeaseProvider.Connection connection = dbForgeLeaseProvider.acquire("mysql", "dedicated", "single");
            dbForgeConnectionConfig = DataMap.create();
            dbForgeConnectionConfig.put("host", connection.required("host"));
            dbForgeConnectionConfig.put("port", connection.requiredPort());
            dbForgeConnectionConfig.put("database", connection.required("database"));
            dbForgeConnectionConfig.put("user", connection.firstRequired("user", "username"));
            dbForgeConnectionConfig.put("password", connection.required("password"));
            dbForgeConnectionConfig.put("highPerformance", true);
            System.out.printf("[IT] DBForge MySQL lease acquired: leaseId=%s, host=%s, port=%s, database=%s, user=%s%n",
                    dbForgeLeaseProvider.getLeaseId(), dbForgeConnectionConfig.getString("host"),
                    dbForgeConnectionConfig.getInteger("port"), dbForgeConnectionConfig.getString("database"),
                    dbForgeConnectionConfig.getString("user"));
        }
        DataMap config = DataMap.create();
        config.putAll(dbForgeConnectionConfig);
        return config;
    }

    @AfterAll
    void releaseDbForgeLease() {
        if (dbForgeLeaseProvider == null) {
            return;
        }
        try {
            dbForgeLeaseProvider.release();
            System.out.println("[IT] DBForge MySQL lease released");
        } catch (Exception error) {
            System.err.println("[IT] Failed to release DBForge MySQL lease: " + error.getMessage());
        }
    }

    private static final class Capture {
        private final List<Long> ids = Collections.synchronizedList(new ArrayList<>());
        private final List<Map<String, Object>> rows = Collections.synchronizedList(new ArrayList<>());
        private final List<TapEvent> events = Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<Object> lastOffset = new AtomicReference<>();
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private StreamReadConsumer consumer;
        private Thread thread;

        private void stop() {
            if (consumer != null) consumer.streamReadEnded();
            if (thread != null) thread.interrupt();
        }
    }
}
