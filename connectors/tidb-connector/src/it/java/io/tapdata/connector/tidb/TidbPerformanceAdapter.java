package io.tapdata.connector.tidb;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.performance.PerformanceAdapter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;

final class TidbPerformanceAdapter implements PerformanceAdapter {
    private static final int NUMERIC_FIELDS = 16;
    private static final int STRING_FIELDS = 32;
    private final DataMap config;
    private final String tableName;
    private final TapTable table;

    TidbPerformanceAdapter(DataMap config) {
        this.config = config;
        tableName = "TAP_PERF_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        table = createTapTable();
    }

    public TapTable table() { return table; }
    public void createTable() throws SQLException { StringBuilder sql = new StringBuilder("CREATE TABLE ").append(qualified()).append(" (ID VARCHAR(36) NOT NULL PRIMARY KEY, EVENT_TIME DATETIME(6) NOT NULL"); for (int index = 1; index <= NUMERIC_FIELDS; index++) sql.append(", ").append(fieldName("N", index)).append(" BIGINT NOT NULL"); for (int index = 1; index <= STRING_FIELDS; index++) sql.append(", ").append(fieldName("S", index)).append(" VARCHAR(32) NOT NULL"); try (Connection connection = open(); Statement statement = connection.createStatement()) { statement.executeUpdate(sql.append(')').toString()); } }
    public void insertRows(List<Map<String, Object>> rows) throws SQLException { if (rows.isEmpty()) return; try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(insertSql())) { connection.setAutoCommit(false); for (Map<String, Object> row : rows) { int parameter = 1; statement.setString(parameter++, String.valueOf(row.get("ID"))); statement.setTimestamp(parameter++, (Timestamp) row.get("EVENT_TIME")); for (int index = 1; index <= NUMERIC_FIELDS; index++) statement.setLong(parameter++, ((Number) row.get(fieldName("N", index))).longValue()); for (int index = 1; index <= STRING_FIELDS; index++) statement.setString(parameter++, String.valueOf(row.get(fieldName("S", index)))); statement.addBatch(); } statement.executeBatch(); connection.commit(); } }
    public long countRows() throws SQLException { try (Connection connection = open(); Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM " + qualified())) { rows.next(); return rows.getLong(1); } }
    public void dropTable() throws SQLException { try (Connection connection = open(); Statement statement = connection.createStatement()) { statement.executeUpdate("DROP TABLE IF EXISTS " + qualified()); } }
    private TapTable createTapTable() { TapTable result = TapSimplify.table(tableName); TapField id = TapSimplify.field("ID", "VARCHAR(36)").tapType(TapSimplify.tapString()); id.isPrimaryKey(true).primaryKeyPos(1).nullable(false); result.add(id); result.add(TapSimplify.field("EVENT_TIME", "DATETIME(6)").tapType(TapSimplify.tapDateTime()).nullable(false)); for (int index = 1; index <= NUMERIC_FIELDS; index++) result.add(TapSimplify.field(fieldName("N", index), "BIGINT").tapType(TapSimplify.tapNumber().bit(64)).nullable(false)); for (int index = 1; index <= STRING_FIELDS; index++) result.add(TapSimplify.field(fieldName("S", index), "VARCHAR(32)").tapType(TapSimplify.tapString()).nullable(false)); return result; }
    private Connection open() throws SQLException { return DriverManager.getConnection("jdbc:mysql://" + config.getString("host") + ":" + config.getInteger("port") + "/" + config.getString("database"), config.getString("user"), config.getString("password")); }
    private String qualified() { return "`" + config.getString("database") + "`.`" + tableName + "`"; }
    private String insertSql() { StringBuilder sql = new StringBuilder("INSERT INTO ").append(qualified()).append(" VALUES ("); for (int index = 0; index < 2 + NUMERIC_FIELDS + STRING_FIELDS; index++) { if (index > 0) sql.append(','); sql.append('?'); } return sql.append(')').toString(); }
    private static String fieldName(String prefix, int index) { return String.format("%s%02d", prefix, index); }
}
