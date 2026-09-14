package io.tapdata.connector.mysql;

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

/**
 * MySQL 模式性能适配器：与 {@code PerformanceConnectorIT} 配套。
 * <p>
 * 表结构对齐其余连接器（{@code ID VARCHAR(36)} 主键 + {@code EVENT_TIME} + 16 个 {@code BIGINT}
 * + 32 个 {@code VARCHAR(32)}），通过 MySQL JDBC 旁路建表/写入/计数。写入复用
 * {@link PerformanceAdapter#insertRows(int, long, int)} 的默认实现（批量 {@code addBatch}），
 * 使用标准 JDBC batch，避免性能结果依赖手写多 values SQL。
 */
final class MySQLPerformanceAdapter implements PerformanceAdapter {

    private static final int NUMERIC_FIELDS = 16;
    private static final int STRING_FIELDS = 32;

    private final DataMap connectionConfig;
    private final String tableName;
    private final TapTable table;

    MySQLPerformanceAdapter(DataMap connectionConfig) {
        this.connectionConfig = connectionConfig;
        this.tableName = "TAP_PERF_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        this.table = createTapTable();
    }

    @Override
    public TapTable table() {
        return table;
    }

    @Override
    public void createTable() throws SQLException {
        StringBuilder sql = new StringBuilder("CREATE TABLE ").append(qualified())
                .append(" (ID VARCHAR(36) NOT NULL PRIMARY KEY, EVENT_TIME DATETIME(6) NOT NULL");
        for (int index = 1; index <= NUMERIC_FIELDS; index++) {
            sql.append(", ").append(fieldName("N", index)).append(" BIGINT NOT NULL");
        }
        for (int index = 1; index <= STRING_FIELDS; index++) {
            sql.append(", ").append(fieldName("S", index)).append(" VARCHAR(32) NOT NULL");
        }
        sql.append(')');
        try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql.toString());
        }
    }

    @Override
    public void insertRows(List<Map<String, Object>> rows) throws SQLException {
        if (rows.isEmpty()) {
            return;
        }
        try (Connection connection = openConnection(); PreparedStatement statement = connection.prepareStatement(insertSql())) {
            connection.setAutoCommit(false);
            executeRows(statement, rows);
            connection.commit();
        }
    }

    @Override
    public long countRows() throws SQLException {
        try (Connection connection = openConnection(); Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + qualified())) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    @Override
    public void dropTable() throws SQLException {
        try (Connection connection = openConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS " + qualified());
        }
    }

    private TapTable createTapTable() {
        TapTable tapTable = TapSimplify.table(tableName);
        TapField id = TapSimplify.field("ID", "VARCHAR(36)").tapType(TapSimplify.tapString());
        id.isPrimaryKey(true).primaryKeyPos(1).nullable(false);
        tapTable.add(id);
        tapTable.add(TapSimplify.field("EVENT_TIME", "DATETIME(6)")
                .tapType(TapSimplify.tapDateTime()).nullable(false));
        for (int index = 1; index <= NUMERIC_FIELDS; index++) {
            tapTable.add(TapSimplify.field(fieldName("N", index), "BIGINT")
                    .tapType(TapSimplify.tapNumber().bit(64)).nullable(false));
        }
        for (int index = 1; index <= STRING_FIELDS; index++) {
            tapTable.add(TapSimplify.field(fieldName("S", index), "VARCHAR(32)")
                    .tapType(TapSimplify.tapString()).nullable(false));
        }
        return tapTable;
    }

    private Connection openConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://" + connectionConfig.getString("host") + ":"
                        + connectionConfig.getInteger("port") + "/" + connectionConfig.getString("database"),
                connectionConfig.getString("user"), connectionConfig.getString("password"));
    }

    private String qualified() {
        return "`" + connectionConfig.getString("database") + "`.`" + tableName + "`";
    }

    private String insertSql() {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(qualified()).append(" VALUES (");
        for (int index = 0; index < 2 + NUMERIC_FIELDS + STRING_FIELDS; index++) {
            if (index > 0) {
                sql.append(',');
            }
            sql.append('?');
        }
        return sql.append(')').toString();
    }

    private void executeRows(PreparedStatement statement, List<Map<String, Object>> rows) throws SQLException {
        int pending = 0;
        for (Map<String, Object> row : rows) {
            int parameter = 1;
            statement.setString(parameter++, String.valueOf(row.get("ID")));
            statement.setTimestamp(parameter++, (Timestamp) row.get("EVENT_TIME"));
            for (int index = 1; index <= NUMERIC_FIELDS; index++) {
                statement.setLong(parameter++, ((Number) row.get(fieldName("N", index))).longValue());
            }
            for (int index = 1; index <= STRING_FIELDS; index++) {
                statement.setString(parameter++, String.valueOf(row.get(fieldName("S", index))));
            }
            statement.addBatch();
            pending++;
            if (pending == 1000) {
                statement.executeBatch();
                statement.clearBatch();
                pending = 0;
            }
        }
        if (pending > 0) {
            statement.executeBatch();
            statement.clearBatch();
        }
    }

    private static String fieldName(String prefix, int index) {
        return String.format("%s%02d", prefix, index);
    }
}
