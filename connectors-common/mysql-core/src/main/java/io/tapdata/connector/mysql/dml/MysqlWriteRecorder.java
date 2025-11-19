package io.tapdata.connector.mysql.dml;

import io.tapdata.common.dml.NormalWriteRecorder;
import io.tapdata.common.dml.WritePolicyEnum;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author lemon
 */
public class MysqlWriteRecorder extends NormalWriteRecorder {

    public MysqlWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
        setEscapeChar('`');
    }

    protected void largeInsert(Map<String, Object> after) {
        largeSqlValues.add("(" + allColumn.stream().map(k -> object2String(after.get(k))).collect(Collectors.joining(", ")) + ")");
    }

    protected String getLargeInsertSql() {
        if (WritePolicyEnum.UPDATE_ON_EXISTS == insertPolicy) {
            return "INSERT INTO " + getSchemaAndTable() + " ("
                    + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") VALUES "
                    + String.join(", ", largeSqlValues) + " ON DUPLICATE KEY UPDATE "
                    + allColumn.stream().map(k -> quoteAndEscape(k) + "=values(" + quoteAndEscape(k) + ")").collect(Collectors.joining(", "));
        } else if (WritePolicyEnum.IGNORE_ON_EXISTS == insertPolicy) {
            return "INSERT IGNORE INTO " + getSchemaAndTable() + " ("
                    + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") VALUES "
                    + String.join(", ", largeSqlValues);
        } else {
            return "INSERT INTO " + getSchemaAndTable() + " ("
                    + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") VALUES "
                    + String.join(", ", largeSqlValues);
        }
    }

    @Override
    protected void upsert(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            preparedStatement = connection.prepareStatement(getUpsertSql());
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
        }
    }

    @Override
    public String getUpsertSql(Map<String, Object> after) throws SQLException {
        String sql = getUpsertSql();
        for (String key : allColumn) {
            sql = sql.replaceFirst("\\?", formatValueForSql(after.get(key), columnTypeMap.get(key)));
        }
        for (String key : updatedColumn) {
            sql = sql.replaceFirst("\\?", formatValueForSql(after.get(key), columnTypeMap.get(key)));
        }
        return sql;
    }

    protected String getUpsertSql() {
        return "INSERT INTO " + getSchemaAndTable() + " ("
                + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") " +
                "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON DUPLICATE KEY UPDATE "
                + allColumn.stream().map(k -> quoteAndEscape(k) + "=values(" + quoteAndEscape(k) + ")").collect(Collectors.joining(", "));
    }

    protected void insertIgnore(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            preparedStatement = connection.prepareStatement(getInsertIgnoreSql());
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
        }
    }

    protected String getInsertIgnoreSql() {
        return "INSERT IGNORE INTO " + getSchemaAndTable() + " ("
                + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") " +
                "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ")";
    }

    protected void insertUpdate(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        Map<String, Object> all = new HashMap<>(before);
        all.putAll(after);
        String preparedStatementKey = String.join(",", after.keySet());
        if (preparedStatementKey.equals(this.preparedStatementKey)) {
            preparedStatement = preparedStatementMap.get(preparedStatementKey);
        } else {
            if (EmptyKit.isNull(this.preparedStatementKey)) {
                preparedStatement = connection.prepareStatement(getInsertUpdateSql(after, before));
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(getInsertUpdateSql(after, before));
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
            this.preparedStatementKey = preparedStatementKey;
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterValue(all.get(key), columnTypeMap.get(key)));
        }
    }

    protected String getInsertUpdateSql(Map<String, Object> after, Map<String, Object> before) {
        return "INSERT INTO " + getSchemaAndTable() + " ("
                + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") " +
                "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON DUPLICATE KEY UPDATE "
                + after.keySet().stream().map(k -> quoteAndEscape(k) + "=values(" + quoteAndEscape(k) + ")").collect(Collectors.joining(", "));
    }

    protected Object filterValue(Object value, String dataType) {
        if (dataType.startsWith("char")) {
            return StringKit.trimTailBlank(value);
        }
        return value;
    }

    @Override
    protected String getUpdateSql(Map<String, Object> after, Map<String, Object> before, boolean containsNull) {
        return "UPDATE " + getSchemaAndTable() + " SET " +
                after.keySet().stream().map(k -> quoteAndEscape(k) + "=?").collect(Collectors.joining(", ")) + " WHERE " +
                before.keySet().stream().map(k -> castFloatAndQuoteEscape(k) + "<=>?").collect(Collectors.joining(" AND "));
    }

    @Override
    protected String getDeleteSql(Map<String, Object> before, boolean containsNull) {
        return "DELETE FROM " + getSchemaAndTable() + " WHERE " +
                before.keySet().stream().map(k -> castFloatAndQuoteEscape(k) + "<=>?").collect(Collectors.joining(" AND "));
    }

    private String castFloatAndQuoteEscape(String value) {
        if (columnTypeMap.get(value).contains("float")) {
            return "CAST(" + quoteAndEscape(value) + " AS decimal(10,6))";
        } else {
            return quoteAndEscape(value);
        }
    }

}
