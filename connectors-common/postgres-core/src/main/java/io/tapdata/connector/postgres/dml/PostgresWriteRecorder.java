package io.tapdata.connector.postgres.dml;

import io.netty.buffer.ByteBufInputStream;
import io.tapdata.common.dml.NormalWriteRecorder;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgSQLXML;
import org.postgresql.util.PGobject;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static io.tapdata.common.dml.WritePolicyEnum.LOG_ON_NONEXISTS;

public class PostgresWriteRecorder extends NormalWriteRecorder {

    private final Map<String, String> oidColumnTypeMap;

    public PostgresWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
        oidColumnTypeMap = tapTable.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> getAlias(StringKit.removeParentheses(v.getValue().getDataType().replace(" array", "")))));
    }

    private String getAlias(String type) {
        switch (type) {
            case "character":
                return "char";
            case "character varying":
                return "varchar";
            case "timestamp without time zone":
                return "timestamp";
            case "timestamp with time zone":
                return "timestamptz";
            case "double precision":
                return "float8";
            case "real":
                return "float4";
            case "time without time zone":
                return "time";
            case "time with time zone":
                return "timetz";
        }
        return type;
    }

    @Override
    protected void insertUpdate(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        boolean containsNull = !hasPk && uniqueCondition.stream().anyMatch(v -> EmptyKit.isNull(before.get(v)));
        Map<String, Object> all = new HashMap<>(before);
        all.putAll(after);
        String preparedStatementKey = "|" + containsNull;
        if (preparedStatementKey.equals(this.preparedStatementKey)) {
            preparedStatement = preparedStatementMap.get(preparedStatementKey);
        } else {
            if (EmptyKit.isNull(this.preparedStatementKey)) {
                preparedStatement = connection.prepareStatement(getInsertUpdateSql(containsNull));
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(getInsertUpdateSql(containsNull));
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
            this.preparedStatementKey = preparedStatementKey;
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : updatedColumn) {
            preparedStatement.setObject(pos++, filterValue(all.get(key), columnTypeMap.get(key)));
        }
        if (!containsNull) {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, filterValue(before.get(key), columnTypeMap.get(key)));
            }
        } else {
            for (String key : uniqueCondition) {
                preparedStatement.setObject(pos++, filterValue(before.get(key), columnTypeMap.get(key)));
                preparedStatement.setObject(pos++, filterValue(before.get(key), columnTypeMap.get(key)));
            }
        }
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, filterValue(all.get(key), columnTypeMap.get(key)));
        }
    }

    protected String getInsertUpdateSql(boolean containsNull) {
        if (!containsNull) {
            return "WITH upsert AS (UPDATE " + getSchemaAndTable() + " SET " + updatedColumn.stream().map(k -> quoteAndEscape(k) + "=?")
                    .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> quoteAndEscape(k) + "=?")
                    .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO " + getSchemaAndTable() + " ("
                    + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") SELECT "
                    + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
        } else {
            return "WITH upsert AS (UPDATE " + getSchemaAndTable() + " SET " + updatedColumn.stream().map(k -> quoteAndEscape(k) + "=?")
                    .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "(" + quoteAndEscape(k) + "=? OR (" + quoteAndEscape(k) + " IS NULL AND ?::text IS NULL))")
                    .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO " + getSchemaAndTable() + " ("
                    + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") SELECT "
                    + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
        }
    }

    protected String getUpdateSql(Map<String, Object> after, Map<String, Object> before, boolean containsNull) {
        if (!containsNull) {
            return "UPDATE " + getSchemaAndTable() + " SET " +
                    after.keySet().stream().map(k -> quoteAndEscape(k) + "=?").collect(Collectors.joining(", ")) + " WHERE " +
                    before.keySet().stream().map(k -> quoteAndEscape(k) + "=?").collect(Collectors.joining(" AND "));
        } else {
            return "UPDATE " + getSchemaAndTable() + " SET " +
                    after.keySet().stream().map(k -> quoteAndEscape(k) + "=?").collect(Collectors.joining(", ")) + " WHERE " +
                    before.keySet().stream().map(k -> "(" + quoteAndEscape(k) + "=? OR (" + quoteAndEscape(k) + " IS NULL AND ?::text IS NULL))")
                            .collect(Collectors.joining(" AND "));
        }
    }

    protected String getDeleteSql(Map<String, Object> before, boolean containsNull) {
        if (!containsNull) {
            return "DELETE FROM " + getSchemaAndTable() + " WHERE " +
                    before.keySet().stream().map(k -> quoteAndEscape(k) + "=?").collect(Collectors.joining(" AND "));
        } else {
            return "DELETE FROM " + getSchemaAndTable() + " WHERE " +
                    before.keySet().stream().map(k -> "(" + quoteAndEscape(k) + "=? OR (" + quoteAndEscape(k) + " IS NULL AND ?::text IS NULL))")
                            .collect(Collectors.joining(" AND "));
        }
    }

    @Override
    protected Object filterValue(Object value, String dataType) throws SQLException {
        if (EmptyKit.isNull(value)) {
            return null;
        }
        if ("uuid".equalsIgnoreCase(dataType)) {
            return value instanceof UUID ? value : UUID.fromString(String.valueOf(value));
        }
        if (dataType.startsWith("bit")) {
            PGobject pGobject = new PGobject();
            pGobject.setType("bit");
            if (value instanceof Boolean) {
                pGobject.setValue((Boolean) value ? "1" : "0");
            } else {
                pGobject.setValue(String.valueOf(value));
            }
            return pGobject;
        }
        if (value instanceof List) {
            return connection.createArrayOf(dataType, ((List) value).toArray());
        }
        switch (dataType) {
            case "interval":
            case "point":
            case "line":
            case "lseg":
            case "box":
            case "path":
            case "polygon":
            case "circle":
            case "money":
            case "cidr":
            case "inet":
            case "macaddr":
            case "json":
            case "geometry":
            case "jsonb":
                PGobject pGobject = new PGobject();
                pGobject.setType(dataType);
                pGobject.setValue(String.valueOf(value));
                return pGobject;
            case "xml":
                return new PgSQLXML(connection.unwrap(BaseConnection.class), String.valueOf(value));
        }
        if (dataType.endsWith("with time zone") && value instanceof LocalDateTime) {
            Timestamp timestamp = Timestamp.valueOf(((LocalDateTime) value));
            timestamp.setTime(timestamp.getTime() + TimeZone.getDefault().getRawOffset());
            return timestamp;
        }
        if (value instanceof String) {
            return ((String) value).replace("\u0000", "");
        }
        if (value instanceof Boolean && dataType.contains("int")) {
            return Boolean.TRUE.equals(value) ? 1 : 0;
        }
        return value;
    }

    public void addAndCheckCommit(TapRecordEvent recordEvent, WriteListResult<TapRecordEvent> listResult) {
        batchCacheSize++;
        if (updatePolicy == LOG_ON_NONEXISTS && recordEvent instanceof TapUpdateRecordEvent || deletePolicy == LOG_ON_NONEXISTS && recordEvent instanceof TapDeleteRecordEvent) {
            batchCache.add(recordEvent);
        }
    }

    public void fileInput() throws SQLException {
        try (ByteBufInputStream byteBufInputStream = new ByteBufInputStream(buffer)) {
            new CopyManager(connection.unwrap(BaseConnection.class)).copyIn("COPY " + getSchemaAndTable() + " FROM STDIN DELIMITER ','", byteBufInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void fileInsert(Map<String, Object> after) {
        buffer.writeBytes((allColumn.stream().map(v -> parseObject(after.get(v))).collect(Collectors.joining(",")) + "\n").getBytes());
    }

    private String parseObject(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String) {
            return StringKit.replaceEscape((String) value, "\n\r,".toCharArray());
        }
        if (value instanceof byte[]) {
            throw new UnsupportedOperationException("binary type not supported in file input");
        }
        if (value instanceof PGobject) {
            return ((PGobject) value).getValue();
        }
        return String.valueOf(value);
    }

    protected void setPrepareStatement(int pos, Map<String, Object> data, String key) throws SQLException {
        String dataType = columnTypeMap.get(key);
        if (EmptyKit.isNotNull(dataType) && dataType.endsWith(" array")) {
            preparedStatement.setObject(pos, filterValue(data.get(key), oidColumnTypeMap.get(key)));
        } else if (null != dataType && dataType.startsWith("numeric") && data.get(key) instanceof BigDecimal) {
            preparedStatement.setString(pos, data.get(key).toString());
        } else {
            preparedStatement.setObject(pos, filterValue(data.get(key), columnTypeMap.get(key)));
        }
    }

    //conflict
    @Override
    protected void upsert(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            preparedStatement = connection.prepareStatement(getUpsertSql());
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            setPrepareStatement(pos++, after, key);
        }
        for (String key : updatedColumn) {
            setPrepareStatement(pos++, after, key);
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


    @Override
    public String formatValueForSql(Object value, String dataType) {
        if (dataType.startsWith("bit")) {
            if (value instanceof Boolean) {
                value = ((Boolean) value) ? "1" : "0";
            }else{
                value = String.valueOf(value);
            }
        }
        if(value instanceof byte[]){
            return "'\\\\x" + StringKit.convertToHexString((byte[]) value) + "'";
        }

        if (dataType.endsWith("with time zone") && value instanceof LocalDateTime) {
            Timestamp timestamp = Timestamp.valueOf(((LocalDateTime) value));
            timestamp.setTime(timestamp.getTime() + TimeZone.getDefault().getRawOffset());
            value = timestamp;
        }
        if (value instanceof Boolean && dataType.contains("int")) {
            value = Boolean.TRUE.equals(value) ? 1 : 0;
        }
        if (value instanceof Boolean && dataType.contains("boolean")) {
            value = Boolean.TRUE.equals(value) ? "true" : "false";
        }
        return object2String(value);
    }


    protected String getUpsertSql() {
        return "INSERT INTO " + getSchemaAndTable() + " ("
                + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") " +
                "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON CONFLICT("
                + uniqueCondition.stream().map(this::quoteAndEscape).collect(Collectors.joining(", "))
                + ") DO UPDATE SET " + updatedColumn.stream().map(k -> quoteAndEscape(k) + "=?").collect(Collectors.joining(", "));
    }

    @Override
    protected void insertIgnore(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            preparedStatement = connection.prepareStatement(getInsertIgnoreSql());
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            setPrepareStatement(pos++, after, key);
        }
    }

    protected String getInsertIgnoreSql() {
        return "INSERT INTO " + getSchemaAndTable() + " ("
                + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") " +
                "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON CONFLICT("
                + uniqueCondition.stream().map(this::quoteAndEscape).collect(Collectors.joining(", "))
                + ") DO NOTHING ";
    }

    //old postgres
    protected void oldUpsert(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        boolean containsNull = !hasPk && uniqueCondition.stream().anyMatch(v -> EmptyKit.isNull(after.get(v)));
        String preparedStatementKey = "|" + containsNull;
        if (preparedStatementKey.equals(this.preparedStatementKey)) {
            preparedStatement = preparedStatementMap.get(preparedStatementKey);
        } else {
            if (EmptyKit.isNull(this.preparedStatementKey)) {
                preparedStatement = connection.prepareStatement(getInsertUpdateSql(containsNull));
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(getInsertUpdateSql(containsNull));
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
            this.preparedStatementKey = preparedStatementKey;
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : updatedColumn) {
            setPrepareStatement(pos++, after, key);
        }
        if (!containsNull) {
            for (String key : uniqueCondition) {
                setPrepareStatement(pos++, after, key);
            }
        } else {
            for (String key : uniqueCondition) {
                setPrepareStatement(pos++, after, key);
                setPrepareStatement(pos++, after, key);
            }
        }
        for (String key : allColumn) {
            setPrepareStatement(pos++, after, key);
        }
    }

    protected void oldInsertIgnore(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        boolean containsNull = !hasPk && uniqueCondition.stream().anyMatch(v -> EmptyKit.isNull(after.get(v)));
        String preparedStatementKey = "|" + containsNull;
        if (preparedStatementKey.equals(this.preparedStatementKey)) {
            preparedStatement = preparedStatementMap.get(preparedStatementKey);
        } else {
            if (EmptyKit.isNull(this.preparedStatementKey)) {
                preparedStatement = connection.prepareStatement(getOldInsertIgnoreSql(containsNull));
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(getOldInsertIgnoreSql(containsNull));
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
            this.preparedStatementKey = preparedStatementKey;
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            setPrepareStatement(pos++, after, key);
        }
        if (!containsNull) {
            for (String key : uniqueCondition) {
                setPrepareStatement(pos++, after, key);
            }
        } else {
            for (String key : uniqueCondition) {
                setPrepareStatement(pos++, after, key);
                setPrepareStatement(pos++, after, key);
            }
        }
    }

    protected String getOldInsertIgnoreSql(boolean containsNull) {
        if (!containsNull) {
            return "INSERT INTO " + getSchemaAndTable() + " ("
                    + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") SELECT "
                    + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT 1 FROM " + getSchemaAndTable()
                    + "  WHERE " + uniqueCondition.stream().map(k -> quoteAndEscape(k) + "=?").collect(Collectors.joining(" AND ")) + " )";
        } else {
            return "INSERT INTO " + getSchemaAndTable() + " ("
                    + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") SELECT "
                    + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT 1 FROM " + getSchemaAndTable()
                    + "  WHERE " + uniqueCondition.stream().map(k -> "(" + quoteAndEscape(k) + "=? OR (" + quoteAndEscape(k) + " IS NULL AND ?::text IS NULL))").collect(Collectors.joining(" AND ")) + " )";
        }
    }
}
