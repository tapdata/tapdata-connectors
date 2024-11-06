package io.tapdata.connector.postgres.dml;

import io.tapdata.common.dml.NormalWriteRecorder;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgSQLXML;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

public class PostgresWriteRecorder extends NormalWriteRecorder {

    public PostgresWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
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
            return "WITH upsert AS (UPDATE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " SET " + updatedColumn.stream().map(k -> escapeChar + k + escapeChar + "=?")
                    .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> escapeChar + k + escapeChar + "=?")
                    .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " ("
                    + allColumn.stream().map(k -> escapeChar + k + escapeChar).collect(Collectors.joining(", ")) + ") SELECT "
                    + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
        } else {
            return "WITH upsert AS (UPDATE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " SET " + updatedColumn.stream().map(k -> escapeChar + k + escapeChar + "=?")
                    .collect(Collectors.joining(", ")) + " WHERE " + uniqueCondition.stream().map(k -> "(" + escapeChar + k + escapeChar + "=? OR (" + escapeChar + k + escapeChar + " IS NULL AND ?::text IS NULL))")
                    .collect(Collectors.joining(" AND ")) + " RETURNING *) INSERT INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " ("
                    + allColumn.stream().map(k -> escapeChar + k + escapeChar).collect(Collectors.joining(", ")) + ") SELECT "
                    + StringKit.copyString("?", allColumn.size(), ",") + " WHERE NOT EXISTS (SELECT * FROM upsert)";
        }
    }

    @Override
    protected String getDeleteSql(Map<String, Object> before, boolean containsNull) {
        if (!containsNull) {
            return "DELETE FROM " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " WHERE " +
                    before.keySet().stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(" AND "));
        } else {
            return "DELETE FROM " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " WHERE " +
                    before.keySet().stream().map(k -> "(" + escapeChar + k + escapeChar + "=? OR (" + escapeChar + k + escapeChar + " IS NULL AND ?::text IS NULL))")
                            .collect(Collectors.joining(" AND "));
        }
    }

    @Override
    protected String getUpdateSql(Map<String, Object> after, Map<String, Object> before, boolean containsNull) {
        if (!containsNull) {
            return "UPDATE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " SET " +
                    after.keySet().stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(", ")) + " WHERE " +
                    before.keySet().stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(" AND "));
        } else {
            return "UPDATE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " SET " +
                    after.keySet().stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(", ")) + " WHERE " +
                    before.keySet().stream().map(k -> "(" + escapeChar + k + escapeChar + "=? OR (" + escapeChar + k + escapeChar + " IS NULL AND ?::text IS NULL))")
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
}
