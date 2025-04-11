package io.tapdata.connector.greenplum;

import io.tapdata.connector.postgres.dml.OldPostgresWriteRecorder;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgSQLXML;
import org.postgresql.util.PGobject;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class GreenplumWriteRecorder extends OldPostgresWriteRecorder {

    private final PreparedStatement queryStatement;
    private final PreparedStatement queryStatementWithNull;

    public GreenplumWriteRecorder(Connection connection, TapTable tapTable, String schema) throws SQLException {
        super(connection, tapTable, schema);
        queryStatement = connection.prepareStatement(getQuerySql(false));
        queryStatementWithNull = connection.prepareStatement(getQuerySql(true));
    }

    @Override
    protected void upsert(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        boolean containsNull = !hasPk && uniqueCondition.stream().anyMatch(v -> EmptyKit.isNull(after.get(v)));
        int pos = 1;
        if (!containsNull) {
            queryStatement.clearParameters();
            for (String key : uniqueCondition) {
                queryStatement.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
            }
            try (ResultSet resultSet = queryStatement.executeQuery()) {
                if (resultSet.next()) {
                    justUpdate(DbKit.getAfterForUpdate(after, new HashMap<>(), allColumn, uniqueCondition), DbKit.getBeforeForUpdate(after, new HashMap<>(), allColumn, uniqueCondition), listResult);
                } else {
                    justInsert(after);
                }
            }
        } else {
            queryStatementWithNull.clearParameters();
            for (String key : uniqueCondition) {
                queryStatementWithNull.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
                queryStatementWithNull.setObject(pos++, filterValue(after.get(key), columnTypeMap.get(key)));
            }
            try (ResultSet resultSet = queryStatementWithNull.executeQuery()) {
                if (resultSet.next()) {
                    justUpdate(after, DbKit.getBeforeForUpdate(after, new HashMap<>(), allColumn, uniqueCondition), listResult);
                } else {
                    justInsert(after);
                }
            }
        }
    }

    protected String getQuerySql(boolean containsNull) {
        if (!containsNull) {
            return "SELECT 1 FROM " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " WHERE " +
                    uniqueCondition.stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(" AND "));
        } else {
            return "SELECT 1 FROM " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " WHERE " +
                    uniqueCondition.stream().map(k -> "(" + escapeChar + k + escapeChar + "=? OR (" + escapeChar + k + escapeChar + " IS NULL AND ?::text IS NULL))")
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
            return ((String) value).replace("\u0000", "").replace("\u008e", "");
        }
        if (value instanceof Boolean && dataType.contains("int")) {
            return Boolean.TRUE.equals(value) ? 1 : 0;
        }
        return value;
    }

}
