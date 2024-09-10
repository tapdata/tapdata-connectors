package io.tapdata.connector.greenplum;

import io.tapdata.connector.postgres.dml.OldPostgresWriteRecorder;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
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
                    justUpdate(after, DbKit.getBeforeForUpdate(after, new HashMap<>(), allColumn, uniqueCondition), listResult);
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

}
