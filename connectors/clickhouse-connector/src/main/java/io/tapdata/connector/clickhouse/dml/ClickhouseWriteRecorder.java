package io.tapdata.connector.clickhouse.dml;

import io.tapdata.common.dml.NormalWriteRecorder;
import io.tapdata.connector.clickhouse.config.ClickhouseConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ClickhouseWriteRecorder extends NormalWriteRecorder {

    public ClickhouseWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
        setEscapeChar('`');
    }

    public void addIsDeleted() {
        allColumn.add("is_deleted");
    }

    @Override
    public void addAndCheckCommit(TapRecordEvent recordEvent, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (recordEvent instanceof TapInsertRecordEvent) {
            batchCacheSize++;
        }
    }

    @Override
    protected void upsert(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        justInsert(after);
    }

    public void addUpdateBatch(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        //去除After和Before的多余字段
        Map<String, Object> lastBefore = DbKit.getBeforeForUpdate(after, before, allColumn, uniqueCondition);
        Map<String, Object> lastAfter = DbKit.getAfterForUpdate(after, before, allColumn, uniqueCondition);
        if (EmptyKit.isEmpty(lastAfter)) {
            return;
        }
        switch (updatePolicy) {
            case INSERT_ON_NONEXISTS:
                justInsert(lastAfter);
                preparedStatement.addBatch();
                break;
            default:
                justUpdate(lastAfter, lastBefore, listResult);
                preparedStatement.executeUpdate();
                break;
        }
        atomicLong.incrementAndGet();
    }

    @Override
    protected String getUpdateSql(Map<String, Object> after, Map<String, Object> before, boolean containsNull) {
        if (!containsNull) {
            return "ALTER TABLE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " UPDATE " +
                    after.keySet().stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(", ")) + " WHERE " +
                    before.keySet().stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(" AND "));
        } else {
            return "ALTER TABLE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " UPDATE " +
                    after.keySet().stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(", ")) + " WHERE " +
                    before.keySet().stream().map(k -> "(" + escapeChar + k + escapeChar + "=? OR (" + escapeChar + k + escapeChar + " IS NULL AND ? IS NULL))")
                            .collect(Collectors.joining(" AND "));
        }
    }

    public void addDeleteBatch(Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(before)) {
            return;
        }
        Map<String, Object> lastBefore = new HashMap<>();
        uniqueCondition.stream().filter(before::containsKey).forEach(v -> lastBefore.put(v, before.get(v)));
        //Mongo为源端时，非_id为更新条件时，lastBefore为空，此时需要原始before直接删除
        if (EmptyKit.isEmpty(lastBefore)) {
            justDelete(before, listResult);
        } else {
            justDelete(lastBefore, listResult);
        }
        preparedStatement.executeUpdate();
        atomicLong.incrementAndGet();
    }

    protected String getDeleteSql(Map<String, Object> before, boolean containsNull) {
        if (!containsNull) {
            return "ALTER TABLE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " DELETE WHERE " +
                    before.keySet().stream().map(k -> escapeChar + k + escapeChar + "=?").collect(Collectors.joining(" AND "));
        } else {
            return "ALTER TABLE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " DELETE WHERE " +
                    before.keySet().stream().map(k -> "(" + escapeChar + k + escapeChar + "=? OR (" + escapeChar + k + escapeChar + " IS NULL AND ? IS NULL))")
                            .collect(Collectors.joining(" AND "));
        }
    }

    @Override
    protected Object filterValue(Object value, String dataType) {
        if (EmptyKit.isNull(dataType)) {
            return value;
        }
        if (EmptyKit.isNull(value)) {
            return null;
        }
        if (dataType.contains("Int")) {
            if (value instanceof Float) {
                return ((Float) value).intValue();
            } else if (value instanceof Double) {
                return ((Double) value).intValue();
            }
        }
        if (dataType.startsWith("Decimal")) {
            return value.toString();
        }
        return value;
    }

}
