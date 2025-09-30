package io.tapdata.connector.adb.dml;

import io.tapdata.common.dml.NormalWriteRecorder;
import io.tapdata.connector.mysql.util.MysqlUtil;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.tapdata.common.dml.WritePolicyEnum.LOG_ON_NONEXISTS;

public class AliyunADBWriteRecorder extends NormalWriteRecorder {

    private final Statement statement;

    public AliyunADBWriteRecorder(Connection connection, TapTable tapTable, String schema) throws SQLException {
        super(connection, tapTable, schema);
        statement = connection.createStatement();
        setEscapeChar('`');
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

    protected String getUpsertSql() {
        return "INSERT INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " ("
                + allColumn.stream().map(k -> escapeChar + k + escapeChar).collect(Collectors.joining(", ")) + ") " +
                "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON DUPLICATE KEY UPDATE "
                + allColumn.stream().map(k -> escapeChar + k + escapeChar + "=values(" + escapeChar + k + escapeChar + ")").collect(Collectors.joining(", "));
    }

    protected void insertIgnore(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
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
                insertUpdate(lastAfter, lastBefore, listResult);
                preparedStatement.addBatch();
                break;
            default:
                justUpdate(lastAfter, lastBefore, listResult);
                break;
        }
    }

    protected void justUpdate(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        int res = statement.executeUpdate("UPDATE " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " SET " +
                after.keySet().stream().map(k -> filterSetOrWhere(after, k, false)).collect(Collectors.joining(", ")) + " WHERE " +
                before.keySet().stream().map(k -> filterSetOrWhere(before, k, true)).collect(Collectors.joining(" AND ")));
        if (0 >= res && LOG_ON_NONEXISTS == updatePolicy) {
            tapLogger.info("update record ignored, after: {}, before: {}", after, before);
        }
        atomicLong.incrementAndGet();
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
        return "INSERT INTO " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " ("
                + allColumn.stream().map(k -> escapeChar + k + escapeChar).collect(Collectors.joining(", ")) + ") " +
                "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ON DUPLICATE KEY UPDATE "
                + after.keySet().stream().map(k -> escapeChar + k + escapeChar + "=values(" + escapeChar + k + escapeChar + ")").collect(Collectors.joining(", "));
    }

    private String filterSetOrWhere(Map<String, Object> map, String key, boolean isWhere) {
        if (isWhere && EmptyKit.isNull(map.get(key))) {
            return escapeChar + key + escapeChar + " is null ";
        } else {
            return escapeChar + key + escapeChar + "=" + MysqlUtil.object2String(map.get(key));
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
            statement.executeUpdate("DELETE FROM " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " WHERE " +
                    before.keySet().stream().map(k -> filterSetOrWhere(before, k, true)).collect(Collectors.joining(" AND ")));
        } else {
            statement.executeUpdate("DELETE FROM " + escapeChar + schema + escapeChar + "." + escapeChar + tapTable.getId() + escapeChar + " WHERE " +
                    lastBefore.keySet().stream().map(k -> filterSetOrWhere(lastBefore, k, true)).collect(Collectors.joining(" AND ")));
        }
        atomicLong.incrementAndGet();
    }

    @Override
    public void executeBatch(WriteListResult<TapRecordEvent> listResult) throws SQLException {
        long succeed = batchCacheSize;
        if (succeed <= 0) {
            return;
        }
        try {
            if (preparedStatement != null) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
                batchCache.clear();
                batchCacheSize = 0;
            }
        } catch (SQLException e) {
            Map<TapRecordEvent, Throwable> map = batchCache.stream().collect(Collectors.toMap(Function.identity(), (v) -> e));
            listResult.addErrors(map);
            throw e;
        }
        atomicLong.addAndGet(succeed);
    }

    public void releaseResource() {
        preparedStatementMap.forEach((key, value) -> EmptyKit.closeQuietly(value));
        EmptyKit.closeQuietly(statement);
    }
}
