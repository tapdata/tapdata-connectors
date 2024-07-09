package io.tapdata.connector.hive.write;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class HiveJdbcWrite {

    private static final String TAG = HiveJdbcWrite.class.getSimpleName();
    protected static final String INSERT_SQL_TEMPLATE = "INSERT INTO `%s`.`%s` values(%s)";
    protected static final String UPDATE_SQL_TEMPLATE = "UPDATE `%s`.`%s` set %s WHERE %s";
    protected static final String DELETE_SQL_TEMPLATE = "DELETE FROM `%s`.`%s` WHERE %s";
    //不支持带字段插入
    protected static final String BATCH_INSERT_SQL = "INSERT INTO `%s`.`%s` VALUES ";

    private final Map<String, String> batchInsertColumnSql = new LRUMap<>(10);

    private final Map<String, String> batchInsertValueSql = new LRUMap<>(10);

    protected AtomicBoolean running = new AtomicBoolean(true);

    protected final Map<String, JdbcCache> jdbcCacheMap = new ConcurrentHashMap<>();

    protected HiveJdbcContext hiveJdbcContext;

    public static final int MAX_BATCH_SAVE_SIZE = 10000;


    private HiveConfig hiveConfig;
    public HiveJdbcWrite(HiveJdbcContext hiveJdbcContext, HiveConfig hiveConfig) {
        this.hiveConfig = hiveConfig;
        this.hiveJdbcContext = hiveJdbcContext;
    }


    public WriteListResult<TapRecordEvent> writeJdbcRecord(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        TapRecordEvent errorRecord = null;
        List<TapRecordEvent> tapRecordEventList = new ArrayList<>();
        try {
            int msgCnt = 0;
            for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
                if (!running.get()) break;
                try {
                    if (tapRecordEvent instanceof TapInsertRecordEvent) {
                        tapRecordEventList.add(tapRecordEvent);
                        msgCnt++;
                        if (msgCnt >= MAX_BATCH_SAVE_SIZE) {
                            WriteListResult<TapRecordEvent> result = batchInsert(tapConnectorContext, tapTable, tapRecordEventList);
                            tapRecordEventList.clear();
                            msgCnt = 0;
                            sumResult(writeListResult, result);

                        }
                    } else {
                        if (CollectionUtils.isNotEmpty(tapRecordEventList)) {
                            WriteListResult<TapRecordEvent> result = batchInsert(tapConnectorContext, tapTable, tapRecordEventList);
                            tapRecordEventList.clear();
                            msgCnt = 0;
                            sumResult(writeListResult, result);

                        }
                        WriteListResult<TapRecordEvent> result = writeOne(tapConnectorContext, tapTable, Arrays.asList(tapRecordEvent));
                        sumResult(writeListResult, result);
                    }
                } catch (Throwable e) {
                    TapLogger.error("target database process message failed", "table name:{},error msg:{}", tapTable.getId(), e.getMessage(), e);
                    errorRecord = tapRecordEvent;
                    throw e;
                }
            }
            if (CollectionUtils.isNotEmpty(tapRecordEventList)) {
                WriteListResult<TapRecordEvent> result = batchInsert(tapConnectorContext, tapTable, tapRecordEventList);
                tapRecordEventList.clear();
                msgCnt = 0;
                sumResult(writeListResult, result);
            }
        } catch (Throwable e) {
            if (null != errorRecord) writeListResult.addError(errorRecord, e);
            throw e;
        }
        return writeListResult;
    }

    public WriteListResult<TapRecordEvent> writeOne(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        TapRecordEvent errorRecord = null;
        try {
            for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
                if (!running.get()) break;
                try {
                    if (tapRecordEvent instanceof TapInsertRecordEvent) {
                        int insertRow = doInsertOne(tapConnectorContext, tapTable, tapRecordEvent);
                        writeListResult.incrementInserted(insertRow);
                    } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                        int updateRow = doUpdateOne(tapConnectorContext, tapTable, tapRecordEvent);
                        writeListResult.incrementModified(updateRow);
                    } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                        int deleteRow = doDeleteOne(tapConnectorContext, tapTable, tapRecordEvent);
                        writeListResult.incrementRemove(deleteRow);
                    } else {
                        writeListResult.addError(tapRecordEvent, new Exception("Event type \"" + tapRecordEvent.getClass().getSimpleName() + "\" not support: " + tapRecordEvent));
                    }
                } catch (Throwable e) {
                    errorRecord = tapRecordEvent;
                    throw e;
                }
            }
//            Hive1JdbcContext.tryCommit(connection);
        } catch (Throwable e) {
            writeListResult.setInsertedCount(0);
            writeListResult.setModifiedCount(0);
            writeListResult.setRemovedCount(0);
            if (null != errorRecord) writeListResult.addError(errorRecord, e);
            throw e;
        }
        return writeListResult;
    }


    public WriteListResult<TapRecordEvent> batchInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEventList) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        int row = 0;
        PreparedStatement pstmt = null;
        try {
            String cloneInsertColumnSql = getCloneInsertColumnSql(tapConnectorContext, tapTable);
            if (StringUtils.isBlank(cloneInsertColumnSql)) {
                throw new RuntimeException("Does not found table " + tapTable.getId() + " 's fields ");
            }
            StringBuilder insertColumnSql = new StringBuilder(cloneInsertColumnSql);
            for (TapRecordEvent tapRecordEvent : tapRecordEventList) {
                Map<String, Object> after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
                if (MapUtils.isNotEmpty(after)) {
                    insertColumnSql.append(getCloneInsertValueSql(tapTable)).append(",");
                    row++;
                }
            }
            writeListResult.incrementInserted(row);

            int parameterIndex = 1;
            pstmt = getConnection().prepareStatement(StringUtils.removeEnd(insertColumnSql.toString(), ","));
            for (TapRecordEvent tapRecordEvent : tapRecordEventList) {
                Map<String, Object> after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
                if (MapUtils.isNotEmpty(after)) {
                    for (String fieldName : tapTable.getNameFieldMap().keySet()) {
                        pstmt.setObject(parameterIndex++, after.get(fieldName));
                    }
                }
            }

            pstmt.execute();
        } catch (SQLException e) {
            writeListResult = batchErrorHandle(tapConnectorContext, tapTable, tapRecordEventList, pstmt, e);
        } finally {
            if (null != pstmt) {
                pstmt.close();
            }
        }
        return writeListResult;
    }

    private String getCloneInsertColumnSql(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Exception {
        String table = tapTable.getId();
        String sql = batchInsertColumnSql.get(table);
        if (StringUtils.isBlank(sql)) {
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("get insert column error, table \"" + tapTable.getId() + "\"'s fields is empty");
            }
            String database = tapConnectorContext.getConnectionConfig().getString("database");
            sql = String.format(BATCH_INSERT_SQL, database, table.toLowerCase());
            batchInsertColumnSql.put(table, sql);
        }
        return sql;
    }


    private String getCloneInsertValueSql(TapTable tapTable) {
        String table = tapTable.getId();
        String sql = batchInsertValueSql.get(table);
        if (StringUtils.isBlank(sql)) {
            StringBuilder insertValueSB = new StringBuilder("(");
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            nameFieldMap.keySet().forEach(k -> insertValueSB.append("?,"));
            sql = insertValueSB.toString();
            sql = StringUtils.removeEnd(sql, ",") + ")";
            batchInsertValueSql.put(table, sql);
        }
        return sql;
    }

    protected int doInsertOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        int row;
        try {
            final Map<String, PreparedStatement> insertMap = getJdbcCache().getInsertMap();
            PreparedStatement insertPreparedStatement = getInsertPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, insertMap);
            setPreparedStatementValues(tapTable, tapRecordEvent, insertPreparedStatement);
            try {
                row = insertPreparedStatement.executeUpdate();
            } catch (Throwable e) {
                throw new RuntimeException("Insert data failed, sql: " + insertPreparedStatement + ", message: " + e.getMessage(), e);
            }
        } catch (Throwable e) {
            throw e;
        }
        return row;
    }

    protected WriteListResult<TapRecordEvent> batchErrorHandle(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents, PreparedStatement preparedStatement, Exception e) throws Throwable {
        TapLogger.warn("Batch insert data failed,", "fail reason:{},will retry one by one insert,stacks:{}", e.getMessage(), e);
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
            WriteListResult<TapRecordEvent> result = writeOne(tapConnectorContext, tapTable, Arrays.asList(tapRecordEvent));
            sumResult(writeListResult, result);
        }
        return writeListResult;
    }



    protected PreparedStatement getInsertPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, Map<String, PreparedStatement> insertMap) throws Throwable {
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = insertMap.get(key);
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String name = connectionConfig.getString("name");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create insert prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
            }
            List<String> fields = new ArrayList<>();
            nameFieldMap.forEach((fieldName, field) -> {
                if (!needAddIntoPreparedStatementValues(field, tapRecordEvent)) {
                    return;
                }
                fields.add("`" + fieldName + "`");
            });
            List<String> questionMarks = fields.stream().map(f -> "?").collect(Collectors.toList());
            String sql = String.format(INSERT_SQL_TEMPLATE, database, tableId, String.join(",", questionMarks));

            try {
                preparedStatement = getConnection().prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create insert prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create insert prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            insertMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }

    protected int doUpdateOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        final Map<String, PreparedStatement> updateMap = getJdbcCache().getUpdateMap();
        PreparedStatement updatePreparedStatement = getUpdatePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, updateMap);
        int parameterIndex = setPreparedStatementValues(tapTable, tapRecordEvent, updatePreparedStatement);
        setPreparedStatementWhere(tapTable, tapRecordEvent, updatePreparedStatement, parameterIndex);
        int row = updatePreparedStatement.executeUpdate();
        return row + 1;
    }


    protected int doDeleteOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        final Map<String, PreparedStatement> deleteMap = getJdbcCache().getDeleteMap();
        PreparedStatement deletePreparedStatement = getDeletePreparedStatement(tapConnectorContext, tapTable, tapRecordEvent, deleteMap);
        setPreparedStatementWhere(tapTable, tapRecordEvent, deletePreparedStatement, 1);
        int row;
        try {
            row = deletePreparedStatement.executeUpdate();
        } catch (Throwable e) {
            throw new Exception("Delete data failed, sql: " + deletePreparedStatement + ", message: " + e.getMessage(), e);
        }
        return row + 1;
    }

    protected PreparedStatement getDeletePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, Map<String, PreparedStatement> deleteMap) throws Throwable {
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = deleteMap.get(key);
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create delete prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + database + "\"'s database");
            }
            List<String> whereList = new ArrayList<>();
            Collection<String> uniqueKeys = getUniqueKeys(tapTable);
            for (String uniqueKey : uniqueKeys) {
                whereList.add("`" + uniqueKey + "`=?");
            }
            String sql = String.format(DELETE_SQL_TEMPLATE, database, tableId, String.join(" AND ", whereList));
            try {
                preparedStatement = getConnection().prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create delete prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create delete prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            deleteMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }


    protected PreparedStatement getUpdatePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, Map<String, PreparedStatement> updateMap) throws Throwable {
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = updateMap.get(key);
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create update prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + database + "\"'s database");
            }
            List<String> setList = new ArrayList<>();
            Collection<String> uniqueKeys = getUniqueKeys(tapTable);
            nameFieldMap.forEach((fieldName, field) -> {
                if (!needAddIntoPreparedStatementValues(field, tapRecordEvent)) {
                    return;
                }
                //不更新主键
                if (!uniqueKeys.contains(fieldName))
                    setList.add("`" + fieldName.toLowerCase() + "`=?");
            });
            List<String> whereList = new ArrayList<>();
            for (String uniqueKey : uniqueKeys) {
                whereList.add("`" + uniqueKey + "`=?");
            }
            String sql = String.format(UPDATE_SQL_TEMPLATE, database, tableId, String.join(",", setList), String.join(" AND ", whereList));
            try {
                preparedStatement = getConnection().prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create update prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create update prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            updateMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }



    protected int setPreparedStatementValues(TapTable tapTable, TapRecordEvent tapRecordEvent, PreparedStatement preparedStatement) throws Throwable {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        int parameterIndex = 1;
        Map<String, Object> after = getAfter(tapRecordEvent);
        if (MapUtils.isEmpty(after)) {
            throw new Exception("Set prepared statement values failed, after is empty: " + tapRecordEvent);
        }
        List<String> afterKeys = new ArrayList<>(after.keySet());
        Collection<String> uniqueKeys = getUniqueKeys(tapTable);
        for (String fieldName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(fieldName);
            if (!needAddIntoPreparedStatementValues(tapField, tapRecordEvent)) {
                continue;
            }
            if (!(tapRecordEvent instanceof TapUpdateRecordEvent) || !uniqueKeys.contains(fieldName)) {
                preparedStatement.setObject(parameterIndex++, after.get(fieldName));
            }
            afterKeys.remove(fieldName);
        }
        if (CollectionUtils.isNotEmpty(afterKeys)) {
            Map<String, Object> missingAfter = new HashMap<>();
            afterKeys.forEach(k -> missingAfter.put(k, after.get(k)));
            TapLogger.warn(TAG, "Found fields in after data not exists in schema fields, will skip it: " + missingAfter);
        }
        return parameterIndex;
    }




    protected void setPreparedStatementWhere(TapTable tapTable, TapRecordEvent tapRecordEvent, PreparedStatement preparedStatement, int parameterIndex) throws Throwable {
        if (parameterIndex <= 1) {
            parameterIndex = 1;
        }
        Map<String, Object> before = getBefore(tapRecordEvent);
        Map<String, Object> after = getAfter(tapRecordEvent);
        if (MapUtils.isEmpty(before) && MapUtils.isEmpty(after)) {
            throw new Exception("Set prepared statement where clause failed, before and after both empty: " + tapRecordEvent);
        }
        Map<String, Object> data;
        if (MapUtils.isNotEmpty(before)) {
            data = before;
        } else {
            data = after;
        }
        Collection<String> uniqueKeys = getUniqueKeys(tapTable);
        for (String uniqueKey : uniqueKeys) {
            if (!data.containsKey(uniqueKey)) {
                throw new Exception("Set prepared statement where clause failed, unique key \"" + uniqueKey + "\" not exists in data: " + tapRecordEvent);
            }
            Object value = data.get(uniqueKey);
            preparedStatement.setObject(parameterIndex++, value);
        }
    }


    protected String getKey(TapTable tapTable, TapRecordEvent tapRecordEvent) {
        Map<String, Object> after = getAfter(tapRecordEvent);
        Map<String, Object> before = getBefore(tapRecordEvent);
        Map<String, Object> data;
        if (MapUtils.isNotEmpty(after)) {
            data = after;
        } else {
            data = before;
        }
        Set<String> keys = data.keySet();
        String keyString = String.join("-", keys);
        return tapTable.getId() + "-" + keyString;
    }


    protected Map<String, Object> getBefore(TapRecordEvent tapRecordEvent) {
        Map<String, Object> before = null;
        if (tapRecordEvent instanceof TapUpdateRecordEvent) {
            before = ((TapUpdateRecordEvent) tapRecordEvent).getBefore();
        } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
            before = ((TapDeleteRecordEvent) tapRecordEvent).getBefore();
        }
        return before;
    }

    protected Map<String, Object> getAfter(TapRecordEvent tapRecordEvent) {
        Map<String, Object> after = null;
        if (tapRecordEvent instanceof TapInsertRecordEvent) {
            after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
        } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
            after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
        }
        return after;
    }


    protected Collection<String> getUniqueKeys(TapTable tapTable) {
        return tapTable.primaryKeys(true);
    }

    protected boolean needAddIntoPreparedStatementValues(TapField field, TapRecordEvent tapRecordEvent) {
        Map<String, Object> after = getAfter(tapRecordEvent);
        if (null == after) {
            return false;
        }
        if (!after.containsKey(field.getName())) {
            TapLogger.warn(TAG, "Found schema field not exists in after data, will skip it: " + field.getName());
            return false;
        }
        return true;
    }

    private JdbcCache getJdbcCache() {
        String name = Thread.currentThread().getName();
        JdbcCache jdbcCache = jdbcCacheMap.get(name);
        if (null == jdbcCache) {
            jdbcCache = new JdbcCache();
            jdbcCacheMap.put(name, jdbcCache);
        }
        return jdbcCache;
    }

    public void sumResult(WriteListResult<TapRecordEvent> writeListResult, WriteListResult<TapRecordEvent> result) {
        writeListResult.incrementInserted(writeListResult.getInsertedCount() + result.getInsertedCount());
        writeListResult.incrementRemove(writeListResult.getRemovedCount() + result.getRemovedCount());
        writeListResult.incrementModified(writeListResult.getModifiedCount() + result.getModifiedCount());
    }

    protected Connection getConnection() throws SQLException {
        return hiveJdbcContext.getConnection();
    }
    protected static class LRUOnRemoveMap<K, V> extends LRUMap<K, V> {

        private Consumer<Map.Entry<K, V>> onRemove;

        public LRUOnRemoveMap(int maxSize, Consumer<Map.Entry<K, V>> onRemove) {
            super(maxSize);
            this.onRemove = onRemove;
        }

        @Override
        protected boolean removeLRU(LinkEntry<K, V> entry) {
            onRemove.accept(entry);
            return super.removeLRU(entry);
        }

        @Override
        public void clear() {
            Set<Map.Entry<K, V>> entries = this.entrySet();
            for (Map.Entry<K, V> entry : entries) {
                onRemove.accept(entry);
            }
            super.clear();
        }

        @Override
        protected void removeEntry(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
            onRemove.accept(entry);
            super.removeEntry(entry, hashIndex, previous);
        }

        @Override
        protected void removeMapping(HashEntry<K, V> entry, int hashIndex, HashEntry<K, V> previous) {
            onRemove.accept(entry);
            super.removeMapping(entry, hashIndex, previous);
        }
    }

    protected static class JdbcCache {
        private final Map<String, PreparedStatement> insertMap = new LRUOnRemoveMap<>(10, entry -> closeQuietly(entry.getValue()));
        private final Map<String, PreparedStatement> updateMap = new LRUOnRemoveMap<>(10, entry -> closeQuietly(entry.getValue()));
        private final Map<String, PreparedStatement> deleteMap = new LRUOnRemoveMap<>(10, entry -> closeQuietly(entry.getValue()));
        private final Map<String, PreparedStatement> checkExistsMap = new LRUOnRemoveMap<>(10, entry -> closeQuietly(entry.getValue()));

        public Map<String, PreparedStatement> getInsertMap() {
            return insertMap;
        }

        public Map<String, PreparedStatement> getUpdateMap() {
            return updateMap;
        }

        public Map<String, PreparedStatement> getDeleteMap() {
            return deleteMap;
        }

        public Map<String, PreparedStatement> getCheckExistsMap() {
            return checkExistsMap;
        }

        public void clear() {
            this.insertMap.clear();
            this.updateMap.clear();
            this.deleteMap.clear();
            this.checkExistsMap.clear();
        }
    }

    public static void closeQuietly(AutoCloseable c) {
        try {
            if (null != c) {
                c.close();
            }
        } catch (Throwable ignored) {
        }
    }
}
