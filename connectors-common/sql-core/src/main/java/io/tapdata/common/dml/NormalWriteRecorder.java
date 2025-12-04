package io.tapdata.common.dml;

import io.netty.buffer.ByteBuf;
import io.tapdata.constant.DMLType;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.tapdata.common.dml.WritePolicyEnum.LOG_ON_NONEXISTS;

public abstract class NormalWriteRecorder {

    protected final Connection connection;
    protected final TapTable tapTable;
    protected final String schema;

    protected final List<String> allColumn;
    protected List<String> updatedColumn;
    protected List<String> removedColumn;
    protected final List<String> uniqueCondition;
    protected final Map<String, String> columnTypeMap;
    protected boolean hasPk = false;
    protected boolean targetNeedEncode = false;
    protected String fromCharset;
    protected String toCharset;

    protected String version;
    protected WritePolicyEnum insertPolicy;
    protected Boolean fileInput = false;
    protected ByteBuf buffer;
    protected WritePolicyEnum updatePolicy;
    protected WritePolicyEnum deletePolicy;
    protected char escapeChar = '"';

    protected String preparedStatementKey;
    protected Map<String, PreparedStatement> preparedStatementMap = new HashMap<>();
    protected PreparedStatement preparedStatement = null;
    protected List<String> largeSqlValues;
    protected LinkedHashMap<String, String> largeSqlValuesMap;
    protected boolean largeSql = false;

    protected final AtomicLong atomicLong = new AtomicLong(0); //record counter
    protected final List<TapRecordEvent> batchCache = TapSimplify.list(); //event cache
    protected int batchCacheSize = 0;
    protected Log tapLogger;
    protected List<String> autoIncFields;
    protected DMLType dmlType;

    public void setAutoIncFields(List<String> autoIncFields) {
        this.autoIncFields = autoIncFields;
    }

    public NormalWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        this.connection = connection;
        this.tapTable = tapTable;
        this.schema = schema;
        this.allColumn = tapTable.getNameFieldMap().entrySet().stream().sorted(Comparator.comparing(v ->
                EmptyKit.isNull(v.getValue().getPos()) ? 99999 : v.getValue().getPos())).map(Map.Entry::getKey).collect(Collectors.toList());
        if (EmptyKit.isNotEmpty(tapTable.primaryKeys(false))) {
            hasPk = true;
            uniqueCondition = new ArrayList<>(tapTable.primaryKeys(false));
        } else {
            uniqueCondition = new ArrayList<>(tapTable.primaryKeys(true));
        }
        if (EmptyKit.isEmpty(uniqueCondition)) {
            removeBigColumnAsCondition();
        }
        updatedColumn = allColumn.stream().filter(v -> !uniqueCondition.contains(v)).collect(Collectors.toList());
        if (EmptyKit.isEmpty(updatedColumn)) {
            updatedColumn.addAll(allColumn);
        }
        columnTypeMap = tapTable.getNameFieldMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getDataType()));
    }

    protected void removeBigColumnAsCondition() {

    }

    public void setLargeSql(boolean largeSql) {
        this.largeSql = largeSql;
        if (largeSql) {
            largeSqlValues = new ArrayList<>();
            largeSqlValuesMap = new LinkedHashMap<>();
        }
    }

    public void setDmlType(DMLType dmlType) {
        this.dmlType = dmlType;
    }

    public void setRemovedColumn(List<String> removedColumn) {
        this.removedColumn = removedColumn;
        allColumn.removeAll(removedColumn);
        updatedColumn.removeAll(removedColumn);
        if (EmptyKit.isEmpty(updatedColumn)) {
            updatedColumn.addAll(allColumn);
        }
        uniqueCondition.removeAll(removedColumn);
    }

    /**
     * batch write events
     *
     * @param listResult results of WriteRecord
     */
    public void executeBatch(WriteListResult<TapRecordEvent> listResult) throws SQLException {
        long succeed = batchCacheSize;
        if (succeed <= 0) {
            return;
        }
        if (largeSql) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(getLargeInsertSql());
                largeSqlValues.clear();
                largeSqlValuesMap.clear();
                batchCacheSize = 0;
            }
            atomicLong.addAndGet(succeed);
            return;
        }
        if (fileInput) {
            fileInput();
            buffer.clear();
            batchCacheSize = 0;
            atomicLong.addAndGet(succeed);
            return;
        }
        try {
            if (preparedStatement != null) {
                int[] writeResults = preparedStatement.executeBatch();
                if (LOG_ON_NONEXISTS == updatePolicy) {
                    Iterator<TapRecordEvent> iterator = batchCache.iterator();
                    int index = 0;
                    while (iterator.hasNext()) {
                        TapRecordEvent event = iterator.next();
                        if (0 >= writeResults[index++]) {
                            tapLogger.warn("update record ignored: {}", event);
                        }
                    }
                }
                if (LOG_ON_NONEXISTS == deletePolicy) {
                    Iterator<TapRecordEvent> iterator = batchCache.iterator();
                    int index = 0;
                    while (iterator.hasNext()) {
                        TapRecordEvent event = iterator.next();
                        if (0 >= writeResults[index++]) {
                            tapLogger.warn("delete record ignored: {}", event);
                        }
                    }
                }
                preparedStatement.clearBatch();
                batchCache.clear();
                batchCacheSize = 0;
            }
        } catch (SQLException e) {
//            Map<TapRecordEvent, Throwable> map = batchCache.stream().collect(Collectors.toMap(Function.identity(), (v) -> e));
//            listResult.addErrors(map);
            batchCacheSize = 0;
            throw e;
        }
        atomicLong.addAndGet(succeed);
    }

    //commit when cacheSize >= 1000
    public void addAndCheckCommit(TapRecordEvent recordEvent, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        batchCacheSize++;
        if (updatePolicy == LOG_ON_NONEXISTS && recordEvent instanceof TapUpdateRecordEvent || deletePolicy == LOG_ON_NONEXISTS && recordEvent instanceof TapDeleteRecordEvent) {
            batchCache.add(recordEvent);
        }
        if (batchCacheSize >= 1000) {
            executeBatch(listResult);
        }
    }

    public void addBatchCacheSize() {
        batchCacheSize++;
    }

    public void releaseResource() {
        preparedStatementMap.forEach((key, value) -> EmptyKit.closeQuietly(value));
        Optional.ofNullable(buffer).ifPresent(ByteBuf::release);
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setInsertPolicy(String insertPolicy) {
        this.insertPolicy = WritePolicyEnum.valueOf(insertPolicy.toUpperCase());
    }

    public void enableFileInput(ByteBuf buffer) {
        this.fileInput = true;
        this.buffer = buffer;
    }

    public void setUpdatePolicy(String updatePolicy) {
        this.updatePolicy = WritePolicyEnum.valueOf(updatePolicy.toUpperCase());
    }

    public void setDeletePolicy(String deletePolicy) {
        this.deletePolicy = WritePolicyEnum.valueOf(deletePolicy.toUpperCase());
    }

    public void setTapLogger(Log tapLogger) {
        this.tapLogger = tapLogger;
    }

    public void setEscapeChar(char escapeChar) {
        this.escapeChar = escapeChar;
    }

    public AtomicLong getAtomicLong() {
        return atomicLong;
    }

    //many types of insert data
    public void addInsertBatch(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        if (largeSql) {
            largeInsert(after);
            return;
        }
        if (EmptyKit.isEmpty(uniqueCondition)) {
            if (fileInput) {
                fileInsert(after);
                return;
            } else {
                justInsert(after);
            }
        } else {
            if (hasPk && uniqueCondition.stream().anyMatch(v -> EmptyKit.isNull(after.get(v)))) {
                tapLogger.warn("primary key has null value, record ignored or string => '': {}", after);
                boolean canWrite = true;
                for (Map.Entry<String, Object> entry : after.entrySet()) {
                    if (uniqueCondition.contains(entry.getKey()) && EmptyKit.isNull(entry.getValue())) {
                        if (EmptyKit.isNotNull(columnTypeMap.get(entry.getKey())) && columnTypeMap.get(entry.getKey()).toLowerCase().contains("char")) {
                            after.put(entry.getKey(), "");
                        } else {
                            canWrite = false;
                            break;
                        }
                    }
                }
                if (!canWrite) {
                    return;
                }
            }
            switch (insertPolicy) {
                case UPDATE_ON_EXISTS:
                    upsert(after, listResult);
                    break;
                case IGNORE_ON_EXISTS:
                    insertIgnore(after, listResult);
                    break;
                default:
                    if (fileInput) {
                        fileInsert(after);
                        return;
                    } else {
                        justInsert(after);
                    }
                    break;
            }
        }
        preparedStatement.addBatch();
    }

    protected void generatePrepareStatement(String sql, boolean containsNull, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        String preparedStatementKey = "|" + containsNull;
        if (preparedStatementKey.equals(this.preparedStatementKey)) {
            preparedStatement = preparedStatementMap.get(preparedStatementKey);
        } else {
            if (EmptyKit.isNull(this.preparedStatementKey)) {
                preparedStatement = connection.prepareStatement(sql);
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(sql);
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
            this.preparedStatementKey = preparedStatementKey;
        }
        preparedStatement.clearParameters();
    }

    //插入唯一键冲突时转更新
    protected void upsert(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        throw new UnsupportedOperationException("upsert is not supported");
    }

    public String getUpsertSql(Map<String, Object> after) throws SQLException {
        throw new UnsupportedOperationException("upsert is not supported");
    }
    public String getUpsertSqlByAfter(Map<String, Object> after) throws SQLException {
        throw new UnsupportedOperationException("upsert is not supported");
    }
    public String getInsertIgnoreSqlByAfter(Map<String, Object> after) throws SQLException {
        throw new UnsupportedOperationException("insertIgnore is not supported");
    }

    public String getDeleteSql(Map<String, Object> before) throws SQLException {
        boolean containsNull = !hasPk && before.containsValue(null);
        String sql = getDeleteSql(before,containsNull);
        if(!containsNull){
            for (String key : before.keySet()) {
                sql = sql.replaceFirst("\\?", formatValueForSql(before.get(key), columnTypeMap.get(key)));
            }
        }else{
            for (String key : before.keySet()) {
                sql = sql.replaceFirst("\\?", formatValueForSql(before.get(key), columnTypeMap.get(key)));
                sql = sql.replaceFirst("\\?", formatValueForSql(before.get(key), columnTypeMap.get(key)));
            }
        }
        return sql;
    }

    public String formatValueForSql(Object value, String dataType) throws SQLException {
        return object2String(filterValue(value, dataType));
    }

    //插入唯一键冲突时忽略
    protected void insertIgnore(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        throw new UnsupportedOperationException("insertIgnore is not supported");
    }

    protected void largeInsert(Map<String, Object> after) throws SQLException {
        throw new UnsupportedOperationException("largeInsert is not supported");
    }

    protected String quoteAndEscape(String value) {
        return escapeChar + StringKit.escape(value, escapeChar) + escapeChar;
    }

    protected String getSchemaAndTable() {
        return quoteAndEscape(schema) + "." + quoteAndEscape(tapTable.getId());
    }

    //直接插入
    protected void justInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "INSERT INTO " + getSchemaAndTable() + " ("
                    + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") " +
                    "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            setPrepareStatement(pos++, after, key);
        }
    }

    protected void fileInsert(Map<String, Object> after) {
        buffer.writeBytes((allColumn.stream().map(v -> String.valueOf(after.get(v))).collect(Collectors.joining(",")) + "\n").getBytes());
    }

    protected void fileInput() throws SQLException {
        throw new UnsupportedOperationException("fileInput is not supported");
    }

    public void addUpdateBatch(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        //去除After和Before的多余字段
        Map<String, Object> lastBefore = DbKit.getBeforeForUpdate(after, before, allColumn, uniqueCondition);
        switch (updatePolicy) {
            case INSERT_ON_NONEXISTS:
                insertUpdate(after, lastBefore, listResult);
                break;
            default:
                Map<String, Object> lastAfter = DbKit.getAfterForUpdate(after, before, allColumn, uniqueCondition);
                if (EmptyKit.isEmpty(lastAfter)) {
                    return;
                }
                justUpdate(lastAfter, lastBefore, listResult);
                break;
        }
        preparedStatement.addBatch();
    }

    //未更新到数据时转插入
    protected void insertUpdate(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        throw new UnsupportedOperationException("upsert is not supported");
    }

    //直接更新（未更新到数据时忽略）
    protected void justUpdate(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        boolean containsNull = !hasPk && before.containsValue(null);
        String preparedStatementKey = String.join(",", after.keySet()) + "|" + containsNull;
        if (preparedStatementKey.equals(this.preparedStatementKey)) {
            preparedStatement = preparedStatementMap.get(preparedStatementKey);
        } else {
            if (EmptyKit.isNull(this.preparedStatementKey)) {
                preparedStatement = connection.prepareStatement(getUpdateSql(after, before, containsNull));
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(getUpdateSql(after, before, containsNull));
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
            this.preparedStatementKey = preparedStatementKey;
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : after.keySet()) {
            setPrepareStatement(pos++, after, key);
        }
        setBeforeValue(containsNull, before, pos);
    }

    protected String getLargeInsertSql() {
        return "INSERT INTO " + getSchemaAndTable() + " ("
                + allColumn.stream().map(this::quoteAndEscape).collect(Collectors.joining(", ")) + ") VALUES "
                + String.join(", ", largeSqlValues);
    }

    protected String getUpdateSql(Map<String, Object> after, Map<String, Object> before, boolean containsNull) {
        if (!containsNull) {
            return "UPDATE " + getSchemaAndTable() + " SET " +
                    after.keySet().stream().map(k -> quoteAndEscape(k) + "=?").collect(Collectors.joining(", ")) + " WHERE " +
                    before.keySet().stream().map(k -> quoteAndEscape(k) + "=?").collect(Collectors.joining(" AND "));
        } else {
            return "UPDATE " + getSchemaAndTable() + " SET " +
                    after.keySet().stream().map(k -> quoteAndEscape(k) + "=?").collect(Collectors.joining(", ")) + " WHERE " +
                    before.keySet().stream().map(k -> "(" + quoteAndEscape(k) + "=? OR (" + quoteAndEscape(k) + " IS NULL AND ? IS NULL))")
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
            lastBefore.putAll(before);
        }
        if (EmptyKit.isNotEmpty(removedColumn)) {
            removedColumn.forEach(lastBefore::remove);
        }
        justDelete(lastBefore, listResult);
        preparedStatement.addBatch();
    }

    //直接删除
    protected void justDelete(Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        boolean containsNull = !hasPk && before.containsValue(null);
        String preparedStatementKey = "|" + containsNull;
        if (preparedStatementKey.equals(this.preparedStatementKey)) {
            preparedStatement = preparedStatementMap.get(preparedStatementKey);
        } else {
            if (EmptyKit.isNull(this.preparedStatementKey)) {
                preparedStatement = connection.prepareStatement(getDeleteSql(before, containsNull));
                preparedStatementMap.put(preparedStatementKey, preparedStatement);
            } else {
                executeBatch(listResult);
                preparedStatement = preparedStatementMap.get(preparedStatementKey);
                if (EmptyKit.isNull(preparedStatement)) {
                    preparedStatement = connection.prepareStatement(getDeleteSql(before, containsNull));
                    preparedStatementMap.put(preparedStatementKey, preparedStatement);
                }
            }
            this.preparedStatementKey = preparedStatementKey;
        }
        preparedStatement.clearParameters();
        int pos = 1;
        setBeforeValue(containsNull, before, pos);
    }

    protected String getDeleteSql(Map<String, Object> before, boolean containsNull) {
        if (!containsNull) {
            return "DELETE FROM " + getSchemaAndTable() + " WHERE " +
                    before.keySet().stream().map(k -> quoteAndEscape(k) + "=?").collect(Collectors.joining(" AND "));
        } else {
            return "DELETE FROM " + getSchemaAndTable() + " WHERE " +
                    before.keySet().stream().map(k -> "(" + quoteAndEscape(k) + "=? OR (" + quoteAndEscape(k) + " IS NULL AND ? IS NULL))")
                            .collect(Collectors.joining(" AND "));
        }
    }

    protected void setPrepareStatement(int pos, Map<String, Object> data, String key) throws SQLException {
        preparedStatement.setObject(pos, filterValue(data.get(key), columnTypeMap.get(key)));
    }

    protected void setBeforeValue(boolean containsNull, Map<String, Object> before, int pos) throws SQLException {
        if (!containsNull) {
            for (String key : before.keySet()) {
                setPrepareStatement(pos++, before, key);
            }
        } else {
            for (String key : before.keySet()) {
                setPrepareStatement(pos++, before, key);
                setPrepareStatement(pos++, before, key);
            }
        }
    }

    protected Object filterValue(Object value, String dataType) throws SQLException {
        return value;
    }


    public void setTargetNeedEncode(boolean targetNeedEncode) {
        this.targetNeedEncode = targetNeedEncode;
    }

    public void setFromCharset(String fromCharset) {
        this.fromCharset = fromCharset;
    }
    public void setToCharset(String toCharset) {
        this.toCharset = toCharset;
    }
    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    protected String object2String(Object obj) {
        String result;
        if (null == obj) {
            result = "null";
        } else if (obj instanceof String) {
            result = "'" + ((String) obj).replace("\\", "\\\\").replace("'", "\\'").replace("(", "\\(").replace(")", "\\)") + "'";
        } else if (obj instanceof Number) {
            result = obj.toString();
        } else if (obj instanceof Date) {
            result = "'" + dateFormat.format(obj) + "'";
        } else if (obj instanceof Instant) {
            result = "'" + LocalDateTime.ofInstant((Instant) obj, ZoneId.of("GMT")).format(dateTimeFormatter) + "'";
        } else if (obj instanceof byte[]) {
            String hexString = StringKit.convertToHexString((byte[]) obj);
            return "X'" + hexString + "'";
        } else if (obj instanceof Boolean) {
            if ("true".equalsIgnoreCase(obj.toString())) {
                return "1";
            }
            return "0";
        } else {
            return "'" + obj + "'";
        }
        return result;
    }

    protected String byteArrayToHexString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return "0x"+ sb;
    }
}
