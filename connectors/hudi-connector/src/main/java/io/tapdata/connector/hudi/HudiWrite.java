package io.tapdata.connector.hudi;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hive.write.HiveJdbcWrite;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HudiWrite extends HiveJdbcWrite {

    private static final String TAG = HudiWrite.class.getSimpleName();

    private static ReentrantLock lock = new ReentrantLock();
    private final Map<String, Connection> connectionCacheMap = new LRUOnRemoveMap<>(10, entry -> closeQuietly(entry.getValue()));


    private  final  static  String  FILE_NAME = "Hive3.txt";

    private String LOAD_SQL = "load data local inpath %s overwrite into table %s";

    protected static final String INSERT_NOT_EXISTS_SQL = "INSERT INTO `%s`.`%s` SELECT ";
    protected static final String QUERY_RECORD_SQL = "SELECT * FROM `%s`.`%s` WHERE ";
    protected static final String UPDATE_INSERT_SQL = "INSERT INTO `%s`.`%s` VALUES (";
    protected static final String QUERY_TABLE_TYPE = "DESCRIBE FORMATTED `%s`.`%s`";
    private static Map<String, Boolean> morTableMap = new HashMap<>();

    public HudiWrite(HiveJdbcContext hiveJdbcContext, HudiConfig hudiConfig) {
        super(hiveJdbcContext,hudiConfig);

    }


    public void onDestroy() {
        this.running.set(false);
        this.jdbcCacheMap.values().forEach(JdbcCache::clear);
        for (Connection connection : this.connectionCacheMap.values()) {
            try {
                connection.close();
            } catch (SQLException e) {
                TapLogger.error(TAG, "connection:{} close fail:{}", connection, e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    public Connection getConnection() {
        String name = Thread.currentThread().getName();
        Connection connection2 = connectionCacheMap.get(name);
        if (connection2 == null) {
            try {
                lock.lock();
                connection2 = connectionCacheMap.get(name);
                if (connection2 == null) {
                    connection2 = hiveJdbcContext.getConnection();
                    connectionCacheMap.put(name, connection2);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
        return connection2;
    }

    @Override
    public WriteListResult<TapRecordEvent> writeJdbcRecord(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        boolean isMor = queryTableType(tapConnectorContext, tapTable);
        if (isMor) throw new RuntimeException("Mor tables are not supported currently.");
        String insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
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
                            WriteListResult<TapRecordEvent> result;
                            if (ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertDmlPolicy)){
                                result = notExistsInsert(tapConnectorContext, tapTable, tapRecordEventList);
                            }else {
                                result = batchInsert(tapConnectorContext, tapTable, tapRecordEventList);
                            }
                            tapRecordEventList.clear();
                            msgCnt = 0;
                            sumResult(writeListResult, result);

                        }
                    } else {
                        if (CollectionUtils.isNotEmpty(tapRecordEventList)) {
//                            WriteListResult<TapRecordEvent> result = batchInsert(tapConnectorContext, tapTable, tapRecordEventList);
                            WriteListResult<TapRecordEvent> result;
                            if (ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertDmlPolicy)){
                                result = notExistsInsert(tapConnectorContext, tapTable, tapRecordEventList);
                            }else {
                                result = batchInsert(tapConnectorContext, tapTable, tapRecordEventList);
                            }
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
                WriteListResult<TapRecordEvent> result;
                if (ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertDmlPolicy)){
                    result = notExistsInsert(tapConnectorContext, tapTable, tapRecordEventList);
                }else {
                    result = batchInsert(tapConnectorContext, tapTable, tapRecordEventList);
                }
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

    public WriteListResult<TapRecordEvent> notExistsInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEventList) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        int row = 0;
        PreparedStatement pstmt = null;
        try {
            String database = tapConnectorContext.getConnectionConfig().getString("database");
            StringBuilder insertColumnSql = new StringBuilder();
            Collection<String> primaryKeys = tapTable.primaryKeys(true);
            for (TapRecordEvent tapRecordEvent : tapRecordEventList) {
                notExistsInsertOne(tapConnectorContext,tapTable,tapRecordEvent,pstmt,database,insertColumnSql,primaryKeys);
                row++;
            }
            writeListResult.incrementInserted(row);

        } catch (SQLException e) {
            writeListResult = batchErrorHandle(tapConnectorContext, tapTable, tapRecordEventList, pstmt, e);
        }
        return writeListResult;
    }
    private String getCloneInsertValueSql(TapTable tapTable, Map<String, Object> after) {
        String sql = "";
        if (StringUtils.isBlank(sql)) {
            StringBuilder insertValueSB = new StringBuilder();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            nameFieldMap.keySet().stream().filter(k->after.containsKey(k)).forEach(k -> insertValueSB.append("?,"));
            sql = insertValueSB.toString();
            sql = StringUtils.removeEnd(sql, ",");
        }
        return sql;
    }

    @Override
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
    @Override
    protected int doInsertOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        String insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        StringBuilder insertColumnSql = new StringBuilder();
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        if (ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertDmlPolicy)){
            return notExistsInsertOne(tapConnectorContext,tapTable,tapRecordEvent,null,database,insertColumnSql,primaryKeys);
        }else {
            return super.doInsertOne(tapConnectorContext,tapTable,tapRecordEvent);
        }
    }

    private int notExistsInsertOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent,PreparedStatement pstmt,String database, StringBuilder insertColumnSql,Collection<String> primaryKeys) throws SQLException{
        Integer parameterIndex = 1;
        Map<String, Object> after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
        if (MapUtils.isNotEmpty(after)) {
            insertColumnSql.append(String.format(INSERT_NOT_EXISTS_SQL, database, tapTable.getId()));
            //insert into test_tapdata.hudi10 SELECT 8,'4' where not exists (SELECT * FROM test_tapdata.hudi10 WHERE col1=8)
            insertColumnSql.append(getCloneInsertValueSql(tapTable,after));
            insertColumnSql.append(String.format(" where not exists (select * from `%s`.`%s` where ", database, tapTable.getId()));
            primaryKeys.forEach(k -> insertColumnSql.append(k).append("=?").append(" and "));
        }
        pstmt = getConnection().prepareStatement(StringUtils.removeEnd(insertColumnSql.toString()," and ")+")");
        if (MapUtils.isNotEmpty(after)) {
            for (String fieldName : tapTable.getNameFieldMap().keySet()) {
                if (!after.containsKey(fieldName)) continue;
                pstmt.setObject(parameterIndex++, after.get(fieldName));
            }
            for (String primaryKey : primaryKeys) {
                pstmt.setObject(parameterIndex++, after.get(primaryKey));
            }
        }
        boolean executeSuccess = pstmt.execute();
        insertColumnSql.setLength(0);
        if (executeSuccess){
            return 1;
        }
        return 0;
    }
    @Override
    protected int doUpdateOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        String updateDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        if (ConnectionOptions.DML_UPDATE_POLICY_INSERT_ON_NON_EXISTS.equals(updateDmlPolicy)){
            boolean ifExists = queryRecordById(tapConnectorContext, tapTable, tapRecordEvent);
            if (ifExists){
                return super.doUpdateOne(tapConnectorContext,tapTable,tapRecordEvent);
            }
            else {
                return doUpdateInsert(tapConnectorContext,tapTable,tapRecordEvent);
            }
        }else {
            return super.doUpdateOne(tapConnectorContext,tapTable,tapRecordEvent);
        }
    }
    private boolean queryRecordById(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws SQLException{
        PreparedStatement pstmt = null;
        int parameterIndex = 1;
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        StringBuilder queryRecordSql = new StringBuilder();
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        Map<String, Object> after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
        if (MapUtils.isNotEmpty(after)) {
            queryRecordSql.append(String.format(QUERY_RECORD_SQL, database, tapTable.getId()));
            primaryKeys.forEach(k -> queryRecordSql.append(k).append("=?").append(" and "));
        }
        pstmt = getConnection().prepareStatement(StringUtils.removeEnd(queryRecordSql.toString()," and "));
        if (MapUtils.isNotEmpty(after)) {
            for (String primaryKey : primaryKeys) {
                pstmt.setObject(parameterIndex++, after.get(primaryKey));
            }
        }
        ResultSet resultSet = pstmt.executeQuery();
        if (resultSet.next()){
            return true;
        }
        return false;
    }
    private int doUpdateInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws SQLException{
        PreparedStatement pstmt = null;
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        StringBuilder insertColumnSql = new StringBuilder();
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        Integer parameterIndex = 1;
        Map<String, Object> after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
        if (MapUtils.isNotEmpty(after)) {
            insertColumnSql.append(String.format(UPDATE_INSERT_SQL, database, tapTable.getId()));
            after.keySet().stream().forEach(k->insertColumnSql.append("?,"));
        }
        pstmt = getConnection().prepareStatement(StringUtils.removeEnd(insertColumnSql.toString(),",")+")");
        if (MapUtils.isNotEmpty(after)) {
            for (String fieldName : tapTable.getNameFieldMap().keySet()) {
                if (!after.containsKey(fieldName)) continue;
                pstmt.setObject(parameterIndex++, after.get(fieldName));
            }
        }
        boolean executeSuccess = pstmt.execute();
        if (executeSuccess){
            return 1;
        }
        return 0;
    }
    private boolean queryTableType(TapConnectorContext tapConnectorContext, TapTable tapTable) throws SQLException{
        String database = tapConnectorContext.getConnectionConfig().getString("database");
        if (morTableMap.containsKey(database+"."+tapTable.getId())) return morTableMap.get(database+"."+tapTable.getId());
        PreparedStatement pstmt = null;
        String queryTableSql = String.format(QUERY_TABLE_TYPE, database, tapTable.getId());
        pstmt = getConnection().prepareStatement(queryTableSql);
        ResultSet resultSet = pstmt.executeQuery();
        while (resultSet.next()){
            String colName = resultSet.getString(1);
            if(null == colName || (null != colName && !"Table Properties".equals(colName))) continue;
            String tableProperties = resultSet.getString(2);
            Pattern pattern = Pattern.compile("type=(...)");
             Matcher matcher = pattern.matcher(tableProperties);
            if(matcher.find()){
                String tableType = matcher.group(1);
                if ("mor".equals(tableType)) {
                    morTableMap.put(database+"."+tapTable.getId(),true);
                    return true;
                }
            }
        }
        return false;
    }
}