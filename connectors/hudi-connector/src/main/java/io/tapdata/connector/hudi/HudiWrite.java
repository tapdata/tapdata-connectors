package io.tapdata.connector.hudi;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hive.write.HiveJdbcWrite;
import io.tapdata.connector.hudi.config.HudiConfig;
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
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class HudiWrite extends HiveJdbcWrite {

    private static final String TAG = HudiWrite.class.getSimpleName();

    private static ReentrantLock lock = new ReentrantLock();
    private final Map<String, Connection> connectionCacheMap = new LRUOnRemoveMap<>(10, entry -> closeQuietly(entry.getValue()));


    private  final  static  String  FILE_NAME = "Hive3.txt";

    private String LOAD_SQL = "load data local inpath %s overwrite into table %s";

    protected static final String INSERT_NOT_EXISTS_SQL = "INSERT INTO `%s`.`%s` SELECT ";

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
        String insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
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

    public WriteListResult<TapRecordEvent> notExistsInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEventList) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        int row = 0;
        PreparedStatement pstmt = null;
        try {
            String database = tapConnectorContext.getConnectionConfig().getString("database");
            StringBuilder insertColumnSql = new StringBuilder(String.format(INSERT_NOT_EXISTS_SQL, database, tapTable.getId()));
            Collection<String> primaryKeys = tapTable.primaryKeys();
            for (TapRecordEvent tapRecordEvent : tapRecordEventList) {
                Map<String, Object> after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
                if (MapUtils.isNotEmpty(after)) {
                    //insert into test_tapdata.hudi10 SELECT 8,'4' where not exists (SELECT * FROM test_tapdata.hudi10 WHERE col1=8)
                    insertColumnSql.append(getCloneInsertValueSql(tapTable));
                    insertColumnSql.append(String.format(" where not exists (select * from `%s`.`%s` where", database, tapTable.getId()));
                    primaryKeys.forEach(k -> insertColumnSql.append(k).append("=?").append(" and "));
                    row++;
                }
            }
            writeListResult.incrementInserted(row);

            int parameterIndex = 1;
            pstmt = getConnection().prepareStatement(StringUtils.removeEnd(insertColumnSql.toString(), " and "));
            for (TapRecordEvent tapRecordEvent : tapRecordEventList) {
                Map<String, Object> after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
                if (MapUtils.isNotEmpty(after)) {
                    for (String fieldName : tapTable.getNameFieldMap().keySet()) {
                        pstmt.setObject(parameterIndex++, after.get(fieldName));
                    }
                    for (String primaryKey : primaryKeys) {
                        pstmt.setObject(parameterIndex++, after.get(primaryKey));
                    }
                }
            }

            pstmt.execute();
        } catch (SQLException e) {
            writeListResult = batchErrorHandle(tapConnectorContext, tapTable, tapRecordEventList, pstmt, e);
        }
        return writeListResult;
    }
    private String getCloneInsertValueSql(TapTable tapTable) {
        String sql = "";
        if (StringUtils.isBlank(sql)) {
            StringBuilder insertValueSB = new StringBuilder("(");
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            nameFieldMap.keySet().forEach(k -> insertValueSB.append("?,"));
            sql = insertValueSB.toString();
            sql = StringUtils.removeEnd(sql, ",") + ")";
        }
        return sql;
    }

    }

