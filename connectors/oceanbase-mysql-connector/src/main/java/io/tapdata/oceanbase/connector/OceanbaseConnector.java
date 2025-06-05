package io.tapdata.oceanbase.connector;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.connector.mysql.MysqlExceptionCollector;
import io.tapdata.connector.mysql.ddl.sqlmaker.MysqlDDLSqlGenerator;
import io.tapdata.connector.mysql.dml.MysqlRecordWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.oceanbase.OceanbaseJdbcContext;
import io.tapdata.oceanbase.OceanbaseTest;
import io.tapdata.oceanbase.bean.OceanbaseConfig;
import io.tapdata.oceanbase.cdc.OceanbaseReaderV2;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.*;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author dayun
 * @date 2022/6/23 15:56
 */
@TapConnectorClass("oceanbase-spec.json")
public class OceanbaseConnector extends MysqlConnector {

    private String connectionTimezone;
    private ArrayList<String> prepareSqlBeforeQuery;

    /**
     * The method invocation life circle is below,
     * initiated -> connectionTest -> destroy -> ended
     * <p>
     * You need to create the connection to your data source and release the connection in destroy method.
     * In connectionContext, you can get the connection config which is the user input for your connection application, described in your json file.
     * <p>
     * consumer can call accept method multiple times to test different items
     *
     * @param connectionContext
     * @return
     */
    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        //Assume below tests are successfully, below tests are recommended, but not required.
        //Connection test
        mysqlConfig = new OceanbaseConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        try (
                OceanbaseTest oceanbaseTest = new OceanbaseTest((OceanbaseConfig) mysqlConfig, consumer, connectionOptions)
        ) {
            oceanbaseTest.testOneByOne();
            return connectionOptions;
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadWithoutOffset);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        connectorFunctions.supportStreamRead(this::streamRead);
        //If database need insert record before table created, then please implement the below two methods.
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportDropTable(this::dropTable);

        //If database need insert record before table created, please implement the custom codec for the TapValue that data types in spec.json didn't cover.
        //TapTimeValue, TapMapValue, TapDateValue, TapArrayValue, TapYearValue, TapNumberValue, TapBooleanValue, TapDateTimeValue, TapBinaryValue, TapRawValue, TapStringValue
        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) {
                return toJson(tapRawValue.getValue());
            }
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null)
                return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return toJson(tapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapBooleanValue.class, "boolean", tapValue -> {
            if (tapValue != null) {
                Boolean value = tapValue.getValue();
                if (value != null && value) {
                    return 1;
                }
            }
            return 0;
        });
        codecRegistry.registerFromTapValue(TapBinaryValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null && tapValue.getValue().getValue() != null)
                return toJson(tapValue.getValue().getValue());
            return "null";
        });
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            if (EmptyKit.isNotNull(tapDateTimeValue.getValue().getTimeZone())) {
                tapDateTimeValue.getValue().setTimeZone(TimeZone.getTimeZone("UTC"));
            } else {
                tapDateTimeValue.getValue().setTimeZone(timeZone);
            }
            return tapDateTimeValue.getValue().isContainsIllegal() ? tapDateTimeValue.getValue().getIllegalDate() : formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS");
        });
        //date类型通过jdbc读取时，会自动转换为当前时区的时间，所以设置为当前时区
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().isContainsIllegal() ? tapDateValue.getValue().getIllegalDate() : tapDateValue.getValue().toInstant());
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTimeStr());
        codecRegistry.registerFromTapValue(TapYearValue.class, TapValue::getOriginValue);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> mysqlJdbcContext.getConnection(), this::isAlive, c));
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);
        //ddl
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
    }

    /**
     * @param tapConnectorContext
     * @param tapRecordEvents
     * @param tapTable
     * @param writeListResultConsumer
     */
    protected void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        String insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        new MysqlRecordWriter(mysqlJdbcContext, tapTable)
                .setInsertPolicy(insertDmlPolicy)
                .setUpdatePolicy(updateDmlPolicy)
                .setTapLogger(tapLogger)
                .write(tapRecordEvents, writeListResultConsumer, this::isAlive);
    }

    @Override
    protected long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        try {
            AtomicLong count = new AtomicLong(0);
            String sql = "select count(1) from " + getSchemaAndTable(tapTable.getId());
            jdbcContext.nextQueryWithTimeout(sql, resultSet -> count.set(resultSet.getLong(1)), prepareSqlBeforeQuery);
            return count.get();
        } catch (SQLException e) {
            exceptionCollector.collectReadPrivileges("batchCount", Collections.emptyList(), e);
            throw e;
        }
    }


    @Override
    protected String getBatchReadSelectSql(TapTable tapTable) {
        String columns = tapTable.getNameFieldMap().keySet().stream().map(c -> commonDbConfig.getEscapeChar() + StringKit.escape(c, commonDbConfig.getEscapeChar()) + commonDbConfig.getEscapeChar()).collect(Collectors.joining(","));
        return String.format("SELECT /*+ PARALLEL(8) */ %s FROM " + getSchemaAndTable(tapTable.getId()), columns);
    }

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        prepareSqlBeforeQuery = new ArrayList<>();
        long queryTimeout = 86400L * 30 * 1000 * 1000;
        prepareSqlBeforeQuery.add("SET @@ob_trx_timeout = " + queryTimeout);
        prepareSqlBeforeQuery.add("SET @@ob_query_timeout = " + queryTimeout);

        mysqlConfig = new OceanbaseConfig().load(tapConnectionContext.getConnectionConfig());
        mysqlConfig.load(tapConnectionContext.getNodeConfig());
        mysqlJdbcContext = new OceanbaseJdbcContext(mysqlConfig);
        commonDbConfig = mysqlConfig;
        isConnectorStarted(tapConnectionContext, connectorContext -> {
            firstConnectorId = (String) connectorContext.getStateMap().get("firstConnectorId");
            if (EmptyKit.isNull(firstConnectorId)) {
                firstConnectorId = UUID.randomUUID().toString().replace("-", "");
                connectorContext.getStateMap().put("firstConnectorId", firstConnectorId);
            }
        });
        jdbcContext = mysqlJdbcContext;
        tapLogger = tapConnectionContext.getLog();
        commonSqlMaker = new CommonSqlMaker('`');
        exceptionCollector = new MysqlExceptionCollector();
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.connectionTimezone = tapConnectionContext.getConnectionConfig().getString("timezone");
            if ("Database Timezone".equals(this.connectionTimezone) || StringUtils.isBlank(this.connectionTimezone)) {
                this.zoneId = mysqlJdbcContext.queryTimeZone().toZoneId();
            }
            ddlSqlGenerator = new MysqlDDLSqlGenerator(version, ((TapConnectorContext) tapConnectionContext).getTableMap());
        }
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        EmptyKit.closeQuietly(mysqlJdbcContext);
    }

    private TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) throws Throwable {
        DataMap dataMap = mysqlJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("TABLE_ROWS")));
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("DATA_LENGTH")));
        return tableInfo;
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) throws SQLException {
        if (EmptyKit.isNotNull(offsetStartTime)) {
            return offsetStartTime / 1000L;
        }
        AtomicLong offset = new AtomicLong(0);
        mysqlJdbcContext.queryWithNext("select current_timestamp()", resultSet -> {
            offset.set(resultSet.getTimestamp(1).getTime() / 1000L);
        });
        return offset.get();
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        OceanbaseReaderV2 oceanbaseReader = new OceanbaseReaderV2((OceanbaseConfig) mysqlConfig, firstConnectorId);
        oceanbaseReader.init(tableList, nodeContext.getTableMap(), offsetState, recordSize, consumer);
        oceanbaseReader.start(this::isAlive);
    }

    private List<String> getTablePartitions(TapConnectorContext tapConnectorContext, TapTable tapTable) {
        String sql = String.format("SELECT TABLE_NAME, PARTITION_NAME, PARTITION_ORDINAL_POSITION FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = '%s'", tapTable.getId());
        List<String> partitions = new ArrayList<>();
        try {
            mysqlJdbcContext.queryWithTimeout(sql, resultSet -> {
                while (resultSet.next()) {
                    partitions.add(resultSet.getString(2));
                }
            }, prepareSqlBeforeQuery);
        } catch (SQLException e) {
            exceptionCollector.collectReadPrivileges("getTablePartitions", Collections.emptyList(), e);
        }
        return partitions;
    }

    protected void batchReadWithoutHashSplit(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = getBatchReadSelectSql(tapTable);
        mysqlJdbcContext.streamQueryWithTimeout(sql, resultSetConsumer(tapTable, eventBatchSize, eventsOffsetConsumer), prepareSqlBeforeQuery, Integer.MIN_VALUE);
    }

    private void batchReadWorker(String sql, TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Exception {
        int retry = 20;
        while (retry-- > 0 && isAlive()) {
            try {
                tapLogger.info("batchRead, sql: {}", sql);
                mysqlJdbcContext.streamQueryWithTimeout(sql, resultSet -> {
                    List<TapEvent> tapEvents = list();
                    //get all column names
                    List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
                    while (isAlive() && resultSet.next()) {
                        DataMap dataMap = DbKit.getRowFromResultSet(resultSet, columnNames);
                        processDataMap(dataMap, tapTable);
                        tapEvents.add(insertRecordEvent(dataMap, tapTable.getId()));
                        if (tapEvents.size() == eventBatchSize) {
                            syncEventSubmit(tapEvents, eventsOffsetConsumer);
                            tapEvents = list();
                        }
                    }
                    //last events those less than eventBatchSize
                    if (EmptyKit.isNotEmpty(tapEvents)) {
                        syncEventSubmit(tapEvents, eventsOffsetConsumer);
                    }
                }, prepareSqlBeforeQuery, Integer.MIN_VALUE);
                break;
            } catch (Exception e) {
                if (retry == 0 || !(e instanceof SQLRecoverableException || e instanceof IOException)) {
                    throw e;
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected void batchReadWithPartition(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = getBatchReadSelectSql(tapTable);
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(commonDbConfig.getBatchReadThreadSize());
        ExecutorService executorService = Executors.newFixedThreadPool(commonDbConfig.getBatchReadThreadSize());
        List<String> tablePartitions = getTablePartitions(tapConnectorContext, tapTable);
        Integer threadSize = commonDbConfig.getBatchReadThreadSize();
        if (threadSize > tablePartitions.size()) {
            threadSize = tablePartitions.size();
        }

        // 计算每个线程应该处理的分区数
        int partitionsPerThread = tablePartitions.size() / threadSize;
        int remainingPartitions = tablePartitions.size() % threadSize;

        try {
            for (int i = 0; i < threadSize; i++) {
                final int threadIndex = i;
                // 计算每个线程的起始和结束索引
                int startIndex = threadIndex * partitionsPerThread + Math.min(threadIndex, remainingPartitions);
                int endIndex = startIndex + partitionsPerThread + (threadIndex < remainingPartitions ? 1 : 0);

                // 提交任务给线程池
                executorService.submit(() -> {
                    try {
                        // 获取当前线程要处理的分区子列表
                        List<String> threadPartitions = tablePartitions.subList(startIndex, endIndex);

                        // 打印线程信息和对应的分区
                        System.out.println("Thread " + threadIndex + " processing partitions: " + threadPartitions);

                        // 这里可以添加处理每个分区的具体逻辑
                        for (String partition : threadPartitions) {
                            String splitSql = sql + " partition(" + partition + ")";
                            try {
                                batchReadWorker(splitSql, tapTable, eventBatchSize, eventsOffsetConsumer);
                            } catch (Exception e) {
                                throwable.set(e);
                            }
                            if (EmptyKit.isNotNull(throwable.get())) {
                                exceptionCollector.collectTerminateByServer(throwable.get());
                                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), throwable.get());
                                exceptionCollector.revealException(throwable.get());
                                try {
                                    throw throwable.get();
                                } catch (Throwable e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (EmptyKit.isNotNull(throwable.get())) {
                exceptionCollector.collectTerminateByServer(throwable.get());
                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), throwable.get());
                exceptionCollector.revealException(throwable.get());
                throw throwable.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
            executorService.shutdown();
        }
    }

    protected void batchReadWithHashSplit(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        List<String> tablePartitions = getTablePartitions(tapConnectorContext, tapTable);
        if (tablePartitions.size() >= 4) {
            batchReadWithPartition(tapConnectorContext, tapTable, offsetState, eventBatchSize, eventsOffsetConsumer);
            return;
        }

        String sql = getBatchReadSelectSql(tapTable);
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(commonDbConfig.getBatchReadThreadSize());
        ExecutorService executorService = Executors.newFixedThreadPool(commonDbConfig.getBatchReadThreadSize());
        Integer threadSize = commonDbConfig.getBatchReadThreadSize();

        try {
            for (int i = 0; i < threadSize; i++) {
                final int threadIndex = i;
                executorService.submit(() -> {
                    try {
                        for (int ii = threadIndex; ii < commonDbConfig.getMaxSplit(); ii += commonDbConfig.getBatchReadThreadSize()) {
                            String splitSql = sql + " WHERE " + getHashSplitModConditions(tapTable, commonDbConfig.getMaxSplit(), ii);
                            tapLogger.info("batchRead, splitSql[{}]: {}", ii + 1, splitSql);
                            batchReadWorker(splitSql, tapTable, eventBatchSize, eventsOffsetConsumer);
                        }
                    } catch (Exception e) {
                        throwable.set(e);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (EmptyKit.isNotNull(throwable.get())) {
                exceptionCollector.collectTerminateByServer(throwable.get());
                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), throwable.get());
                exceptionCollector.revealException(throwable.get());
                throw throwable.get();
            }
        } finally {
            executorService.shutdown();
        }
    }

    protected Map<String, Object> filterTimeForMysql(
            ResultSet resultSet, ResultSetMetaData metaData, Set<String> dateTypeSet, TapRecordEvent recordEvent,
            IllegalDateConsumer illegalDateConsumer) throws SQLException {
        Map<String, Object> data = new HashMap<>();
        List<String> illegalDateFieldName = new ArrayList<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i + 1);
            if (!dateTypeSet.contains(columnName)) {
                data.put(columnName, resultSet.getObject(i + 1));
            } else {
                Object value;
                try {
                    value = resultSet.getObject(i + 1);
                } catch (Exception e) {
                    value = null;
                }
                String string = resultSet.getString(i + 1);
                //非法时间
                if (EmptyKit.isNull(value) && EmptyKit.isNotNull(string)) {
                    if (null == illegalDateConsumer || null == recordEvent) {
                        data.put(columnName, null);
                    } else {
                        data.put(columnName, buildIllegalDate(recordEvent, illegalDateConsumer, string, illegalDateFieldName, columnName));
                    }
                } else if (null == value) {
                    data.put(columnName, null);
                } else {
                    if ("TIME".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                        data.put(columnName, string);
                    } else if ("YEAR".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                        data.put(columnName, resultSet.getInt(i + 1));
                    } else if ("TIMESTAMP".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                        data.put(columnName, ((Timestamp) value).toLocalDateTime().atZone(ZoneOffset.UTC));
                    } else if ("DATE".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                        if (value instanceof java.sql.Date) {
                            data.put(columnName, ((java.sql.Date) value).toLocalDate().atStartOfDay());
                        } else {
                            data.put(columnName, value);
                        }
                    } else if ("DATETIME".equalsIgnoreCase(metaData.getColumnTypeName(i + 1)) && value instanceof Timestamp) {
                        data.put(columnName, ((Timestamp) value).toLocalDateTime().minusHours(zoneOffsetHour));
                    } else {
                        data.put(columnName, value);
                    }
                }
            }
        }
        if (null != illegalDateConsumer && null != recordEvent && !EmptyKit.isEmpty(illegalDateFieldName)) {
            illegalDateConsumer.buildIllegalDateFieldName(recordEvent, illegalDateFieldName);
        }
        return data;
    }

}
