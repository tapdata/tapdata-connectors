package io.tapdata.connector.mysql;

import com.mysql.cj.exceptions.StatementIsClosedException;
import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.connector.mysql.bean.MysqlColumn;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.constant.DeployModeEnum;
import io.tapdata.connector.mysql.ddl.sqlmaker.MysqlDDLSqlGenerator;
import io.tapdata.connector.mysql.dml.MysqlRecordWriter;
import io.tapdata.connector.mysql.util.MysqlUtil;
import io.tapdata.connector.mysql.writer.MysqlSqlBatchWriter;
import io.tapdata.connector.mysql.writer.MysqlWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.exception.TapPdkRetryableEx;
import io.tapdata.kit.EmptyKit;
import io.tapdata.partition.DatabaseReadPartitionSplitter;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionOptions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import io.tapdata.pdk.apis.partition.splitter.StringCaseInsensitiveSplitter;
import io.tapdata.pdk.apis.partition.splitter.TypeSplitterMap;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2022-04-25 15:09
 **/
@TapConnectorClass("mysql-spec.json")
public class MysqlConnector extends CommonDbConnector {

    protected MysqlJdbcContextV2 mysqlJdbcContext;
    protected MysqlConfig mysqlConfig;
    protected MysqlReader mysqlReader;
    protected MysqlWriter mysqlWriter;
    protected String version;
    protected TimeZone timezone;

    protected final AtomicBoolean started = new AtomicBoolean(false);
    public static final String MASTER_NODE_KEY = "MASTER_NODE";
    public HashMap<String, MysqlJdbcContextV2> contextMapForMasterSlave;


    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        mysqlConfig = new MysqlConfig().load(tapConnectionContext.getConnectionConfig());
        mysqlConfig.load(tapConnectionContext.getNodeConfig());
        contextMapForMasterSlave = MysqlUtil.buildContextMapForMasterSlave(mysqlConfig);
        MysqlUtil.buildMasterNode(mysqlConfig, contextMapForMasterSlave);
        MysqlJdbcContextV2 contextV2 = contextMapForMasterSlave.get(mysqlConfig.getHost() + mysqlConfig.getPort());
        if (null != contextV2) {
            mysqlJdbcContext = contextV2;
        } else {
            mysqlJdbcContext = new MysqlJdbcContextV2(mysqlConfig);
        }
        commonDbConfig = mysqlConfig;
        jdbcContext = mysqlJdbcContext;
        commonSqlMaker = new CommonSqlMaker('`');
        tapLogger = tapConnectionContext.getLog();
        exceptionCollector = new MysqlExceptionCollector();
        this.version = mysqlJdbcContext.queryVersion();
        ArrayList<Map<String, Object>> inconsistentNodes = MysqlUtil.compareMasterSlaveCurrentTime(mysqlConfig, contextMapForMasterSlave);
        if (null != inconsistentNodes && inconsistentNodes.size() == 2) {
            Map<String, Object> node1 = inconsistentNodes.get(0);
            Map<String, Object> node2 = inconsistentNodes.get(1);
            tapLogger.warn(String.format("The time of each node is inconsistent, please check nodes: %s and %s", node1.toString(), node2.toString()));
        }
        if (tapConnectionContext instanceof TapConnectorContext) {
            if (DeployModeEnum.fromString(mysqlConfig.getDeploymentMode()) == DeployModeEnum.MASTER_SLAVE) {
                KVMap<Object> stateMap = ((TapConnectorContext) tapConnectionContext).getStateMap();
                Object masterNode = stateMap.get(MASTER_NODE_KEY);
                if (null != masterNode && null != mysqlConfig.getMasterNode()) {
                    if (!masterNode.toString().contains(mysqlConfig.getMasterNode().toString()))
                        tapLogger.warn(String.format("The master node has switched, please pay attention to whether the data is consistent, current master node: %s", mysqlConfig.getMasterNode()));
                }
            }
            this.mysqlWriter = new MysqlSqlBatchWriter(mysqlJdbcContext, this::isAlive);
            this.mysqlReader = new MysqlReader(mysqlJdbcContext, tapLogger, this::isAlive);
            this.timezone = mysqlJdbcContext.queryTimeZone();
            ddlSqlGenerator = new MysqlDDLSqlGenerator(version, ((TapConnectorContext) tapConnectionContext).getTableMap());
        }
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
        started.set(true);
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapMapValue.class, "json", tapValue -> toJson(tapValue.getValue()));
        codecRegistry.registerFromTapValue(TapArrayValue.class, "json", tapValue -> toJson(tapValue.getValue()));

        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            if (tapDateTimeValue.getValue() != null && tapDateTimeValue.getValue().getTimeZone() == null) {
                if ("timestamp".equals(tapDateTimeValue.getOriginType())) {
                    tapDateTimeValue.getValue().setTimeZone(timezone);
                }
            }
            return formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS");
        });
        //date类型通过jdbc读取时，会自动转换为当前时区的时间，所以设置为当前时区
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> {
            if (tapDateValue.getValue() != null && tapDateValue.getValue().getTimeZone() == null) {
                tapDateValue.getValue().setTimeZone(TimeZone.getDefault());
            }
            return formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd");
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTimeStr());
        codecRegistry.registerFromTapValue(TapYearValue.class, tapYearValue -> {
            if (tapYearValue.getValue() != null && tapYearValue.getValue().getTimeZone() == null) {
                tapYearValue.getValue().setTimeZone(timezone);
            }
            return formatTapDateTime(tapYearValue.getValue(), "yyyy");
        });

        codecRegistry.registerFromTapValue(TapBooleanValue.class, "tinyint(1)", TapValue::getValue);

        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadV2);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportQueryIndexes(this::queryIndexes);
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> mysqlJdbcContext.getConnection(), this::isAlive, c));
        connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);
        //connectorFunctions.supportQueryFieldMinMaxValueFunction(this::minMaxValue);
        //connectorFunctions.supportGetReadPartitionsFunction(this::getReadPartitions);
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);
        connectorFunctions.supportTransactionBeginFunction(this::begin);
        connectorFunctions.supportTransactionCommitFunction(this::commit);
        connectorFunctions.supportTransactionRollbackFunction(this::rollback);
    }

    private void rollback(TapConnectorContext tapConnectorContext) {
    }

    private void commit(TapConnectorContext tapConnectorContext) {
    }

    private void begin(TapConnectorContext tapConnectorContext) {
    }

    private void getReadPartitions(TapConnectorContext connectorContext, TapTable table, GetReadPartitionOptions options) throws Throwable {
        DatabaseReadPartitionSplitter.calculateDatabaseReadPartitions(connectorContext, table, options)
                .queryFieldMinMaxValue(this::minMaxValue)
                .typeSplitterMap(options.getTypeSplitterMap().registerSplitter(TypeSplitterMap.TYPE_STRING, StringCaseInsensitiveSplitter.INSTANCE))
                .startSplitting();
    }

    private void partitionRead(TapConnectorContext connectorContext, TapTable table, ReadPartition readPartition, int eventBatchSize, Consumer<List<TapEvent>> consumer) {

    }

    private FieldMinMaxValue minMaxValue(TapConnectorContext tapConnectorContext, TapTable tapTable, TapAdvanceFilter tapPartitionFilter, String fieldName) {
        SqlMaker sqlMaker = new MysqlMaker();
        FieldMinMaxValue fieldMinMaxValue = FieldMinMaxValue.create().fieldName(fieldName);
        String selectSql, aaa;
        try {
            selectSql = sqlMaker.selectSql(tapConnectorContext, tapTable, TapPartitionFilter.create().fromAdvanceFilter(tapPartitionFilter));
        } catch (Throwable e) {
            throw new RuntimeException("Build sql with partition filter failed", e);
        }
        // min value
        String minSql = selectSql.replaceFirst("SELECT \\* FROM", String.format("SELECT MIN(`%s`) AS MIN_VALUE FROM", fieldName));
        AtomicReference<Object> minObj = new AtomicReference<>();
        try {
            mysqlJdbcContext.query(minSql, rs -> {
                if (rs.next()) {
                    minObj.set(rs.getObject("MIN_VALUE"));
                }
            });
        } catch (Throwable e) {
            throw new RuntimeException("Query min value failed, sql: " + minSql, e);
        }
        Optional.ofNullable(minObj.get()).ifPresent(min -> fieldMinMaxValue.min(min).detectType(min));
        // max value
        String maxSql = selectSql.replaceFirst("SELECT \\* FROM", String.format("SELECT MAX(`%s`) AS MAX_VALUE FROM", fieldName));
        AtomicReference<Object> maxObj = new AtomicReference<>();
        try {
            mysqlJdbcContext.query(maxSql, rs -> {
                if (rs.next()) {
                    maxObj.set(rs.getObject("MAX_VALUE"));
                }
            });
        } catch (Throwable e) {
            throw new RuntimeException("Query max value failed, sql: " + maxSql, e);
        }
        Optional.ofNullable(maxObj.get()).ifPresent(max -> fieldMinMaxValue.max(max).detectType(max));
        return fieldMinMaxValue;
    }

    protected RetryOptions errorHandle(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
        RetryOptions retryOptions = RetryOptions.create();
        retryOptions.setNeedRetry(true);
        retryOptions.beforeRetryMethod(() -> {
            try {
                synchronized (this) {
                    //mysqlJdbcContext是否有效
                    if (mysqlJdbcContext == null || !checkValid() || !started.get() || checkStatementClosed(throwable)) {
                        //如果无效执行onStop,有效就return
                        this.onStop(tapConnectionContext);
                        if (isAlive()) {
                            this.onStart(tapConnectionContext);
                        }
                    } else {
                        mysqlWriter.selfCheck();
                        if (EmptyKit.isNotNull(mysqlReader)) {
                            EmptyKit.closeQuietly(mysqlReader);
                        }
                        mysqlReader = new MysqlReader(mysqlJdbcContext, tapLogger, this::isAlive);
                    }
                }
            } catch (Throwable ignore) {
            }
        });
        return retryOptions;
    }

    private boolean checkStatementClosed(Throwable throwable) {
        Throwable cause = matchThrowable(throwable, StatementIsClosedException.class);
        if (throwable instanceof TapPdkRetryableEx && null != cause && "S1009".equals(((StatementIsClosedException) cause).getSQLState())) {
            return true;
        } else {
            return false;
        }
    }

    private boolean checkValid() {
        try {
            mysqlJdbcContext.queryVersion();
            return true;
        } catch (Throwable ignored) {
            return false;
        }
    }


    @Override
    public void onStop(TapConnectionContext connectionContext) {
        started.set(false);
        if (connectionContext instanceof TapConnectorContext && null != mysqlConfig && DeployModeEnum.fromString(mysqlConfig.getDeploymentMode()) == DeployModeEnum.MASTER_SLAVE) {
            KVMap<Object> stateMap = ((TapConnectorContext) connectionContext).getStateMap();
            if (null != stateMap) {
                stateMap.put(MASTER_NODE_KEY, mysqlConfig.getMasterNode());
                ((TapConnectorContext) connectionContext).setStateMap(stateMap);
            }
        }
        try {
            Optional.ofNullable(this.mysqlReader).ifPresent(MysqlReader::close);
        } catch (Exception ignored) {
        }
        if (null != mysqlJdbcContext) {
            try {
                this.mysqlJdbcContext.close();
                this.mysqlJdbcContext = null;
            } catch (Exception e) {
                tapLogger.error("Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
            }
        }
        if (EmptyKit.isNotEmpty(contextMapForMasterSlave)) {
            contextMapForMasterSlave.forEach((hostPort, context) -> {
                try {
                    context.close();
                } catch (Exception e) {
                    tapLogger.error("Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
                }
            });
            contextMapForMasterSlave = null;
        }
    }

    protected TapField makeTapField(DataMap dataMap) {
        return new MysqlColumn(dataMap).withVersion(version).getTapField();
    }

    protected CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws SQLException {
        if (Boolean.TRUE.equals(mysqlConfig.getDoubleActive())) {
            createDoubleActiveTempTable();
        }
        CreateTableOptions createTableOptions = new CreateTableOptions();
        try {
            if (mysqlJdbcContext.queryAllTables(Collections.singletonList(tapCreateTableEvent.getTableId())).size() > 0) {
                DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
                String database = connectionConfig.getString("database");
                String tableId = tapCreateTableEvent.getTableId();
                createTableOptions.setTableExists(true);
                tapLogger.info("Table \"{}.{}\" exists, skip auto create table", database, tableId);
            } else {
                String mysqlVersion = mysqlJdbcContext.queryVersion();
                SqlMaker sqlMaker = new MysqlMaker();
                if (null == tapCreateTableEvent.getTable()) {
                    tapLogger.warn("Create table event's tap table is null, will skip it: " + tapCreateTableEvent);
                    return createTableOptions;
                }
                String[] createTableSqls = sqlMaker.createTable(tapConnectorContext, tapCreateTableEvent, mysqlVersion);
                mysqlJdbcContext.batchExecute(Arrays.asList(createTableSqls));
                createTableOptions.setTableExists(false);
            }

        } catch (Throwable t) {
            exceptionCollector.collectWritePrivileges("createTable", Collections.emptyList(), t);
            throw new RuntimeException("Create table failed, message: " + t.getMessage(), t);
        }
        List<TapIndex> indexList = tapCreateTableEvent.getTable().getIndexList();
        if (EmptyKit.isNotEmpty(indexList) && tapConnectorContext.getNodeConfig() != null && tapConnectorContext.getNodeConfig().getValue("syncIndex", false)) {
            List<String> sqlList = TapSimplify.list();
            List<TapIndex> createIndexList = new ArrayList<>();
            List<TapIndex> existsIndexList = discoverIndex(tapCreateTableEvent.getTable().getId());
            // 如果索引已经存在，就不再创建; 名字相同视为存在; 字段以及顺序相同, 也视为存在
            if (EmptyKit.isNotEmpty(existsIndexList)) {
                for (TapIndex tapIndex : indexList) {
                    boolean exists = false;
                    for (TapIndex existsIndex : existsIndexList) {
                        if (tapIndex.getName().equals(existsIndex.getName())) {
                            exists = true;
                            break;
                        }
                        if (tapIndex.getIndexFields().size() == existsIndex.getIndexFields().size()) {
                            boolean same = true;
                            for (int i = 0; i < tapIndex.getIndexFields().size(); i++) {
                                if (!tapIndex.getIndexFields().get(i).getName().equals(existsIndex.getIndexFields().get(i).getName())
                                        || tapIndex.getIndexFields().get(i).getFieldAsc() != existsIndex.getIndexFields().get(i).getFieldAsc()) {
                                    same = false;
                                    break;
                                }
                            }
                            if (same) {
                                exists = true;
                                break;
                            }
                        }
                    }
                    if (!exists) {
                        createIndexList.add(tapIndex);
                    }
                }
            } else {
                createIndexList.addAll(indexList);
            }
            tapLogger.info("Table: {} will create Index list: {}", tapCreateTableEvent.getTable().getName(), createIndexList);
            if (EmptyKit.isNotEmpty(createIndexList)) {
                createIndexList.stream().filter(i -> !i.isPrimary()).forEach(i ->
                        sqlList.add(getCreateIndexSql(tapCreateTableEvent.getTable(), i)));
            }
            jdbcContext.batchExecute(sqlList);
        }
        return createTableOptions;
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
//        WriteListResult<TapRecordEvent> writeListResult = this.mysqlWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
//        consumer.accept(writeListResult);
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
                .write(tapRecordEvents, consumer, this::isAlive);
    }


    private Map<String, Object> filterTimeForMysql(ResultSet resultSet, ResultSetMetaData metaData, Set<String> dateTypeSet) throws SQLException {
        Map<String, Object> data = new HashMap<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i + 1);
            try {
                Object value;
                if ("TIME".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                    value = resultSet.getString(i + 1);
                } else if ("DATE".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                    value = resultSet.getString(i + 1);
                } else {
                    try {
                        value = resultSet.getObject(i + 1);
                    } catch (Exception ignore) {
                        value = resultSet.getString(i + 1);
                    }
                    if (null == value && dateTypeSet.contains(columnName)) {
                        value = resultSet.getString(i + 1);
                    }
                }
                if (value != null && dateTypeSet.contains(columnName)) {
                    String valueS = value.toString();
                    // 如果是0000开头的时间，或者包含 -00, 就认为是null
                    if (valueS.startsWith("0000") || valueS.contains("-00")) {
                        value = null;
                    }
                }
                data.put(columnName, value);
            } catch (Exception e) {
                throw new RuntimeException("Read column value failed, column name: " + columnName + ", data: " + data + "; Error: " + e.getMessage(), e);
            }
        }
        return data;
    }

    private void batchReadV2(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql;
        if (tapTable.getNameFieldMap().size() > 50) {
            sql = "SELECT * FROM `" + mysqlConfig.getDatabase() + "`.`" + tapTable.getId() + "`";
        } else {
            String columns = tapTable.getNameFieldMap().keySet().stream().map(c -> "`" + c + "`").collect(Collectors.joining(","));
            sql = String.format("SELECT %s FROM `" + mysqlConfig.getDatabase() + "`.`" + tapTable.getId() + "`", columns);
        }
        mysqlJdbcContext.query(sql, resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            Set<String> dateTypeSet = dateFields(tapTable);
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (isAlive() && resultSet.next()) {
                tapEvents.add(insertRecordEvent(filterTimeForMysql(resultSet, metaData, dateTypeSet), tapTable.getId()));
                if (tapEvents.size() == eventBatchSize) {
                    eventsOffsetConsumer.accept(tapEvents, new HashMap<>());
                    tapEvents = list();
                }
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                eventsOffsetConsumer.accept(tapEvents, new HashMap<>());
            }
        });

    }

    @Override
    protected void queryByAdvanceFilterWithOffset(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = commonSqlMaker.buildSelectClause(table, filter, false) + getSchemaAndTable(table.getId()) + commonSqlMaker.buildSqlByAdvanceFilter(filter);
        int batchSize = null != filter.getBatchSize() && filter.getBatchSize().compareTo(0) > 0 ? filter.getBatchSize() : BATCH_ADVANCE_READ_LIMIT;
        jdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            //get all column names
            Set<String> dateTypeSet = dateFields(table);
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (isAlive() && resultSet.next()) {
                filterResults.add(filterTimeForMysql(resultSet, metaData, dateTypeSet));
                if (filterResults.getResults().size() == batchSize) {
                    consumer.accept(filterResults);
                    filterResults = new FilterResults();
                }
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
            }
        });
    }

    private Set<String> dateFields(TapTable tapTable) {
        Set<String> dateTypeSet = new HashSet<>();
        tapTable.getNameFieldMap().forEach((n, v) -> {
            switch (v.getTapType().getType()) {
                case TapType.TYPE_DATE:
                case TapType.TYPE_DATETIME:
                    dateTypeSet.add(n);
                    break;
                default:
                    break;
            }
        });
        return dateTypeSet;
    }


    private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) throws Throwable {
        mysqlReader.readBinlog(tapConnectorContext, tables, offset, batchSize, DDLParserType.MYSQL_CCJ_SQL_PARSER, consumer, contextMapForMasterSlave);
    }


    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        mysqlConfig = new MysqlConfig().load(connectionContext.getConnectionConfig());
        contextMapForMasterSlave = MysqlUtil.buildContextMapForMasterSlave(mysqlConfig);
        MysqlUtil.buildMasterNode(mysqlConfig, contextMapForMasterSlave);
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(mysqlConfig.getConnectionString());
        try (
                MysqlConnectionTest mysqlConnectionTest = new MysqlConnectionTest(mysqlConfig, consumer)
        ) {
            mysqlConnectionTest.testOneByOne();
        }
        return connectionOptions;
    }

    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long startTime) throws Throwable {
        if (null == startTime) {
            return this.mysqlJdbcContext.readBinlogPosition();
        }
        return startTime;
    }


    private TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        DataMap dataMap = mysqlJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("TABLE_ROWS")));
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("DATA_LENGTH")));
        return tableInfo;
    }

}
