package io.tapdata.connector.mysql;

import com.mysql.cj.exceptions.StatementIsClosedException;
import io.debezium.type.TapIllegalDate;
import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.mysql.bean.MysqlColumn;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.constant.DeployModeEnum;
import io.tapdata.connector.mysql.ddl.sqlmaker.MysqlDDLSqlGenerator;
import io.tapdata.connector.mysql.dml.MysqlRecordWriter;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.mysql.util.MysqlUtil;
import io.tapdata.connector.mysql.writer.MysqlSqlBatchWriter;
import io.tapdata.connector.mysql.writer.MysqlWriter;
import io.tapdata.entity.TapConstraintException;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.constraint.TapCreateConstraintEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.*;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.type.TapYear;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.exception.TapPdkRetryableEx;
import io.tapdata.kit.DbKit;
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
import io.tapdata.pdk.apis.functions.connector.common.vo.TapHashResult;
import io.tapdata.pdk.apis.functions.connector.source.GetReadPartitionOptions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import io.tapdata.pdk.apis.partition.FieldMinMaxValue;
import io.tapdata.pdk.apis.partition.ReadPartition;
import io.tapdata.pdk.apis.partition.TapPartitionFilter;
import io.tapdata.pdk.apis.partition.splitter.StringCaseInsensitiveSplitter;
import io.tapdata.pdk.apis.partition.splitter.TypeSplitterMap;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    protected TimeZone timeZone;
    protected TimeZone dbTimeZone;
    protected ZoneId zoneId;
    protected ZoneId dbZoneId;
    protected int zoneOffsetHour;
    protected long autoIncrementValue = 1;
    protected long autoIncCacheValue = 1;
    protected long autoStartValue = 1;

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
        if (Boolean.TRUE.equals(mysqlConfig.getCreateAutoInc())) {
            commonSqlMaker.createAutoInc(true);
        }
        if (Boolean.TRUE.equals(mysqlConfig.getApplyDefault())) {
            commonSqlMaker.applyDefault(true);
        }
        tapLogger = tapConnectionContext.getLog();
        exceptionCollector = new MysqlExceptionCollector();
        ((MysqlExceptionCollector) exceptionCollector).setMysqlConfig(mysqlConfig);
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
            this.dbTimeZone = mysqlJdbcContext.queryTimeZone();
            if (mysqlConfig.getOldVersionTimezone()) {
                this.timeZone = dbTimeZone;
            } else {
                this.timeZone = TimeZone.getTimeZone("GMT" + mysqlConfig.getTimezone());
            }
            this.dbZoneId = dbTimeZone.toZoneId();
            this.zoneId = timeZone.toZoneId();
            this.zoneOffsetHour = timeZone.getRawOffset() / 1000 / 60 / 60;
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
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws SQLException {
        mysqlJdbcContext.normalQuery("SHOW VARIABLES LIKE 'auto_inc%'", rs -> {
            while (rs.next()) {
                String variableName = rs.getString("Variable_name");
                if ("auto_increment_increment".equals(variableName)) {
                    autoIncrementValue = rs.getLong("Value");
                } else if ("auto_increment_offset".equals(variableName)) {
                    autoStartValue = rs.getLong("Value");
                }
            }
        });
        mysqlJdbcContext.normalQuery("SHOW VARIABLES LIKE 'innodb_autoinc_lock_mode'", rs -> {
            if (rs.next()) {
                String value = rs.getString("Value");
                if ("0".equals(value)) {
                    autoIncCacheValue = 1;
                } else if ("1".equals(value)) {
                    autoIncCacheValue = 100;
                } else {
                    autoIncCacheValue = 1000;
                }
            }
        });
        super.discoverSchema(connectionContext, tables, tableSize, consumer);
    }

    @Override
    public void onLightStart(TapConnectionContext tapConnectionContext) throws Throwable {
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
        ((MysqlExceptionCollector) exceptionCollector).setMysqlConfig(mysqlConfig);
        this.version = "";
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.mysqlWriter = new MysqlSqlBatchWriter(mysqlJdbcContext, this::isAlive);
            this.dbZoneId = ZoneId.of("GMT");
            this.zoneId = dbZoneId;
            this.dbTimeZone = TimeZone.getTimeZone(dbZoneId);
            this.timeZone = dbTimeZone;
            this.zoneOffsetHour = timeZone.getRawOffset() / 1000 / 60 / 60;
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
            if (!mysqlConfig.getOldVersionTimezone()) {
                if (EmptyKit.isNotNull(tapDateTimeValue.getValue().getTimeZone())) {
                    tapDateTimeValue.getValue().setTimeZone(TimeZone.getTimeZone("UTC"));
                } else {
                    tapDateTimeValue.getValue().setTimeZone(timeZone);
                }
            } else if (tapDateTimeValue.getValue() != null && tapDateTimeValue.getValue().getTimeZone() == null) {
                tapDateTimeValue.getValue().setTimeZone(TimeZone.getDefault());
            }
            return tapDateTimeValue.getValue().isContainsIllegal() ? tapDateTimeValue.getValue().getIllegalDate() : formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS");
        });
        //date类型通过jdbc读取时，会自动转换为当前时区的时间，所以设置为当前时区
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().isContainsIllegal() ? tapDateValue.getValue().getIllegalDate() : tapDateValue.getValue().toInstant());
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTimeStr());
        codecRegistry.registerFromTapValue(TapYearValue.class, TapValue::getOriginValue);

        codecRegistry.registerFromTapValue(TapBooleanValue.class, "tinyint(1)", TapValue::getValue);

        codecRegistry.registerToTapValue(TapIllegalDate.class, new ToTapValueCodec<TapValue<?, ?>>() {
            @Override
            public TapValue<?, ?> toTapValue(Object value, TapType tapType) {
                String originDate = null;
                if (value instanceof TapIllegalDate) {
                    originDate = ((TapIllegalDate) value).getOriginDate();
                }
                if (tapType instanceof TapDate) {
                    return new TapDateValue(new DateTime(originDate, DateTime.DATE_TYPE));
                } else if (tapType instanceof TapTime) {
                    return new TapTimeValue(new DateTime(originDate, DateTime.TIME_TYPE));
                } else if (tapType instanceof TapYear) {
                    return new TapYearValue(new DateTime(originDate, DateTime.YEAR_TYPE));
                } else {
                    return new TapDateTimeValue(new DateTime(originDate, DateTime.DATETIME_TYPE));
                }
            }
        });
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadWithoutOffset);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);
        connectorFunctions.supportCountByPartitionFilterFunction(this::countByAdvanceFilter);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportAfterInitialSync(this::afterInitialSync);
        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportQueryIndexes(this::queryIndexes);
        connectorFunctions.supportCreateConstraint(this::createConstraint);
        connectorFunctions.supportQueryConstraints(this::queryConstraint);
        connectorFunctions.supportDropConstraint(this::dropConstraint);
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
        connectorFunctions.supportTransactionBeginFunction(this::beginTransaction);
        connectorFunctions.supportTransactionCommitFunction(this::commitTransaction);
        connectorFunctions.supportTransactionRollbackFunction(this::rollbackTransaction);
        connectorFunctions.supportQueryHashByAdvanceFilterFunction(this::queryTableHash);

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
        return new MysqlColumn(dataMap).withVersion(version)
                .withSeedValue(autoStartValue)
                .withIncrementValue(autoIncrementValue)
                .withAutoIncCacheValue(autoIncCacheValue)
                .getTapField();
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
                MysqlMaker sqlMaker = new MysqlMaker();
                sqlMaker.setCreateAutoInc(mysqlConfig.getCreateAutoInc());
                sqlMaker.setApplyDefault(mysqlConfig.getApplyDefault());
                if (null == tapCreateTableEvent.getTable()) {
                    tapLogger.warn("Create table event's tap table is null, will skip it: " + tapCreateTableEvent);
                    return createTableOptions;
                }
                String[] createTableSqls = sqlMaker.createTable(tapConnectorContext, tapCreateTableEvent, mysqlVersion);
                mysqlJdbcContext.batchExecute(Arrays.asList(createTableSqls));
                createTableOptions.setTableExists(false);
            }

        } catch (Throwable t) {
            exceptionCollector.collectWritePrivileges("createTable :" + tapCreateTableEvent.getTableId(), Collections.emptyList(), t);
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

    protected void beforeWriteRecord(TapTable tapTable) throws SQLException {
        super.beforeWriteRecord(tapTable);
        List<String> autoIncFields = new ArrayList<>();
        if (mysqlConfig.getCreateAutoInc()) {
            if (!writtenTableMap.get(tapTable.getId()).containsKey(HAS_AUTO_INCR)) {
                List<TapField> fields = tapTable.getNameFieldMap().values().stream().filter(TapField::getAutoInc).collect(Collectors.toList());
                autoIncFields.addAll(fields.stream().map(TapField::getName).collect(Collectors.toList()));
                writtenTableMap.get(tapTable.getId()).put(HAS_AUTO_INCR, autoIncFields);
            } else {
                autoIncFields.addAll(writtenTableMap.get(tapTable.getId()).getValue(HAS_AUTO_INCR, new ArrayList<>()));
            }
        }
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        beforeWriteRecord(tapTable);
        List<String> autoIncFields = writtenTableMap.get(tapTable.getId()).getValue(HAS_AUTO_INCR, new ArrayList<>());
        String insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        NormalRecordWriter mysqlRecordWriter;
        if (isTransaction) {
            String threadName = Thread.currentThread().getName();
            Connection connection;
            if (transactionConnectionMap.containsKey(threadName)) {
                connection = transactionConnectionMap.get(threadName);
            } else {
                connection = mysqlJdbcContext.getConnection();
                transactionConnectionMap.put(threadName, connection);
            }
            mysqlRecordWriter = new MysqlRecordWriter(mysqlJdbcContext, connection, tapTable)
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setTapLogger(tapLogger);

        } else {
            mysqlRecordWriter = new MysqlRecordWriter(mysqlJdbcContext, tapTable)
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setTapLogger(tapLogger);
        }
        mysqlRecordWriter.closeConstraintCheck();
        if (mysqlConfig.getCreateAutoInc() && EmptyKit.isNotEmpty(autoIncFields)
                && "CDC".equals(tapRecordEvents.get(0).getInfo().get(TapRecordEvent.INFO_KEY_SYNC_STAGE))) {
            mysqlRecordWriter.setAutoIncFields(autoIncFields);
            mysqlRecordWriter.write(tapRecordEvents, consumer, this::isAlive);
            if (EmptyKit.isNotEmpty(mysqlRecordWriter.getAutoIncMap())) {
                List<String> alterSqls = new ArrayList<>();
                mysqlRecordWriter.getAutoIncMap().forEach((k, v) -> {
                    alterSqls.add("alter table " + getSchemaAndTable(tapTable.getId()) + " auto_increment " + (Long.parseLong(String.valueOf(v)) + mysqlConfig.getAutoIncJumpValue()));
                });
                jdbcContext.batchExecute(alterSqls);
            }
        } else {
            mysqlRecordWriter.write(tapRecordEvents, consumer, this::isAlive);
        }
    }

    protected void afterInitialSync(TapConnectorContext connectorContext, TapTable tapTable) throws Throwable {
        beforeWriteRecord(tapTable);
        List<String> autoIncFields = writtenTableMap.get(tapTable.getId()).getValue(HAS_AUTO_INCR, new ArrayList<>());
        autoIncFields.forEach(field -> {
            try (
                    Connection connection = jdbcContext.getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("select max(" + field + ") from " + getSchemaAndTable(tapTable.getId()))
            ) {
                if (resultSet.next()) {
                    statement.execute("alter table " + getSchemaAndTable(tapTable.getId()) + " auto_increment " + (resultSet.getLong(1) + mysqlConfig.getAutoIncJumpValue()));
                    connection.commit();
                }
            } catch (SQLException e) {
                tapLogger.warn("Failed to get auto increment value for table {} field {}", tapTable.getId(), field, e);
            }
        });
    }

    protected Map<String, Object> filterTimeForMysql(
            ResultSet resultSet, ResultSetMetaData metaData, Set<String> dateTypeSet) throws SQLException {
        return filterTimeForMysql(resultSet, metaData, dateTypeSet, null, null);
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
                        if (mysqlConfig.getOldVersionTimezone()) {
                            data.put(columnName, resultSet.getString(i + 1));
                        } else if (value instanceof java.sql.Date) {
                            data.put(columnName, ((java.sql.Date) value).toLocalDate().atStartOfDay());
                        } else {
                            data.put(columnName, value);
                        }
                    } else if ("DATETIME".equalsIgnoreCase(metaData.getColumnTypeName(i + 1)) && value instanceof LocalDateTime) {
                        if (mysqlConfig.getOldVersionTimezone()) {
                            data.put(columnName, ((LocalDateTime) value).toInstant(ZoneOffset.ofTotalSeconds(TimeZone.getDefault().getRawOffset() / 1000)));
                        } else {
                            data.put(columnName, ((LocalDateTime) value).minusHours(zoneOffsetHour));
                        }
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

    protected static Object buildIllegalDate(TapRecordEvent recordEvent, IllegalDateConsumer illegalDateConsumer,
                                             String valueS, List<String> illegalDateFieldName, String columnName) {
        Object value;
        TapIllegalDate date = new TapIllegalDate();
        StringBuilder sb = new StringBuilder();
        String str = valueS.replaceAll("T", "-")
                .replaceAll(" ", "-")
                .replaceAll(":", "-")
                .replaceAll("\\.", "-")
                .replaceAll("Z", "");
        sb.append(str);
        date.setOriginDate(sb.toString());
        date.setOriginDateType(Integer.class);
        value = date;
        illegalDateConsumer.containsIllegalDate(recordEvent, true);
        illegalDateFieldName.add(columnName);
        return value;
    }

    protected interface IllegalDateConsumer {
        void containsIllegalDate(TapRecordEvent event, boolean containsIllegalDate);

        void buildIllegalDateFieldName(TapRecordEvent event, List<String> illegalDateFieldName);
    }

    @Override
    protected String getHashSplitStringSql(TapTable tapTable) {
        Collection<String> pks = tapTable.primaryKeys(true);
        if (pks.isEmpty()) {
            pks = tapTable.getNameFieldMap().keySet();
        }
        if (pks.size() == 1) {
            return "CRC32(" + pks.iterator().next() + ")";
        }
        return "CRC32(CONCAT_WS(',', `" + String.join("`, `", pks) + "`))";
    }

    protected ResultSetConsumer resultSetConsumer(TapTable tapTable, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
        return resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            Set<String> dateTypeSet = dateFields(tapTable);
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (isAlive() && resultSet.next()) {
                TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent().init();
                Map<String, Object> data = filterTimeForMysql(resultSet, metaData, dateTypeSet, tapInsertRecordEvent, new IllegalDateConsumer() {
                    @Override
                    public void containsIllegalDate(TapRecordEvent event, boolean containsIllegalDate) {
                        event.setContainsIllegalDate(containsIllegalDate);
                    }

                    @Override
                    public void buildIllegalDateFieldName(TapRecordEvent event, List<String> illegalDateFieldName) {
                        ((TapInsertRecordEvent) event).setAfterIllegalDateFieldName(illegalDateFieldName);
                    }
                });
                tapInsertRecordEvent.after(data).table(tapTable.getId());
                tapEvents.add(tapInsertRecordEvent);
                if (tapEvents.size() == eventBatchSize) {
                    eventsOffsetConsumer.accept(tapEvents, new HashMap<>());
                    tapEvents = list();
                }
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                eventsOffsetConsumer.accept(tapEvents, new HashMap<>());
            }
        };
    }

    @Override
    protected String getBatchReadSelectSql(TapTable tapTable) {
        if (tapTable.getNameFieldMap().size() > 50) {
            return String.format("SELECT * FROM %s", getSchemaAndTable(tapTable.getId()));
        } else {
            return super.getBatchReadSelectSql(tapTable);
        }
    }

    @Override
    protected void batchReadWithHashSplit(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = getBatchReadSelectSql(tapTable);
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(commonDbConfig.getBatchReadThreadSize());
        ExecutorService executorService = Executors.newFixedThreadPool(commonDbConfig.getBatchReadThreadSize());
        try {
            for (int i = 0; i < commonDbConfig.getBatchReadThreadSize(); i++) {
                final int threadIndex = i;
                executorService.submit(() -> {
                    try {
                        for (int ii = threadIndex; ii < commonDbConfig.getMaxSplit(); ii += commonDbConfig.getBatchReadThreadSize()) {
                            String splitSql = sql + " WHERE " + getHashSplitModConditions(tapTable, commonDbConfig.getMaxSplit(), ii);
                            tapLogger.info("batchRead, splitSql[{}]: {}", ii + 1, splitSql);
                            mysqlJdbcContext.queryWithStream(splitSql, resultSetConsumer(tapTable, eventBatchSize, eventsOffsetConsumer));
                        }
                    } catch (Throwable e) {
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

    protected void batchReadWithoutHashSplit(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = getBatchReadSelectSql(tapTable);
        mysqlJdbcContext.queryWithStream(sql, resultSetConsumer(tapTable, eventBatchSize, eventsOffsetConsumer));
    }

    @Override
    protected void queryByAdvanceFilterWithOffset(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = commonSqlMaker.buildSelectClause(table, filter, false) + getSchemaAndTable(table.getId()) + commonSqlMaker.buildSqlByAdvanceFilter(filter);
        int batchSize = null != filter.getBatchSize() && filter.getBatchSize().compareTo(0) > 0 ? filter.getBatchSize() : BATCH_ADVANCE_READ_LIMIT;
        mysqlJdbcContext.queryWithStream(sql, resultSet -> {
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
                case TapType.TYPE_TIME:
                case TapType.TYPE_YEAR:
                    dateTypeSet.add(n);
                    break;
                default:
                    break;
            }
        });
        return dateTypeSet;
    }


    private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) throws Throwable {
        throwNonSupportWhenLightInit();
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
                MysqlConnectionTest mysqlConnectionTest = new MysqlConnectionTest(mysqlConfig, consumer, connectionOptions)
        ) {
            mysqlConnectionTest.testOneByOne();
        }
        return connectionOptions;
    }

    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long startTime) throws Throwable {
        if (null == startTime) {
            MysqlBinlogPosition mysqlBinlogPosition = this.mysqlJdbcContext.readBinlogPosition();
            if (mysqlBinlogPosition == null) {
                String solutionSuggestions = "please open mysql binlog config";
                Throwable cause = new Exception(" Binlog config is close");
                ((MysqlExceptionCollector) exceptionCollector).collectCdcConfigInvalid(solutionSuggestions, cause);
            }
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

    private String buildHashSql(TapAdvanceFilter filter, TapTable table) {
        StringBuilder sql = new StringBuilder("select sum(mod(cast(conv(substring(md5(CONCAT_WS('',");
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        Iterator<Map.Entry<String, TapField>> entryIterator = nameFieldMap.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, TapField> next = entryIterator.next();
            TapField field = nameFieldMap.get(next.getKey());
            byte type = next.getValue().getTapType().getType();
            String fieldName = next.getKey();
            if (type == TapType.TYPE_NUMBER && (field.getDataType().toLowerCase().contains("double") ||
                    field.getDataType().toLowerCase().contains("decimal") ||
                    field.getDataType().contains("float"))) {
                sql.append("TRUNCATE(" + "`" + fieldName + "`" + ",0)").append(",");
                continue;
            }

            if (type == TapType.TYPE_BOOLEAN && field.getDataType().toLowerCase().contains("bit")) {
                sql.append("CAST(" + "`" + fieldName + "`" + " AS unsigned)").append(",");
                continue;
            }

            switch (type) {
                case TapType.TYPE_DATETIME:
                    sql.append("round(UNIX_TIMESTAMP( CAST(").append("`" + fieldName + "`").append(" as char(19)) )),");
                    break;
                case TapType.TYPE_BINARY:
                    break;
                default:
                    sql.append("`" + fieldName + "`").append(",");
                    break;
            }
        }
        sql = new StringBuilder(sql.substring(0, sql.length() - 1));
        sql.append(")), 1, 16), 16, 10) as unsigned), 64)) as md5 from ").append(table.getName() + " ");
        sql.append(commonSqlMaker.buildCommandWhereSql(filter, ""));
        return sql.toString();
    }

    protected void queryTableHash(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<TapHashResult<String>> consumer) throws SQLException {
        String sql = buildHashSql(filter, table);
        jdbcContext.query(sql, resultSet -> {
            if (isAlive() && resultSet.next()) {
                consumer.accept(TapHashResult.create().withHash(resultSet.getString(1)));
            }
        });
    }

    protected void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws SQLException {
        if (jdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() >= 1) {
            List<String> sqls = new ArrayList<>();
            sqls.add("SET FOREIGN_KEY_CHECKS=0");
            sqls.add("truncate table " + getSchemaAndTable(tapClearTableEvent.getTableId()));
            jdbcContext.batchExecute(sqls);
        } else {
            tapLogger.warn("Table {} not exists, skip truncate", tapClearTableEvent.getTableId());
        }
    }

    protected void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws SQLException {
        if (jdbcContext.queryAllTables(Collections.singletonList(tapDropTableEvent.getTableId())).size() >= 1) {
            List<String> sqls = new ArrayList<>();
            sqls.add("SET FOREIGN_KEY_CHECKS=0");
            sqls.add("drop table " + getSchemaAndTable(tapDropTableEvent.getTableId()));
            jdbcContext.batchExecute(sqls);
        } else {
            tapLogger.warn("Table {} not exists, skip drop", tapDropTableEvent.getTableId());
        }
    }

    protected void createConstraint(TapConnectorContext connectorContext, TapTable tapTable, TapCreateConstraintEvent createConstraintEvent, boolean create) {
        List<TapConstraint> constraintList = createConstraintEvent.getConstraintList();
        if (EmptyKit.isNotEmpty(constraintList)) {
            List<String> constraintSqlList = new ArrayList<>();
            TapConstraintException exception = new TapConstraintException(tapTable.getId());
            constraintList.forEach(c -> {
                String sql = getCreateConstraintSql(tapTable, c);
                if (create) {
                    try {
                        jdbcContext.execute(sql);
                    } catch (Exception e) {
                        if (e instanceof SQLException && ((SQLException) e).getErrorCode() == 3780) {
                            TapTable referenceTable = connectorContext.getTableMap().get(c.getReferencesTableName());
                            c.getMappingFields().stream().filter(m -> Boolean.TRUE.equals(referenceTable.getNameFieldMap().get(m.getReferenceKey()).getAutoInc()) && referenceTable.getNameFieldMap().get(m.getReferenceKey()).getDataType().startsWith("decimal")).forEach(m -> {
                                try {
                                    jdbcContext.execute("alter table " + getSchemaAndTable(tapTable.getId()) + " modify `" + m.getForeignKey() + "` bigint");
                                } catch (SQLException e1) {
                                    exception.addException(c, "alter table modify column failed", e1);
                                }
                            });
                            try {
                                jdbcContext.execute(sql);
                            } catch (Exception e1) {
                                exception.addException(c, sql, e1);
                            }
                        } else if (e instanceof SQLException && (((SQLException) e).getErrorCode() == 1826 || ((SQLException) e).getErrorCode() == 1823)) {
                            String rename = c.getName() + "_" + UUID.randomUUID().toString().replaceAll("-", "").substring(28);
                            c.setName(rename);
                            sql = getCreateConstraintSql(tapTable, c);
                            try {
                                jdbcContext.execute(sql);
                            } catch (Exception e1) {
                                exception.addException(c, sql, e1);
                            }
                        } else {
                            exception.addException(c, sql, e);
                        }
                    }
                } else {
                    constraintSqlList.add(sql);
                }
            });
            if (!create) {
                createConstraintEvent.setConstraintSqlList(constraintSqlList);
            }
            if (EmptyKit.isNotEmpty(exception.getExceptions())) {
                throw exception;
            }
        }
    }

    protected TapIndex makeTapIndex(String key, List<DataMap> value) {
        TapIndex index = new TapIndex();
        index.setName(key);
        List<TapIndexField> fieldList = TapSimplify.list();
        value.forEach(v -> {
            TapIndexField field = new TapIndexField();
            field.setFieldAsc("1".equals(v.getString("isAsc")));
            field.setName(v.getString("columnName"));
            field.setSubPosition(v.getInteger("subPosition"));
            fieldList.add(field);
        });
        index.setUnique(value.stream().anyMatch(v -> ("1".equals(v.getString("isUnique")))));
        index.setPrimary(value.stream().anyMatch(v -> ("1".equals(v.getString("isPk")))));
        index.setIndexFields(fieldList);
        return index;
    }

    protected String getCreateIndexSql(TapTable tapTable, TapIndex tapIndex) {
        StringBuilder sb = new StringBuilder("create ");
        char escapeChar = commonDbConfig.getEscapeChar();
        if (tapIndex.isUnique()) {
            sb.append("unique ");
        }
        sb.append("index ");
        if (EmptyKit.isNotBlank(tapIndex.getName())) {
            sb.append(escapeChar).append(tapIndex.getName()).append(escapeChar);
        } else {
            String indexName = DbKit.buildIndexName(tapTable.getId());
            tapIndex.setName(indexName);
            sb.append(escapeChar).append(indexName).append(escapeChar);
        }
        sb.append(" on ").append(getSchemaAndTable(tapTable.getId())).append('(')
                .append(tapIndex.getIndexFields().stream().map(f -> escapeChar + f.getName() + escapeChar + (EmptyKit.isNotNull(f.getSubPosition()) ? "(" + f.getSubPosition() + ")" : " ") + (f.getFieldAsc() ? "asc" : "desc"))
                        .collect(Collectors.joining(","))).append(')');
        return sb.toString();
    }

    protected List<String> getAfterUniqueAutoIncrementFields(TapTable tapTable, List<TapIndex> indexList) {
        if (!mysqlConfig.getCreateAutoInc()) return new ArrayList<>();
        String sql = "ALTER TABLE `%s` MODIFY COLUMN `%s` %s AUTO_INCREMENT";
        List<String> uniqueFields = new ArrayList<>();
        List<String> uniqueAutoIncrementFields = new ArrayList<>();
        indexList.forEach(index -> {
            if (index.isUnique()) {
                uniqueFields.addAll(index.getIndexFields().stream().map(TapIndexField::getName).collect(Collectors.toList()));
            }
        });
        tapTable.getNameFieldMap().values().forEach(f -> {
            if (f.getAutoInc() && !f.getPrimaryKey() && uniqueFields.contains(f.getName())) {
                uniqueAutoIncrementFields.add(f.getName());
            }
        });

        return uniqueAutoIncrementFields.stream().map(f -> String.format(sql, tapTable.getId(), f, tapTable.getNameFieldMap().get(f).getDataType())).collect(Collectors.toList());
    }

}
