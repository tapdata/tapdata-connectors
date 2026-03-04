package io.tapdata.connector.klustron;

import com.mysql.cj.exceptions.StatementIsClosedException;
import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.klustron.config.KunLunCdcConfig;
import io.tapdata.connector.klustron.config.KunLunMysqlConfig;
import io.tapdata.connector.klustron.config.KunLunPgConfig;
import io.tapdata.connector.klustron.context.KunLunMysqlContext;
import io.tapdata.connector.klustron.context.KunLunPgContext;
import io.tapdata.connector.mysql.MysqlExceptionCollector;
import io.tapdata.connector.mysql.MysqlReaderV2;
import io.tapdata.connector.mysql.constant.DeployModeEnum;
import io.tapdata.connector.mysql.ddl.sqlmaker.MysqlDDLSqlGenerator;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.mysql.util.MysqlBinlogPositionUtil;
import io.tapdata.connector.postgres.PostgresSqlMaker;
import io.tapdata.connector.postgres.dml.PostgresRecordWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.exception.TapPdkRetryableEx;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGlseg;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.jdbc.PgArray;
import org.postgresql.jdbc.PgSQLXML;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * PDK for Klustron
 *
 * @author Gavin'Xiao
 * @date 2025/03/03 13:14.520
 */
@TapConnectorClass("spec_klustron.json")
public class KlustronConnector extends CommonDbConnector {
    protected KunLunPgContext pgJdbcContext;
    protected KunLunMysqlContext mysqlJdbcContext;
    protected KunLunMysqlContext cdcJdbcContext;

//    protected MysqlReader mysqlReader;
//    protected MysqlWriter mysqlWriter;

    private KunLunCdcConfig ofCdcConfig;
    private KunLunMysqlConfig ofMysqlConfig;
    private KunLunPgConfig ofPgConfig;

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
    public java.util.HashMap<String, KunLunMysqlContext> contextMapForMasterSlave;

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        ofCdcConfig = (KunLunCdcConfig) new KunLunCdcConfig().load(tapConnectionContext.getConnectionConfig());
        ofCdcConfig.load(tapConnectionContext.getNodeConfig());
        cdcJdbcContext = new KunLunMysqlContext(ofCdcConfig);

        ofMysqlConfig = (KunLunMysqlConfig) new KunLunMysqlConfig().load(tapConnectionContext.getConnectionConfig());
        ofMysqlConfig.load(tapConnectionContext.getNodeConfig());
        mysqlJdbcContext = new KunLunMysqlContext(ofMysqlConfig);

        ofPgConfig = (KunLunPgConfig) new KunLunPgConfig().load(tapConnectionContext.getConnectionConfig());
        ofPgConfig.load(tapConnectionContext.getNodeConfig());
        ofPgConfig.setUser(ofPgConfig.getUsername());
        pgJdbcContext = new KunLunPgContext(ofPgConfig);

        isConnectorStarted(tapConnectionContext, tapConnectorContext -> {
            firstConnectorId = (String) tapConnectorContext.getStateMap().get("firstConnectorId");
            if (EmptyKit.isNull(firstConnectorId)) {
                firstConnectorId = UUID.randomUUID().toString().replace("-", "");
                tapConnectorContext.getStateMap().put("firstConnectorId", firstConnectorId);
            }
        });
        tapLogger = tapConnectionContext.getLog();
        if (ofMysqlConfig.getFileLog()) {
            tapLogger.info("Starting Jdbc Logging, connectorId: {}", firstConnectorId);
            ofMysqlConfig.startJdbcLog(firstConnectorId);
        }
        contextMapForMasterSlave = KunLunUtil.buildContextMapForMasterSlave(ofMysqlConfig);
        KunLunUtil.buildMasterNode(ofMysqlConfig, contextMapForMasterSlave);
        KunLunMysqlContext contextV2 = contextMapForMasterSlave.get(ofMysqlConfig.getHost() + ofMysqlConfig.getPort());
        if (null != contextV2) {
            mysqlJdbcContext = contextV2;
        } else {
            mysqlJdbcContext = new KunLunMysqlContext(ofMysqlConfig);
        }
        commonDbConfig = ofMysqlConfig;
        jdbcContext = pgJdbcContext;
        commonSqlMaker = new PostgresSqlMaker();
        if (Boolean.TRUE.equals(ofPgConfig.getCreateAutoInc())) {
            commonSqlMaker.createAutoInc(true);
        }
        if (Boolean.TRUE.equals(ofPgConfig.getApplyDefault())) {
            commonSqlMaker.applyDefault(true);
        }
        exceptionCollector = new MysqlExceptionCollector();
        ((MysqlExceptionCollector) exceptionCollector).setMysqlConfig(ofMysqlConfig);
        this.version = pgJdbcContext.queryVersion();
        pgJdbcContext.withPostgresVersion(this.version);
        ArrayList<Map<String, Object>> inconsistentNodes = KunLunUtil.compareMasterSlaveCurrentTime(ofMysqlConfig, contextMapForMasterSlave);
        if (null != inconsistentNodes && inconsistentNodes.size() == 2) {
            Map<String, Object> node1 = inconsistentNodes.get(0);
            Map<String, Object> node2 = inconsistentNodes.get(1);
            tapLogger.warn(String.format("The time of each node is inconsistent, please check nodes: %s and %s", node1.toString(), node2.toString()));
        }
        if (tapConnectionContext instanceof TapConnectorContext) {
            if (DeployModeEnum.fromString(ofMysqlConfig.getDeploymentMode()) == DeployModeEnum.MASTER_SLAVE) {
                KVMap<Object> stateMap = ((TapConnectorContext) tapConnectionContext).getStateMap();
                Object masterNode = stateMap.get(MASTER_NODE_KEY);
                if (null != masterNode && null != ofMysqlConfig.getMasterNode()) {
                    if (!masterNode.toString().contains(ofMysqlConfig.getMasterNode().toString()))
                        tapLogger.warn(String.format("The master node has switched, please pay attention to whether the data is consistent, current master node: %s", ofMysqlConfig.getMasterNode()));
                }
            }
//            this.mysqlWriter = new MysqlSqlBatchWriter(mysqlJdbcContext, this::isAlive);
//            this.mysqlReader = new MysqlReader(mysqlJdbcContext, tapLogger, this::isAlive);
            this.dbTimeZone = pgJdbcContext.queryTimeZone();
            if (ofMysqlConfig.getOldVersionTimezone()) {
                this.timeZone = dbTimeZone;
            } else {
                this.timeZone = TimeZone.getTimeZone("GMT" + ofMysqlConfig.getTimezone());
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
    public void onStop(TapConnectionContext connectionContext) {
        started.set(false);
        if (connectionContext instanceof TapConnectorContext
                && null != ofMysqlConfig
                && DeployModeEnum.fromString(ofMysqlConfig.getDeploymentMode()) == DeployModeEnum.MASTER_SLAVE) {
            KVMap<Object> stateMap = ((TapConnectorContext) connectionContext).getStateMap();
            if (null != stateMap) {
                stateMap.put(MASTER_NODE_KEY, ofMysqlConfig.getMasterNode());
                ((TapConnectorContext) connectionContext).setStateMap(stateMap);
            }
        }
//        try {
//            Optional.ofNullable(this.mysqlReader).ifPresent(MysqlReader::close);
//        } catch (Exception ignored) {
//        }
        if (null != mysqlJdbcContext) {
            try {
                this.mysqlJdbcContext.close();
                this.mysqlJdbcContext = null;
            } catch (Exception e) {
                tapLogger.error("Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
            }
        }
        if (null != cdcJdbcContext) {
            try {
                this.cdcJdbcContext.close();
                this.cdcJdbcContext = null;
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

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws SQLException {
        jdbcContext = pgJdbcContext;
        pgJdbcContext.query("SHOW VARIABLES LIKE 'auto_inc%'", rs -> {
            while (rs.next()) {
                String variableName = rs.getString("Variable_name");
                if ("auto_increment_increment".equals(variableName)) {
                    autoIncrementValue = rs.getLong("Value");
                } else if ("auto_increment_offset".equals(variableName)) {
                    autoStartValue = rs.getLong("Value");
                }
            }
        });
        pgJdbcContext.normalQuery("SHOW VARIABLES LIKE 'innodb_autoinc_lock_mode'", rs -> {
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

    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                if (EmptyKit.isNotNull(tapValue.getOriginType())) {
                    if (tapValue.getOriginType().endsWith(" array")) {
                        if (tapValue.getOriginValue() instanceof PgArray) {
                            return tapValue.getOriginValue();
                        } else {
                            return tapValue.getValue();
                        }
                    }
                }
                return toJson(tapValue.getValue());
            }
            return "null";
        });

        codecRegistry.registerToTapValue(PgArray.class, (value, tapType) -> {
            PgArray pgArray = (PgArray) value;
            try (
                    ResultSet resultSet = pgArray.getResultSet()
            ) {
                return new TapArrayValue(DbKit.getDataArrayByColumnName(resultSet, "VALUE"));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        codecRegistry.registerToTapValue(PgSQLXML.class, (value, tapType) -> {
            try {
                return new TapStringValue(((PgSQLXML) value).getString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        codecRegistry.registerToTapValue(PGbox.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGcircle.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGline.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGlseg.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpath.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGobject.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpoint.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGpolygon.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(UUID.class, (value, tapType) -> new TapStringValue(value.toString()));
        codecRegistry.registerToTapValue(PGInterval.class, (value, tapType) -> {
            //P1Y1M1DT12H12M12.312312S
            PGInterval pgInterval = (PGInterval) value;
            String interval = "P" + pgInterval.getYears() + "Y" +
                    pgInterval.getMonths() + "M" +
                    pgInterval.getDays() + "DT" +
                    pgInterval.getHours() + "H" +
                    pgInterval.getMinutes() + "M" +
                    pgInterval.getSeconds() + "S";
            return new TapStringValue(interval);
        });
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> {
            if (ofPgConfig.getOldVersionTimezone()) {
                return tapTimeValue.getValue().toTime();
            } else if (EmptyKit.isNotNull(tapTimeValue.getValue().getTimeZone())) {
                return tapTimeValue.getValue().toInstant().atZone(ZoneId.systemDefault()).toLocalTime();
            } else {
                return tapTimeValue.getValue().toInstant().atZone(ofPgConfig.getZoneId()).toLocalTime();
            }
        });
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            if (ofPgConfig.getOldVersionTimezone() || EmptyKit.isNotNull(tapDateTimeValue.getValue().getTimeZone())) {
                return tapDateTimeValue.getValue().toTimestamp();
            } else {
                return tapDateTimeValue.getValue().toInstant().atZone(ofPgConfig.getZoneId()).toLocalDateTime();
            }
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
        codecRegistry.registerFromTapValue(TapYearValue.class, "character(4)", TapValue::getOriginValue);
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
        //connectorFunctions.supportAfterInitialSync(this::afterInitialSync);
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
        connectorFunctions.supportExecuteCommandV2Function(this::executeCommandV2);
        connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);
        //connectorFunctions.supportQueryFieldMinMaxValueFunction(this::minMaxValue);
        //connectorFunctions.supportGetReadPartitionsFunction(this::getReadPartitions);
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);
        connectorFunctions.supportTransactionBeginFunction(this::beginTransaction);
        connectorFunctions.supportTransactionCommitFunction(this::commitTransaction);
        connectorFunctions.supportTransactionRollbackFunction(this::rollbackTransaction);
        //connectorFunctions.supportQueryHashByAdvanceFilterFunction(this::queryTableHash);
        //connectorFunctions.supportExportEventSqlFunction(this::exportEventSql);

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


    private void streamRead(TapConnectorContext tapConnectorContext, List<String> tables, Object offset, int batchSize, StreamReadConsumer consumer) throws Throwable {
        throwNonSupportWhenLightInit();
        MysqlReaderV2 mysqlReaderV2 = new MysqlReaderV2(cdcJdbcContext, tapLogger, dbTimeZone);
        mysqlReaderV2.init(tables, tapConnectorContext.getTableMap(), offset, batchSize, consumer);
        mysqlReaderV2.startMiner(this::isAlive);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ofMysqlConfig = (KunLunMysqlConfig) new KunLunMysqlConfig().load(connectionContext.getConnectionConfig());
        contextMapForMasterSlave = KunLunUtil.buildContextMapForMasterSlave(ofMysqlConfig);
        KunLunUtil.buildMasterNode(ofMysqlConfig, contextMapForMasterSlave);
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(ofMysqlConfig.getConnectionString());
        try (
                KunLunConnectionTest mysqlConnectionTest = new KunLunConnectionTest(ofMysqlConfig, consumer, connectionOptions)
        ) {
            mysqlConnectionTest.testOneByOne();
        }
        return connectionOptions;
    }

    private Object timestampToStreamOffset(TapConnectorContext tapConnectorContext, Long startTime) throws Throwable {
        if (null == startTime) {
            MysqlBinlogPosition mysqlBinlogPosition = this.cdcJdbcContext.readBinlogPosition();
            if (mysqlBinlogPosition == null) {
                String solutionSuggestions = "please open mysql binlog config";
                Throwable cause = new Exception(" Binlog config is close");
                ((MysqlExceptionCollector) exceptionCollector).collectCdcConfigInvalid(solutionSuggestions, cause);
            }
            return this.cdcJdbcContext.readBinlogPosition();
        }
        try (MysqlBinlogPositionUtil ins = new MysqlBinlogPositionUtil(
                ((KunLunMysqlConfig) ofMysqlConfig).getDataHost(),
                ((KunLunMysqlConfig) ofMysqlConfig).getDataPort(),
                ((KunLunMysqlConfig) ofMysqlConfig).getDataUsername(),
                ((KunLunMysqlConfig) ofMysqlConfig).getDataPassword())) {
            MysqlBinlogPosition mysqlBinlogPosition = ins.findByLessTimestamp(startTime, true);
            if (null == mysqlBinlogPosition) {
                throw new RuntimeException("Not found binlog of sync time: " + startTime);
            }
            return mysqlBinlogPosition;
        }
    }

    private TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        DataMap dataMap = pgJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("TABLE_ROWS")));
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("DATA_LENGTH")));
        return tableInfo;
    }

    protected CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws SQLException {
        if (Boolean.TRUE.equals(ofPgConfig.getCreateAutoInc()) && Integer.parseInt(version) > 100000) {
            createTableEvent.getTable().getNameFieldMap().entrySet().stream().filter(entry -> EmptyKit.isNotBlank(entry.getValue().getSequenceName())).forEach(entry -> {
                StringBuilder sequenceSql = new StringBuilder("CREATE SEQUENCE IF NOT EXISTS " + getSchemaAndTable(entry.getValue().getSequenceName()));
                if (EmptyKit.isNotNull(entry.getValue().getAutoIncStartValue())) {
                    sequenceSql.append(" START ").append(entry.getValue().getAutoIncStartValue());
                }
                if (EmptyKit.isNotNull(entry.getValue().getAutoIncrementValue())) {
                    sequenceSql.append(" INCREMENT ").append(entry.getValue().getAutoIncrementValue());
                }
                try {
                    tapLogger.info("Create sequence sql: {}", sequenceSql.toString());
                    pgJdbcContext.execute(sequenceSql.toString());
                } catch (SQLException e) {
                    tapLogger.warn("Failed to create sequence for table {} field {}", createTableEvent.getTable().getId(), entry.getKey(), e);
                }
            });
        }
        CreateTableOptions options = createTable(connectorContext, createTableEvent, false, "");
        if (EmptyKit.isNotBlank(ofPgConfig.getTableOwner())) {
            tapLogger.info("Change table {} owner to {}", createTableEvent.getTableId(), ofPgConfig.getTableOwner());
            jdbcContext.execute(String.format("alter table %s owner to %s", getSchemaAndTable(createTableEvent.getTableId()), ofPgConfig.getTableOwner()));
        }
        return options;
    }

    @Override
    protected CreateTableOptions createTable(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent, Boolean commentInField, String append) throws SQLException {
        if (Boolean.TRUE.equals(ofPgConfig.getDoubleActive())) {
            createDoubleActiveTempTable();
        }
        TapTable tapTable = createTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (pgJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }

        Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
        for (String field : fieldMap.keySet()) {
            Object defaultValue = fieldMap.get(field).getDefaultValue();
            if (defaultValue instanceof String) {
                String fieldDefault = (String) fieldMap.get(field).getDefaultValue();
                if (EmptyKit.isNotEmpty(fieldDefault) && !Boolean.TRUE.equals(fieldMap.get(field).getAutoInc()) && EmptyKit.isNull(fieldMap.get(field).getDefaultFunction())) {
                    if (fieldDefault.contains("'")) {
                        fieldDefault = fieldDefault.replaceAll("'", "''");
                        fieldMap.get(field).setDefaultValue(fieldDefault);
                    }
                }
            }
        }
        List<String> sqlList = TapSimplify.list();
        sqlList.add(getCreateTableSql(tapTable, commentInField) + " " + append);
        if (!commentInField) {
            //comment on table and column
            if (EmptyKit.isNotNull(tapTable.getComment())) {
                sqlList.add(getTableCommentSql(tapTable));
            }
            for (String fieldName : fieldMap.keySet()) {
                TapField field = fieldMap.get(fieldName);
                String fieldComment = field.getComment();
                if (EmptyKit.isNotNull(fieldComment)) {
                    sqlList.add(getColumnCommentSql(tapTable, field));
                }
            }
        }
        try {
            tapLogger.info("Create table sqls: {}", sqlList);
            pgJdbcContext.batchExecute(sqlList);
        } catch (SQLException e) {
            exceptionCollector.collectWritePrivileges("createTable", Collections.emptyList(), e);
            throw e;
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private String getTableCommentSql(TapTable tapTable) {
        return "comment on table " + getSchemaAndTable(tapTable.getId()) +
                " is '" + tapTable.getComment().replace("'", "''") + '\'';
    }

    private String getColumnCommentSql(TapTable tapTable, TapField tapField) {
        char escapeChar = ofPgConfig.getEscapeChar();
        return "comment on column " + getSchemaAndTable(tapTable.getId()) + '.' +
                escapeChar + tapField.getName() + escapeChar +
                " is '" + tapField.getComment().replace("'", "''") + '\'';
    }

    private String getCreateTableSql(TapTable tapTable, Boolean commentInField) {
        char escapeChar = ofPgConfig.getEscapeChar();
        StringBuilder sb = new StringBuilder("create table ");
        sb.append(getSchemaAndTable(tapTable.getId())).append('(').append(commonSqlMaker.buildColumnDefinition(tapTable, commentInField));
        Collection<String> primaryKeys = tapTable.primaryKeys();
        if (EmptyKit.isNotEmpty(primaryKeys)) {
            sb.append(", primary key (").append(escapeChar)
                    .append(primaryKeys.stream().map(pk -> StringKit.escape(pk, escapeChar)).collect(Collectors.joining(escapeChar + "," + escapeChar)))
                    .append(escapeChar).append(')');
        }
        sb.append(')');
        if (commentInField && EmptyKit.isNotBlank(tapTable.getComment())) {
            commentOnTable(sb, tapTable);
        }
        return sb.toString();
    }


    protected void openIdentity(TapTable tapTable) {
        if (EmptyKit.isEmpty(tapTable.primaryKeys())) {
            try {
                pgJdbcContext.execute("ALTER TABLE \"" + pgJdbcContext.getConfig().getSchema() + "\".\"" + tapTable.getId() + "\" REPLICA IDENTITY FULL");
            } catch (Exception e) {
                tapLogger.warn("Failed to open identity for table " + tapTable, e);
            }
        }
    }

    protected List<DataMap> findIndexes(TapTable tapTable) throws SQLException {
        return pgJdbcContext.queryAllIndexes(Collections.singletonList(tapTable.getId()));
    }

    protected void beforeWriteRecord(TapTable tapTable) throws SQLException {
        if (EmptyKit.isNull(writtenTableMap.get(tapTable.getId()))) {
            openIdentity(tapTable);
            List<DataMap> indexes = findIndexes(tapTable);
            boolean hasUniqueIndex = indexes.stream().anyMatch(v -> "1".equals(v.getString("isUnique")));
            boolean hasMultiUniqueIndex = indexes.stream().filter(v -> "1".equals(v.getString("isUnique"))).count() > 1;
            writtenTableMap.put(tapTable.getId(), DataMap.create().kv(HAS_UNIQUE_INDEX, hasUniqueIndex));
            writtenTableMap.get(tapTable.getId()).put(HAS_MULTI_UNIQUE_INDEX, hasMultiUniqueIndex);
        }
        if (ofPgConfig.getCreateAutoInc() && Integer.parseInt(version) > 100000) {
            if (!writtenTableMap.get(tapTable.getId()).containsKey(HAS_AUTO_INCR)) {
                List<String> autoIncFields = tapTable.getNameFieldMap().values().stream().filter(TapField::getAutoInc).map(TapField::getName).collect(Collectors.toList());
                writtenTableMap.get(tapTable.getId()).put(HAS_AUTO_INCR, autoIncFields);
            }
        }
    }


    protected void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        jdbcContext = pgJdbcContext;
        beforeWriteRecord(tapTable);
        boolean hasUniqueIndex = writtenTableMap.get(tapTable.getId()).getValue(HAS_UNIQUE_INDEX, false);
        boolean hasMultiUniqueIndex = writtenTableMap.get(tapTable.getId()).getValue(HAS_MULTI_UNIQUE_INDEX, false);
        List<String> autoIncFields = writtenTableMap.get(tapTable.getId()).getValue(HAS_AUTO_INCR, new ArrayList<>());
        String insertDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        String deleteDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_DELETE_POLICY);
        if (deleteDmlPolicy == null) {
            deleteDmlPolicy = ConnectionOptions.DML_DELETE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        NormalRecordWriter postgresRecordWriter;
        if (isTransaction) {
            String threadName = Thread.currentThread().getName();
            Connection connection;
            if (transactionConnectionMap.containsKey(threadName)) {
                connection = transactionConnectionMap.get(threadName);
            } else {
                connection = pgJdbcContext.getConnection();
                transactionConnectionMap.put(threadName, connection);
            }
            postgresRecordWriter = new PostgresRecordWriter(pgJdbcContext, connection, tapTable, (hasUniqueIndex && !hasMultiUniqueIndex) ? version : "90500")
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setDeletePolicy(deleteDmlPolicy)
                    .setTapLogger(tapLogger);
        } else {
            postgresRecordWriter = new PostgresRecordWriter(pgJdbcContext, tapTable, (hasUniqueIndex && !hasMultiUniqueIndex) ? version : "90500")
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setDeletePolicy(deleteDmlPolicy)
                    .setTapLogger(tapLogger);
        }
        if (Boolean.TRUE.equals(ofPgConfig.getAllowReplication())) {
            if (EmptyKit.isNull(writtenTableMap.get(tapTable.getId()).get(CANNOT_CLOSE_CONSTRAINT))) {
                boolean canClose = postgresRecordWriter.closeConstraintCheck();
                writtenTableMap.get(tapTable.getId()).put(CANNOT_CLOSE_CONSTRAINT, !canClose);
            } else if (Boolean.FALSE.equals(writtenTableMap.get(tapTable.getId()).get(CANNOT_CLOSE_CONSTRAINT))) {
                postgresRecordWriter.closeConstraintCheck();
            }
        }
        if (ofPgConfig.getCreateAutoInc() && Integer.parseInt(version) > 100000 && EmptyKit.isNotEmpty(autoIncFields)
                && "CDC".equals(tapRecordEvents.get(0).getInfo().get(TapRecordEvent.INFO_KEY_SYNC_STAGE))) {
            postgresRecordWriter.setAutoIncFields(autoIncFields);
            postgresRecordWriter.write(tapRecordEvents, writeListResultConsumer, this::isAlive);
            if (EmptyKit.isNotEmpty(postgresRecordWriter.getAutoIncMap())) {
                List<String> alterSqls = new ArrayList<>();
                postgresRecordWriter.getAutoIncMap().forEach((k, v) -> {
                    String sequenceName = tapTable.getNameFieldMap().get(k).getSequenceName();
                    AtomicLong actual = new AtomicLong(0);
                    if (EmptyKit.isNotBlank(sequenceName)) {
                        try {
                            pgJdbcContext.queryWithNext("select last_value from " + getSchemaAndTable(sequenceName), resultSet -> actual.set(resultSet.getLong(1)));
                        } catch (SQLException ignore) {
                        }
                        if (actual.get() >= (Long.parseLong(String.valueOf(v)) + ofPgConfig.getAutoIncJumpValue())) {
                            return;
                        }
                        alterSqls.add("select setval('" + getSchemaAndTable(sequenceName) + "'," + (Long.parseLong(String.valueOf(v)) + ofPgConfig.getAutoIncJumpValue()) + ", false) ");
                    } else {
                        AtomicReference<String> actualSequenceName = new AtomicReference<>();
                        try {
                            pgJdbcContext.queryWithNext("select pg_get_serial_sequence('" + getSchemaAndTable(tapTable.getId()) + "', '\"" + k + "\"')", resultSet -> actualSequenceName.set(resultSet.getString(1)));
                            pgJdbcContext.queryWithNext("select last_value from " + actualSequenceName.get(), resultSet -> actual.set(resultSet.getLong(1)));
                        } catch (SQLException ignore) {
                        }
                        if (actual.get() >= (Long.parseLong(String.valueOf(v)) + ofPgConfig.getAutoIncJumpValue())) {
                            return;
                        }
                        alterSqls.add("ALTER TABLE " + getSchemaAndTable(tapTable.getId()) + " ALTER COLUMN \"" + k + "\" SET GENERATED BY DEFAULT RESTART WITH " + (Long.parseLong(String.valueOf(v)) + ofPgConfig.getAutoIncJumpValue()));
                    }
                });
                pgJdbcContext.batchExecute(alterSqls);
            }
        } else {
            postgresRecordWriter.write(tapRecordEvents, writeListResultConsumer, this::isAlive);
        }
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws SQLException {
        return pgJdbcContext.queryAllTables(null).size();
    }

    @Override
    protected long batchCount(TapConnectorContext tapConnectorContext, TapTable tapTable) throws Throwable {
        try {
            AtomicLong count = new AtomicLong(0);
            String sql = "select count(1) from " + getSchemaAndTable(tapTable.getId());
            pgJdbcContext.queryWithNext(sql, resultSet -> count.set(resultSet.getLong(1)));
            return count.get();
        } catch (SQLException e) {
            exceptionCollector.collectReadPrivileges("batchCount", Collections.emptyList(), e);
            throw e;
        }
    }

    @Override
    protected void batchReadWithHashSplit(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = getBatchReadSelectSql(tapTable);
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(ofPgConfig.getBatchReadThreadSize());
        ExecutorService executorService = Executors.newFixedThreadPool(ofPgConfig.getBatchReadThreadSize());
        try {
            for (int i = 0; i < ofPgConfig.getBatchReadThreadSize(); i++) {
                final int threadIndex = i;
                executorService.submit(() -> {
                    try {
                        for (int ii = threadIndex; ii < ofPgConfig.getMaxSplit(); ii += ofPgConfig.getBatchReadThreadSize()) {
                            String splitSql = sql + " WHERE " + getHashSplitModConditions(tapTable, ofPgConfig.getMaxSplit(), ii);
                            tapLogger.info("batchRead, splitSql[{}]: {}", ii + 1, splitSql);
                            int retry = 20;
                            while (retry-- > 0 && isAlive()) {
                                try {
                                    pgJdbcContext.query(splitSql, resultSet -> {
                                        List<TapEvent> tapEvents = list();
                                        //get all column names
                                        List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
                                        Map<String, String> typeAndName = new HashMap<>();
                                        tapTable.getNameFieldMap().forEach((key, value) -> {
                                            typeAndName.put(key, value.getDataType());
                                        });
                                        while (isAlive() && resultSet.next()) {
                                            tapEvents.add(insertRecordEvent(filterTimeForPG(resultSet, typeAndName, columnNames), tapTable.getId()));
                                            if (tapEvents.size() == eventBatchSize) {
                                                syncEventSubmit(tapEvents, eventsOffsetConsumer);
                                                tapEvents = list();
                                            }
                                        }
                                        //last events those less than eventBatchSize
                                        if (EmptyKit.isNotEmpty(tapEvents)) {
                                            syncEventSubmit(tapEvents, eventsOffsetConsumer);
                                        }
                                    });
                                    break;
                                } catch (Exception e) {
                                    if (retry == 0 || !(e instanceof SQLRecoverableException || e instanceof IOException)) {
                                        throw e;
                                    }
                                    tapLogger.warn("batchRead, splitSql[{}]: {} failed, retrying...", ii + 1, splitSql);
                                }
                            }
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

    @Override
    protected String getBatchReadSelectSql(TapTable tapTable) {
        String columns = tapTable.getNameFieldMap().keySet()
                .stream()
                .map(c -> commonDbConfig.getEscapeChar() + StringKit.escape(c, commonDbConfig.getEscapeChar()) + commonDbConfig.getEscapeChar())
                .collect(Collectors.joining(","));
        return "SELECT " + columns + " FROM " + getSchemaAndTable(tapTable.getId());
    }

    @Override
    protected String getSchemaAndTable(String tableId) {
        StringBuilder sb = new StringBuilder();
        char escapeChar = ofPgConfig.getEscapeChar();
        if (EmptyKit.isNotBlank(ofPgConfig.getSchema())) {
            sb.append(escapeChar).append(StringKit.escape(ofPgConfig.getSchema(), escapeChar)).append(escapeChar).append('.');
        }
        sb.append(escapeChar).append(StringKit.escape(tableId, escapeChar)).append(escapeChar);
        return sb.toString();
    }

    @Override
    protected void batchReadWithoutHashSplit(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = getBatchReadSelectSql(tapTable);
        pgJdbcContext.query(sql, resultSet -> {
            List<TapEvent> tapEvents = list();
            //get all column names
            List<String> columnNames = DbKit.getColumnsFromResultSet(resultSet);
            Map<String, String> typeAndName = new HashMap<>();
            tapTable.getNameFieldMap().forEach((key, value) -> {
                typeAndName.put(key, value.getDataType());
            });
            try {
                while (isAlive() && resultSet.next()) {
                    tapEvents.add(insertRecordEvent(filterTimeForPG(resultSet, typeAndName, columnNames), tapTable.getId()));
                    if (tapEvents.size() == eventBatchSize) {
                        eventsOffsetConsumer.accept(tapEvents, new HashMap<>());
                        tapEvents = list();
                    }
                }
            } catch (SQLException e) {
                exceptionCollector.collectTerminateByServer(e);
                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), e);
                exceptionCollector.revealException(e);
                throw e;
            }
            //last events those less than eventBatchSize
            if (EmptyKit.isNotEmpty(tapEvents)) {
                eventsOffsetConsumer.accept(tapEvents, new HashMap<>());
            }
        });
    }

    private Map<String, Object> filterTimeForPG(ResultSet resultSet, Map<String, String> typeAndName, List<String> allColumn) {
        DataMap dataMap = DataMap.create();
        int columnIndex = 1;
        for (String colName : allColumn) {
            String dataType = typeAndName.get(colName);
            try {
                if (null == dataType) {
                    dataMap.put(colName, resultSet.getObject(colName));
                } else if (dataType.endsWith("without time zone")) {
                    switch (resultSet.getMetaData().getColumnTypeName(columnIndex)) {
                        case "timestamp": {
                            String timestampString = resultSet.getString(colName);
                            if (StringUtils.isNotEmpty(timestampString)) {
                                LocalDateTime localDateTime = LocalDateTime.parse(timestampString.replace(" ", "T"));
                                dataMap.put(colName, localDateTime.minusHours(ofPgConfig.getZoneOffsetHour()));
                            } else {
                                dataMap.put(colName, null);
                            }
                            break;
                        }
                        case "time": {
                            LocalTime localTime = resultSet.getObject(colName, LocalTime.class);
                            if (localTime != null) {
                                dataMap.put(colName, localTime.atDate(LocalDate.ofYearDay(1970, 1)).minusHours(ofPgConfig.getZoneOffsetHour()));
                            } else {
                                dataMap.put(colName, null);
                            }
                            break;
                        }
                    }

                } else if (dataType.equals("money")) {
                    String money = resultSet.getString(colName);
                    if ("null".equals(money)) {
                        dataMap.put(colName, null);
                    } else {
                        dataMap.put(colName, new BigDecimal(money.replaceAll("[^\\d.-]", "")));
                    }
                } else {
                    dataMap.put(colName, processData(resultSet.getObject(colName), dataType));
                }
                columnIndex++;
            } catch (Exception e) {
                throw new CoreException("Read column value failed, column name: {}, type: {}, data: {}, error: {}", colName, dataMap, dataMap, e.getMessage());
            }
        }
        return dataMap;
    }

    protected Object processData(Object value, String dataType) {
        if (!ofPgConfig.getOldVersionTimezone()) {
            if (value instanceof Timestamp) {
                if (!dataType.endsWith("with time zone")) {
                    value = ((Timestamp) value).toLocalDateTime().minusHours(ofPgConfig.getZoneOffsetHour());
                } else {
                    value = (((Timestamp) value).toLocalDateTime().minusHours(TimeZone.getDefault().getRawOffset() / 3600000).atZone(ZoneOffset.UTC));
                }
            } else if (value instanceof Date) {
                value = (Instant.ofEpochMilli(((Date) value).getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime());
            } else if (value instanceof Time) {
                if (!dataType.endsWith("with time zone")) {
                    value = (Instant.ofEpochMilli(((Time) value).getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime().minusHours(ofPgConfig.getZoneOffsetHour()));
                } else {
                    value = (Instant.ofEpochMilli(((Time) value).getTime()).atZone(ZoneOffset.UTC));
                }
            }
        }
        return value;
    }

    protected void batchReadWithoutOffset(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        if (Boolean.TRUE.equals(ofPgConfig.getHashSplit())) {
            batchReadWithHashSplit(tapConnectorContext, tapTable, offsetState, eventBatchSize, eventsOffsetConsumer);
        } else {
            batchReadWithoutHashSplit(tapConnectorContext, tapTable, offsetState, eventBatchSize, eventsOffsetConsumer);
        }
    }
}
