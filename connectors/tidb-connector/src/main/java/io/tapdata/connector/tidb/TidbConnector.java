package io.tapdata.connector.tidb;

import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.common.util.FileUtil;
import io.tapdata.connector.mysql.bean.MysqlColumn;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.tidb.cdc.process.thread.Activity;
import io.tapdata.connector.tidb.cdc.process.thread.ProcessHandler;
import io.tapdata.connector.tidb.cdc.process.thread.TiCDCShellManager;
import io.tapdata.connector.tidb.cdc.util.ProcessLauncher;
import io.tapdata.connector.tidb.cdc.util.ProcessSearch;
import io.tapdata.connector.tidb.cdc.util.ZipUtils;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.connector.tidb.ddl.TidbDDLSqlGenerator;
import io.tapdata.connector.tidb.dml.TidbReader;
import io.tapdata.connector.tidb.dml.TidbRecordWriter;
import io.tapdata.connector.tidb.util.HttpUtil;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapBooleanValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapValue;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.PDKMethod;
import io.tapdata.pdk.apis.functions.connection.RetryOptions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;


@TapConnectorClass("spec_tidb.json")
public class TidbConnector extends CommonDbConnector {
    private TidbConfig tidbConfig;
    private TidbJdbcContext tidbJdbcContext;
    private TimeZone timezone;
    private TidbReader tidbReader;
    AtomicReference<Throwable> throwableCatch = new AtomicReference<>();

    protected final AtomicBoolean started = new AtomicBoolean(false);

    protected void initTimeZone() {
        if (EmptyKit.isBlank(tidbConfig.getTimezone())) {
            tidbConfig.setTimezone("+00:00");
        }
        timezone = TimeZone.getTimeZone(ZoneId.of(tidbConfig.getTimezone()));
    }

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        this.tidbConfig = new TidbConfig().load(tapConnectionContext.getConnectionConfig());
        tidbJdbcContext = new TidbJdbcContext(tidbConfig);
        commonDbConfig = tidbConfig;
        jdbcContext = tidbJdbcContext;
        initTimeZone();
        tapLogger = tapConnectionContext.getLog();
        started.set(true);

        commonSqlMaker = new CommonSqlMaker('`');
        tidbReader = new TidbReader(tidbJdbcContext);
        ddlSqlGenerator = new TidbDDLSqlGenerator();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
    }

    @Override
    protected RetryOptions errorHandle(TapConnectionContext tapConnectionContext, PDKMethod pdkMethod, Throwable throwable) {
        RetryOptions retryOptions = super.errorHandle(tapConnectionContext, pdkMethod, throwable);
        retryOptions.setNeedRetry(
                !(throwable instanceof CoreException && ((CoreException) throwable).getCode() == TiCDCShellManager.CDC_TOOL_NOT_EXISTS)
                && !(throwable instanceof CoreException && ((CoreException) throwable).getCode() == HttpUtil.ERROR_START_TS_BEFORE_GC)
        );
        return retryOptions;
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportReleaseExternalFunction(this::onDestroy);
        // target functions
        connectorFunctions.supportCreateTableV2(this::createTableV3);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);
        connectorFunctions.supportCountByPartitionFilterFunction(this::countByAdvanceFilter);

        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> tidbJdbcContext.getConnection(), c));

        // source functions
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadV3);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);

        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            if (tapDateTimeValue.getValue() != null && tapDateTimeValue.getValue().getTimeZone() == null) {
                tapDateTimeValue.getValue().setTimeZone(timezone);
            }
            return formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS");
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> {
            if (tapDateValue.getValue() != null && tapDateValue.getValue().getTimeZone() == null) {
                tapDateValue.getValue().setTimeZone(timezone);
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

        codecRegistry.registerFromTapValue(TapMapValue.class, "longtext", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "longtext", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });

    }

    protected void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) {
        String feedId = genericFeedId(nodeContext.getStateMap());
        String cdcServer = String.valueOf(Optional.ofNullable(nodeContext.getStateMap().get(ProcessHandler.CDC_SERVER)).orElse("127.0.0.1:8300"));
        nodeContext.getLog().info("Source timezone: {}", timezone.toZoneId().toString());
        ProcessHandler.ProcessInfo info = new ProcessHandler.ProcessInfo()
                .withZone(timezone)
                .withCdcServer(cdcServer)
                .withFeedId(feedId)
                .withAlive(this::isAlive)
                .withTapConnectorContext(nodeContext)
                .withCdcTable(tableList)
                .withThrowableCollector(throwableCatch)
                .withCdcOffset(offsetState)
                .withTiDBConfig(tidbConfig)
                .withGcTtl(86400)
                .withDatabase(tidbConfig.getDatabase());
        try (ProcessHandler handler = ProcessHandler.of(info, consumer)) {
            handler.doActivity();
            doWait(handler);
        } catch (Exception e) {
            throw new CoreException("TiCDC execute failed, message: {}", e.getMessage(), e);
        }
    }

    protected String genericFeedId(KVMap<Object> kvMap) {
        String feedId = (String) kvMap.get(ProcessHandler.FEED_ID);
        if (null == feedId) {
            String id = UUID.randomUUID().toString();
            while (id.contains("-")) {
                id = id.replace("-", "");
            }
            feedId = id;
            kvMap.put(ProcessHandler.FEED_ID, feedId);
        }
        return feedId;
    }

    protected void doWait(ProcessHandler handler) {
        while (isAlive()) {
            if (null != throwableCatch.get()) {
                throw new CoreException(0, throwableCatch.get(), "TiCDC execute with error, message: {}", throwableCatch.get().getMessage());
            }
            handler.aliveCheck();
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    protected Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) {
        try {
            if (null == offsetStartTime) {
                MysqlBinlogPosition mysqlBinlogPosition = tidbJdbcContext.readBinlogPosition();
                return mysqlBinlogPosition.getPosition();
            }
            long safeGcPoint = tidbJdbcContext.querySafeGcPoint();
            if (offsetStartTime.compareTo(safeGcPoint) <= 0) {
                connectorContext.getLog().warn("Fail to create or maintain change feed because start-ts {} is earlier than or equal to GC safe point at {}. " +
                                "Extending the GC lifecycle can prevent future GC from cleaning up data prematurely. You can set the GC lifecycle to a longer time. " +
                                "For example, set to 24 hours: SET GLOBAL tidb_gc_life_time = '24h'; ",
                        offsetStartTime,
                        safeGcPoint);
                offsetStartTime = safeGcPoint + 1;
            }
            return Activity.getTOSTime(offsetStartTime);
        } catch (Throwable e) {
            throw new CoreException("Read TiDB stream offset failed, message: {}", e.getMessage(), e);
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Exception {
        started.set(false);
        EmptyKit.closeQuietly(tidbJdbcContext);
        ErrorKit.ignoreAnyError(() -> cleanCDC(connectionContext, false));
    }

    protected void checkTiServerAndStop(HttpUtil httpUtil, String cdcServer, TapConnectorContext connectorContext) throws IOException {
        Log log = connectorContext.getLog();
        log.debug("Cdc server is alive, will check change feed list");
        synchronized (TiCDCShellManager.PROCESS_LOCK) {
            if (httpUtil.queryChangeFeedsList(cdcServer) <= 0) {
                log.debug("There is not any change feed with cdc server: {}, will stop cdc server", cdcServer);
                TiCDCShellManager.ShellConfig config = new TiCDCShellManager.ShellConfig();
                config.withPdIpPorts(connectorContext.getConnectionConfig().getString("pdServer"));
                String pdServer = config.getPdIpPorts();
                String processes = ProcessSearch.getProcessesPortsAsLine(" ", log, TiCDCShellManager.getCdcPsGrepFilter(pdServer, cdcServer));
                if (null != processes) {
                    String killCmd = TiCDCShellManager.setProperties("kill -9 ${pid}", "pid", processes);
                    log.debug("After release cdc resource, kill cdc server, kill cmd: {}", killCmd);
                    ProcessLauncher.execCmdWaitResult(killCmd, "stop cdc server failed, message: {}", log);
                }
                ErrorKit.ignoreAnyError(() -> ZipUtils.deleteFile(FileUtil.paths(Activity.BASE_CDC_CACHE_DATA_DIR, ProcessHandler.pdServerPath(pdServer)), log));
                ErrorKit.ignoreAnyError(() -> ZipUtils.deleteFile(FileUtil.paths(Activity.BASE_CDC_LOG_DIR, ProcessHandler.pdServerPath(pdServer) + ".log"), log));
            }
        }
    }

    protected void cleanCDC(TapConnectionContext connectionContext, boolean stopServer) {
        if (!(connectionContext instanceof TapConnectorContext)) {
            return;
        }
        TapConnectorContext connectorContext = (TapConnectorContext) connectionContext;
        Log log = connectorContext.getLog();
        KVMap<Object> stateMap = connectorContext.getStateMap();
        String feedId = (String) stateMap.get(ProcessHandler.FEED_ID);
        String cdcServer = (String) stateMap.get(ProcessHandler.CDC_SERVER);
        String filePath = (String) stateMap.get(ProcessHandler.CDC_FILE_PATH);
        if (EmptyKit.isNotNull(feedId) && EmptyKit.isNotEmpty(cdcServer)) {
            try (HttpUtil httpUtil = HttpUtil.of(log)) {
                log.debug("Start to delete change feed: {}", feedId);
                ErrorKit.ignoreAnyError(() -> httpUtil.deleteChangeFeed(feedId, cdcServer));
                if (StringUtils.isNotBlank(filePath)) {
                    log.debug("Start to clean cdc data dir: {}", filePath);
                    ErrorKit.ignoreAnyError(() -> ZipUtils.deleteFile(filePath, log));
                }
                if (!stopServer) {
                    return;
                }
                log.debug("Start to check cdc server heath: {}", cdcServer);
                ErrorKit.ignoreAnyError(() -> this.checkTiServerAndStop(httpUtil, cdcServer, connectorContext));
            }
        }
    }

    protected void onDestroy(TapConnectorContext connectorContext) {
        ErrorKit.ignoreAnyError(() -> cleanCDC(connectorContext, true));
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        String insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        new TidbRecordWriter(tidbJdbcContext, tapTable)
                .setInsertPolicy(insertDmlPolicy)
                .setUpdatePolicy(updateDmlPolicy)
                .write(tapRecordEvents, consumer, this::isAlive);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext databaseContext, Consumer<TestItem> consumer) {
        tidbConfig = new TidbConfig().load(databaseContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(tidbConfig.getConnectionString());
        try (
                TidbConnectionTest connectionTest = new TidbConnectionTest(tidbConfig, consumer, connectionOptions)
        ) {
            connectionTest.testOneByOne();
        }
        return connectionOptions;
    }

    private void queryByAdvanceFilter(TapConnectorContext tapConnectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable tapTable, Consumer<FilterResults> consumer) {
        FilterResults filterResults = new FilterResults();
        filterResults.setFilter(tapAdvanceFilter);
        try {
            tidbReader.readWithFilter(tapConnectorContext, tapTable, tapAdvanceFilter, n -> !isAlive(), data -> {
                processDataMap(data, tapTable);
                filterResults.add(data);
                if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                    consumer.accept(filterResults);
                    filterResults.getResults().clear();
                }
            });
            if (CollectionUtils.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
                filterResults.getResults().clear();
            }
        } catch (Throwable e) {
            filterResults.setError(e);
            consumer.accept(filterResults);
        }
    }

    @Override
    protected TapField makeTapField(DataMap dataMap) {
        return new MysqlColumn(dataMap).getTapField();
    }

    private TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        DataMap dataMap = tidbJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("TABLE_ROWS")));
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("DATA_LENGTH")));
        return tableInfo;
    }

    @Override
    protected void processDataMap(DataMap dataMap, TapTable tapTable) throws RuntimeException {
        for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
            Object value = entry.getValue();
            TapField field = tapTable.getNameFieldMap().get(entry.getKey());
            String dataType = field.getDataType();
            boolean isTimestamp = dataType.startsWith("timestamp") || dataType.startsWith("TIMESTAMP");
            if (value instanceof LocalDateTime) {
                if (!tapTable.getNameFieldMap().containsKey(entry.getKey())) {
                    continue;
                }
                if (isTimestamp) {
                    entry.setValue(((LocalDateTime) value).atZone(ZoneOffset.UTC));
                } else {
                    entry.setValue(((LocalDateTime) value).minusHours(tidbConfig.getZoneOffsetHour()));
                }
            }  else if (value instanceof java.sql.Date) {
                ZoneId zoneId = null == timezone ? ZoneId.systemDefault() : timezone.toZoneId();
                entry.setValue(Instant.ofEpochMilli(((Date) value).getTime()).atZone(zoneId).toLocalDateTime().minusHours(tidbConfig.getZoneOffsetHour()));
            } else if (value instanceof Timestamp) {
                if (isTimestamp) {
                    entry.setValue(((Timestamp) value).toLocalDateTime().atZone(ZoneOffset.UTC));
                } else {
                    entry.setValue(((Timestamp) value).toLocalDateTime().minusHours(tidbConfig.getZoneOffsetHour()));
                }
            } else if (value instanceof Time) {
                entry.setValue(Instant.ofEpochMilli(((Time) value).getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime().minusHours(tidbConfig.getZoneOffsetHour()));
            }
        }
    }
}

