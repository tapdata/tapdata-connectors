package io.tapdata.connector.mysql;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.debezium.type.TapIllegalDate;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.wrapper.DDLWrapperConfig;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.constant.DeployModeEnum;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.connector.mysql.entity.MysqlSnapshotOffset;
import io.tapdata.connector.mysql.entity.MysqlStreamEvent;
import io.tapdata.connector.mysql.entity.MysqlStreamOffset;
import io.tapdata.connector.mysql.util.MysqlBinlogPositionUtil;
import io.tapdata.connector.mysql.util.MysqlUtil;
import io.tapdata.connector.mysql.util.StringCompressUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.entity.utils.TypeHolder;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetUtils;

import java.io.Closeable;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.sql.ResultSetMetaData;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.tapdata.connector.mysql.util.MysqlUtil.randomServerId;

/**
 * @author samuel
 * @Description
 * @create 2022-05-05 20:13
 **/
public class MysqlReader implements Closeable {
    private static final String TAG = MysqlReader.class.getSimpleName();
    public static final String SERVER_NAME_KEY = "SERVER_NAME";
    public static final String MYSQL_SCHEMA_HISTORY = "MYSQL_SCHEMA_HISTORY";
    private static final String SOURCE_RECORD_DDL_KEY = "ddl";
    public static final String FIRST_TIME_KEY = "FIRST_TIME";
    private static final DDLWrapperConfig DDL_WRAPPER_CONFIG = CCJBaseDDLWrapper.CCJDDLWrapperConfig.create().split("`");
    public static final long SAVE_DEBEZIUM_SCHEMA_HISTORY_INTERVAL_SEC = 2L;
    private String serverName;
    private final Supplier<Boolean> isAlive;
    private final MysqlJdbcContextV2 mysqlJdbcContext;
    private EmbeddedEngine embeddedEngine;
    private LinkedBlockingQueue<MysqlStreamEvent> eventQueue;
    private StreamReadConsumer streamReadConsumer;
    private ScheduledExecutorService mysqlSchemaHistoryMonitor;
    private KVReadOnlyMap<TapTable> tapTableMap;
    private DDLParserType ddlParserType = DDLParserType.MYSQL_CCJ_SQL_PARSER;
    private static final int MIN_BATCH_SIZE = 1000;
    private TimeZone timeZone;
    private TimeZone dbTimeZone;
    private final AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<>();
    private final ExceptionCollector exceptionCollector;
    private String dropTransactionId = null;
    private final MysqlConfig mysqlConfig;
    protected Log tapLogger;
    private long diff = 0;

    public MysqlReader(MysqlJdbcContextV2 mysqlJdbcContext, Log tapLogger, Supplier<Boolean> isAlive) {
        this.mysqlJdbcContext = mysqlJdbcContext;
        this.mysqlConfig = (MysqlConfig) mysqlJdbcContext.getConfig();
        this.isAlive = isAlive;
        this.tapLogger = tapLogger;
        this.exceptionCollector = new MysqlExceptionCollector();
        LocalDateTime dt = LocalDateTime.now();
        ZonedDateTime fromZonedDateTime;
        try {
            this.dbTimeZone = mysqlJdbcContext.queryTimeZone();
            if (mysqlConfig.getOldVersionTimezone()) {
                this.timeZone = dbTimeZone;
                fromZonedDateTime = dt.atZone(TimeZone.getDefault().toZoneId());
            } else {
                this.timeZone = TimeZone.getTimeZone("GMT" + mysqlConfig.getTimezone());
                fromZonedDateTime = dt.atZone(mysqlConfig.getZoneId());
            }
            ZonedDateTime toZonedDateTime = dt.atZone(TimeZone.getTimeZone("GMT").toZoneId());
            diff = Duration.between(toZonedDateTime, fromZonedDateTime).toMillis();
        } catch (Exception ignore) {

        }
    }

    public void readWithOffset(TapConnectorContext tapConnectorContext, TapTable tapTable, MysqlSnapshotOffset mysqlSnapshotOffset,
                               Predicate<?> stop, BiConsumer<Map<String, Object>, MysqlSnapshotOffset> consumer) throws Throwable {
        SqlMaker sqlMaker = new MysqlMaker();
        String sql = sqlMaker.selectSql(tapConnectorContext, tapTable, mysqlSnapshotOffset);
        Collection<String> pks = tapTable.primaryKeys(true);
        AtomicLong row = new AtomicLong(0L);
        try {
            Set<String> dateTypeSet = dateFields(tapTable);
            this.mysqlJdbcContext.queryWithStream(sql, rs -> {
                ResultSetMetaData metaData = rs.getMetaData();
                while ((null == stop || !stop.test(null)) && rs.next()) {
                    row.incrementAndGet();
                    Map<String, Object> data = new HashMap<>();
                    for (int i = 0; i < metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i + 1);
                        try {
                            Object value;
                            // 抹除 time 字段的时区，兼容 "-838:59:59", "838:59:59" 格式数据
                            if ("TIME".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                                value = rs.getString(i + 1);
                            } else {
                                value = rs.getObject(i + 1);
                                if (null == value && dateTypeSet.contains(columnName)) {
                                    value = rs.getString(i + 1);
                                }
                            }
                            data.put(columnName, value);
                            if (pks.contains(columnName)) {
                                mysqlSnapshotOffset.getOffset().put(columnName, value);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Read column value failed, row: " + row.get() + ", column name: " + columnName + ", data: " + data + "; Error: " + e.getMessage(), e);
                        }
                    }
                    consumer.accept(data, mysqlSnapshotOffset);
                }
            });
        } catch (Throwable e) {
            if (null != stop && stop.test(null)) {
                // ignored error
            } else {
                throw e;
            }
        }
    }

    public void readWithFilter(TapConnectorContext tapConnectorContext, TapTable tapTable, TapAdvanceFilter tapAdvanceFilter,
                               Predicate<?> stop, Consumer<Map<String, Object>> consumer) throws Throwable {
        SqlMaker sqlMaker = new MysqlMaker();
        String sql = sqlMaker.selectSql(tapConnectorContext, tapTable, tapAdvanceFilter);
        AtomicLong row = new AtomicLong(0L);
        try {
            Set<String> dateTypeSet = dateFields(tapTable);
            this.mysqlJdbcContext.queryWithStream(sql, rs -> {
                ResultSetMetaData metaData = rs.getMetaData();
                while (rs.next()) {
                    if (null != stop && stop.test(null)) {
                        break;
                    }
                    row.incrementAndGet();
                    Map<String, Object> data = new HashMap<>();
                    for (int i = 0; i < metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i + 1);
                        try {
                            Object value;
                            // 抹除 time 字段的时区，兼容 "-838:59:59", "838:59:59" 格式数据
                            if ("TIME".equalsIgnoreCase(metaData.getColumnTypeName(i + 1))) {
                                value = rs.getString(i + 1);
                            } else {
                                value = rs.getObject(i + 1);
                                if (null == value && dateTypeSet.contains(columnName)) {
                                    value = rs.getString(i + 1);
                                }
                            }
                            data.put(columnName, value);
                        } catch (Exception e) {
                            throw new RuntimeException("Read column value failed, row: " + row.get() + ", column name: " + columnName + ", data: " + data + "; Error: " + e.getMessage(), e);
                        }
                    }
                    consumer.accept(data);
                }
            });
        } catch (Throwable e) {
            if (null != stop && stop.test(null)) {
                // ignored error
            } else {
                throw e;
            }
        }
    }

    public void readBinlog(TapConnectorContext tapConnectorContext, List<String> tables,
                           Object offset, int batchSize, DDLParserType ddlParserType, StreamReadConsumer consumer, HashMap<String, MysqlJdbcContextV2> contextMapForMasterSlave) throws Throwable {
        MysqlUtil.buildMasterNode(mysqlConfig, contextMapForMasterSlave);
        try {
            initDebeziumServerName(tapConnectorContext);
            this.tapTableMap = tapConnectorContext.getTableMap();
            this.ddlParserType = ddlParserType;
            String offsetStr = "";
            JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
            String deploymentMode = mysqlConfig.getDeploymentMode();
            if (DeployModeEnum.fromString(deploymentMode) == DeployModeEnum.MASTER_SLAVE) {
                if (offset instanceof MysqlStreamOffset) {
                    Map<String, String> offset1 = ((MysqlStreamOffset) offset).getOffset();
                    AtomicReference<String> os = new AtomicReference<>();
                    offset1.forEach((k, v) -> os.set(v));
                    HashMap map = jsonParser.fromJson(os.get(), HashMap.class);
                    Integer ts = (Integer) map.get("ts_sec");
                    if (null != ts) {
                        offset = TimeUnit.SECONDS.toMillis(new Long(ts));
                    }
                }
            }
            MysqlStreamOffset mysqlStreamOffset = null;
            if (offset instanceof MysqlStreamOffset) {
                mysqlStreamOffset = (MysqlStreamOffset) offset;
            } else if (offset instanceof MysqlBinlogPosition) {
                mysqlStreamOffset = binlogPosition2MysqlStreamOffset((MysqlBinlogPosition) offset, jsonParser);
            } else if (offset instanceof Long) {
                try (MysqlBinlogPositionUtil ins = new MysqlBinlogPositionUtil(
                        mysqlConfig.getHost(),
                        mysqlConfig.getPort(),
                        mysqlConfig.getUser(),
                        mysqlConfig.getPassword())) {
                    MysqlBinlogPosition mysqlBinlogPosition = ins.findByLessTimestamp((Long) offset, true);
                    if (null == mysqlBinlogPosition) {
                        throw new RuntimeException("Not found binlog of sync time: " + offset);
                    }
                    mysqlStreamOffset = binlogPosition2MysqlStreamOffset(mysqlBinlogPosition, jsonParser);
                }
            }
            if (null != mysqlStreamOffset) {
                offsetStr = jsonParser.toJson(mysqlStreamOffset);
            }
            tapLogger.info("Starting mysql cdc, server name: " + serverName);
            this.eventQueue = new LinkedBlockingQueue<>(10);
            this.streamReadConsumer = consumer;
            initMysqlSchemaHistory(tapConnectorContext);
            this.mysqlSchemaHistoryMonitor = new ScheduledThreadPoolExecutor(1);
            this.mysqlSchemaHistoryMonitor.scheduleAtFixedRate(() -> saveMysqlSchemaHistory(tapConnectorContext),
                    SAVE_DEBEZIUM_SCHEMA_HISTORY_INTERVAL_SEC, SAVE_DEBEZIUM_SCHEMA_HISTORY_INTERVAL_SEC, TimeUnit.SECONDS);
            Configuration.Builder builder = Configuration.create()
                    .with("name", serverName)
                    .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
                    .with("database.hostname", mysqlConfig.getHost())
                    .with("database.port", mysqlConfig.getPort())
                    .with("database.user", mysqlConfig.getUser())
                    .with("database.password", EmptyKit.isNull(mysqlConfig.getPassword()) ? "" : mysqlConfig.getPassword())
                    .with("database.server.name", serverName)
                    .with("threadName", "Debezium-Mysql-Connector-" + serverName)
                    .with("database.history.skip.unparseable.ddl", true)
                    .with("database.history.store.only.monitored.tables.ddl", true)
                    .with("database.history.store.only.captured.tables.ddl", true)
                    .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.NONE)
                    .with("max.queue.size", mysqlConfig.getMaximumQueueSize())
                    .with("max.batch.size", mysqlConfig.getMaximumQueueSize() / 8)
                    .with(MySqlConnectorConfig.SERVER_ID, randomServerId())
                    .with("converters", "geometry")
                    .with("geometry.type", "io.tapdata.connector.mysql.GeometryConverter")
                    .with("geometry.schema.name", "io.debezium.mysql.type.Geometry")
                    .with("time.precision.mode", "adaptive_time_microseconds")
//					.with("converters", "time")
//					.with("time.type", "io.tapdata.connector.mysql.converters.TimeConverter")
//					.with("time.schema.name", "io.debezium.mysql.type.Time")
                    .with("enable.time.adjuster",false)
                    .with("snapshot.locking.mode", "none");
//            if (EmptyKit.isNotBlank(mysqlConfig.getTimezone())) {
//                builder.with("database.serverTimezone", mysqlJdbcContext.queryTimeZone());
//            }
            List<String> dbTableNames = tables.stream().map(t -> mysqlConfig.getDatabase() + "." + t).collect(Collectors.toList());
            if (mysqlConfig.getDoubleActive()) {
                dbTableNames.add(mysqlConfig.getDatabase() + "._tap_double_active");
            }
            builder.with(MySqlConnectorConfig.DATABASE_INCLUDE_LIST, mysqlConfig.getDatabase());
            builder.with(MySqlConnectorConfig.TABLE_INCLUDE_LIST, String.join(",", dbTableNames));
            builder.with(EmbeddedEngine.OFFSET_STORAGE, "io.tapdata.connector.mysql.PdkPersistenceOffsetBackingStore");
            if (StringUtils.isNotBlank(offsetStr)) {
                builder.with("pdk.offset.string", offsetStr);
            }
            if (Boolean.TRUE.equals(mysqlConfig.getDoubleActive())) {
                builder.with("provide.transaction.metadata", true);
            }
			/*
				todo At present, the schema loading logic will load the schema of all current tables each time it is started. When there is ddl in the historical data, it will cause a parsing error
				todo The main scenario is shared mining, which dynamically modifies the table include list. If the last cached model list is used, debezium will not load the newly added table model, resulting in a parsing error when reading: whose schema isn't known to this connector
				todo Best practice, need to change the debezium source code, add a configuration that supports partial update of some table schemas, and logic implementation
			*/
            builder.with("snapshot.mode", MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY_RECOVERY);
            if (null != throwableAtomicReference.get()) {
                String lastErrorMessage = throwableAtomicReference.get().getMessage();
                if (StringUtils.isNotBlank(lastErrorMessage) && lastErrorMessage.contains("Could not find existing binlog information while attempting schema only recovery snapshot")) {
                    builder.with("snapshot.mode", MySqlConnectorConfig.SnapshotMode.SCHEMA_ONLY);
                }
            }
//			builder.with("database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
            builder.with("database.history", "io.tapdata.connector.mysql.StateMapHistoryBackingStore");
            //开启ssl
            if (mysqlConfig.getUseSSL()) {
                builder.with("database.useSSL", "true");
                builder.with("database.requireSSL", "true");
                if ("true".equals(mysqlConfig.getProperties().getProperty("verifyServerCertificate"))) {
                    builder.with("database.ssl.mode", "required");
                    builder.with("database.database.ssl.keystore", mysqlConfig.getProperties().getProperty("clientCertificateKeyStoreUrl").replace("file:", ""));
                    builder.with("database.database.ssl.keystore.password", mysqlConfig.getProperties().getProperty("clientCertificateKeyStorePassword"));
                    builder.with("database.database.ssl.truststore", mysqlConfig.getProperties().getProperty("trustCertificateKeyStoreUrl").replace("file:", ""));
                    builder.with("database.database.ssl.truststore.password", mysqlConfig.getProperties().getProperty("trustCertificateKeyStorePassword"));
                }
            }
            Configuration configuration = builder.build();
            StringBuilder configStr = new StringBuilder("Starting binlog reader with config {\n");
            configuration.withMaskedPasswords().asMap().forEach((k, v) -> configStr.append("  ")
                    .append(k)
                    .append(": ")
                    .append(v)
                    .append("\n"));
            configStr.append("}");
            tapLogger.info(configStr.toString());
            embeddedEngine = (EmbeddedEngine) new EmbeddedEngine.BuilderImpl()
                    .using(configuration)
                    .notifying(this::consumeRecords)
                    .using(new DebeziumEngine.ConnectorCallback() {
                        @Override
                        public void taskStarted() {
                            streamReadConsumer.streamReadStarted();
                        }
                    })
                    .using((numberOfMessagesSinceLastCommit, timeSinceLastCommit) ->
                            numberOfMessagesSinceLastCommit >= batchSize || timeSinceLastCommit.getSeconds() >= 5)
                    .using((result, message, throwable) -> {
                        tapConnectorContext.configContext();
                        if (result) {
                            if (StringUtils.isNotBlank(message)) {
                                tapLogger.info("CDC engine stopped: " + message);
                            } else {
                                tapLogger.info("CDC engine stopped");
                            }
                            throwableAtomicReference.set(null);
                        } else {
                            if (null != throwable) {
                                if (StringUtils.isNotBlank(message)) {
                                    handleFailed(new RuntimeException(message + "\n " + throwable.getMessage(), throwable));
                                } else {
                                    handleFailed(throwable);
                                }
                            } else {
                                throwableAtomicReference.set(null);
                            }
                        }
                        streamReadConsumer.streamReadEnded();
                    })
                    .build();
            embeddedEngine.run();
            if (null != throwableAtomicReference.get()) {
                Throwable e = ErrorKit.getLastCause(throwableAtomicReference.get());
                ((MysqlExceptionCollector) exceptionCollector).setMysqlConfig(mysqlConfig);
                exceptionCollector.collectTerminateByServer(e);
                exceptionCollector.collectOffsetInvalid(offset, e);
                exceptionCollector.collectCdcConfigInvalid(e);
                throw e;
            }
        } finally {
            Optional.ofNullable(mysqlSchemaHistoryMonitor).ifPresent(ExecutorService::shutdownNow);
            tapLogger.info("Mysql binlog reader stopped");
        }
    }

    private void handleFailed(Throwable throwable) {
        throwableAtomicReference.set(new RuntimeException(throwable));
    }

    private MysqlStreamOffset binlogPosition2MysqlStreamOffset(MysqlBinlogPosition offset, JsonParser jsonParser) throws Throwable {
        String serverId = mysqlJdbcContext.getServerId();
        Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put("server", serverName);
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("file", offset.getFilename());
        offsetMap.put("pos", offset.getPosition());
        offsetMap.put("server_id", serverId);
        MysqlStreamOffset mysqlStreamOffset = new MysqlStreamOffset();
        mysqlStreamOffset.setName(serverName);
        mysqlStreamOffset.setOffset(new HashMap<String, String>() {{
            put(jsonParser.toJson(partitionMap), jsonParser.toJson(offsetMap));
        }});
        return mysqlStreamOffset;
    }

    protected void initMysqlSchemaHistory(TapConnectorContext tapConnectorContext) {
        // block init cdc schema by a lock, because mysqlSchemaHistory(string) may too long,
        //   memory size may over 1G and cause engine OOM when start tasks sync
        tapLogger.debug("Mysql init/save cdc schema lock, {}", System.currentTimeMillis());
        try {
            synchronized (MysqlReader.class) {
                KVMap<Object> stateMap = tapConnectorContext.getStateMap();
                Object mysqlSchemaHistory = stateMap.get(MYSQL_SCHEMA_HISTORY);
                if (mysqlSchemaHistory instanceof String) {
                    try {
                        mysqlSchemaHistory = StringCompressUtil.uncompress((String) mysqlSchemaHistory);
                    } catch (IOException e) {
                        throw new CoreException("Uncompress Mysql schema history failed, message: {}, string: {}", e.getMessage(), (((String) mysqlSchemaHistory).length() > 65535 ? "...(mysql Schema History too long, more than 655350)" : mysqlSchemaHistory), e);
                    }
                    mysqlSchemaHistory = InstanceFactory.instance(JsonParser.class).fromJson((String) mysqlSchemaHistory,
                            new TypeHolder<Map<String, LinkedHashSet<String>>>() {
                            });
                    MysqlSchemaHistoryTransfer.historyMap.putAll((Map) mysqlSchemaHistory);
                }
            }
        } finally {
            tapLogger.debug("Mysql init/save cdc schema unlock, {}", System.currentTimeMillis());
        }
    }

    protected void saveMysqlSchemaHistory(TapConnectorContext tapConnectorContext) {
        tapLogger.debug("Mysql save/init cdc schema lock, {}", System.currentTimeMillis());
        try {
            tapConnectorContext.configContext();
            Thread.currentThread().setName("Save-Mysql-Schema-History-" + serverName);
            if (!MysqlSchemaHistoryTransfer.isSave()) {
                MysqlSchemaHistoryTransfer.executeWithLock(n -> !isAlive.get(), () -> {
                    String json = InstanceFactory.instance(JsonParser.class).toJson(MysqlSchemaHistoryTransfer.historyMap);
                    try {
                        json = StringCompressUtil.compress(json);
                    } catch (IOException e) {
                        tapLogger.warn("Compress Mysql schema history failed, string: " + (json.length() > 65535 ? "...(mysql Schema History too long, more than 655350)" : json) + ", error message: " + e.getMessage() + "\n" + TapSimplify.getStackString(e));
                        return;
                    }
                    tapConnectorContext.getStateMap().put(MYSQL_SCHEMA_HISTORY, json);
                    MysqlSchemaHistoryTransfer.save();
                });
            }
        } finally {
            tapLogger.debug("Mysql save/init cdc schema unlock, {}", System.currentTimeMillis());
        }
    }

    private void initDebeziumServerName(TapConnectorContext tapConnectorContext) {
        this.serverName = UUID.randomUUID().toString().toLowerCase();
        KVMap<Object> stateMap = tapConnectorContext.getStateMap();
        Object serverNameFromStateMap = stateMap.get(SERVER_NAME_KEY);
        if (serverNameFromStateMap instanceof String) {
            this.serverName = String.valueOf(serverNameFromStateMap);
            stateMap.put(SERVER_NAME_KEY, serverNameFromStateMap);
            stateMap.put(FIRST_TIME_KEY, false);
        } else {
            stateMap.put(FIRST_TIME_KEY, true);
            stateMap.put(SERVER_NAME_KEY, this.serverName);
        }
    }

    @Override
    public void close() {
        Optional.ofNullable(embeddedEngine).ifPresent(engine -> {
            try {
                engine.close();
            } catch (IOException e) {
                tapLogger.warn("Close CDC engine failed, error: " + e.getMessage() + "\n" + TapSimplify.getStackString(e));
            }
        });
        Optional.ofNullable(mysqlSchemaHistoryMonitor).ifPresent(ExecutorService::shutdownNow);
    }

    private void consumeRecords(List<SourceRecord> sourceRecords, DebeziumEngine.RecordCommitter<SourceRecord> committer) {
        if (null != throwableAtomicReference.get()) {
            throw new RuntimeException(throwableAtomicReference.get());
        }
        List<MysqlStreamEvent> mysqlStreamEvents = new ArrayList<>();
        for (SourceRecord record : sourceRecords) {
            if (null == record || null == record.value()) continue;
            Schema valueSchema = record.valueSchema();
            if (null != valueSchema.field("op")) {
                MysqlStreamEvent mysqlStreamEvent = wrapDML(record);
                Optional.ofNullable(mysqlStreamEvent).ifPresent(mysqlStreamEvents::add);
            } else if (null != valueSchema.field("ddl")) {
                mysqlStreamEvents.addAll(Objects.requireNonNull(wrapDDL(record)));
            } else if ("io.debezium.connector.common.Heartbeat".equals(valueSchema.name())) {
                Optional.ofNullable((Struct) record.value())
                        .map(value -> value.getInt64("ts_ms"))
                        .map(TapSimplify::heartbeatEvent)
                        .map(heartbeatEvent -> new MysqlStreamEvent(heartbeatEvent, getMysqlStreamOffset(record)))
                        .ifPresent(mysqlStreamEvents::add);
            }
        }
        if (CollectionUtils.isNotEmpty(mysqlStreamEvents)) {
            List<TapEvent> tapEvents = new ArrayList<>();
            MysqlStreamOffset mysqlStreamOffset = null;
            for (MysqlStreamEvent mysqlStreamEvent : mysqlStreamEvents) {
                tapEvents.add(mysqlStreamEvent.getTapEvent());
                mysqlStreamOffset = mysqlStreamEvent.getMysqlStreamOffset();
            }
            streamReadConsumer.accept(tapEvents, mysqlStreamOffset);
        }
    }

    private void sourceRecordConsumer(SourceRecord record) {
        if (null != throwableAtomicReference.get()) {
            throw new RuntimeException(throwableAtomicReference.get());
        }
        if (null == record || null == record.value()) return;
        Schema valueSchema = record.valueSchema();
        List<MysqlStreamEvent> mysqlStreamEvents = new ArrayList<>();
        if (null != valueSchema.field("op")) {
            MysqlStreamEvent mysqlStreamEvent = wrapDML(record);
            Optional.ofNullable(mysqlStreamEvent).ifPresent(mysqlStreamEvents::add);
        } else if (null != valueSchema.field("ddl")) {
            mysqlStreamEvents = wrapDDL(record);
        } else if ("io.debezium.connector.common.Heartbeat".equals(valueSchema.name())) {
            Optional.ofNullable((Struct) record.value())
                    .map(value -> value.getInt64("ts_ms"))
                    .map(TapSimplify::heartbeatEvent)
                    .map(heartbeatEvent -> new MysqlStreamEvent(heartbeatEvent, getMysqlStreamOffset(record)))
                    .ifPresent(mysqlStreamEvents::add);
        }
        if (CollectionUtils.isNotEmpty(mysqlStreamEvents)) {
            List<TapEvent> tapEvents = new ArrayList<>();
            MysqlStreamOffset mysqlStreamOffset = null;
            for (MysqlStreamEvent mysqlStreamEvent : mysqlStreamEvents) {
                tapEvents.add(mysqlStreamEvent.getTapEvent());
                mysqlStreamOffset = mysqlStreamEvent.getMysqlStreamOffset();
            }
            streamReadConsumer.accept(tapEvents, mysqlStreamOffset);
        }
    }

    protected MysqlStreamEvent wrapDML(SourceRecord record) {
        TapRecordEvent tapRecordEvent = null;
        MysqlStreamEvent mysqlStreamEvent;
        Schema valueSchema = record.valueSchema();
        Struct value = (Struct) record.value();
        Struct source = value.getStruct("source");
        Long eventTime = source.getInt64("ts_ms");
        String table = Optional.of(record.topic().split("\\.")).map(arr -> {
            if (arr.length > 0) {
                return URLDecoder.decode(arr[arr.length - 1]);
            }
            return null;
        }).orElse(source.getString("table"));
        //双活情形下，需要过滤_tap_double_active记录的同事务数据
        if (Boolean.TRUE.equals(mysqlConfig.getDoubleActive())) {
            if ("_tap_double_active".equals(table)) {
                dropTransactionId = String.valueOf(record.sourceOffset().get("transaction_id"));
                return null;
            } else {
                if (null != dropTransactionId) {
                    if (dropTransactionId.equals(String.valueOf(record.sourceOffset().get("transaction_id")))) {
                        return null;
                    } else {
                        dropTransactionId = null;
                    }
                }
            }
        }
        String op = value.getString("op");
        MysqlOpType mysqlOpType = MysqlOpType.fromOp(op);
        if (null == mysqlOpType) {
            tapLogger.debug("Unrecognized operation type: " + op + ", will skip it, record: " + record);
            return null;
        }
        Map<String, TapIllegalDate> beforeInvalidMap = new HashMap<>();
        if (null != value.schema().field("beforeInvalid")) {
            Struct invalid = value.getStruct("beforeInvalid");
            for (Field field : invalid.schema().fields()) {
                byte[] bytes = invalid.getBytes(field.name());
                try {
                    Object o = TapIllegalDate.byteToIllegalDate(bytes);
                    if (o instanceof TapIllegalDate) {
                        beforeInvalidMap.put(field.name(), (TapIllegalDate) o);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("byte to tap illegal date error", e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("byte to tap illegal date error", e);
                }
            }
        }
        Map<String, TapIllegalDate> afterInvalidMap = new HashMap<>();
        if (null != value.schema().field("afterInvalid")) {
            Struct invalid = value.getStruct("afterInvalid");
            for (Field field : invalid.schema().fields()) {
                byte[] bytes = invalid.getBytes(field.name());
                try {
                    Object o = TapIllegalDate.byteToIllegalDate(bytes);
                    if (o instanceof TapIllegalDate) {
                        afterInvalidMap.put(field.name(), (TapIllegalDate) o);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("byte to tap illegal date error", e);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("byte to tap illegal date error", e);
                }
            }
        }
        Map<String, Object> before = null;
        Map<String, Object> after = null;
        switch (mysqlOpType) {
            case INSERT:
                tapRecordEvent = new TapInsertRecordEvent().init();
                if (null == valueSchema.field("after"))
                    throw new RuntimeException("Found insert record does not have after: " + record);
                after = struct2Map(value.getStruct("after"), table);
                if (!EmptyKit.isEmpty(afterInvalidMap)) {
                    after.putAll(afterInvalidMap);
                    tapRecordEvent.setContainsIllegalDate(true);
                    ((TapInsertRecordEvent) tapRecordEvent).setAfterIllegalDateFieldName(afterInvalidMap.keySet().stream().collect(Collectors.toList()));
                }
                ((TapInsertRecordEvent) tapRecordEvent).setAfter(after);
                break;
            case UPDATE:
                tapRecordEvent = new TapUpdateRecordEvent().init();
                if (null != valueSchema.field("before")) {
                    before = struct2Map(value.getStruct("before"), table);
                    if (!EmptyKit.isEmpty(beforeInvalidMap)) {
                        before.putAll(beforeInvalidMap);
                        tapRecordEvent.setContainsIllegalDate(true);
                        ((TapUpdateRecordEvent) tapRecordEvent).setBeforeIllegalDateFieldName(beforeInvalidMap.keySet().stream().collect(Collectors.toList()));
                    }
                    ((TapUpdateRecordEvent) tapRecordEvent).setBefore(before);
                }
                if (null == valueSchema.field("after"))
                    throw new RuntimeException("Found update record does not have after: " + record);
                after = struct2Map(value.getStruct("after"), table);
                if (!EmptyKit.isEmpty(beforeInvalidMap) || !EmptyKit.isEmpty(afterInvalidMap)) {
                    before.putAll(beforeInvalidMap);
                    after.putAll(afterInvalidMap);
                    tapRecordEvent.setContainsIllegalDate(true);
                    ((TapUpdateRecordEvent) tapRecordEvent).setBeforeIllegalDateFieldName(beforeInvalidMap.keySet().stream().collect(Collectors.toList()));
                    ((TapUpdateRecordEvent) tapRecordEvent).setAfterIllegalDateFieldName(afterInvalidMap.keySet().stream().collect(Collectors.toList()));
                }
                ((TapUpdateRecordEvent) tapRecordEvent).setAfter(after);
                break;
            case DELETE:
                tapRecordEvent = new TapDeleteRecordEvent().init();
                if (null == valueSchema.field("before"))
                    throw new RuntimeException("Found delete record does not have before: " + record);
                before = struct2Map(value.getStruct("before"), table);
                ((TapDeleteRecordEvent) tapRecordEvent).setBefore(before);
                if (!EmptyKit.isEmpty(beforeInvalidMap)) {
                    before.putAll(beforeInvalidMap);
                    tapRecordEvent.setContainsIllegalDate(true);
                    ((TapDeleteRecordEvent) tapRecordEvent).setBeforeIllegalDateFieldName(beforeInvalidMap.keySet().stream().collect(Collectors.toList()));
                }
                break;
            default:
                break;
        }
        tapRecordEvent.setTableId(table);
        tapRecordEvent.setReferenceTime(eventTime);
        MysqlStreamOffset mysqlStreamOffset = getMysqlStreamOffset(record);
        tapLogger.debug("Read DML - Table: " + table + "\n  - Operation: " + mysqlOpType.getOp()
                + "\n  - Before: " + before + "\n  - After: " + after + "\n  - Offset: " + mysqlStreamOffset);
        mysqlStreamEvent = new MysqlStreamEvent(tapRecordEvent, mysqlStreamOffset);
        return mysqlStreamEvent;
    }

    protected List<MysqlStreamEvent> wrapDDL(SourceRecord record) {
        List<MysqlStreamEvent> mysqlStreamEvents = new ArrayList<>();
        Object value = record.value();
        if (!(value instanceof Struct)) {
            return null;
        }
        Struct structValue = (Struct) value;
        Struct source = structValue.getStruct("source");
        Long eventTime = source.getInt64("ts_ms");
        String ddlStr = structValue.getString(SOURCE_RECORD_DDL_KEY);
        MysqlStreamOffset mysqlStreamOffset = getMysqlStreamOffset(record);
        if (StringUtils.isNotBlank(ddlStr)) {
            try {
                DDLFactory.ddlToTapDDLEvent(
                        ddlParserType,
                        ddlStr,
                        DDL_WRAPPER_CONFIG,
                        tapTableMap,
                        tapDDLEvent -> {
                            MysqlStreamEvent mysqlStreamEvent = new MysqlStreamEvent(tapDDLEvent, mysqlStreamOffset);
                            tapDDLEvent.setTime(System.currentTimeMillis());
                            tapDDLEvent.setReferenceTime(eventTime);
                            tapDDLEvent.setOriginDDL(ddlStr);
                            mysqlStreamEvents.add(mysqlStreamEvent);
                            tapLogger.info("Read DDL: " + ddlStr + ", about to be packaged as some event(s)");
                        }
                );
            } catch (Throwable e) {
                TapDDLEvent tapDDLEvent = new TapDDLUnknownEvent();
                MysqlStreamEvent mysqlStreamEvent = new MysqlStreamEvent(tapDDLEvent, mysqlStreamOffset);
                tapDDLEvent.setTime(System.currentTimeMillis());
                tapDDLEvent.setReferenceTime(eventTime);
                tapDDLEvent.setOriginDDL(ddlStr);
                mysqlStreamEvents.add(mysqlStreamEvent);
//                throw new RuntimeException("Handle ddl failed: " + ddlStr + ", error: " + e.getMessage(), e);
            }
        }
        printDDLEventLog(mysqlStreamEvents);
        return mysqlStreamEvents;
    }

    private void printDDLEventLog(List<MysqlStreamEvent> mysqlStreamEvents) {
        if (CollectionUtils.isEmpty(mysqlStreamEvents)) {
            return;
        }
        for (MysqlStreamEvent mysqlStreamEvent : mysqlStreamEvents) {
            if (null == mysqlStreamEvent || null == mysqlStreamEvent.getTapEvent()) {
                continue;
            }
            TapEvent tapEvent = mysqlStreamEvent.getTapEvent();
            if (!(tapEvent instanceof TapDDLEvent)) {
                continue;
            }
            tapLogger.info("DDL event  - Table: " + ((TapDDLEvent) tapEvent).getTableId()
                    + "\n  - Event type: " + tapEvent.getClass().getSimpleName()
                    + "\n  - Offset: " + mysqlStreamEvent.getMysqlStreamOffset());
        }
    }

    protected Map<String, Object> struct2Map(Struct struct, String table) {
        if (null == struct) return null;
        Map<String, Object> result = new HashMap<>();
        Schema schema = struct.schema();
        if (null == schema) return null;
        for (Field field : schema.fields()) {
            String fieldName = field.name();
            Object value = struct.getWithoutDefault(fieldName);
            if (null != field.schema().name() && field.schema().name().startsWith("io.debezium.time.")) {
                if (field.schema().type() == Schema.Type.INT64 && value instanceof Long && Long.MIN_VALUE == ((Long) value)) {
                    result.put(fieldName, null);
                    continue;
                } else if (field.schema().type() == Schema.Type.INT32 && value instanceof Integer && Integer.MIN_VALUE == ((Integer) value)) {
                    result.put(fieldName, null);
                    continue;
                } else if (field.schema().name().equals("illegalDate")) {
                    result.put(fieldName, value);
                    continue;
                }
            }
            if (value instanceof ByteBuffer) {
                value = ((ByteBuffer) value).array();
            }
            if (mysqlConfig.getOldVersionTimezone()) {
                value = handleDatetimeWithOldVersion(table, fieldName, value);
            } else {
                value = handleDatetime(table, fieldName, value);
            }
            result.put(fieldName, value);
        }
        return result;
    }

    protected Object handleDatetime(String table, String fieldName, Object value) {
        TapTable tapTable = tapTableMap.get(table);
        if (null == tapTable) return value;
        if (null == tapTable.getNameFieldMap()) {
            return value;
        }
        TapField tapField = tapTable.getNameFieldMap().get(fieldName);
        if (null == tapField) return value;
        TapType tapType = tapField.getTapType();
        if (tapType instanceof TapDateTime) {
            int fraction = ((TapDateTime) tapType).getFraction();
            if (value instanceof Long) {
                if (fraction > 3) {
                    value = ((Long) value + diff * 1000) / (long) Math.pow(10, 6 - fraction);
                } else {
                    value = ((Long) value + diff) / (long) Math.pow(10, 3 - fraction);
                }
            } else if (value instanceof String) {
                if (StringUtils.isNotBlank((CharSequence) value)) {
                    value = Instant.parse((CharSequence) value).atZone(dbTimeZone.toZoneId()).toLocalDateTime().atZone(ZoneOffset.UTC);
                }
            }
        } else if (tapType instanceof TapDate && (value instanceof Integer)) {
            value = ((Integer) value).longValue() * 24 * 60 * 60 * 1000L;
        }
        return value;
    }

    private Object handleDatetimeWithOldVersion(String table, String fieldName, Object value) {
        TapTable tapTable = tapTableMap.get(table);
        if (null == tapTable) return value;
        if (null == tapTable.getNameFieldMap()) {
            return value;
        }
        TapField tapField = tapTable.getNameFieldMap().get(fieldName);
        if (null == tapField) return value;
        TapType tapType = tapField.getTapType();
        LocalDateTime dt = LocalDateTime.now();
        ZonedDateTime fromZonedDateTime = dt.atZone(TimeZone.getDefault().toZoneId());
        ZonedDateTime toZonedDateTime = dt.atZone(TimeZone.getTimeZone("GMT").toZoneId());
        long diff = Duration.between(toZonedDateTime, fromZonedDateTime).toMillis();
        if (tapType instanceof TapDateTime) {
            int fraction = ((TapDateTime) tapType).getFraction();
            if (value instanceof Long) {
                if (fraction > 3) {
                    value = ((Long) value + diff * 1000) / (long) Math.pow(10, 6 - fraction);
                } else {
                    value = ((Long) value + diff) / (long) Math.pow(10, 3 - fraction);
                }
            } else if (value instanceof String) {
                try {
                    Instant instant = Instant.parse((CharSequence) value);
                    long milliOffset = timeZone.getRawOffset() + diff;
                    value = instant.getEpochSecond() * (long) Math.pow(10, fraction) + instant.getNano() / (long) Math.pow(10, 9 - fraction) + (long) (milliOffset * Math.pow(10, fraction - 3));
                } catch (Exception ignored) {
                }
            }
        } else if (tapType instanceof TapDate && (value instanceof Integer)) {
            value = ((Integer) value).longValue() * 24 * 60 * 60 * 1000L + diff;
        }
        return value;
    }

    private MysqlStreamOffset getMysqlStreamOffset(SourceRecord record) {
        MysqlStreamOffset mysqlStreamOffset = new MysqlStreamOffset();
        Map<String, Object> partition = (Map<String, Object>) record.sourcePartition();
        Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();
        // Offsets are specified as schemaless to the converter, using whatever internal schema is appropriate
        // for that data. The only enforcement of the format is here.
        OffsetUtils.validateFormat(partition);
        OffsetUtils.validateFormat(offset);
        // When serializing the key, we add in the namespace information so the key is [namespace, real key]
        Map<String, String> offsetMap = new HashMap<>(1);
        String key = InstanceFactory.instance(JsonParser.class).toJson(partition);
        String value = InstanceFactory.instance(JsonParser.class).toJson(offset);
        offsetMap.put(key, value);
        mysqlStreamOffset.setOffset(offsetMap);
        mysqlStreamOffset.setName(serverName);
        return mysqlStreamOffset;
    }

    private void eventQueueConsumer() {
        while (isAlive.get()) {
            MysqlStreamEvent mysqlStreamEvent;
            try {
                mysqlStreamEvent = eventQueue.poll(3L, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                break;
            }
            if (null == mysqlStreamEvent) continue;
            ArrayList<TapEvent> events = new ArrayList<>(1);
            events.add(mysqlStreamEvent.getTapEvent());
            streamReadConsumer.accept(events, mysqlStreamEvent.getMysqlStreamOffset());
        }
    }

    private void enqueue(MysqlStreamEvent mysqlStreamEvent) {
        while (isAlive.get()) {
            try {
                if (eventQueue.offer(mysqlStreamEvent, 3L, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    enum MysqlOpType {
        INSERT("c"),
        UPDATE("u"),
        DELETE("d"),
        ;
        private String op;

        MysqlOpType(String op) {
            this.op = op;
        }

        public String getOp() {
            return op;
        }

        private static Map<String, MysqlOpType> map;

        static {
            map = new HashMap<>();
            for (MysqlOpType value : MysqlOpType.values()) {
                map.put(value.getOp(), value);
            }
        }

        public static MysqlOpType fromOp(String op) {
            return map.get(op);
        }
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


}
