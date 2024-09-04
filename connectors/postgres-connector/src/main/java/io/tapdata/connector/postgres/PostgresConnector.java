package io.tapdata.connector.postgres;

import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.postgres.bean.PostgresColumn;
import io.tapdata.connector.postgres.cdc.PostgresCdcRunner;
import io.tapdata.connector.postgres.cdc.WalLogMinerV2;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.ddl.PostgresDDLSqlGenerator;
import io.tapdata.connector.postgres.dml.PostgresRecordWriter;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapHashResult;
import io.tapdata.pdk.apis.functions.connector.source.ConnectionConfigWithTables;
import org.postgresql.geometric.*;
import org.postgresql.jdbc.PgArray;
import org.postgresql.jdbc.PgSQLXML;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * PDK for Postgresql
 *
 * @author Jarad
 * @date 2022/4/18
 */
@TapConnectorClass("spec_postgres.json")
public class PostgresConnector extends CommonDbConnector {
    protected PostgresConfig postgresConfig;
    protected PostgresJdbcContext postgresJdbcContext;
    private PostgresTest postgresTest;
    private PostgresCdcRunner cdcRunner; //only when task start-pause this variable can be shared
    private Object slotName; //must be stored in stateMap
    protected String postgresVersion;
    protected Map<String, Boolean> writtenTableMap = new ConcurrentHashMap<>();

    @Override
    public void onStart(TapConnectionContext connectorContext) {
        initConnection(connectorContext);
    }

    protected TapField makeTapField(DataMap dataMap) {
        return new PostgresColumn(dataMap).getTapField();
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(postgresConfig.getConnectionString());
        try (
                PostgresTest postgresTest = new PostgresTest(postgresConfig, consumer, connectionOptions).initContext()
        ) {
            postgresTest.testOneByOne();
            connectionOptions.setInstanceUniqueId(StringKit.md5(String.join("|"
                    , postgresConfig.getUser()
                    , postgresConfig.getHost()
                    , String.valueOf(postgresConfig.getPort())
                    , postgresConfig.getDatabase()
                    , postgresConfig.getLogPluginName()
            )));
            connectionOptions.setNamespaces(Collections.singletonList(postgresConfig.getSchema()));
            return connectionOptions;
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //test
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        //need to clear resource outer
        connectorFunctions.supportReleaseExternalFunction(this::onDestroy);
        // target
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
//        connectorFunctions.supportQueryIndexes(this::queryIndexes);
//        connectorFunctions.supportDeleteIndex(this::dropIndexes);
        // source
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadWithoutOffset);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        // query
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);
        // ddl
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> postgresJdbcContext.getConnection(), this::isAlive, c));
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);
        connectorFunctions.supportCountRawCommandFunction(this::countRawCommand);
        connectorFunctions.supportCountByPartitionFilterFunction(this::countByAdvanceFilter);

        codecRegistry.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
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
            if (postgresConfig.getOldVersionTimezone()) {
                return tapTimeValue.getValue().toTime();
            } else if (EmptyKit.isNotNull(tapTimeValue.getValue().getTimeZone())) {
                return tapTimeValue.getValue().toInstant().atZone(ZoneId.systemDefault()).toLocalTime();
            } else {
                return tapTimeValue.getValue().toInstant().atZone(postgresConfig.getZoneId()).toLocalTime();
            }
        });
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            if (postgresConfig.getOldVersionTimezone() || EmptyKit.isNotNull(tapDateTimeValue.getValue().getTimeZone())) {
                return tapDateTimeValue.getValue().toTimestamp();
            } else {
                return tapDateTimeValue.getValue().toInstant().atZone(postgresConfig.getZoneId()).toLocalDateTime();
            }
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
        codecRegistry.registerFromTapValue(TapYearValue.class, "character(4)", TapValue::getOriginValue);
        connectorFunctions.supportGetTableInfoFunction(this::getTableInfo);
        connectorFunctions.supportTransactionBeginFunction(this::beginTransaction);
        connectorFunctions.supportTransactionCommitFunction(this::commitTransaction);
        connectorFunctions.supportTransactionRollbackFunction(this::rollbackTransaction);
        connectorFunctions.supportQueryHashByAdvanceFilterFunction(this::queryTableHash);
        connectorFunctions.supportStreamReadMultiConnectionFunction(this::streamReadMultiConnection);

    }

    //clear resource outer and jdbc context
    private void onDestroy(TapConnectorContext connectorContext) throws Throwable {
        try {
            onStart(connectorContext);
            if (EmptyKit.isNotNull(cdcRunner)) {
                cdcRunner.closeCdcRunner();
                cdcRunner = null;
            }
            if (EmptyKit.isNotNull(slotName)) {
                clearSlot();
            }
        } finally {
            onStop(connectorContext);
        }
    }

    //clear postgres slot
    private void clearSlot() throws Throwable {
        postgresJdbcContext.queryWithNext("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name='" + slotName + "' AND active='false'", resultSet -> {
            if (resultSet.getInt(1) > 0) {
                postgresJdbcContext.execute("SELECT pg_drop_replication_slot('" + slotName + "')");
            }
        });
    }

    private void buildSlot(TapConnectorContext connectorContext, Boolean needCheck) throws Throwable {
        if (EmptyKit.isNull(slotName)) {
            slotName = "tapdata_cdc_" + UUID.randomUUID().toString().replaceAll("-", "_");
            postgresJdbcContext.execute("SELECT pg_create_logical_replication_slot('" + slotName + "','" + postgresConfig.getLogPluginName() + "')");
            tapLogger.info("new logical replication slot created, slotName:{}", slotName);
            connectorContext.getStateMap().put("tapdata_pg_slot", slotName);
        } else if (needCheck) {
            AtomicBoolean existSlot = new AtomicBoolean(true);
            postgresJdbcContext.queryWithNext("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name='" + slotName + "'", resultSet -> {
                if (resultSet.getInt(1) <= 0) {
                    existSlot.set(false);
                }
            });
            if (existSlot.get()) {
                tapLogger.info("Using an existing logical replication slot, slotName:{}", slotName);
            } else {
                tapLogger.warn("The previous logical replication slot no longer exists. Although it has been rebuilt, there is a possibility of data loss. Please check");
            }
        }
    }

    private static final String PG_REPLICATE_IDENTITY = "select relname, relreplident from pg_class\n" +
            "where relnamespace=(select oid from pg_namespace where nspname='%s') and relname in (%s)";

    private void testReplicateIdentity(KVReadOnlyMap<TapTable> tableMap) {
        if ("pgoutput".equals(postgresConfig.getLogPluginName())) {
            tapLogger.warn("The pgoutput plugin may cause before of data loss, if you need, please use another plugin instead, such as wal2json");
            return;
        }
        if (EmptyKit.isNull(tableMap)) {
            return;
        }
        List<String> tableList = new ArrayList<>();
        List<String> hasPrimary = new ArrayList<>();
        Iterator<Entry<TapTable>> iterator = tableMap.iterator();
        while (iterator.hasNext()) {
            Entry<TapTable> entry = iterator.next();
            tableList.add(entry.getKey());
            if (EmptyKit.isNotEmpty(entry.getValue().primaryKeys())) {
                hasPrimary.add(entry.getKey());
            }
        }
        List<String> noPrimaryOrFull = new ArrayList<>(); //无主键表且identity不为full
        List<String> primaryNotDefaultOrFull = new ArrayList<>(); //有主键表但identity不为full也不为default
        try {
            postgresJdbcContext.query(String.format(PG_REPLICATE_IDENTITY, postgresConfig.getSchema(), StringKit.joinString(tableList, "'", ",")), resultSet -> {
                while (resultSet.next()) {
                    if (!hasPrimary.contains(resultSet.getString("relname")) && !"f".equals(resultSet.getString("relreplident"))) {
                        noPrimaryOrFull.add(resultSet.getString("relname"));
                    }
                    if (hasPrimary.contains(resultSet.getString("relname")) && !"f".equals(resultSet.getString("relreplident")) && !"d".equals(resultSet.getString("relreplident"))) {
                        primaryNotDefaultOrFull.add(resultSet.getString("relname"));
                    }
                }
            });
        } catch (Exception e) {
            return;
        }
        if (EmptyKit.isNotEmpty(noPrimaryOrFull)) {
            tapLogger.warn("The following tables do not have a primary key and the identity is not full, which may cause before of data loss: {}", String.join(",", noPrimaryOrFull));
        }
        if (EmptyKit.isNotEmpty(primaryNotDefaultOrFull)) {
            tapLogger.warn("The following tables have a primary key, but the identity is not full or default, which may cause before of data loss: {}", String.join(",", primaryNotDefaultOrFull));
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        ErrorKit.ignoreAnyError(() -> {
            if (EmptyKit.isNotNull(cdcRunner)) {
                cdcRunner.closeCdcRunner();
            }
        });
        EmptyKit.closeQuietly(postgresTest);
        EmptyKit.closeQuietly(postgresJdbcContext);
    }

    //initialize jdbc context, slot name, version
    private void initConnection(TapConnectionContext connectionContext) {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectionContext.getConnectionConfig());
        postgresTest = new PostgresTest(postgresConfig, testItem -> {
        }, null).initContext();
        postgresJdbcContext = new PostgresJdbcContext(postgresConfig);
        commonDbConfig = postgresConfig;
        jdbcContext = postgresJdbcContext;
        isConnectorStarted(connectionContext, tapConnectorContext -> {
            slotName = tapConnectorContext.getStateMap().get("tapdata_pg_slot");
            postgresConfig.load(tapConnectorContext.getNodeConfig());
        });
        commonSqlMaker = new PostgresSqlMaker().closeNotNull(postgresConfig.getCloseNotNull());
        postgresVersion = postgresJdbcContext.queryVersion();
        ddlSqlGenerator = new PostgresDDLSqlGenerator();
        tapLogger = connectionContext.getLog();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
        exceptionCollector = new PostgresExceptionCollector();
    }

    private void openIdentity(TapTable tapTable) throws SQLException {
        if (EmptyKit.isEmpty(tapTable.primaryKeys())
                && (EmptyKit.isEmpty(tapTable.getIndexList()) || tapTable.getIndexList().stream().noneMatch(TapIndex::isUnique))) {
            jdbcContext.execute("ALTER TABLE \"" + jdbcContext.getConfig().getSchema() + "\".\"" + tapTable.getId() + "\" REPLICA IDENTITY FULL");
        }
    }

    protected boolean makeSureHasUnique(TapTable tapTable) throws SQLException {
        return jdbcContext.queryAllIndexes(Collections.singletonList(tapTable.getId())).stream().anyMatch(v -> "1".equals(v.getString("isUnique")));
    }

    //write records as all events, prepared
    protected void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        boolean hasUniqueIndex;
        if (EmptyKit.isNull(writtenTableMap.get(tapTable.getId()))) {
            openIdentity(tapTable);
            hasUniqueIndex = makeSureHasUnique(tapTable);
            writtenTableMap.put(tapTable.getId(), hasUniqueIndex);
        } else {
            hasUniqueIndex = writtenTableMap.get(tapTable.getId());
        }
        String insertDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        if (isTransaction) {
            String threadName = Thread.currentThread().getName();
            Connection connection;
            if (transactionConnectionMap.containsKey(threadName)) {
                connection = transactionConnectionMap.get(threadName);
            } else {
                connection = postgresJdbcContext.getConnection();
                transactionConnectionMap.put(threadName, connection);
            }
            new PostgresRecordWriter(postgresJdbcContext, connection, tapTable, hasUniqueIndex ? postgresVersion : "90500")
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setTapLogger(tapLogger)
                    .write(tapRecordEvents, writeListResultConsumer, this::isAlive);

        } else {
            new PostgresRecordWriter(postgresJdbcContext, tapTable, hasUniqueIndex ? postgresVersion : "90500")
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setTapLogger(tapLogger)
                    .write(tapRecordEvents, writeListResultConsumer, this::isAlive);
        }
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        if ("walminer".equals(postgresConfig.getLogPluginName())) {
            new WalLogMinerV2(postgresJdbcContext, tapLogger)
                    .watch(tableList, nodeContext.getTableMap())
                    .offset(offsetState)
                    .registerConsumer(consumer, recordSize)
                    .startMiner(this::isAlive);
        } else {
            cdcRunner = new PostgresCdcRunner(postgresJdbcContext);
            testReplicateIdentity(nodeContext.getTableMap());
            buildSlot(nodeContext, true);
            cdcRunner.useSlot(slotName.toString()).watch(tableList).offset(offsetState).registerConsumer(consumer, recordSize);
            cdcRunner.startCdcRunner();
            if (EmptyKit.isNotNull(cdcRunner) && EmptyKit.isNotNull(cdcRunner.getThrowable().get())) {
                Throwable throwable = ErrorKit.getLastCause(cdcRunner.getThrowable().get());
                if (throwable instanceof SQLException) {
                    exceptionCollector.collectTerminateByServer(throwable);
                    exceptionCollector.collectCdcConfigInvalid(throwable);
                    exceptionCollector.revealException(throwable);
                }
                throw throwable;
            }
        }
    }

    private void streamReadMultiConnection(TapConnectorContext nodeContext, List<ConnectionConfigWithTables> connectionConfigWithTables, Object offsetState, int batchSize, StreamReadConsumer consumer) throws Throwable {
        Map<String, List<String>> schemaTableMap = new HashMap<>();
        for (ConnectionConfigWithTables withTables : connectionConfigWithTables) {
            if (null == withTables.getConnectionConfig())
                throw new RuntimeException("Not found connection config");
            if (null == withTables.getConnectionConfig().get("schema"))
                throw new RuntimeException("Not found connection schema");
            if (null == withTables.getTables())
                throw new RuntimeException("Not found connection tables");

            schemaTableMap.compute(String.valueOf(withTables.getConnectionConfig().get("schema")), (schema, tableList) -> {
                if (null == tableList) {
                    tableList = new ArrayList<>();
                }

                for (String tableName : withTables.getTables()) {
                    if (!tableList.contains(tableName)) {
                        tableList.add(tableName);
                    }
                }
                return tableList;
            });
        }
        if ("walminer".equals(postgresConfig.getLogPluginName())) {
            new WalLogMinerV2(postgresJdbcContext, tapLogger)
                    .watch(schemaTableMap, nodeContext.getTableMap())
                    .offset(offsetState)
                    .registerConsumer(consumer, batchSize)
                    .startMiner(this::isAlive);
        } else {
            cdcRunner = new PostgresCdcRunner(postgresJdbcContext);
            testReplicateIdentity(nodeContext.getTableMap());
            buildSlot(nodeContext, true);
            cdcRunner.useSlot(slotName.toString()).watch(schemaTableMap).offset(offsetState).registerConsumer(consumer, batchSize);
            cdcRunner.startCdcRunner();
            if (EmptyKit.isNotNull(cdcRunner) && EmptyKit.isNotNull(cdcRunner.getThrowable().get())) {
                Throwable throwable = ErrorKit.getLastCause(cdcRunner.getThrowable().get());
                if (throwable instanceof SQLException) {
                    exceptionCollector.collectTerminateByServer(throwable);
                    exceptionCollector.collectCdcConfigInvalid(throwable);
                    exceptionCollector.revealException(throwable);
                }
                throw throwable;
            }
        }
    }

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) throws Throwable {
        if ("walminer".equals(postgresConfig.getLogPluginName())) {
            String timestamp = timestampToWalLsnV2(offsetStartTime);
            tapLogger.info("timestampToStreamOffset start at {}", timestamp);
            return timestamp;
        }
        if (EmptyKit.isNotNull(offsetStartTime)) {
            tapLogger.warn("Postgres specified time start increment is not supported except walminer, use the current time as the start increment");
        }
        //test streamRead log plugin
        boolean canCdc = Boolean.TRUE.equals(postgresTest.testStreamRead());
        if (canCdc) {
            if ("pgoutput".equals(postgresConfig.getLogPluginName()) && postgresVersion.compareTo("100000") > 0) {
                createPublicationIfNotExist();
            }
            testReplicateIdentity(connectorContext.getTableMap());
            buildSlot(connectorContext, false);
        }
        return new PostgresOffset();
    }

    private String getTimestampOffset(Long offsetStartTime) throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        AtomicReference<Timestamp> timestamp = new AtomicReference<>();
        if (EmptyKit.isNull(offsetStartTime)) {
            postgresJdbcContext.queryWithNext("SELECT clock_timestamp()", resultSet -> timestamp.set(resultSet.getTimestamp(1)));
            return sdf.format(timestamp.get());
        } else {
            return sdf.format(offsetStartTime);
        }
    }

    private String timestampToWalLsnV2(Long offsetStartTime) throws SQLException {
        String walDirectory = getWalDirectory();
        AtomicReference<String> lsn = new AtomicReference<>();
        if (EmptyKit.isNull(offsetStartTime)) {
            postgresJdbcContext.queryWithNext("SELECT pg_current_wal_lsn()", resultSet -> lsn.set(resultSet.getString(1)));
        } else {
            postgresJdbcContext.prepareQuery("SELECT * FROM pg_ls_waldir() where modification>? order by modification", Collections.singletonList(new Timestamp(offsetStartTime)), resultSet -> {
                if (resultSet.next()) {
                    lsn.set(resultSet.getString(1));
                }
            });
            try (
                    Connection connection = jdbcContext.getConnection();
                    Statement statement = connection.createStatement();
                    PreparedStatement preparedStatement = connection.prepareStatement("select * from walminer_contents where timestamp >= ? order by start_lsn limit 1")
            ) {
                statement.execute(String.format("select walminer_wal_add('%s')", walDirectory + lsn.get()));
                statement.execute("select walminer_all()");
                preparedStatement.setObject(1, new Timestamp(offsetStartTime));
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        lsn.set(resultSet.getString("start_lsn"));
                    }
                }
                statement.execute("select walminer_stop()");
            }
        }
        return lsn.get() + "," + lsn.get() + ",0";
    }

    private String timestampToWalLsn(Long offsetStartTime) throws SQLException {
        String walDirectory = getWalDirectory();
        AtomicReference<String> fileAndLsn = new AtomicReference<>();
        if (EmptyKit.isNull(offsetStartTime)) {
            postgresJdbcContext.queryWithNext("SELECT pg_current_wal_lsn()", resultSet -> fileAndLsn.set(resultSet.getString(1)));
            postgresJdbcContext.queryWithNext(String.format("select pg_walfile_name('%s')", fileAndLsn.get()), resultSet -> {
                fileAndLsn.set(resultSet.getString(1) + "," + fileAndLsn.get());
            });
        } else {
            postgresJdbcContext.prepareQuery("SELECT * FROM pg_ls_waldir() where modification>? order by modification", Collections.singletonList(new Timestamp(offsetStartTime)), resultSet -> {
                if (resultSet.next()) {
                    fileAndLsn.set(resultSet.getString(1));
                }
            });
            try (
                    Connection connection = jdbcContext.getConnection();
                    Statement statement = connection.createStatement();
                    PreparedStatement preparedStatement = connection.prepareStatement("select * from walminer_contents where timestamp >= ? order by start_lsn limit 1")
            ) {
                statement.execute(String.format("select walminer_wal_add('%s')", walDirectory + fileAndLsn.get()));
                statement.execute("select walminer_all()");
                preparedStatement.setObject(1, new Timestamp(offsetStartTime));
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        fileAndLsn.set(fileAndLsn.get() + "," + resultSet.getString("start_lsn"));
                    }
                }
                statement.execute("select walminer_stop()");
            }
        }
        return walDirectory + fileAndLsn.get();
    }

    protected String getWalDirectory() throws SQLException {
        AtomicReference<String> walDirectory = new AtomicReference<>();
        postgresJdbcContext.query("SELECT name, setting FROM pg_settings WHERE name = 'wal_dir' or name = 'data_directory'", resultSet -> {
            while (resultSet.next()) {
                if ("wal_dir".equals(resultSet.getString(1)) && EmptyKit.isNotEmpty(resultSet.getString(2))) {
                    walDirectory.set(resultSet.getString(2) + "/");
                    break;
                }
                if ("data_directory".equals(resultSet.getString(1)) && EmptyKit.isNotEmpty(resultSet.getString(2))) {
                    walDirectory.set(resultSet.getString(2) + (postgresVersion.compareTo("100000") >= 0 ? "/pg_wal/" : "/pg_xlog/"));
                }
            }
        });
        return walDirectory.get();
    }

    private void createPublicationIfNotExist() throws SQLException {
        String publicationName = postgresConfig.getPartitionRoot() ? "dbz_publication_root" : "dbz_publication";
        AtomicBoolean needCreate = new AtomicBoolean(false);
        postgresJdbcContext.queryWithNext(String.format("SELECT COUNT(1) FROM pg_publication WHERE pubname = '%s'", publicationName), resultSet -> {
            if (resultSet.getInt(1) <= 0) {
                needCreate.set(true);
            }
        });
        if (needCreate.get()) {
            postgresJdbcContext.execute(String.format("CREATE PUBLICATION %s FOR ALL TABLES %s", publicationName, postgresConfig.getPartitionRoot() ? "WITH (publish_via_partition_root = true)" : ""));
        }
    }

    protected TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        DataMap dataMap = postgresJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("size")));
        tableInfo.setStorageSize(new BigDecimal(dataMap.getString("rowcount")).longValue());
        return tableInfo;
    }


    private String buildHashSql(TapAdvanceFilter filter, TapTable table) {
        StringBuilder sql = new StringBuilder("select SUM(MOD(" +
                " (select n.md5 from (" +
                "  select case when t.num < 0 then t.num + 18446744073709551616 when t.num > 0 then t.num end as md5" +
                "  from (select (cast(");
        sql.append("CAST(( 'x' || SUBSTRING(MD5(CONCAT_WS('', ");
        LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
        java.util.Iterator<Map.Entry<String, TapField>> entryIterator = nameFieldMap.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, TapField> next = entryIterator.next();
            String fieldName = next.getKey();
            TapField field = nameFieldMap.get(next.getKey());
            byte type = next.getValue().getTapType().getType();
            if (type == TapType.TYPE_NUMBER && (field.getDataType().toLowerCase().contains("real") ||
                    field.getDataType().toLowerCase().contains("double") ||
                    field.getDataType().toLowerCase().contains("numeric") ||
                    field.getDataType().toLowerCase().contains("float"))) {
                sql.append(String.format("trunc(\"%s\")", fieldName)).append(",");
                continue;
            }

            if (type == TapType.TYPE_STRING && field.getDataType().toLowerCase().contains("character(")) {
                sql.append(String.format("TRIM( \"%s\" )", fieldName)).append(",");
                continue;
            }

            if (type == TapType.TYPE_BOOLEAN && field.getDataType().toLowerCase().contains("boolean")) {
                sql.append(String.format("CAST( \"%s\" as int )", fieldName)).append(",");
                continue;
            }

            if (type == TapType.TYPE_TIME && field.getDataType().toLowerCase().contains("with time zone")) {
                sql.append(String.format("SUBSTRING(cast(\"%s\" as varchar) FROM 1 FOR 8)", fieldName)).append(",");
                continue;
            }

            switch (type) {
                case TapType.TYPE_DATETIME:
                    sql.append(String.format("EXTRACT(epoch FROM CAST(date_trunc('second',\"%s\" ) AS TIMESTAMP))", fieldName)).append(",");
                    break;
                case TapType.TYPE_BINARY:
                    break;
                default:
                    sql.append(String.format("\"%s\"", fieldName)).append(",");
                    break;
            }
        }
        sql = new StringBuilder(sql.substring(0, sql.length() - 1));
        sql.append(" )) FROM 1 FOR 16)) AS bit(64)) as BIGINT)) AS num " +
                "  FROM ").append("\"" + table.getName() + "\"  ");
        sql.append(commonSqlMaker.buildCommandWhereSql(filter, ""));
        sql.append(") t) n),64))");
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

    @Override
    protected void processDataMap(DataMap dataMap, TapTable tapTable) {
        if (!postgresConfig.getOldVersionTimezone()) {
            for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof Timestamp) {
                    if (!tapTable.getNameFieldMap().containsKey(entry.getKey())) {
                        continue;
                    }
                    if (!tapTable.getNameFieldMap().get(entry.getKey()).getDataType().endsWith("with time zone")) {
                        entry.setValue(((Timestamp) value).toLocalDateTime().minusHours(postgresConfig.getZoneOffsetHour()));
                    } else {
                        entry.setValue(((Timestamp) value).toLocalDateTime().minusHours(TimeZone.getDefault().getRawOffset() / 3600000).atZone(ZoneOffset.UTC));
                    }
                } else if (value instanceof Date) {
                    entry.setValue(Instant.ofEpochMilli(((Date) value).getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime());
                } else if (value instanceof Time) {
                    if (!tapTable.getNameFieldMap().containsKey(entry.getKey())) {
                        continue;
                    }
                    if (!tapTable.getNameFieldMap().get(entry.getKey()).getDataType().endsWith("with time zone")) {
                        entry.setValue(Instant.ofEpochMilli(((Time) value).getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime().minusHours(postgresConfig.getZoneOffsetHour()));
                    } else {
                        entry.setValue(Instant.ofEpochMilli(((Time) value).getTime()).atZone(ZoneOffset.UTC));
                    }
                }
            }
        }
    }

    @Override
    protected String getHashSplitStringSql(TapTable tapTable) {
        Collection<String> pks = tapTable.primaryKeys();
        if (pks.isEmpty()) throw new CoreException("No primary keys found for table: " + tapTable.getName());

        return "abs(('x' || MD5(CONCAT_WS(',', \"" + String.join("\", \"", pks) + "\")))::bit(64)::bigint)";
    }
}
