package io.tapdata.connector.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.postgres.bean.PostgresColumn;
import io.tapdata.connector.postgres.cdc.PostgresCdcRunner;
import io.tapdata.connector.postgres.cdc.WalLogMinerV2;
import io.tapdata.connector.postgres.cdc.WalPgtoMiner;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.ddl.PostgresDDLSqlGenerator;
import io.tapdata.connector.postgres.dml.PostgresRecordWriter;
import io.tapdata.connector.postgres.dml.PostgresWriteRecorder;
import io.tapdata.connector.postgres.error.PostgresErrorCode;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.connector.postgres.partition.PostgresPartitionContext;
import io.tapdata.connector.postgres.partition.TableType;
import io.tapdata.entity.TapConstraintException;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.constraint.TapCreateConstraintEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapConstraint;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapType;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.exception.TapCodeException;
import io.tapdata.kit.*;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapHashResult;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapPartitionResult;
import io.tapdata.pdk.apis.functions.connector.source.ConnectionConfigWithTables;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.geometric.*;
import org.postgresql.jdbc.PgArray;
import org.postgresql.jdbc.PgSQLXML;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
    protected PostgresPartitionContext postgresPartitionContext;

    @Override
    public void onStart(TapConnectionContext connectorContext) {
        initConnection(connectorContext);
    }

    protected TapField makeTapField(DataMap dataMap) {
        return new PostgresColumn(dataMap).getTapField();
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws SQLException {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(postgresConfig.getConnectionString());
        try (
                PostgresTest postgresTest = new PostgresTest(postgresConfig, consumer, connectionOptions)
                        .initContext()
                        .withPostgresVersion()
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
        connectorFunctions.supportAfterInitialSync(this::afterInitialSync);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportQueryIndexes(this::queryIndexes);
        connectorFunctions.supportDeleteIndex(this::dropIndexes);
        connectorFunctions.supportQueryConstraints(this::queryConstraint);
        connectorFunctions.supportCreateConstraint(this::createConstraint);
        connectorFunctions.supportDropConstraint(this::dropConstraint);
        connectorFunctions.supportFlushOffsetFunction(this::flushOffset);
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
        connectorFunctions.supportExecuteCommandV2Function(this::executeCommandV2);
        connectorFunctions.supportRunRawCommandFunction(this::runRawCommand);
        connectorFunctions.supportCountRawCommandFunction(this::countRawCommand);
        connectorFunctions.supportCountByPartitionFilterFunction(this::countByAdvanceFilter);
        connectorFunctions.supportGetStreamOffsetFunction(this::getStreamOffsetFromString);

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
        connectorFunctions.supportQueryPartitionTablesByParentName(this::discoverPartitionInfoByParentName);
        connectorFunctions.supportStreamReadMultiConnectionFunction(this::streamReadMultiConnection);
        connectorFunctions.supportExportEventSqlFunction(this::exportEventSql);

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
            if ("walminer".equals(postgresConfig.getLogPluginName())) {
                if (EmptyKit.isNotEmpty(postgresConfig.getPgtoHost())) {
                    //取消订阅
                    HttpKit.sendHttp09Request(postgresConfig.getPgtoHost(), postgresConfig.getPgtoPort(), String.format("DELSUB:%s all", firstConnectorId));
                }
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
            String sql = "SELECT pg_create_logical_replication_slot('" + slotName + "','" + postgresConfig.getLogPluginName() + "')";
            try {
                postgresJdbcContext.execute(sql);
            } catch (SQLException e) {
                throw new TapCodeException(PostgresErrorCode.SELECT_PUBLICATION_FAILED, "Select publication failed. Error message: " + e.getMessage()).dynamicDescriptionParameters(sql);
            }
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

    private static final String PG_REPLICATE_IDENTITY = "select relname, relreplident from pg_class " +
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

    private Object getStreamOffsetFromString(TapConnectorContext connectorContext, String offset) {
        try {
            PostgresOffset postgresOffset = new PostgresOffset();
            postgresOffset.setSourceOffset(offset);
            return postgresOffset;
        } catch (Exception e) {
            throw new RuntimeException("Oracle use scn as offset, invalid scn: " + offset, e);
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
            firstConnectorId = (String) tapConnectorContext.getStateMap().get("firstConnectorId");
            if (EmptyKit.isNull(firstConnectorId)) {
                firstConnectorId = UUID.randomUUID().toString().replace("-", "");
                tapConnectorContext.getStateMap().put("firstConnectorId", firstConnectorId);
            }
            slotName = tapConnectorContext.getStateMap().get("tapdata_pg_slot");
            postgresConfig.load(tapConnectorContext.getNodeConfig());
            if (EmptyKit.isNull(slotName) && StringUtils.isNotBlank(postgresConfig.getCustomSlotName())) {
                slotName = postgresConfig.getCustomSlotName();
            }
        });
        postgresVersion = postgresJdbcContext.queryVersion();
        commonSqlMaker = new PostgresSqlMaker()
                .dbVersion(postgresVersion)
                .schema(postgresConfig.getSchema())
                .closeNotNull(postgresConfig.getCloseNotNull())
                .autoIncCacheValue(postgresConfig.getAutoIncCacheValue());
        if (Boolean.TRUE.equals(postgresConfig.getCreateAutoInc()) && Integer.parseInt(postgresVersion) > 100000) {
            commonSqlMaker.createAutoInc(true);
        }
        if (Boolean.TRUE.equals(postgresConfig.getApplyDefault())) {
            commonSqlMaker.applyDefault(true);
        }
        postgresJdbcContext.withPostgresVersion(postgresVersion);
        postgresTest.withPostgresVersion(postgresVersion);
        ddlSqlGenerator = new PostgresDDLSqlGenerator();
        tapLogger = connectionContext.getLog();
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
        exceptionCollector = new PostgresExceptionCollector();
        postgresPartitionContext = new PostgresPartitionContext(tapLogger)
                .withJdbcContext(jdbcContext)
                .withPostgresVersion(postgresVersion)
                .withPostgresConfig(postgresConfig);
    }

    protected void openIdentity(TapTable tapTable) {
        if (EmptyKit.isEmpty(tapTable.primaryKeys())) {
            try {
                jdbcContext.execute("ALTER TABLE \"" + jdbcContext.getConfig().getSchema() + "\".\"" + tapTable.getId() + "\" REPLICA IDENTITY FULL");
            } catch (Exception e) {
                tapLogger.warn("Failed to open identity for table " + tapTable, e);
            }
        }
    }

    protected boolean makeSureHasUnique(TapTable tapTable) throws SQLException {
        return findIndexes(tapTable).stream().anyMatch(v -> "1".equals(v.getString("isUnique")));
    }

    protected List<DataMap> findIndexes(TapTable tapTable) throws SQLException {
        return jdbcContext.queryAllIndexes(Collections.singletonList(tapTable.getId()));
    }

    protected CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws SQLException {
        if (Boolean.TRUE.equals(postgresConfig.getCreateAutoInc()) && Integer.parseInt(postgresVersion) > 100000) {
            createTableEvent.getTable().getNameFieldMap().entrySet().stream().filter(entry -> EmptyKit.isNotBlank(entry.getValue().getSequenceName())).forEach(entry -> {
                StringBuilder sequenceSql = new StringBuilder("CREATE SEQUENCE IF NOT EXISTS " + getSchemaAndTable(entry.getValue().getSequenceName()));
                if (EmptyKit.isNotNull(entry.getValue().getAutoIncStartValue())) {
                    sequenceSql.append(" START ").append(entry.getValue().getAutoIncStartValue());
                }
                if (EmptyKit.isNotNull(entry.getValue().getAutoIncrementValue())) {
                    sequenceSql.append(" INCREMENT ").append(entry.getValue().getAutoIncrementValue());
                }
                try {
                    postgresJdbcContext.execute(sequenceSql.toString());
                } catch (SQLException e) {
                    tapLogger.warn("Failed to create sequence for table {} field {}", createTableEvent.getTable().getId(), entry.getKey(), e);
                }
            });
        }
        CreateTableOptions options = super.createTableV2(connectorContext, createTableEvent);
        if (EmptyKit.isNotBlank(postgresConfig.getTableOwner())) {
            jdbcContext.execute(String.format("alter table %s owner to %s", getSchemaAndTable(createTableEvent.getTableId()), postgresConfig.getTableOwner()));
        }
        return options;
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
        if (postgresConfig.getCreateAutoInc() && Integer.parseInt(postgresVersion) > 100000) {
            if (!writtenTableMap.get(tapTable.getId()).containsKey(HAS_AUTO_INCR)) {
                List<String> autoIncFields = tapTable.getNameFieldMap().values().stream().filter(TapField::getAutoInc).map(TapField::getName).collect(Collectors.toList());
                writtenTableMap.get(tapTable.getId()).put(HAS_AUTO_INCR, autoIncFields);
            }
        }
    }

    //write records as all events, prepared
    protected void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
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
                connection = postgresJdbcContext.getConnection();
                transactionConnectionMap.put(threadName, connection);
            }
            postgresRecordWriter = new PostgresRecordWriter(postgresJdbcContext, connection, tapTable, (hasUniqueIndex && !hasMultiUniqueIndex) ? postgresVersion : "90500")
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setDeletePolicy(deleteDmlPolicy)
                    .setTapLogger(tapLogger);
        } else {
            postgresRecordWriter = new PostgresRecordWriter(postgresJdbcContext, tapTable, (hasUniqueIndex && !hasMultiUniqueIndex) ? postgresVersion : "90500")
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setDeletePolicy(deleteDmlPolicy)
                    .setTapLogger(tapLogger);
        }
        if (Boolean.TRUE.equals(postgresConfig.getAllowReplication())) {
            if (EmptyKit.isNull(writtenTableMap.get(tapTable.getId()).get(CANNOT_CLOSE_CONSTRAINT))) {
                boolean canClose = postgresRecordWriter.closeConstraintCheck();
                writtenTableMap.get(tapTable.getId()).put(CANNOT_CLOSE_CONSTRAINT, !canClose);
            } else if (Boolean.FALSE.equals(writtenTableMap.get(tapTable.getId()).get(CANNOT_CLOSE_CONSTRAINT))) {
                postgresRecordWriter.closeConstraintCheck();
            }
        }
        if (postgresConfig.getCreateAutoInc() && Integer.parseInt(postgresVersion) > 100000 && EmptyKit.isNotEmpty(autoIncFields)
                && "CDC".equals(tapRecordEvents.get(0).getInfo().get(TapRecordEvent.INFO_KEY_SYNC_STAGE))) {
            postgresRecordWriter.setAutoIncFields(autoIncFields);
            postgresRecordWriter.write(tapRecordEvents, writeListResultConsumer, this::isAlive);
            if (EmptyKit.isNotEmpty(postgresRecordWriter.getAutoIncMap())) {
                List<String> alterSqls = new ArrayList<>();
                postgresRecordWriter.getAutoIncMap().forEach((k, v) -> {
                    String sequenceName = tapTable.getNameFieldMap().get(k).getSequenceName();
                    if (EmptyKit.isNotBlank(sequenceName)) {
                        alterSqls.add("select setval('" + getSchemaAndTable(sequenceName) + "'," + (Long.parseLong(String.valueOf(v)) + postgresConfig.getAutoIncJumpValue()) + ", false) ");
                    } else {
                        alterSqls.add("ALTER TABLE " + getSchemaAndTable(tapTable.getId()) + " ALTER COLUMN \"" + k + "\" SET GENERATED BY DEFAULT RESTART WITH " + (Long.parseLong(String.valueOf(v)) + postgresConfig.getAutoIncJumpValue()));
                    }
                });
                jdbcContext.batchExecute(alterSqls);
            }
        } else {
            postgresRecordWriter.write(tapRecordEvents, writeListResultConsumer, this::isAlive);
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
                    String sequenceName = tapTable.getNameFieldMap().get(field).getSequenceName();
                    if (EmptyKit.isNotBlank(sequenceName)) {
                        statement.execute("select setval('" + getSchemaAndTable(sequenceName) + "'," + (resultSet.getLong(1) + postgresConfig.getAutoIncJumpValue()) + ", false) ");
                    } else {
                        statement.execute("ALTER TABLE " + getSchemaAndTable(tapTable.getId()) + " ALTER COLUMN \"" + field + "\" SET GENERATED BY DEFAULT RESTART WITH " + (resultSet.getLong(1) + postgresConfig.getAutoIncJumpValue()));
                    }
                    connection.commit();
                }
            } catch (SQLException e) {
                tapLogger.warn("Failed to get auto increment value for table {} field {}", tapTable.getId(), field, e);
            }
        });
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        if ("walminer".equals(postgresConfig.getLogPluginName())) {
            if (EmptyKit.isNotEmpty(postgresConfig.getPgtoHost())) {
                new WalPgtoMiner(postgresJdbcContext, firstConnectorId, tapLogger)
                        .watch(tableList, nodeContext.getTableMap())
                        .offset(offsetState)
                        .registerConsumer(consumer, recordSize)
                        .startMiner(this::isAlive);
            } else {
                new WalLogMinerV2(postgresJdbcContext, tapLogger)
                        .watch(tableList, nodeContext.getTableMap())
                        .withWalLogDirectory(getWalDirectory())
                        .offset(offsetState)
                        .registerConsumer(consumer, recordSize)
                        .startMiner(this::isAlive);
            }
        } else {
            cdcRunner = new PostgresCdcRunner(postgresJdbcContext, nodeContext);
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

    private void flushOffset(TapConnectorContext connectorContext, Object offset) {
        if (EmptyKit.isNotNull(cdcRunner)) {
            if (offset instanceof PostgresOffset) {
                String sourceOffset = ((PostgresOffset) offset).getSourceOffset();
                if (sourceOffset == null) {
                    return;
                }
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    Map<String, Object> lastOffset = objectMapper.readValue(sourceOffset, Map.class);
                    cdcRunner.flushOffset(lastOffset);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private void streamReadMultiConnection(TapConnectorContext nodeContext, List<ConnectionConfigWithTables> connectionConfigWithTables, Object offsetState, int batchSize, StreamReadConsumer consumer) throws Throwable {
        cdcRunner = new PostgresCdcRunner(postgresJdbcContext, nodeContext);
        testReplicateIdentity(nodeContext.getTableMap());
        buildSlot(nodeContext, true);
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
                    .withWalLogDirectory(getWalDirectory())
                    .offset(offsetState)
                    .registerConsumer(consumer, batchSize)
                    .startMiner(this::isAlive);
        } else {
            cdcRunner = new PostgresCdcRunner(postgresJdbcContext, nodeContext);
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
            if (EmptyKit.isNotBlank(postgresConfig.getPgtoHost())) {
                if (EmptyKit.isNotNull(offsetStartTime)) {
                    return new PostgresOffset();
                } else {
                    return timestampToWalLsnV2(null).split(",")[0];
                }
            }
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
            if ("pgoutput".equals(postgresConfig.getLogPluginName()) && Integer.parseInt(postgresVersion) > 100000) {
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
                    walDirectory.set(resultSet.getString(2) + (Integer.parseInt(postgresVersion) > 100000 ? "/pg_wal/" : "/pg_xlog/"));
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
            String sql = String.format("CREATE PUBLICATION %s FOR ALL TABLES %s", publicationName, postgresConfig.getPartitionRoot() ? "WITH (publish_via_partition_root = true)" : "");
            try {
                postgresJdbcContext.execute(sql);
            } catch (SQLException e) {
                throw new TapCodeException(PostgresErrorCode.CREATE_PUBLICATION_FAILED, "create publication for all tables failed. Error message: " + e.getMessage()).dynamicDescriptionParameters(sql);
            }
        }
    }

    protected TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        DataMap dataMap = postgresJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(new BigDecimal(dataMap.getString("rowcount")).longValue());
        tableInfo.setStorageSize(Long.valueOf(dataMap.getString("size")));
        return tableInfo;
    }


    private String buildHashSql(TapAdvanceFilter filter, TapTable table) {
        StringBuilder sql = new StringBuilder("select SUM(MOD(n.md5, 64)) from (" +
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
        sql.append(") t) n");
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
    protected void queryByAdvanceFilterWithOffset(TapConnectorContext connectorContext, TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        String sql = commonSqlMaker.buildSelectClause(table, filter, false) + getSchemaAndTable(table.getId()) + commonSqlMaker.buildSqlByAdvanceFilter(filter);
        jdbcContext.query(sql, resultSet -> {
            FilterResults filterResults = new FilterResults();
            try {
                Map<String, String> typeAndName = new HashMap<>();
                table.getNameFieldMap().forEach((key, value) -> {
                    typeAndName.put(key, value.getDataType());
                });
                List<String> allColumn = DbKit.getColumnsFromResultSet(resultSet);
                while (resultSet.next()) {
                    filterResults.add(filterTimeForPG(resultSet, typeAndName, allColumn));
                    if (filterResults.getResults().size() == BATCH_ADVANCE_READ_LIMIT) {
                        consumer.accept(filterResults);
                        filterResults = new FilterResults();
                    }
                }
            } catch (SQLException e) {
                exceptionCollector.collectTerminateByServer(e);
                exceptionCollector.collectReadPrivileges("batchReadWithoutOffset", Collections.emptyList(), e);
                exceptionCollector.revealException(e);
                throw e;
            }
            if (EmptyKit.isNotEmpty(filterResults.getResults())) {
                consumer.accept(filterResults);
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
                } else if (dataType.endsWith("without time zone") && "timestamp".equals(resultSet.getMetaData().getColumnTypeName(columnIndex))) {
                    String tiemstampString = resultSet.getString(colName);
                    if (StringUtils.isNotEmpty(tiemstampString)) {
                        LocalDateTime localDateTime = LocalDateTime.parse(tiemstampString.replace(" ", "T"));
                        dataMap.put(colName, localDateTime.minusHours(postgresConfig.getZoneOffsetHour()));
                    } else {
                        dataMap.put(colName, null);
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
        if (!postgresConfig.getOldVersionTimezone()) {
            if (value instanceof Timestamp) {
                if (!dataType.endsWith("with time zone")) {
                    value = ((Timestamp) value).toLocalDateTime().minusHours(postgresConfig.getZoneOffsetHour());
                } else {
                    value = (((Timestamp) value).toLocalDateTime().minusHours(TimeZone.getDefault().getRawOffset() / 3600000).atZone(ZoneOffset.UTC));
                }
            } else if (value instanceof Date) {
                value = (Instant.ofEpochMilli(((Date) value).getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime());
            } else if (value instanceof Time) {
                if (!dataType.endsWith("with time zone")) {
                    value = (Instant.ofEpochMilli(((Time) value).getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime().minusHours(postgresConfig.getZoneOffsetHour()));
                } else {
                    value = (Instant.ofEpochMilli(((Time) value).getTime()).atZone(ZoneOffset.UTC));
                }
            }
        }
        return value;
    }

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
                            int retry = 20;
                            while (retry-- > 0 && isAlive()) {
                                try {
                                    jdbcContext.query(splitSql, resultSet -> {
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

    protected void batchReadWithoutHashSplit(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = getBatchReadSelectSql(tapTable);
        jdbcContext.query(sql, resultSet -> {
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

    public void discoverPartitionInfoByParentName(TapConnectorContext connectorContext, List<TapTable> table, Consumer<Collection<TapPartitionResult>> consumer) throws SQLException {
        postgresPartitionContext.discoverPartitionInfoByParentName(connectorContext, table, consumer);
    }

    @Override
    public List<TapTable> discoverPartitionInfo(List<TapTable> tapTableList) {
        return postgresPartitionContext.discoverPartitionInfo(tapTableList);
    }

    protected CopyOnWriteArraySet<List<DataMap>> splitTableForMultiDiscoverSchema(List<DataMap> tables, int tableSize) {
        if (Integer.parseInt(postgresVersion) < 100000) {
            return super.splitTableForMultiDiscoverSchema(tables, tableSize);
        }
        return new CopyOnWriteArraySet<>(splitToPieces(tables, tableSize));
    }

    List<List<DataMap>> splitToPieces(List<DataMap> data, int eachPieceSize) {
        if (EmptyKit.isEmpty(data)) {
            return new ArrayList<>();
        }
        if (eachPieceSize <= 0) {
            throw new IllegalArgumentException("Param Error");
        }
        List<List<DataMap>> result = new ArrayList<>();
        List<DataMap> subList = new ArrayList<>();
        result.add(subList);
        for (DataMap datum : data) {
            String tableType = String.valueOf(datum.get("tableType"));
            if (subList.size() >= eachPieceSize && !TableType.CHILD_TABLE.equals(tableType)) {
                subList = new ArrayList<>();
                result.add(subList);
            }
            subList.add(datum);
        }
        return result;
    }

    protected void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) throws SQLException {
        if (jdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() >= 1) {
            jdbcContext.execute("truncate table " + getSchemaAndTable(tapClearTableEvent.getTableId()) + " cascade");
        } else {
            tapLogger.warn("Table {} not exists, skip truncate", tapClearTableEvent.getTableId());
        }
    }

    protected void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws SQLException {
        if (jdbcContext.queryAllTables(Collections.singletonList(tapDropTableEvent.getTableId())).size() >= 1) {
            jdbcContext.execute("drop table " + getSchemaAndTable(tapDropTableEvent.getTableId()) + " cascade");
        } else {
            tapLogger.warn("Table {} not exists, skip drop", tapDropTableEvent.getTableId());
        }
    }

    protected void createIndex(TapConnectorContext connectorContext, TapTable tapTable, TapCreateIndexEvent createIndexEvent) throws SQLException {
        super.createIndex(connectorContext, tapTable, createIndexEvent);
        createIndexEvent.getIndexList().stream().filter(v -> Boolean.TRUE.equals(v.getCluster())).forEach(v -> {
            try {
                jdbcContext.execute("cluster " + getSchemaAndTable(tapTable.getId()) + " using \"" + v.getName() + "\"");
            } catch (SQLException e) {
                tapLogger.warn("Cluster index failed, table:{}, index:{}", tapTable.getId(), v.getName());
            }
        });
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
                        if (e instanceof SQLException && ((SQLException) e).getSQLState().equals("42804")) {
                            TapTable referenceTable = connectorContext.getTableMap().get(c.getReferencesTableName());
                            c.getMappingFields().stream().filter(m -> Boolean.TRUE.equals(referenceTable.getNameFieldMap().get(m.getReferenceKey()).getAutoInc()) && referenceTable.getNameFieldMap().get(m.getReferenceKey()).getDataType().startsWith("numeric")).forEach(m -> {
                                try {
                                    jdbcContext.execute("alter table " + getSchemaAndTable(tapTable.getId()) + " alter column \"" + m.getForeignKey() + "\" type bigint");
                                } catch (SQLException e1) {
                                    exception.addException(c, "alter table alter column failed", e1);
                                }
                            });
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

    public String exportEventSql(TapConnectorContext connectorContext, TapEvent tapEvent, TapTable table) throws SQLException {
        PostgresWriteRecorder writeRecorder = new PostgresWriteRecorder(null, table, jdbcContext.getConfig().getSchema());
        return exportEventSql(writeRecorder, connectorContext, tapEvent, table);
    }

    protected void clearIdleSlot() throws SQLException {
        postgresJdbcContext.query("select pg_drop_replication_slot(a.slot_name)\n" +
                "from (\n" +
                "select * from pg_replication_slots where active='false' and slot_name like 'tapdata_cdc_%') a", resultSet -> {
        });
    }
}
