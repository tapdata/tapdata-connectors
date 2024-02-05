package io.tapdata.connector.gauss;

import com.huawei.opengauss.jdbc.core.types.PGClob;
import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.gauss.cdc.CdcOffset;
import io.tapdata.connector.gauss.cdc.GaussDBRunner;
import io.tapdata.connector.gauss.core.GaussColumn;
import io.tapdata.connector.gauss.core.GaussDBConfig;
import io.tapdata.connector.gauss.core.GaussDBJdbcContext;
import io.tapdata.connector.gauss.enums.CdcConstant;
import io.tapdata.connector.gauss.util.TimeUtil;
import io.tapdata.connector.postgres.PostgresSqlMaker;
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
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapNumber;
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
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import org.postgresql.geometric.*;
import org.postgresql.jdbc.PgArray;
import org.postgresql.jdbc.PgSQLXML;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * @author Gavin'Xiao
 * @date 2024/01/16 18:37:00
 * gauss db as source and target
 * */
@TapConnectorClass("spec_gauss_db.json")
public class OpenGaussDBConnector extends CommonDbConnector {
    protected GaussDBConfig gaussDBConfig;
    protected GaussDBJdbcContext gaussJdbcContext;
    private GaussDBTest gaussDBTest;
    private GaussDBRunner cdcRunner; //only when task start-pause this variable can be shared
    private Object slotName; //must be stored in stateMap
    protected String postgresVersion;
    protected Map<String, Boolean> writtenTableMap = new ConcurrentHashMap<>();

    @Override
    public void onStart(TapConnectionContext connectorContext) throws ClassNotFoundException {
        initConnection(connectorContext);
    }

    protected TapField makeTapField(DataMap dataMap) {
        return new GaussColumn(dataMap).getTapField();
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        gaussDBConfig = (GaussDBConfig) new GaussDBConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(gaussDBConfig.getConnectionString());
        try (GaussDBTest gaussDBTest = new GaussDBTest(gaussDBConfig, consumer).initContext()) {
            gaussDBTest.testOneByOne();
            return connectionOptions;
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry codec) {
        functions.supportErrorHandleFunction(this::errorHandle)
                //need to clear resource outer
                .supportReleaseExternalFunction(this::onDestroy)
                // target
                .supportWriteRecord(this::writeRecord)
                .supportCreateTableV2(this::createTableV2)
                .supportClearTable(this::clearTable)
                .supportDropTable(this::dropTable)
                .supportCreateIndex(this::createIndex)
                .supportQueryIndexes(this::queryIndexes)
                .supportDeleteIndex(this::dropIndexes)
                // source
                .supportBatchCount(this::batchCount)
                .supportBatchRead(this::batchReadWithoutOffset)
                .supportStreamRead(this::streamRead)
                .supportTimestampToStreamOffset(this::timestampToStreamOffset)
                // query
                .supportQueryByFilter(this::queryByFilter)
                .supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset)
                // ddl
                .supportNewFieldFunction(this::fieldDDLHandler)
                .supportAlterFieldNameFunction(this::fieldDDLHandler)
                .supportAlterFieldAttributesFunction(this::fieldDDLHandler)
                .supportDropFieldFunction(this::fieldDDLHandler)
                .supportGetTableNamesFunction(this::getTableNames)
                .supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> gaussJdbcContext.getConnection(), this::isAlive, c))
                .supportRunRawCommandFunction(this::runRawCommand)
                .supportCountRawCommandFunction(this::countRawCommand)
                .supportCountByPartitionFilterFunction(this::countByAdvanceFilter)
                .supportGetTableInfoFunction(this::getTableInfo)
                .supportTransactionBeginFunction(this::beginTransaction)
                .supportTransactionCommitFunction(this::commitTransaction)
                .supportTransactionRollbackFunction(this::rollbackTransaction);
        codec.registerFromTapValue(TapRawValue.class, "text", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        }).registerFromTapValue(TapMapValue.class, "text", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        }).registerFromTapValue(TapArrayValue.class, "text", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        }).registerToTapValue(PgArray.class, (value, tapType) -> {
            PgArray pgArray = (PgArray) value;
            try (ResultSet resultSet = pgArray.getResultSet()) {
                return new TapArrayValue(DbKit.getDataArrayByColumnName(resultSet, "VALUE"));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }).registerToTapValue(com.huawei.opengauss.jdbc.jdbc.PgArray.class, (value, tapType) -> {
            PgArray pgArray = (PgArray) value;
            try (ResultSet resultSet = pgArray.getResultSet()) {
                return new TapArrayValue(DbKit.getDataArrayByColumnName(resultSet, "VALUE"));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }).registerToTapValue(PgSQLXML.class, (value, tapType) -> {
            try {
                return new TapStringValue(((PgSQLXML) value).getString());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }).registerToTapValue(com.huawei.opengauss.jdbc.jdbc.PgSQLXML.class, (value, tapType) -> {
            com.huawei.opengauss.jdbc.jdbc.PgSQLXML xml = (com.huawei.opengauss.jdbc.jdbc.PgSQLXML)value;
            try {
                TapStringValue tapStringValue = new TapStringValue(xml.getString());
                tapStringValue.setOriginValue(value);
                return tapStringValue;
            } catch (Exception e) {
                TapStringValue tapStringValue = new TapStringValue(null);
                tapStringValue.setOriginValue(value);
                return tapStringValue;
            }
        }).registerToTapValue(PGbox.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(PGClob.class, (value, tapType) -> {
            if (value instanceof PGClob) {
                PGClob clob = (PGClob) value;
                try {
                    long length = clob.length();
                    if (length > 0) {
                        return new TapStringValue(clob.getSubString(1, (int)length));
                    }
                } catch (SQLException ignore) {

                }
            }
            return new TapStringValue(null);
        }).registerToTapValue(com.huawei.opengauss.jdbc.geometric.PGbox.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(PGcircle.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(com.huawei.opengauss.jdbc.geometric.PGcircle.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(com.huawei.opengauss.jdbc.geometric.PGline.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(PGline.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(com.huawei.opengauss.jdbc.geometric.PGlseg.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(PGlseg.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(com.huawei.opengauss.jdbc.geometric.PGpath.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(PGpath.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(PGobject.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(com.huawei.opengauss.jdbc.util.PGobject.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(PGpoint.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(com.huawei.opengauss.jdbc.geometric.PGpoint.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(PGpolygon.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(com.huawei.opengauss.jdbc.geometric.PGpolygon.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(UUID.class, (value, tapType) -> new TapStringValue(value.toString()))
        .registerToTapValue(PGInterval.class, (value, tapType) -> {
            //P1Y1M1DT12H12M12.312312S
            PGInterval pgInterval = (PGInterval) value;
            String interval = "P" + pgInterval.getYears() + "Y" +
                    pgInterval.getMonths() + "M" +
                    pgInterval.getDays() + "DT" +
                    pgInterval.getHours() + "H" +
                    pgInterval.getMinutes() + "M" +
                    pgInterval.getSeconds() + "S";
            return new TapStringValue(interval);
        })
        .registerToTapValue(com.huawei.opengauss.jdbc.util.PGInterval.class, (value, tapType) -> {
            //P1Y1M1DT12H12M12.312312S
            PGInterval pgInterval = (PGInterval) value;
            String interval = "P" + pgInterval.getYears() + "Y" +
                    pgInterval.getMonths() + "M" +
                    pgInterval.getDays() + "DT" +
                    pgInterval.getHours() + "H" +
                    pgInterval.getMinutes() + "M" +
                    pgInterval.getSeconds() + "S";
            return new TapStringValue(interval);
        })
        .registerToTapValue(byte[].class, (value, tapType) -> {
            byte[] bytes = (byte[])value;
            if (bytes.length == 0) return new TapStringValue("");
            byte type = tapType.getType();
            String dataValue = new String(bytes);
            switch (type) {
                case TapType.TYPE_DATETIME:
                    TapDateTime tapDateTime = (TapDateTime)tapType;
                    DateTime dateTime = new DateTime(TimeUtil.parseDateTime(dataValue, tapDateTime.getFraction(), tapDateTime.getWithTimeZone()));
                    return new TapDateValue(dateTime);
                case TapType.TYPE_DATE:
                    TapDate tapDate = (TapDate)tapType;
                    DateTime date = new DateTime(TimeUtil.parseDateTime(dataValue, 0, tapDate.getWithTimeZone()));
                    return new TapDateValue(date);
                case TapType.TYPE_ARRAY:
                    try {
                        return new TapArrayValue((List<Object>) fromJson(dataValue));
                    } catch (Exception e) {
                        return new TapRawValue(value);
                    }
                case TapType.TYPE_MAP:
                    try {
                        return new TapMapValue((Map<String, Object>) fromJson(dataValue));
                    } catch (Exception e) {
                        return new TapRawValue(value);
                    }
                case TapType.TYPE_BOOLEAN:
                    return new TapBooleanValue(Boolean.parseBoolean(dataValue));
                case TapType.TYPE_YEAR:
                    DateTime year = new DateTime(TimeUtil.parseDate(dataValue, "yyyy"));
                    return new TapYearValue(year);
                case TapType.TYPE_TIME:
                    DateTime time = new DateTime(TimeUtil.parseDate(dataValue, "hh:mm:ss"));
                    return new TapTimeValue(time);
                case TapType.TYPE_RAW:
                    return new TapRawValue(value);
                case TapType.TYPE_NUMBER:
                    TapNumberValue numberValue = new TapNumberValue();
                    TapNumber tNumber = (TapNumber)tapType;
                    Integer bit = tNumber.getBit();
                    try {
                        numberValue.setValue(Double.parseDouble(dataValue));
                    } catch (Exception e) {
                        return new TapRawValue(value);
                    }
                    try {
                        if (null == bit) {
                            numberValue.setOriginValue(dataValue);
                        } else if(bit <= 4) {
                            numberValue.setOriginValue(Byte.parseByte(dataValue));
                        } else if (bit <= 16) {
                            numberValue.setOriginValue(Short.parseShort(dataValue));
                        } else if (bit <= 32) {
                            numberValue.setOriginValue(Integer.parseInt(dataValue));
                        } else {
                            numberValue.setOriginValue(Long.parseLong(dataValue));
                        }
                    } catch (Exception e) {
                        numberValue.setOriginValue(dataValue);
                    }
                    return numberValue;
                case TapType.TYPE_BINARY:
                    return new TapBinaryValue(bytes);
                case TapType.TYPE_STRING:
                    return new TapStringValue(dataValue);
                default:
                    return new TapRawValue(value);
            }
        })
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        .registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime())
        .registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp())
        .registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate())
        .registerFromTapValue(TapYearValue.class, "character(4)", tapYearValue -> formatTapDateTime(tapYearValue.getValue(), "yyyy"))
        .registerFromTapValue(TapStringValue.class, "xml", value -> {
            return value.getOriginValue();
//            if (null == value || null == value.getValue() ||  "".equals(value.getValue())) return value;
//            return " xmlparse(DOCUMENT '" + value.getValue() + "' wellformed) ";wellformed
        }).registerFromTapValue(TapStringValue.class, "xmltype", value -> {
//            if (null == value || null == value.getValue() ||  "".equals(value.getValue())) return value;
//            return " xmltype.('" + value.getValue() + "') ";
            return value.getOriginValue();
        }).registerFromTapValue(TapStringValue.class, "json", value -> {
            try {
                PGobject pGobject = new PGobject();
                pGobject.setType("json");
                pGobject.setValue(value.getValue());
                return pGobject;
            } catch (Exception e) {
                return null;
            }
        })
        ;
    }

    //clear resource outer and jdbc context
    private void onDestroy(TapConnectorContext connectorContext) throws Throwable {
        try {
            onStart(connectorContext);
            connectorContext.getStateMap().remove(CdcConstant.GAUSS_DB_SLOT_TAG);
            if (EmptyKit.isNotNull(cdcRunner)) {
                cdcRunner.closeCdcRunner();
                cdcRunner = null;
            }
            if (EmptyKit.isNotNull(slotName)) {
                clearSlot();
            }
        } catch (Exception e) {
            connectorContext.getLog().error("Connection reset fail, error message: {}", e.getMessage());
            throw e;
        } finally {
            slotName = null;
            onStop(connectorContext);
        }
    }

    //clear postgres slot
    private void clearSlot() throws Throwable {
        gaussJdbcContext.queryWithNext("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name='" + slotName + "' AND active='false'", resultSet -> {
            if (resultSet.getInt(1) > 0) {
                gaussJdbcContext.execute("SELECT pg_drop_replication_slot('" + slotName + "')");
            }
        });
    }

    private void buildSlot(TapConnectorContext connectorContext, Boolean needCheck) throws Throwable {
        if (EmptyKit.isNull(slotName)) {
            // https://support.huaweicloud.com/intl/zh-cn/centralized-devg-v2-gaussdb/devg_03_1324.html
            //逻辑复制槽名称必须小于64个字符
            String plugin = Optional.ofNullable(connectorContext.getConnectionConfig().getString(""))
                    .orElse(CdcConstant.GAUSS_DB_SLOT_DEFAULT_PLUGIN);
            slotName = CdcConstant.GAUSS_DB_SLOT_SFF + UUID.randomUUID().toString().replaceAll("-", "_");
            final String sql = String.format(
                    "SELECT pg_create_logical_replication_slot('%s','%s')",
                    slotName,
                    plugin);
            gaussJdbcContext.execute(sql);
            connectorContext.getStateMap().put(CdcConstant.GAUSS_DB_SLOT_TAG, slotName);
            try {
                TimeUnit.SECONDS.sleep(3L);
            } catch (InterruptedException interruptedException) {
                interruptedException.printStackTrace();
            }
            tapLogger.info("Create logical replication slot completed, slot name: {}", slotName);
        } else if (needCheck) {
            AtomicBoolean existSlot = new AtomicBoolean(true);
            gaussJdbcContext.queryWithNext("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name='" + slotName + "'", resultSet -> {
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
        if ("pgoutput".equals(gaussDBConfig.getLogPluginName())) {
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
            gaussJdbcContext.query(String.format(PG_REPLICATE_IDENTITY, gaussDBConfig.getSchema(), StringKit.joinString(tableList, "'", ",")), resultSet -> {
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
        EmptyKit.closeQuietly(gaussDBTest);
        EmptyKit.closeQuietly(gaussJdbcContext);
    }

    //initialize jdbc context, slot name, version
    private void initConnection(TapConnectionContext connectionContext) {
        gaussDBConfig = (GaussDBConfig) new GaussDBConfig().load(connectionContext.getConnectionConfig());
        gaussDBTest = new GaussDBTest(gaussDBConfig, testItem -> { }).initContext();
        gaussJdbcContext = new GaussDBJdbcContext(gaussDBConfig);
        commonDbConfig = gaussDBConfig;
        jdbcContext = gaussJdbcContext;
        isConnectorStarted(connectionContext, tapConnectorContext -> {
            slotName = tapConnectorContext.getStateMap().get(CdcConstant.GAUSS_DB_SLOT_TAG);
            gaussDBConfig.load(tapConnectorContext.getNodeConfig());
        });
        commonSqlMaker = new PostgresSqlMaker().closeNotNull(gaussDBConfig.getCloseNotNull());
        postgresVersion = gaussJdbcContext.queryVersion();
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
                connection = gaussJdbcContext.getConnection();
                transactionConnectionMap.put(threadName, connection);
            }
            new PostgresRecordWriter(gaussJdbcContext, connection, tapTable, hasUniqueIndex ? postgresVersion : "90500")
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setTapLogger(tapLogger)
                    .write(tapRecordEvents, writeListResultConsumer, this::isAlive);

        } else {
            new PostgresRecordWriter(gaussJdbcContext, tapTable, hasUniqueIndex ? postgresVersion : "90500")
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setTapLogger(tapLogger)
                    .write(tapRecordEvents, writeListResultConsumer, this::isAlive);
        }
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        List<String> tables = new ArrayList<>();
        String schema = (String)nodeContext.getConnectionConfig().get("schema");
        if (null != tableList && !tableList.isEmpty()) {
            for (String s : tableList) {
                tables.add(String.format("%s.%s", schema, s));
            }
        }
        cdcRunner = new GaussDBRunner((GaussDBConfig) new GaussDBConfig().load(nodeContext.getConnectionConfig()), nodeContext.getLog());
        testReplicateIdentity(nodeContext.getTableMap());
        buildSlot(nodeContext, true);
        cdcRunner.useSlot(slotName.toString())
                .watch(tables)
                .supplierIsAlive(this::isAlive)
                .offset(offsetState)
                .waitTime(Optional.ofNullable(nodeContext.getNodeConfig().getInteger("flushLsn"))
                        .orElse(0) * 60 * 1000)
                .registerConsumer(consumer, recordSize);
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

    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long offsetStartTime) throws Throwable {
        if (EmptyKit.isNotNull(offsetStartTime)) {
            tapLogger.warn("Postgres specified time start increment is not supported, use the current time as the start increment");
        }
        //test streamRead log plugin
        boolean canCdc = Boolean.TRUE.equals(gaussDBTest.testStreamRead());
        if (canCdc) {
            testReplicateIdentity(connectorContext.getTableMap());
            buildSlot(connectorContext, false);
        }
        return new CdcOffset();
    }

    protected TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        DataMap dataMap = gaussJdbcContext.getTableInfo(tableName);
        TableInfo tableInfo = TableInfo.create();
        tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("size")));
        tableInfo.setStorageSize(new BigDecimal(dataMap.getString("rowcount")).longValue());
        return tableInfo;
    }
}
