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
import io.tapdata.kit.EmptyKit;
import io.tapdata.oceanbase.OceanbaseJdbcContext;
import io.tapdata.oceanbase.OceanbaseTest;
import io.tapdata.oceanbase.bean.OceanbaseConfig;
import io.tapdata.oceanbase.cdc.OceanbaseReader;
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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author dayun
 * @date 2022/6/23 15:56
 */
@TapConnectorClass("oceanbase-spec.json")
public class OceanbaseConnector extends MysqlConnector {

    private String connectionTimezone;

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
            if (tapValue != null && tapValue.getValue() != null)
                return toJson(tapValue.getValue());
            return "null";
        });
        //TapTimeValue, TapDateTimeValue and TapDateValue's value is DateTime, need convert into Date object.
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTime());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
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
    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
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
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        mysqlConfig = new OceanbaseConfig().load(tapConnectionContext.getConnectionConfig());
        mysqlJdbcContext = new OceanbaseJdbcContext(mysqlConfig);
        commonDbConfig = mysqlConfig;
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
            return offsetStartTime;
        }
        AtomicLong offset = new AtomicLong(0);
        mysqlJdbcContext.queryWithNext("select current_timestamp()", resultSet -> {
            offset.set(resultSet.getTimestamp(1).getTime() / 1000L);
        });
        return offset.get();
    }

    private void streamRead(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        OceanbaseReader oceanbaseReader = new OceanbaseReader((OceanbaseConfig) mysqlConfig);
        oceanbaseReader.init(tableList, nodeContext.getTableMap(), offsetState, recordSize, consumer);
        oceanbaseReader.start(this::isAlive);
    }

    protected void batchReadWithoutHashSplit(TapConnectorContext tapConnectorContext, TapTable tapTable, Object offsetState, int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String sql = getBatchReadSelectSql(tapTable);
        mysqlJdbcContext.query(sql, resultSetConsumer(tapTable, eventBatchSize, eventsOffsetConsumer));
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
