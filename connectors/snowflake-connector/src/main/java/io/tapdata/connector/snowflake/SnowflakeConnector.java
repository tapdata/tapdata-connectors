package io.tapdata.connector.snowflake;

import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.connector.snowflake.config.SnowflakeConfig;
import io.tapdata.connector.snowflake.dml.SnowflakeRecordWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Snowflake PDK Connector
 *
 * @author Jarad
 * @date 2026/03/24
 */
@TapConnectorClass("spec_snowflake.json")
public class SnowflakeConnector extends CommonDbConnector {

    protected SnowflakeJdbcContext snowflakeJdbcContext;
    protected SnowflakeConfig snowflakeConfig;

    @Override
    public void onStart(TapConnectionContext connectorContext) {
        initConnection(connectorContext);
    }

    @Override
    protected void singleThreadDiscoverSchema(List<DataMap> subList, Consumer<List<TapTable>> consumer) throws SQLException {
        List<TapTable> tapTableList = TapSimplify.list();
        List<String> subTableNames = subList.stream().map(v -> v.getString("tableName")).collect(Collectors.toList());
        List<DataMap> columnList = snowflakeJdbcContext.queryAllColumns(subTableNames);
        List<DataMap> indexList = snowflakeJdbcContext.queryAllIndexes(subTableNames);
        subList.forEach(subTable -> {
            //2、table name/comment
            String table = subTable.getString("tableName");
            TapTable tapTable = table(table);
            tapTable.setTableAttr(getSpecificAttr(subTable));
            tapTable.setComment(subTable.getString("tableComment"));
            //3、primary key and table index
            List<String> primaryKey = TapSimplify.list();
            List<TapIndex> tapIndexList = TapSimplify.list();
            makePrimaryKeyAndIndex(indexList, table, primaryKey, tapIndexList);
            //4、table columns info
            AtomicInteger keyPos = new AtomicInteger(0);
            columnList.stream().filter(col -> table.equals(col.getString("tableName")))
                    .forEach(col -> {
                        try {
                            TapField tapField = new SnowflakeColumn(col).getTapField();
                            if (null == tapField) return;
                            tapField.setPos(keyPos.incrementAndGet());
                            tapField.setPrimaryKey(primaryKey.contains(tapField.getName()));
                            tapField.setPrimaryKeyPos(primaryKey.indexOf(tapField.getName()) + 1);
                            if (tapField.getPrimaryKey()) {
                                tapField.setNullable(false);
                            }
                            tapTable.add(tapField);
                        } catch (Exception e) {
                            throw new CoreException("Construct field failed, table: " + table + ", column: " + col + ", error: " + e.getMessage());
                        }
                    });
            tapTable.setIndexList(tapIndexList);
            tapTableList.add(tapTable);
        });
        syncSchemaSubmit(discoverPartitionInfo(tapTableList), consumer);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        snowflakeConfig = (SnowflakeConfig) new SnowflakeConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(snowflakeConfig.getConnectionString());
        try (
                SnowflakeTest snowflakeTest = new SnowflakeTest(snowflakeConfig, consumer)
        ) {
            snowflakeTest.testOneByOne();
            return connectionOptions;
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        // Test
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);

        // Target
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportDropTable(this::dropTable);

        // Source
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportBatchRead(this::batchReadWithoutOffset);

        // Query
        connectorFunctions.supportQueryByFilter(this::queryByFilter);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffset);

        // DDL
        connectorFunctions.supportNewFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldNameFunction(this::fieldDDLHandler);
        connectorFunctions.supportAlterFieldAttributesFunction(this::fieldDDLHandler);
        connectorFunctions.supportDropFieldFunction(this::fieldDDLHandler);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> snowflakeJdbcContext.getConnection(), this::isAlive, c));

        codecRegistry.registerFromTapValue(TapRawValue.class, "TEXT", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, tapTimeValue -> tapTimeValue.getValue().toTimeStr());
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> {
            if (EmptyKit.isNotNull(tapDateTimeValue.getValue().getTimeZone())) {
                return tapDateTimeValue.getValue().toTimestamp();
            } else {
                return formatTapDateTime(tapDateTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSSSSS");
            }
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> tapDateValue.getValue().toSqlDate());
        codecRegistry.registerFromTapValue(TapYearValue.class, "TEXT(4)", TapValue::getOriginValue);
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        EmptyKit.closeQuietly(snowflakeJdbcContext);
        if (EmptyKit.isNotNull(snowflakeConfig)) {
            snowflakeConfig.deletePrivateKeyFile();
        }
    }

    private void initConnection(TapConnectionContext connectionContext) {
        snowflakeConfig = (SnowflakeConfig) new SnowflakeConfig().load(connectionContext.getConnectionConfig());
        isConnectorStarted(connectionContext, connectorContext -> {
            snowflakeConfig.load(connectorContext.getNodeConfig());
            snowflakeConfig.setTableConfig(connectorContext.getTableNodeConfig());
        });
        snowflakeJdbcContext = new SnowflakeJdbcContext(snowflakeConfig);

        // Set common fields - only set fields that are compatible
        commonDbConfig = snowflakeConfig;
        jdbcContext = snowflakeJdbcContext;
        // Note: postgresJdbcContext is not set because SnowflakeJdbcContext doesn't extend PostgresJdbcContext

        commonSqlMaker = new SnowflakeSqlMaker();
//        ddlSqlGenerator = new PostgresDDLSqlGenerator();
        tapLogger = connectionContext.getLog();

        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);

        exceptionCollector = new AbstractExceptionCollector() {
        };
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
        String insertDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = connectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        new SnowflakeRecordWriter(snowflakeJdbcContext, tapTable)
                .setInsertPolicy(insertDmlPolicy)
                .setUpdatePolicy(updateDmlPolicy)
                .setTapLogger(tapLogger)
                .write(tapRecordEvents, writeListResultConsumer, this::isAlive);
    }

    @Override
    protected CreateTableOptions createTableV2(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws SQLException {
        TapTable tapTable = createTableEvent.getTable();
        String tableType = snowflakeConfig.getTableType(tapTable.getId());
        boolean dynamic = "DYNAMIC".equalsIgnoreCase(tableType);
        boolean hybrid = "HYBRID".equalsIgnoreCase(tableType);
        if (!dynamic && !hybrid) {
            return super.createTableV2(connectorContext, createTableEvent);
        }
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (snowflakeJdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }
        List<String> sqlList = TapSimplify.list();
        if (dynamic) {
            sqlList.add(getCreateDynamicTableSql(tapTable));
        } else {
            if (EmptyKit.isEmpty(tapTable.primaryKeys())) {
                throw new CoreException("Creating a hybrid table requires a primary key, table: " + tapTable.getId());
            }
            sqlList.add(getSnowflakeCreateTableSql(tapTable, "hybrid "));
        }
        if (EmptyKit.isNotNull(tapTable.getComment())) {
            sqlList.add(getSnowflakeTableCommentSql(tapTable));
        }
        if (!dynamic) {
            for (TapField field : tapTable.getNameFieldMap().values()) {
                if (EmptyKit.isNotNull(field.getComment())) {
                    sqlList.add(getSnowflakeColumnCommentSql(tapTable, field));
                }
            }
        }
        try {
            tapLogger.info("Create table sqls: {}", sqlList);
            snowflakeJdbcContext.batchExecute(sqlList);
        } catch (SQLException e) {
            exceptionCollector.collectWritePrivileges("createTable", Collections.emptyList(), e);
            throw e;
        }
        if (hybrid && EmptyKit.isNotEmpty(tapTable.getIndexList())) {
            tapTable.getIndexList().stream().filter(i -> !i.isPrimary()).forEach(i -> {
                String sql = getCreateIndexSql(tapTable, i);
                try {
                    tapLogger.info("Create index sql: {}", sql);
                    snowflakeJdbcContext.execute(sql);
                } catch (SQLException e) {
                    tapLogger.warn("Create index failed {}, please execute it manually [{}]", e.getMessage(), sql);
                }
            });
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private String getSnowflakeCreateTableSql(TapTable tapTable, String typeKeyword) {
        char escapeChar = snowflakeConfig.getEscapeChar();
        StringBuilder sb = new StringBuilder("create ").append(typeKeyword).append("table ");
        sb.append(getSchemaAndTable(tapTable.getId())).append('(').append(commonSqlMaker.buildColumnDefinition(tapTable, false));
        Collection<String> primaryKeys = tapTable.primaryKeys();
        if (EmptyKit.isNotEmpty(primaryKeys)) {
            sb.append(", primary key (").append(escapeChar)
                    .append(primaryKeys.stream().map(pk -> StringKit.escape(pk, escapeChar)).collect(Collectors.joining(escapeChar + "," + escapeChar)))
                    .append(escapeChar).append(')');
        }
        sb.append(')');
        return sb.toString();
    }

    private String getCreateDynamicTableSql(TapTable tapTable) {
        String query = snowflakeConfig.getDynamicTableQuery(tapTable.getId());
        if (EmptyKit.isBlank(query)) {
            throw new CoreException("Creating a dynamic table requires a query, please set the Dynamic Table Query for table: " + tapTable.getId());
        }
        String lag = snowflakeConfig.getDynamicTableLag(tapTable.getId());
        if (EmptyKit.isBlank(lag)) {
            lag = "1 minute";
        }
        return "create dynamic table " + getSchemaAndTable(tapTable.getId()) +
                " target_lag = '" + lag.replace("'", "''") + "'" +
                " warehouse = " + snowflakeConfig.getWarehouse() +
                " as " + query;
    }

    private String getSnowflakeTableCommentSql(TapTable tapTable) {
        return "comment on table " + getSchemaAndTable(tapTable.getId()) +
                " is '" + tapTable.getComment().replace("'", "''") + '\'';
    }

    private String getSnowflakeColumnCommentSql(TapTable tapTable, TapField tapField) {
        char escapeChar = snowflakeConfig.getEscapeChar();
        return "comment on column " + getSchemaAndTable(tapTable.getId()) + '.' +
                escapeChar + tapField.getName() + escapeChar +
                " is '" + tapField.getComment().replace("'", "''") + '\'';
    }

    @Override
    protected void processDataMap(DataMap dataMap, TapTable tapTable) {
        for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Timestamp) {
                if (!tapTable.getNameFieldMap().get(entry.getKey()).getDataType().startsWith("TIMESTAMP_TZ")) {
                    entry.setValue(((Timestamp) value).toLocalDateTime().minusHours(snowflakeConfig.getZoneOffsetHour()));
                } else {
                    entry.setValue(((Timestamp) value).toLocalDateTime().minusHours(TimeZone.getDefault().getRawOffset() / 3600000).atZone(ZoneOffset.UTC));
                }
            } else if (value instanceof Date) {
                entry.setValue(Instant.ofEpochMilli(((Date) value).getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime());
            } else if (value instanceof Time) {
                entry.setValue(Instant.ofEpochMilli(((Time) value).getTime()).atZone(ZoneId.systemDefault()).toLocalDateTime().minusHours(snowflakeConfig.getZoneOffsetHour()));
            }
        }
    }
}

