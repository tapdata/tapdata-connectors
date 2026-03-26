package io.tapdata.connector.snowflake;

import io.tapdata.common.CommonColumn;
import io.tapdata.common.CommonDbConnector;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.List;
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
        List<DataMap> pkList = snowflakeJdbcContext.queryAllPks(subTableNames);
        List<DataMap> indexList = snowflakeJdbcContext.queryAllIndexes(subTableNames);
        subList.forEach(subTable -> {
            //2、table name/comment
            String table = subTable.getString("tableName");
            TapTable tapTable = table(table);
            tapTable.setTableAttr(getSpecificAttr(subTable));
            tapTable.setComment(subTable.getString("tableComment"));
            //3、primary key and table index
            TapIndex pkIndex = new TapIndex();
            pkIndex.setPrimary(true);
            pkIndex.setUnique(true);
            List<TapIndexField> pkIndexFields = TapSimplify.list();
            List<String> primaryKey = TapSimplify.list();
            pkList.stream().filter(pk -> table.equals(pk.getString("tableName"))).forEach(pk -> {
                primaryKey.add(pk.getString("columnName"));
                pkIndex.setName(pk.getString("constraintName"));
                pkIndexFields.add(new TapIndexField().name(pk.getString("columnName")).fieldAsc(true));
            });
            pkIndex.setIndexFields(pkIndexFields);
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
//        connectorFunctions.supportWriteRecord(this::writeRecord);
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
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        EmptyKit.closeQuietly(snowflakeJdbcContext);
    }

    private void initConnection(TapConnectionContext connectionContext) {
        snowflakeConfig = (SnowflakeConfig) new SnowflakeConfig().load(connectionContext.getConnectionConfig());
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
}

