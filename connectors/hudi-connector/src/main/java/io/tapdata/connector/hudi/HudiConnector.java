package io.tapdata.connector.hudi;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.hive.HiveConnector;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.connector.hudi.write.HuDiWriteBySparkClient;
import io.tapdata.connector.hudi.write.HudiWrite;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_hudi.json")
public class HudiConnector extends HiveConnector {
    private static final String TAG = HudiConnector.class.getSimpleName();
    private HudiWrite hudiWrite;

    private HudiConfig hudiConfig;

    @Override
    public void onStart(TapConnectionContext connectionContext) {
        isConnectorStarted(connectionContext, connectorContext -> {
            firstConnectorId = (String) connectorContext.getStateMap().get("firstConnectorId");
            if (EmptyKit.isNull(firstConnectorId)) {
                firstConnectorId = connectionContext.getId();
                connectorContext.getStateMap().put("firstConnectorId", firstConnectorId);
            }
        });
        hudiConfig = new HudiConfig(firstConnectorId)
                .log(connectionContext.getLog())
                .load(connectionContext.getConnectionConfig());
        hiveJdbcContext = new HudiJdbcContext(hudiConfig);
        commonDbConfig = hiveConfig;
        jdbcContext = hiveJdbcContext;
        commonSqlMaker = new CommonSqlMaker('`');
        hudiWrite = new HuDiWriteBySparkClient(hiveJdbcContext, hudiConfig);

    }


    @Override
    public void onStop(TapConnectionContext connectionContext) {
        TapLogger.info("线程debug", "onStop当前线程为:{}", Thread.currentThread().getName());
        EmptyKit.closeQuietly(hiveJdbcContext);
        if(hudiWrite !=null){
            hudiWrite.onDestroy();
        }
    }
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "string", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) return toJson(tapRawValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapMapValue.class, "string", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) return toJson(tapMapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapArrayValue.class, "string", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) return toJson(tapValue.getValue());
            return "null";
        });
        codecRegistry.registerFromTapValue(TapBinaryValue.class, "string", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null)
                return new String(Base64.encodeBase64(tapValue.getValue()));
            return null;
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, "string", tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss.SS"));
        codecRegistry.registerFromTapValue(TapDateValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "yyyy-MM-dd"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "yyyy-MM-dd HH:mm:ss.SSS"));

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        //target
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportWriteRecord(this::writeRecord);


        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> hiveJdbcContext.getConnection(), this::isAlive, c));
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        HudiConfig hudiConfig = new HudiConfig(null)
                .log(connectionContext.getLog())
                .load(connectionContext.getConnectionConfig());
        try (
                HudiTest hudiTest = new HudiTest(hudiConfig, consumer)
        ) {
            hudiTest.testOneByOne();
        }
        return connectionOptions;
    }


    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = hudiWrite.writeRecord(tapConnectorContext, tapTable, tapRecordEvents);
        consumer.accept(writeListResult);
    }

    public CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        try {
            if (hiveJdbcContext.queryAllTables(Arrays.asList(tapTable.getId())).size() > 0) {
                DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
                String database = connectionConfig.getString("database");
                String tableId = tapCreateTableEvent.getTableId();
                createTableOptions.setTableExists(true);
                TapLogger.info(TAG, "Table \"{}.{}\" exists, skip auto create table", database, tableId);
            } else {
                Collection<String> primaryKeys = tapTable.primaryKeys(true);
                String sql = "CREATE TABLE IF NOT EXISTS " + hudiConfig.getDatabase() + "." + tapTable.getId() + "(" + commonSqlMaker.buildColumnDefinition(tapTable, true);
                StringBuilder pk = new StringBuilder();
                if (EmptyKit.isEmpty(primaryKeys)) throw new RuntimeException(format("Create table {}.{} failed, Please specify the Update Condition field as the primary key.",hudiConfig.getDatabase(), tapTable.getId()));
                for (String field : primaryKeys) {
                    pk.append(field).append(",");
                }
                StringBuilder sb = new StringBuilder();
                sb.append("\n) using hudi");
                sb.append("\noptions (\nprimaryKey = '");
                sb.append(pk);
                sql = sql + StringUtils.removeEnd(sb.toString(), ",") + "')";
                List<String> sqls = TapSimplify.list();
                sqls.add(sql);
                TapLogger.info("table :", "table->{}", tapTable.getId());
                createTableOptions.setTableExists(false);
                jdbcContext.batchExecute(sqls);
            }
        } catch (Exception e) {
            if (e instanceof SQLFeatureNotSupportedException) {
                // version compatibility
                if (e.getMessage() != null && e.getMessage().contains("Method not supported")) {
                    return createTableOptions;
                }
            }
            throw new RuntimeException("Create Table " + tapTable.getId() + " Failed! " + e.getMessage());
        }
        return createTableOptions;
    }

    public void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        try {
            jdbcContext.execute("DROP TABLE IF EXISTS " + hudiConfig.getDatabase() + "." + tapDropTableEvent.getTableId());
        } catch (SQLException e) {
            if (e instanceof SQLFeatureNotSupportedException) {
                // version compatibility
                if (e.getMessage() != null && e.getMessage().contains("Method not supported")) {
                    return;
                }
            }
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " Failed! \n ");
        }

    }

    public void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        try {
            if (hiveJdbcContext.queryAllTables(Collections.singletonList(tapClearTableEvent.getTableId())).size() == 1) {
                hiveJdbcContext.execute("TRUNCATE TABLE " + hudiConfig.getDatabase() + "." + tapClearTableEvent.getTableId());
            }
        } catch (Throwable e) {
            if (e instanceof SQLFeatureNotSupportedException) {
                // version compatibility
                if (e.getMessage() != null && e.getMessage().contains("Method not supported")) {
                    return;
                }
            }
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed! \n ");
        }
    }
    @Override
    protected void singleThreadDiscoverSchema(List<DataMap> subList, Consumer<List<TapTable>> consumer) throws SQLException {
        List<TapTable> tapTableList = hiveJdbcContext.queryTablesDesc(subList.stream().map(v -> v.getString("tableName")).collect(Collectors.toList()));
        syncSchemaSubmit(tapTableList, consumer);
    }
}
