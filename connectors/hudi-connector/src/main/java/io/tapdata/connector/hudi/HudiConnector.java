package io.tapdata.connector.hudi;

import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.hive.HiveConnector;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.connector.hudi.write.HuDiSqlMarker;
import io.tapdata.connector.hudi.write.HuDiWriteBySparkClient;
import io.tapdata.connector.hudi.write.HudiWrite;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_hudi.json")
public class HudiConnector extends HiveConnector {

    private HudiConfig hudiConfig;
    private HudiJdbcContext hudiJdbcContext;

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
                .load(connectionContext.getConnectionConfig())
                .authenticate(new Configuration());
        hiveJdbcContext = hudiJdbcContext = new HudiJdbcContext(hudiConfig);
        commonDbConfig = hiveConfig;
        jdbcContext = hiveJdbcContext;
        commonSqlMaker = new HuDiSqlMarker('`');
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        connectionContext.getLog().info("线程debug: onStop当前线程为:{}", Thread.currentThread().getName());
        EmptyKit.closeQuietly(hiveJdbcContext);
        ErrorKit.ignoreAnyError(hudiConfig::close);
        writeMap.forEach((id, c)-> c.onDestroy());
        writeMap.clear();
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
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, "timestamp", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return tapValue.getValue().toLong();
            }
            return null;
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, "timestamp", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return tapValue.getValue().toLong();
            }
            return null;
        });
        codecRegistry.registerFromTapValue(TapDateValue.class, "timestamp", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return tapValue.getValue().toLong();
            }
            return null;
        });
        codecRegistry.registerFromTapValue(TapNumberValue.class, "smallint", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return tapValue.getValue().intValue();
            }
            return null;
        });
        codecRegistry.registerFromTapValue(TapNumberValue.class, "int", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return tapValue.getValue().intValue();
            }
            return null;
        });
        codecRegistry.registerFromTapValue(TapNumberValue.class, "tinyint", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return tapValue.getValue().intValue();
            }
            return null;
        });
        codecRegistry.registerFromTapValue(TapNumberValue.class, "smallint", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return tapValue.getValue().intValue();
            }
            return null;
        });
        codecRegistry.registerFromTapValue(TapNumberValue.class, "bigint", tapValue -> {
            if (tapValue != null && tapValue.getValue() != null) {
                return tapValue.getValue().longValue();
            }
            return null;
        });

        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        //target
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportWriteRecord(this::writeRecord);

        connectorFunctions.supportBatchCount(null);
        connectorFunctions.supportBatchRead(null);
        //query
        connectorFunctions.supportQueryByAdvanceFilter(null);


        connectorFunctions.supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> hiveJdbcContext.getConnection(), this::isAlive, c));
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        String uuid = UUID.randomUUID().toString().replaceAll("-", "");
        HudiConfig hudiConfig = new HudiConfig(null)
                .log(connectionContext.getLog())
                .load(connectionContext.getConnectionConfig())
                .authenticate(new Configuration());
        HudiTest hudiTest = new HudiTest(hudiConfig, consumer);
        try {
            hudiTest.testOneByOne();
        } finally {
            ErrorKit.ignoreAnyError(hudiTest::close);
            ErrorKit.ignoreAnyError(hudiConfig::close);
        }
        return connectionOptions;
    }


    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        //long s = System.currentTimeMillis();
        WriteListResult<TapRecordEvent> writeListResult = writeClient(tapConnectorContext)
                .writeRecord(tapConnectorContext, tapTable, tapRecordEvents, consumer);
        if(null != writeListResult.getErrorMap() && !writeListResult.getErrorMap().isEmpty()) {
            consumer.accept(writeListResult);
        };
        //System.out.println("[TAP_QPS]" + (System.currentTimeMillis() - s) + ", batch size: " + tapRecordEvents.size());
    }

    Map<String, HuDiWriteBySparkClient> writeMap = new ConcurrentHashMap<>();
    private HudiWrite writeClient(TapConnectorContext tapConnectorContext) {
        String id = Thread.currentThread().getName() + Thread.currentThread().getId() + Thread.currentThread().getThreadGroup().getName();
        HuDiWriteBySparkClient write = null;
        if (!writeMap.containsKey(id) || null == (write = writeMap.get(id))) {
            write = new HuDiWriteBySparkClient(hiveJdbcContext, hudiConfig)
                    .log(tapConnectorContext.getLog());
            writeMap.put(id, write);
        }
        return write;
     }


    public CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        try {
            if (hudiJdbcContext.tableIfExists(tapTable.getId())) {
                DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
                String database = connectionConfig.getString("database");
                String tableId = tapCreateTableEvent.getTableId();
                createTableOptions.setTableExists(true);
                tapConnectorContext.getLog().info("Table \"{}.{}\" exists, skip auto create table", database, tableId);
            } else {
                Collection<String> primaryKeys = tapTable.primaryKeys(true);
                String sql = "CREATE TABLE IF NOT EXISTS " +
                        formatTable(hudiConfig.getDatabase(), tapTable.getId()) + "("
                        + commonSqlMaker.buildColumnDefinition(tapTable, true);
                StringJoiner pk = new StringJoiner(",");
//                if (EmptyKit.isEmpty(primaryKeys)) {
//                    throw new RuntimeException(
//                            format("Create table {}.{} failed, Please specify the Update Condition field as the primary key.",
//                                    hudiConfig.getDatabase(),
//                                    tapTable.getId())
//                    );
//                }
                for (String field : primaryKeys) {
                    pk.add(field);
                }
                String sb = "\n) using hudi \noptions (";
                if (!EmptyKit.isEmpty(primaryKeys)) {
                    sb += ("\nprimaryKey = '" + pk +"' ");
                }
                sql = sql + sb  + ")";
                List<String> sqls = TapSimplify.list();
                sqls.add(sql);
                tapConnectorContext.getLog().info("Table: table-> {}", tapTable.getId());
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
            jdbcContext.execute("DROP TABLE IF EXISTS " + formatTable(hudiConfig.getDatabase(), tapDropTableEvent.getTableId()));
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
        String tableId = tapClearTableEvent.getTableId();
        try {
            if (hudiJdbcContext.tableIfExists(tableId)) {
                hiveJdbcContext.execute("TRUNCATE TABLE " + formatTable(hudiConfig.getDatabase(), tapClearTableEvent.getTableId()));
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

    String formatTable(String database, String tableId) {
        return String.format("`%s`.`%s`", database, tableId);
    }
}
