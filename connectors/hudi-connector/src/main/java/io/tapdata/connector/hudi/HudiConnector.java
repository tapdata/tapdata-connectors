package io.tapdata.connector.hudi;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.SqlExecuteCommandFunction;
import io.tapdata.connector.hive.HiveConnector;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.connector.hudi.util.FileUtil;
import io.tapdata.connector.hudi.write.ClientHandler;
import io.tapdata.connector.hudi.write.HuDiWriteBySparkClient;
import io.tapdata.connector.hudi.write.HudiWrite;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.ddl.table.TapClearTableEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapBinaryValue;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.entity.schema.value.TapValue;
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
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("spec_hudi.json")
public class HudiConnector extends HiveConnector {
    private static final String HU_DI_LIB_ID_TAG = "hudi-lib-id";
    private HudiConfig hudiConfig;
    private HudiJdbcContext hudiJdbcContext;
    private final Map<String, HuDiWriteBySparkClient> writeMap = new ConcurrentHashMap<>();

    @Override
    public void onStart(TapConnectionContext connectionContext) {
        isConnectorStarted(connectionContext, connectorContext -> {
            firstConnectorId = (String) connectorContext.getStateMap().get("firstConnectorId");
            if (EmptyKit.isNull(firstConnectorId)) {
                firstConnectorId = connectionContext.getId();
                connectorContext.getStateMap().put("firstConnectorId", firstConnectorId);
            }
        });
        String id = UUID.randomUUID().toString().replaceAll("-", "");
        hudiConfig = new HudiConfig(id)
                .log(connectionContext.getLog())
                .load(connectionContext.getConnectionConfig())
                .authenticate();
        if (connectionContext instanceof TapConnectorContext) {
            ((TapConnectorContext) connectionContext).getStateMap().put(HU_DI_LIB_ID_TAG, id);
        }
        hiveJdbcContext = hudiJdbcContext = new HudiJdbcContext(hudiConfig);
        commonDbConfig = hiveConfig;
        jdbcContext = hiveJdbcContext;
        commonSqlMaker = new CommonSqlMaker('`');
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) {
        EmptyKit.closeQuietly(hiveJdbcContext);
        ErrorKit.ignoreAnyError(hudiConfig::close);
        writeMap.forEach((id, c)-> c.onDestroy());
        writeMap.clear();
    }


    private void release(TapConnectorContext connectorContext) {
        Object id = connectorContext.getStateMap().get(HU_DI_LIB_ID_TAG);
        if (null != id) {
            FileUtil.release(FileUtil.storeDir("hudi" + id), connectorContext.getLog());
        }
    }

    Object registerString(TapValue<?, ?> value) {
        if (value != null && value.getValue() != null) return toJson(value.getValue());
        return "null";
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        codecRegistry.registerFromTapValue(TapRawValue.class, "string", this::registerString)
                     .registerFromTapValue(TapMapValue.class, "string", this::registerString)
                     .registerFromTapValue(TapArrayValue.class, "string", this::registerString)
                     .registerFromTapValue(TapBinaryValue.class, "string", tapValue -> {
                         if (tapValue != null && tapValue.getValue() != null)
                            return new String(Base64.encodeBase64(tapValue.getValue()));
                         return null;
                     }).registerFromTapValue(TapDateTimeValue.class, "timestamp", tapValue -> {
                         if (tapValue != null && tapValue.getValue() != null) {
                             return tapValue.getValue().toLong();
                         }
                         return null;
                     }).registerFromTapValue(TapTimeValue.class, "timestamp", tapValue -> {
                         if (tapValue != null && tapValue.getValue() != null) {
                             return tapValue.getValue().toLong();
                         }
                         return null;
                     }).registerFromTapValue(TapDateValue.class, "timestamp", tapValue -> {
                         if (tapValue != null && tapValue.getValue() != null) {
                             return tapValue.getValue().toLong();
                         }
                         return null;
                     });

        connectorFunctions.supportErrorHandleFunction(this::errorHandle)
                          .supportReleaseExternalFunction(this::release);

        //source
        connectorFunctions
                        //.supportBatchRead(this::batchReadV3)
                        //.supportBatchCount(this::batchCount)
                        //.supportQueryByAdvanceFilter(this::queryByAdvanceFilterWithOffsetV2)
                        //.supportGetTableNamesFunction(this::getTableNames)
                        //.supportGetTableInfoFunction(this::getTableInfo)
                          .supportExecuteCommandFunction((a, b, c) -> SqlExecuteCommandFunction.executeCommand(a, b, () -> hiveJdbcContext.getConnection(), this::isAlive, c));

        //target
        connectorFunctions.supportCreateTableV2(this::createTableV2)
                          .supportDropTable(this::dropTable)
                          .supportClearTable(this::clearTable)
                          .supportWriteRecord(this::writeRecord)
                          //.supportCreateIndex(this::createIndex)
                          //.supportNewFieldFunction(this::fieldDDLHandler)
                          //.supportAlterFieldNameFunction(this::fieldDDLHandler)
                          //.supportAlterFieldAttributesFunction(this::fieldDDLHandler)
                          //.supportDropFieldFunction(this::fieldDDLHandler)
        ;
    }

    private TableInfo getTableInfo(TapConnectionContext tapConnectorContext, String tableName) {
        TableInfo tableInfo = TableInfo.create();
        try {
            DataMap dataMap = hudiJdbcContext.getTableInfo(tableName);
            tableInfo.setNumOfRows(Long.valueOf(dataMap.getString("TABLE_ROWS")));
            tableInfo.setStorageSize(Long.valueOf(dataMap.getString("DATA_LENGTH")));
        } catch (SQLException e) {
            tapConnectorContext.getLog().error("Execute getTableInfo failed, error: " + e.getMessage(), e);
        }
        return tableInfo;
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        HudiConfig hudiConfig = new HudiConfig(null)
                .log(connectionContext.getLog())
                .load(connectionContext.getConnectionConfig())
                .authenticate();
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
        WriteListResult<TapRecordEvent> writeListResult = writeClient(tapConnectorContext)
                .writeRecord(tapConnectorContext, tapTable, tapRecordEvents, consumer);
        if(null != writeListResult.getErrorMap() && !writeListResult.getErrorMap().isEmpty()) {
            consumer.accept(writeListResult);
        }
    }

    private HudiWrite writeClient(TapConnectorContext tapConnectorContext) {
        String id = Thread.currentThread().getName() + Thread.currentThread().getId() + Thread.currentThread().getThreadGroup().getName();
        return writeMap.computeIfAbsent(id , key -> new HuDiWriteBySparkClient(hiveJdbcContext, hudiConfig, this::isAlive, tapConnectorContext.getLog()));
     }


    public CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) {
        TapTable tapTable = tapCreateTableEvent.getTable();
        String database = hudiConfig.getDatabase();
        String tableId = tapCreateTableEvent.getTableId();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        try {
            boolean tableExists = hudiJdbcContext.tableIfExists(tapTable.getId());
            createTableOptions.setTableExists(tableExists);
            if (tableExists) {
                tapConnectorContext.getLog().info("Table \"{}.{}\" exists, skip auto create table", database, tableId);
            } else {
                Collection<String> primaryKeys = tapTable.primaryKeys(true);
                String tableConfig;
                if (!EmptyKit.isEmpty(primaryKeys)) {
                    tableConfig = " using hudi options (primaryKey = '" + String.join(",", primaryKeys) +"' )";
                } else {
                    tableConfig = " ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";
                }
                jdbcContext.execute(String.format(HudiJdbcContext.CREATE_TABLE_SQL,
                        formatTable(database, tapTable.getId()),
                        commonSqlMaker.buildColumnDefinition(tapTable, true),
                        tableConfig
                ));
            }
        } catch (Exception e) {
            if (e instanceof SQLFeatureNotSupportedException) {
                // version compatibility
                if (e.getMessage() != null && e.getMessage().contains("Method not supported")) {
                    return createTableOptions;
                }
            }
            throw new CoreException("Create Table {} Failed, {}", tapTable.getId(), e.getMessage(), e);
        }
        return createTableOptions;
    }

    public void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) {
        String tableId = tapDropTableEvent.getTableId();
        cleanHdfsPath(tableId);
        try {
            jdbcContext.execute("DROP TABLE IF EXISTS " + formatTable(hudiConfig.getDatabase(), tableId));
        } catch (SQLException e) {
            if (e instanceof SQLFeatureNotSupportedException) {
                // version compatibility
                if (e.getMessage() != null && e.getMessage().contains("Method not supported")) {
                    return;
                }
            }
            throw new RuntimeException("Drop Table " + tapDropTableEvent.getTableId() + " failed, " + e.getMessage());
        }
    }

    private void cleanHdfsPath(String tableId) {
        ClientHandler clientHandler = new ClientHandler(hudiConfig, hudiJdbcContext);
        String tablePath = null;
        try {
            tablePath = clientHandler.getTablePath(tableId);
        } catch (Exception e) {
            return;
        }
        if (null == tablePath) return;
        try {
            FileSystem fs = FSUtils.getFs(tablePath, clientHandler.getHadoopConf());
            Path path = new Path(tablePath);
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
        } catch (IOException e) {
            throw new RuntimeException("Clean hdfs files failed, file path: " + tablePath + ", " + e.getMessage());
        }
    }

    public void clearTable(TapConnectorContext tapConnectorContext, TapClearTableEvent tapClearTableEvent) {
        String tableId = tapClearTableEvent.getTableId();
        cleanHdfsPath(tableId);
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
            throw new RuntimeException("TRUNCATE Table " + tapClearTableEvent.getTableId() + " Failed, " + e.getMessage());
        }
    }
    @Override
    protected void singleThreadDiscoverSchema(List<DataMap> subList, Consumer<List<TapTable>> consumer) throws SQLException {
        List<TapTable> tapTableList = hiveJdbcContext.queryTablesDesc(subList.stream().map(v -> v.getString("tableName")).collect(Collectors.toList()));
        syncSchemaSubmit(tapTableList, consumer);
    }

    private String formatTable(String database, String tableId) {
        return String.format("`%s`.`%s`", database, tableId);
    }
}
