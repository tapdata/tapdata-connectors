package io.tapdata.connector.adb;


import io.tapdata.connector.adb.write.AliyunADBBatchWriter;
import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.writer.MysqlWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;

import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;

/**
 * @author jackin
 * @Description
 * @create 2022-12-12 15:18
 **/
@TapConnectorClass("aliyun-adb-mysql-spec.json")
public class AliyunADBMySQLConnector extends MysqlConnector {
    private static final String TAG = AliyunADBMySQLConnector.class.getSimpleName();
    private MysqlWriter mysqlWriter;
    private MysqlJdbcContextV2 aliyunADBJdbcContext;

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        tapConnectionContext.getConnectionConfig().put("protocolType", "mysql");
        super.onStart(tapConnectionContext);
        this.aliyunADBJdbcContext = new MysqlJdbcContextV2(new MysqlConfig().load(tapConnectionContext.getConnectionConfig()));
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.mysqlWriter = new AliyunADBBatchWriter(aliyunADBJdbcContext);
        }
    }

    @Override
    public void onStop(TapConnectionContext tapConnectionContext) {
        super.onStop(tapConnectionContext);
        try {
            Optional.ofNullable(this.mysqlWriter).ifPresent(MysqlWriter::onDestroy);
        } catch (Exception ignored) {
        }
        if (null != aliyunADBJdbcContext) {
            try {
                this.aliyunADBJdbcContext.close();
                this.aliyunADBJdbcContext = null;
            } catch (Exception e) {
                TapLogger.error(TAG, "Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
            }
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        super.registerCapabilities(connectorFunctions, codecRegistry);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportStreamRead(null);
        connectorFunctions.supportTimestampToStreamOffset(null);
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        mysqlConfig = new MysqlConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(mysqlConfig.getConnectionString());
        try (
                AliyunADBMySQLTest aliyunADBMySQLTest = new AliyunADBMySQLTest(mysqlConfig, consumer)
        ) {
            aliyunADBMySQLTest.testOneByOne();
        }
        return connectionOptions;
    }

    private void writeRecord(TapConnectorContext tapConnectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        WriteListResult<TapRecordEvent> writeListResult = this.mysqlWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
        consumer.accept(writeListResult);
    }

    private String getCreateTableSql(TapTable tapTable, Boolean commentInField) {
        char escapeChar = commonDbConfig.getEscapeChar();
        StringBuilder sb = new StringBuilder("create table ");
        sb.append(getSchemaAndTable(tapTable.getId())).append('(').append(commonSqlMaker.buildColumnDefinition(tapTable, commentInField));
        Collection<String> primaryKeys = tapTable.primaryKeys();
        if (EmptyKit.isNotEmpty(primaryKeys)) {
            sb.append(", primary key (").append(escapeChar)
                    .append(String.join(escapeChar + "," + escapeChar, primaryKeys))
                    .append(escapeChar).append(')');
        }
        sb.append(')');
        if (EmptyKit.isNotEmpty(primaryKeys)) {
            sb.append(" DISTRIBUTE BY HASH(").append(escapeChar).append(primaryKeys.stream().findFirst().orElse("")).append(escapeChar).append(")");
        }
        if (commentInField && EmptyKit.isNotBlank(tapTable.getComment())) {
            sb.append(" comment='").append(tapTable.getComment()).append("'");
        }
        return sb.toString();
    }

    protected CreateTableOptions createTableV3(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws SQLException {
        return createTable(connectorContext, createTableEvent);
    }

    private CreateTableOptions createTable(TapConnectorContext connectorContext, TapCreateTableEvent createTableEvent) throws SQLException {
        TapTable tapTable = createTableEvent.getTable();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        if (jdbcContext.queryAllTables(Collections.singletonList(tapTable.getId())).size() > 0) {
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }

        Map<String, TapField> fieldMap = tapTable.getNameFieldMap();
        for (String field : fieldMap.keySet()) {
            String fieldDefault = (String) fieldMap.get(field).getDefaultValue();
            if (EmptyKit.isNotEmpty(fieldDefault)) {
                if (fieldDefault.contains("'")) {
                    fieldDefault = fieldDefault.replaceAll("'", "''");
                    fieldMap.get(field).setDefaultValue(fieldDefault);
                }
            }
        }
        List<String> sqlList = TapSimplify.list();
        sqlList.add(getCreateTableSql(tapTable, true));
        try {
            jdbcContext.batchExecute(sqlList);
        } catch (SQLException e) {
            exceptionCollector.collectWritePrivileges("createTable", Collections.emptyList(), e);
            throw e;
        }
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

}
