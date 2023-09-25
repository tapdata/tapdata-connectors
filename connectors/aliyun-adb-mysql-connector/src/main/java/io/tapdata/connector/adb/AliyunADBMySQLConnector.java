package io.tapdata.connector.adb;


import io.tapdata.common.CommonSqlMaker;
import io.tapdata.connector.adb.dml.AliyunADBRecordWriter;
import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.connector.mysql.MysqlExceptionCollector;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.MysqlReader;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.ddl.sqlmaker.MysqlDDLSqlGenerator;
import io.tapdata.connector.mysql.writer.MysqlSqlBatchWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.pretty.BiClassHandlers;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author jackin
 * @Description
 * @create 2022-12-12 15:18
 **/
@TapConnectorClass("aliyun-adb-mysql-spec.json")
public class AliyunADBMySQLConnector extends MysqlConnector {

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        mysqlConfig = new AliyunMysqlConfig().load(tapConnectionContext.getConnectionConfig());
        mysqlJdbcContext = new MysqlJdbcContextV2(mysqlConfig);
        commonDbConfig = mysqlConfig;
        jdbcContext = mysqlJdbcContext;
        commonSqlMaker = new CommonSqlMaker('`');
        exceptionCollector = new MysqlExceptionCollector();
        this.version = mysqlJdbcContext.queryVersion();
        if (tapConnectionContext instanceof TapConnectorContext) {
            this.mysqlWriter = new MysqlSqlBatchWriter(mysqlJdbcContext);
            this.mysqlReader = new MysqlReader(mysqlJdbcContext);
            this.timezone = mysqlJdbcContext.queryTimeZone();
            ddlSqlGenerator = new MysqlDDLSqlGenerator(version, ((TapConnectorContext) tapConnectionContext).getTableMap());
        }
        fieldDDLHandlers = new BiClassHandlers<>();
        fieldDDLHandlers.register(TapNewFieldEvent.class, this::newField);
        fieldDDLHandlers.register(TapAlterFieldAttributesEvent.class, this::alterFieldAttr);
        fieldDDLHandlers.register(TapAlterFieldNameEvent.class, this::alterFieldName);
        fieldDDLHandlers.register(TapDropFieldEvent.class, this::dropField);
        started.set(true);
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
//        WriteListResult<TapRecordEvent> writeListResult = this.mysqlWriter.write(tapConnectorContext, tapTable, tapRecordEvents);
//        consumer.accept(writeListResult);
        String insertDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
        if (insertDmlPolicy == null) {
            insertDmlPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
        }
        String updateDmlPolicy = tapConnectorContext.getConnectorCapabilities().getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
        if (updateDmlPolicy == null) {
            updateDmlPolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
        }
        new AliyunADBRecordWriter(jdbcContext, tapTable)
                .setInsertPolicy(insertDmlPolicy)
                .setUpdatePolicy(updateDmlPolicy)
                .setTapLogger(tapLogger)
                .write(tapRecordEvents, consumer, this::isAlive);
    }

}
