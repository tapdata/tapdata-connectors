package io.tapdata.connector.adb;

import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.postgres.PostgresConnector;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * @author jackin
 * @Description
 * @create 2022-12-12 15:18
 **/
@TapConnectorClass("tencent-db-postgres-spec.json")
public class TencentDBPostgresConnector extends PostgresConnector {

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws SQLException {
        postgresConfig = (PostgresConfig) new PostgresConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(postgresConfig.getConnectionString());
        try (
                TencentDBPostgresTest tencentDBPostgresTest = (TencentDBPostgresTest) new TencentDBPostgresTest(postgresConfig, consumer, connectionOptions).initContext().withPostgresVersion()
        ) {
            tencentDBPostgresTest.testOneByOne();
            return connectionOptions;
        }
    }

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
            postgresRecordWriter = new TencentPostgresRecordWriter(postgresJdbcContext, connection, tapTable)
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setDeletePolicy(deleteDmlPolicy)
                    .setTapLogger(tapLogger);
        } else {
            postgresRecordWriter = new TencentPostgresRecordWriter(postgresJdbcContext, tapTable)
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
                    AtomicLong actual = new AtomicLong(0);
                    AtomicReference<String> actualSequenceName = new AtomicReference<>();
                    try {
                        String tableName = EmptyKit.isNotEmpty(commonDbConfig.getSchema()) ? "\"" + commonDbConfig.getSchema() + "\".\"" + tapTable.getId() + "\"" : "\"" + tapTable.getId() + "\"";
                        jdbcContext.queryWithNext("select pg_get_serial_sequence('" + tableName + "', '" + k + "')", resultSet -> actualSequenceName.set(resultSet.getString(1)));
                        jdbcContext.queryWithNext("select last_value from " + actualSequenceName.get(), resultSet -> actual.set(resultSet.getLong(1)));
                    } catch (SQLException ignore) {
                        tapLogger.warn("Failed get auto increment value for table {} errormessage:{}", tapTable.getId(),ignore.getMessage());
                    }
                    if (actualSequenceName.get() == null || actual.get() >= (Long.parseLong(String.valueOf(v)) + postgresConfig.getAutoIncJumpValue())) {
                        return;
                    }
                    alterSqls.add("select setval('" + actualSequenceName.get() + "'," + (Long.parseLong(String.valueOf(v)) + postgresConfig.getAutoIncJumpValue()) + ", false) ");
                });
                jdbcContext.batchExecute(alterSqls);
            }
        } else {
            postgresRecordWriter.write(tapRecordEvents, writeListResultConsumer, this::isAlive);
        }
    }


}
