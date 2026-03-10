package io.tapdata.connector.klustron.target;

import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.postgres.dml.PostgresRecordWriter;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class KunLunNormalWriter extends KunLunWriter<KunLunNormalWriter> {

    public void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException {
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
                connection = pgJdbcContext.getConnection();
                transactionConnectionMap.put(threadName, connection);
            }
            postgresRecordWriter = new PostgresRecordWriter(pgJdbcContext, connection, tapTable, (hasUniqueIndex && !hasMultiUniqueIndex) ? version : "90500")
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setDeletePolicy(deleteDmlPolicy)
                    .setTapLogger(tapLogger);
        } else {
            postgresRecordWriter = new PostgresRecordWriter(pgJdbcContext, tapTable, (hasUniqueIndex && !hasMultiUniqueIndex) ? version : "90500")
                    .setInsertPolicy(insertDmlPolicy)
                    .setUpdatePolicy(updateDmlPolicy)
                    .setDeletePolicy(deleteDmlPolicy)
                    .setTapLogger(tapLogger);
        }
        if (Boolean.TRUE.equals(ofPgConfig.getAllowReplication())) {
            if (EmptyKit.isNull(writtenTableMap.get(tapTable.getId()).get(CANNOT_CLOSE_CONSTRAINT))) {
                boolean canClose = postgresRecordWriter.closeConstraintCheck();
                writtenTableMap.get(tapTable.getId()).put(CANNOT_CLOSE_CONSTRAINT, !canClose);
            } else if (Boolean.FALSE.equals(writtenTableMap.get(tapTable.getId()).get(CANNOT_CLOSE_CONSTRAINT))) {
                postgresRecordWriter.closeConstraintCheck();
            }
        }
        if (ofPgConfig.getCreateAutoInc() && Integer.parseInt(version) > 100000 && EmptyKit.isNotEmpty(autoIncFields)
                && "CDC".equals(tapRecordEvents.get(0).getInfo().get(TapRecordEvent.INFO_KEY_SYNC_STAGE))) {
            postgresRecordWriter.setAutoIncFields(autoIncFields);
            postgresRecordWriter.write(tapRecordEvents, writeListResultConsumer, this.isAlive);
            if (EmptyKit.isNotEmpty(postgresRecordWriter.getAutoIncMap())) {
                List<String> alterSqls = new ArrayList<>();
                postgresRecordWriter.getAutoIncMap().forEach((k, v) -> {
                    String sequenceName = tapTable.getNameFieldMap().get(k).getSequenceName();
                    AtomicLong actual = new AtomicLong(0);
                    if (EmptyKit.isNotBlank(sequenceName)) {
                        try {
                            pgJdbcContext.queryWithNext("select last_value from " + getSchemaAndTable.apply(sequenceName), resultSet -> actual.set(resultSet.getLong(1)));
                        } catch (SQLException ignore) {
                        }
                        if (actual.get() >= (Long.parseLong(String.valueOf(v)) + ofPgConfig.getAutoIncJumpValue())) {
                            return;
                        }
                        alterSqls.add("select setval('" + getSchemaAndTable.apply(sequenceName) + "'," + (Long.parseLong(String.valueOf(v)) + ofPgConfig.getAutoIncJumpValue()) + ", false) ");
                    } else {
                        AtomicReference<String> actualSequenceName = new AtomicReference<>();
                        try {
                            pgJdbcContext.queryWithNext("select pg_get_serial_sequence('" + getSchemaAndTable.apply(tapTable.getId()) + "', '\"" + k + "\"')", resultSet -> actualSequenceName.set(resultSet.getString(1)));
                            pgJdbcContext.queryWithNext("select last_value from " + actualSequenceName.get(), resultSet -> actual.set(resultSet.getLong(1)));
                        } catch (SQLException ignore) {
                        }
                        if (actual.get() >= (Long.parseLong(String.valueOf(v)) + ofPgConfig.getAutoIncJumpValue())) {
                            return;
                        }
                        alterSqls.add("ALTER TABLE " + getSchemaAndTable.apply(tapTable.getId()) + " ALTER COLUMN \"" + k + "\" SET GENERATED BY DEFAULT RESTART WITH " + (Long.parseLong(String.valueOf(v)) + ofPgConfig.getAutoIncJumpValue()));
                    }
                });
                pgJdbcContext.batchExecute(alterSqls);
            }
        } else {
            postgresRecordWriter.write(tapRecordEvents, writeListResultConsumer, this.isAlive);
        }
    }
}