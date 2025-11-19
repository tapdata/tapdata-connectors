package io.tapdata.connector.dws;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.dml.PostgresRecordWriter;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class DwsRecordWriter extends PostgresRecordWriter {

    private Set<String> cannotChange = new HashSet<>();

    public DwsRecordWriter(JdbcContext jdbcContext, TapTable tapTable, boolean hasUnique) throws SQLException {
        super(jdbcContext, tapTable);
        exceptionCollector = new PostgresExceptionCollector();
        insertRecorder = new DwsWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema())
                .replaceBlank(((PostgresConfig) jdbcContext.getConfig()).getReplaceBlank()).hasUnique(hasUnique);
        updateRecorder = new DwsWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema())
                .replaceBlank(((PostgresConfig) jdbcContext.getConfig()).getReplaceBlank()).hasUnique(hasUnique);
        deleteRecorder = new DwsWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema())
                .replaceBlank(((PostgresConfig) jdbcContext.getConfig()).getReplaceBlank()).hasUnique(hasUnique);
        generateCannotChange();
    }

    public DwsRecordWriter(JdbcContext jdbcContext, Connection connection, TapTable tapTable, boolean hasUnique) {
        super(jdbcContext, connection, tapTable);
        exceptionCollector = new PostgresExceptionCollector();
        insertRecorder = new DwsWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema())
                .replaceBlank(((PostgresConfig) jdbcContext.getConfig()).getReplaceBlank()).hasUnique(hasUnique);
        updateRecorder = new DwsWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema())
                .replaceBlank(((PostgresConfig) jdbcContext.getConfig()).getReplaceBlank()).hasUnique(hasUnique);
        deleteRecorder = new DwsWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema())
                .replaceBlank(((PostgresConfig) jdbcContext.getConfig()).getReplaceBlank()).hasUnique(hasUnique);
        generateCannotChange();
    }

    private void generateCannotChange() {
        cannotChange.addAll(tapTable.primaryKeys());
        cannotChange.addAll(((PostgresConfig) commonDbConfig).getDistributedKey());
    }

    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) throws SQLException {
        //result of these events
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        try {
            insertRecorder.setInsertPolicy(insertPolicy);
            insertRecorder.setTapLogger(tapLogger);
            updateRecorder.setUpdatePolicy(updatePolicy);
            updateRecorder.setTapLogger(tapLogger);
            deleteRecorder.setTapLogger(tapLogger);
            //dataSaving
            if (Boolean.TRUE.equals(commonDbConfig.getDataSaving())) {
                updateRecorder.setDataSaving(true);
            }
            //insert,update,delete events must consecutive, so execute the other two first
            for (TapRecordEvent recordEvent : tapRecordEvents) {
                if (null != isAlive && !isAlive.get()) {
                    break;
                }
                if (recordEvent instanceof TapInsertRecordEvent) {
                    updateRecorder.executeBatch(listResult);
                    deleteRecorder.executeBatch(listResult);
                    TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                    insertRecorder.addInsertBatch(insertRecordEvent.getAfter(), listResult);
                    insertRecorder.addAndCheckCommit(recordEvent, listResult);
                } else if (recordEvent instanceof TapUpdateRecordEvent) {
                    insertRecorder.executeBatch(listResult);
                    deleteRecorder.executeBatch(listResult);
                    TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                    Map<String, Object> after = updateRecordEvent.getAfter();
                    Map<String, Object> before = updateRecordEvent.getBefore();
                    if (cannotChange.stream().anyMatch(v -> !Objects.equals(after.get(v), before.get(v)))) {
                        deleteRecorder.addDeleteBatch(before, listResult);
                        deleteRecorder.addAndCheckCommit(recordEvent, listResult);
                        deleteRecorder.executeBatch(listResult);
                        insertRecorder.addInsertBatch(after, listResult);
                        insertRecorder.addAndCheckCommit(recordEvent, listResult);
                    } else {
                        updateRecorder.addUpdateBatch(after, before, listResult);
                        updateRecorder.addAndCheckCommit(recordEvent, listResult);
                    }
                } else if (recordEvent instanceof TapDeleteRecordEvent) {
                    insertRecorder.executeBatch(listResult);
                    updateRecorder.executeBatch(listResult);
                    TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                    deleteRecorder.addDeleteBatch(deleteRecordEvent.getBefore(), listResult);
                    deleteRecorder.addAndCheckCommit(recordEvent, listResult);
                }
            }
            insertRecorder.executeBatch(listResult);
            updateRecorder.executeBatch(listResult);
            deleteRecorder.executeBatch(listResult);
            //some datasource must be auto commit, error will occur when commit
            if (!connection.getAutoCommit() && !isTransaction) {
                connection.commit();
            }
            //release resource

        } catch (SQLException e) {
            exceptionCollector.collectTerminateByServer(e);
            exceptionCollector.collectViolateNull(null, e);
            TapRecordEvent errorEvent = null;
            if (EmptyKit.isNotNull(listResult.getErrorMap())) {
                errorEvent = listResult.getErrorMap().keySet().stream().findFirst().orElse(null);
            }
            exceptionCollector.collectViolateUnique(toJson(tapTable.primaryKeys(true)), errorEvent, null, e);
            exceptionCollector.collectWritePrivileges("writeRecord", Collections.emptyList(), e);
            exceptionCollector.collectWriteType(null, null, errorEvent, e);
            exceptionCollector.collectWriteLength(null, null, errorEvent, e);
            exceptionCollector.revealException(e);
            throw e;
        } finally {
            insertRecorder.releaseResource();
            updateRecorder.releaseResource();
            deleteRecorder.releaseResource();
            if (!isTransaction) {
                connection.close();
            }
            writeListResultConsumer.accept(listResult
                    .insertedCount(insertRecorder.getAtomicLong().get())
                    .modifiedCount(updateRecorder.getAtomicLong().get())
                    .removedCount(deleteRecorder.getAtomicLong().get()));
        }
    }

}
