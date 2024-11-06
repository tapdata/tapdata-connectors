package io.tapdata.connector.tdengine;

import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class TDengineRecordWriter extends NormalRecordWriter {

    public TDengineRecordWriter(TDengineJdbcContext jdbcContext, TapTable tapTable, String timestampField) throws SQLException {
        super(jdbcContext, tapTable);
        insertRecorder = new TDengineWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase(), timestampField);
        deleteRecorder = new TDengineWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase(), timestampField);
    }

    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) throws SQLException {
        //result of these events
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        try {
            insertRecorder.setVersion(version);
            insertRecorder.setInsertPolicy(insertPolicy);
            insertRecorder.setTapLogger(tapLogger);
            deleteRecorder.setVersion(version);
            deleteRecorder.setTapLogger(tapLogger);
            //insert,update,delete events must consecutive, so execute the other two first
            for (TapRecordEvent recordEvent : tapRecordEvents) {
                if (null != isAlive && !isAlive.get()) {
                    break;
                }
                if (recordEvent instanceof TapInsertRecordEvent) {
                    deleteRecorder.executeBatch(listResult);
                    TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                    insertRecorder.addInsertBatch(insertRecordEvent.getAfter(), listResult);
                    insertRecorder.addAndCheckCommit(recordEvent, listResult);
                } else if (recordEvent instanceof TapUpdateRecordEvent) {
                    insertRecorder.executeBatch(listResult);
                    TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                    deleteRecorder.addDeleteBatch(updateRecordEvent.getBefore(), listResult);
                    deleteRecorder.addAndCheckCommit(recordEvent, listResult);
                    deleteRecorder.executeBatch(listResult);
                    insertRecorder.addInsertBatch(updateRecordEvent.getAfter(), listResult);
                    insertRecorder.addAndCheckCommit(recordEvent, listResult);
                } else if (recordEvent instanceof TapDeleteRecordEvent) {
                    insertRecorder.executeBatch(listResult);
                    TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
                    deleteRecorder.addDeleteBatch(deleteRecordEvent.getBefore(), listResult);
                    deleteRecorder.addAndCheckCommit(recordEvent, listResult);
                }
            }
            insertRecorder.executeBatch(listResult);
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
            deleteRecorder.releaseResource();
            if (!isTransaction) {
                connection.close();
            }
            writeListResultConsumer.accept(listResult
                    .insertedCount(insertRecorder.getAtomicLong().get())
                    .removedCount(deleteRecorder.getAtomicLong().get()));
        }
    }

}
