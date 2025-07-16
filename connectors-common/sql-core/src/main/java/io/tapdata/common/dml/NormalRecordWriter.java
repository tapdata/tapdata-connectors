package io.tapdata.common.dml;

import io.netty.buffer.PooledByteBufAllocator;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.JdbcContext;
import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.utils.ErrorCodeUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class NormalRecordWriter {

    protected NormalWriteRecorder insertRecorder;
    protected String insertPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
    protected NormalWriteRecorder updateRecorder;
    protected String updatePolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
    protected NormalWriteRecorder deleteRecorder;
    protected String deletePolicy = ConnectionOptions.DML_DELETE_POLICY_IGNORE_ON_NON_EXISTS;
    protected String version;
    protected Connection connection;
    protected final TapTable tapTable;
    protected ExceptionCollector exceptionCollector = new AbstractExceptionCollector() {
    };
    protected boolean isTransaction = false;
    protected Log tapLogger;
    protected List<String> autoIncFields;
    protected Map<String, Object> autoIncMap = new HashMap<>();
    protected boolean largeSql = false;
    protected CommonDbConfig commonDbConfig;
    protected boolean needCloseIdentity = false;
    protected boolean needDisableTrigger = false;

    public NormalRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        this.commonDbConfig = jdbcContext.getConfig();
        this.connection = jdbcContext.getConnection();
        this.tapTable = tapTable;
    }

    public NormalRecordWriter(JdbcContext jdbcContext, TapTable tapTable, boolean largeSql) throws SQLException {
        this.commonDbConfig = jdbcContext.getConfig();
        this.connection = jdbcContext.getConnection();
        this.tapTable = tapTable;
        this.largeSql = largeSql;
    }

    public NormalRecordWriter(JdbcContext jdbcContext, Connection connection, TapTable tapTable) {
        this.commonDbConfig = jdbcContext.getConfig();
        this.connection = connection;
        this.tapTable = tapTable;
        isTransaction = true;
    }

    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) throws SQLException {
        //result of these events
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        try {
            insertRecorder.setVersion(version);
            insertRecorder.setInsertPolicy(insertPolicy);
            if (commonDbConfig.getEnableFileInput() && ConnectionOptions.DML_INSERT_POLICY_JUST_INSERT.equals(insertPolicy)) {
                insertRecorder.enableFileInput(new PooledByteBufAllocator().directBuffer(commonDbConfig.getBufferCapacity().intValue()));
            }
            insertRecorder.setTapLogger(tapLogger);
            updateRecorder.setVersion(version);
            updateRecorder.setUpdatePolicy(updatePolicy);
            updateRecorder.setTapLogger(tapLogger);
            deleteRecorder.setVersion(version);
            deleteRecorder.setDeletePolicy(deletePolicy);
            deleteRecorder.setTapLogger(tapLogger);
            //doubleActive
            if (Boolean.TRUE.equals(commonDbConfig.getDoubleActive())) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute(upsertDoubleActive());
                }
            }
            //insert,update,delete events must consecutive, so execute the other two first
            writePart(tapRecordEvents, listResult, isAlive);
            //release resource

        } catch (SQLException e) {
            errorHandler(e, null);
            exceptionCollector.revealException(e);
            throw e;
        } finally {
            insertRecorder.releaseResource();
            updateRecorder.releaseResource();
            deleteRecorder.releaseResource();
            if (!isTransaction) {
                if (needCloseIdentity) {
                    openIdentity();
                }
                if(needDisableTrigger) {
                    enableTrigger();
                }
                connection.close();
            }
            writeListResultConsumer.accept(listResult
                    .insertedCount(insertRecorder.getAtomicLong().get())
                    .modifiedCount(updateRecorder.getAtomicLong().get())
                    .removedCount(deleteRecorder.getAtomicLong().get()));
        }
    }

    protected void writePart(List<TapRecordEvent> tapRecordEvents, WriteListResult<TapRecordEvent> listResult, Supplier<Boolean> isAlive) {
        try {
            for (TapRecordEvent recordEvent : tapRecordEvents) {
                if (null != isAlive && !isAlive.get()) {
                    break;
                }
                if (recordEvent instanceof TapInsertRecordEvent) {
                    updateRecorder.executeBatch(listResult);
                    deleteRecorder.executeBatch(listResult);
                    TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
                    insertRecorder.addInsertBatch(insertRecordEvent.getAfter(), listResult);
                    if (EmptyKit.isNotEmpty(autoIncFields)) {
                        autoIncFields.forEach(field -> {
                            if (EmptyKit.isNotNull(insertRecordEvent.getAfter().get(field))) {
                                autoIncMap.put(field, insertRecordEvent.getAfter().get(field));
                            }
                        });
                    }
                    insertRecorder.addAndCheckCommit(recordEvent, listResult);
                } else if (recordEvent instanceof TapUpdateRecordEvent) {
                    insertRecorder.executeBatch(listResult);
                    deleteRecorder.executeBatch(listResult);
                    TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
                    updateRecorder.addUpdateBatch(updateRecordEvent.getAfter(), updateRecordEvent.getBefore(), listResult);
                    updateRecorder.addAndCheckCommit(recordEvent, listResult);
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
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (Exception ignore) {
            }
            if (tapRecordEvents.size() == 1) {
                errorHandler(e, tapRecordEvents.get(0));
                throw new RuntimeException(String.format("Error occurred when retrying write record: %s", tapRecordEvents.get(0)), e);
            } else {
                int eachPieceSize = Math.max(tapRecordEvents.size() / 10, 1);
                tapLogger.warn("writeRecord failed, dismantle them, size: {}", eachPieceSize);
                DbKit.splitToPieces(tapRecordEvents, eachPieceSize).forEach(pieces -> writePart(pieces, listResult, isAlive));
            }
        }
    }

    private void errorHandler(SQLException e, Object data) {
        if (null != data) {
            data = ErrorCodeUtils.truncateData(data);
        }
        exceptionCollector.collectViolateUnique(toJson(tapTable.primaryKeys(true)), data, null, e);
        exceptionCollector.collectWritePrivileges("writeRecord", Collections.emptyList(), e);
        exceptionCollector.collectWriteType(null, null, data, e);
        exceptionCollector.collectWriteLength(null, null, data, e);
    }

    public NormalRecordWriter setVersion(String version) {
        this.version = version;
        return this;
    }

    public NormalRecordWriter setInsertPolicy(String insertPolicy) {
        this.insertPolicy = insertPolicy;
        return this;
    }

    public NormalRecordWriter setUpdatePolicy(String updatePolicy) {
        this.updatePolicy = updatePolicy;
        return this;
    }

    public NormalRecordWriter setDeletePolicy(String deletePolicy) {
        this.deletePolicy = deletePolicy;
        return this;
    }

    public NormalRecordWriter setTapLogger(Log tapLogger) {
        this.tapLogger = tapLogger;
        return this;
    }

    public NormalRecordWriter setRemovedColumn(List<String> removedColumn) {
        insertRecorder.setRemovedColumn(removedColumn);
        updateRecorder.setRemovedColumn(removedColumn);
        deleteRecorder.setRemovedColumn(removedColumn);
        return this;
    }

    public void setAutoIncFields(List<String> autoIncFields) {
        this.autoIncFields = autoIncFields;
    }

    public Map<String, Object> getAutoIncMap() {
        return autoIncMap;
    }

    protected String upsertDoubleActive() {
        char escapeChar = commonDbConfig.getEscapeChar();
        return "update " + escapeChar + commonDbConfig.getSchema() + escapeChar + "." + escapeChar + "_tap_double_active" + escapeChar + " set " +
                escapeChar + "c2" + escapeChar + " = '" + UUID.randomUUID() + "' where " + escapeChar + "c1" + escapeChar + " = '1'";
    }

    public void closeConstraintCheck() throws SQLException {
        String sql = getCloseConstraintCheckSql();
        if (EmptyKit.isNotBlank(sql)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

    protected String getCloseConstraintCheckSql() {
        return null;
    }

    public void closeIdentity() throws SQLException {
        String sql = getIdentitySql();
        if (EmptyKit.isNotBlank(sql)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
        needCloseIdentity = true;
    }

    protected String getIdentitySql() {
        return null;
    }

    public void openIdentity() throws SQLException {
        String sql = getOpenIdentitySql();
        if (EmptyKit.isNotBlank(sql)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

    protected String getOpenIdentitySql() {
        return null;
    }

    public void disableTrigger() throws SQLException {
        String sql = getDisableTriggerSql();
        if (EmptyKit.isNotBlank(sql)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
        needDisableTrigger = true;
    }

    public void enableTrigger() throws SQLException {
        String sql = getEnableTriggerSql();
        if (EmptyKit.isNotBlank(sql)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(sql);
            }
        }
    }

    protected String getDisableTriggerSql() {
        return null;
    }

    protected String getEnableTriggerSql() {
        return null;
    }
}
