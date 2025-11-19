package io.tapdata.connector.postgres.dml;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.utils.ErrorCodeUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class PostgresRecordWriter extends NormalRecordWriter {

    public PostgresRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
    }

    public PostgresRecordWriter(JdbcContext jdbcContext, Connection connection, TapTable tapTable) {
        super(jdbcContext, connection, tapTable);
    }

    public PostgresRecordWriter(JdbcContext jdbcContext, TapTable tapTable, String version) throws SQLException {
        super(jdbcContext, tapTable);
        exceptionCollector = new PostgresExceptionCollector();
        if (Integer.parseInt(version) > 90500) {
            insertRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            updateRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            deleteRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        } else {
            insertRecorder = new OldPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            updateRecorder = new OldPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            deleteRecorder = new OldPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        }
    }

    public PostgresRecordWriter(JdbcContext jdbcContext, Connection connection, TapTable tapTable, String version) {
        super(jdbcContext, connection, tapTable);
        exceptionCollector = new PostgresExceptionCollector();
        if (Integer.parseInt(version) > 90500) {
            insertRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            updateRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            deleteRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        } else {
            insertRecorder = new OldPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            updateRecorder = new OldPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
            deleteRecorder = new OldPostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        }
    }

    @Override
    public void errorHandler(SQLException e, Object data) {
        if (null != data) {
            data = ErrorCodeUtils.truncateData(data);
        }
        exceptionCollector.collectViolateUnique(toJson(tapTable.primaryKeys(true)), data, null, e);
        exceptionCollector.collectWritePrivileges("writeRecord", Collections.emptyList(), e);
        exceptionCollector.collectWriteType(null, null, data, e);
        exceptionCollector.collectWriteLength(null, null, data, e);
        exceptionCollector.collectViolateNull(null, e);
    }

    public boolean closeConstraintCheck() {
        String sql = "SET session_replication_role = 'replica'";
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (Exception e) {
            try {
                connection.rollback();
            } catch (SQLException ignored) {
            }
            return false;
        }
        return true;
    }

}
