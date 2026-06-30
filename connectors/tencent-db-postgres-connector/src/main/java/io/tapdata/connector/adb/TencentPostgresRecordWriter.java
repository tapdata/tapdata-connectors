package io.tapdata.connector.adb;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.postgres.dml.OldPostgresWriteRecorder;
import io.tapdata.connector.postgres.dml.PostgresRecordWriter;
import io.tapdata.connector.postgres.dml.PostgresWriteRecorder;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.utils.ErrorCodeUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static io.tapdata.entity.simplify.TapSimplify.toJson;

public class TencentPostgresRecordWriter extends PostgresRecordWriter {

    public TencentPostgresRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        exceptionCollector = new PostgresExceptionCollector();
        insertRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

    public TencentPostgresRecordWriter(JdbcContext jdbcContext, Connection connection, TapTable tapTable) {
        super(jdbcContext, connection, tapTable);
        exceptionCollector = new PostgresExceptionCollector();
        insertRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new PostgresWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
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
