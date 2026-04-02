package io.tapdata.connector.snowflake.dml;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;
import java.sql.SQLException;

public class SnowflakeRecordWriter extends NormalRecordWriter {
    public SnowflakeRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        exceptionCollector = new AbstractExceptionCollector() {
        };
        insertRecorder = new SnowflakeWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new SnowflakeWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new SnowflakeWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

    public SnowflakeRecordWriter(JdbcContext jdbcContext, TapTable tapTable, boolean largeSql) throws SQLException {
        super(jdbcContext, tapTable, largeSql);
        exceptionCollector = new AbstractExceptionCollector() {
        };
        insertRecorder = new SnowflakeWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new SnowflakeWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new SnowflakeWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

    public SnowflakeRecordWriter(JdbcContext jdbcContext, Connection connection, TapTable tapTable) {
        super(jdbcContext, connection, tapTable);
        exceptionCollector = new AbstractExceptionCollector() {
        };
        insertRecorder = new SnowflakeWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new SnowflakeWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new SnowflakeWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }
}
