package io.tapdata.connector.dws;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.dws.bean.DwsTapTable;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;

import java.sql.Connection;
import java.sql.SQLException;

public class DwsRecordWriter extends NormalRecordWriter {
    public DwsRecordWriter(JdbcContext jdbcContext, DwsTapTable dwsTapTable) throws SQLException {
        super(jdbcContext, dwsTapTable.getTapTable());
        exceptionCollector = new PostgresExceptionCollector();
        insertRecorder = new DwsWriteRecorder(connection, dwsTapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new DwsWriteRecorder(connection, dwsTapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new DwsWriteRecorder(connection, dwsTapTable, jdbcContext.getConfig().getSchema());
    }

    public DwsRecordWriter(JdbcContext jdbcContext, Connection connection, DwsTapTable dwsTapTable) throws SQLException {
        super(jdbcContext, connection, dwsTapTable.getTapTable());
        exceptionCollector = new PostgresExceptionCollector();
        insertRecorder = new DwsWriteRecorder(connection, dwsTapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new DwsWriteRecorder(connection, dwsTapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new DwsWriteRecorder(connection, dwsTapTable, jdbcContext.getConfig().getSchema());
    }

}
