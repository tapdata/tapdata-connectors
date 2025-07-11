package io.tapdata.connector.gauss.core;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.postgres.dml.PostgresRecordWriter;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;
import java.sql.SQLException;

public class GaussDBRecordWriter extends PostgresRecordWriter {

    public GaussDBRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        exceptionCollector = new PostgresExceptionCollector();
        insertRecorder = new GaussDBWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new GaussDBWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new GaussDBWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

    public GaussDBRecordWriter(JdbcContext jdbcContext, Connection connection, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        exceptionCollector = new PostgresExceptionCollector();
        insertRecorder = new GaussDBWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        updateRecorder = new GaussDBWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
        deleteRecorder = new GaussDBWriteRecorder(connection, tapTable, jdbcContext.getConfig().getSchema());
    }

}
