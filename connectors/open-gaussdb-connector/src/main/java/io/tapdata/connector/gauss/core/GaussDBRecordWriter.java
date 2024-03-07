package io.tapdata.connector.gauss.core;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.postgres.dml.PostgresRecordWriter;
import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;
import java.sql.SQLException;

public class GaussDBRecordWriter extends PostgresRecordWriter {
    public GaussDBRecordWriter(JdbcContext jdbcContext, TapTable tapTable, String version) throws SQLException {
        super(jdbcContext, tapTable, version);
    }

    public GaussDBRecordWriter(JdbcContext jdbcContext, Connection connection, TapTable tapTable, String version) throws SQLException {
        super(jdbcContext, connection, tapTable, version);
    }

    public static GaussDBRecordWriter instance(JdbcContext jdbcContext, TapTable tapTable, String version) throws SQLException {
        return new GaussDBRecordWriter(jdbcContext, tapTable, version);
    }
    public static GaussDBRecordWriter instance(JdbcContext jdbcContext, Connection connection, TapTable tapTable, String version) throws SQLException {
        return new GaussDBRecordWriter(jdbcContext, connection, tapTable, version);
    }
}
