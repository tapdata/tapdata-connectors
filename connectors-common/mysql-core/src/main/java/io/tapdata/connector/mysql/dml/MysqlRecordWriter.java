package io.tapdata.connector.mysql.dml;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.mysql.MysqlExceptionCollector;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author lemon
 */
public class MysqlRecordWriter extends NormalRecordWriter {

    public MysqlRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable, true);
        exceptionCollector = new MysqlExceptionCollector();
        ((MysqlExceptionCollector) exceptionCollector).setMysqlConfig((MysqlConfig)jdbcContext.getConfig());
        insertRecorder = new MysqlWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        insertRecorder.setLargeSql(largeSql);
        updateRecorder = new MysqlWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        deleteRecorder = new MysqlWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
    }

    public MysqlRecordWriter(JdbcContext jdbcContext, Connection connection, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable, true);
        exceptionCollector = new MysqlExceptionCollector();
        ((MysqlExceptionCollector) exceptionCollector).setMysqlConfig((MysqlConfig)jdbcContext.getConfig());
        insertRecorder = new MysqlWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        insertRecorder.setLargeSql(largeSql);
        updateRecorder = new MysqlWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        deleteRecorder = new MysqlWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
    }

    protected String getCloseConstraintCheckSql() {
        return "SET FOREIGN_KEY_CHECKS=0";
    }

}
