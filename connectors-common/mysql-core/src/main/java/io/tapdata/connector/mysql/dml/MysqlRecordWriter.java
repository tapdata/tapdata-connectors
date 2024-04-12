package io.tapdata.connector.mysql.dml;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;

/**
 * @author lemon
 */
public class MysqlRecordWriter extends NormalRecordWriter {

    public MysqlRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable, true);
        insertRecorder = new MysqlWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        insertRecorder.setLargeSql(largeSql);
        updateRecorder = new MysqlWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        deleteRecorder = new MysqlWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
    }

}
