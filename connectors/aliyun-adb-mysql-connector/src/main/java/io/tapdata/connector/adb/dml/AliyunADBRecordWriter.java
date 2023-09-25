package io.tapdata.connector.adb.dml;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;

public class AliyunADBRecordWriter extends NormalRecordWriter {

    public AliyunADBRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        insertRecorder = new AliyunADBWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        updateRecorder = new AliyunADBWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        deleteRecorder = new AliyunADBWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
    }
}
