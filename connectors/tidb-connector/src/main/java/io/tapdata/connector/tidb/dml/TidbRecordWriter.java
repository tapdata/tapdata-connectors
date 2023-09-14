package io.tapdata.connector.tidb.dml;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.entity.schema.TapTable;

import java.sql.SQLException;

/**
 * @author lemon
 */
public class TidbRecordWriter extends NormalRecordWriter {

    public TidbRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
        insertRecorder = new TidbWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        updateRecorder = new TidbWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
        deleteRecorder = new TidbWriteRecorder(connection, tapTable, jdbcContext.getConfig().getDatabase());
    }

}
