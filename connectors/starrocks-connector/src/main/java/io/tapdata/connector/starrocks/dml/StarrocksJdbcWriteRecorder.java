package io.tapdata.connector.starrocks.dml;

import io.tapdata.common.dml.NormalWriteRecorder;
import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;

public class StarrocksJdbcWriteRecorder extends NormalWriteRecorder {

    public StarrocksJdbcWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

}
