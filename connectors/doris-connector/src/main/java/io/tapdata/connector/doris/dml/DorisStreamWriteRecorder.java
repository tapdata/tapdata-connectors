package io.tapdata.connector.doris.dml;

import io.tapdata.common.dml.NormalWriteRecorder;
import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;

public class DorisStreamWriteRecorder extends NormalWriteRecorder {

    public DorisStreamWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

}
