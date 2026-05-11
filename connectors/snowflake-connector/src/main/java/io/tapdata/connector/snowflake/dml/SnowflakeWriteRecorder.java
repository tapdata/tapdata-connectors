package io.tapdata.connector.snowflake.dml;

import io.tapdata.common.dml.MergeWriteRecorder;
import io.tapdata.entity.schema.TapTable;

import java.sql.Connection;

public class SnowflakeWriteRecorder extends MergeWriteRecorder {

    public SnowflakeWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    protected String getSystemVirtualTable() {
        return "(select 1)";
    }

}
