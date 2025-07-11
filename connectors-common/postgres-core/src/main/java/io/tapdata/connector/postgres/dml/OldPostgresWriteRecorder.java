package io.tapdata.connector.postgres.dml;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class OldPostgresWriteRecorder extends PostgresWriteRecorder {

    public OldPostgresWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    @Override
    protected void upsert(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        oldUpsert(after, listResult);
    }

    @Override
    protected void insertIgnore(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        oldInsertIgnore(after, listResult);
    }
}
