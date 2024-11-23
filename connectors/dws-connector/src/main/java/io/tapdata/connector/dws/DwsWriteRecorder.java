package io.tapdata.connector.dws;

import io.tapdata.connector.postgres.dml.PostgresWriteRecorder;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class DwsWriteRecorder extends PostgresWriteRecorder {

    private String replaceBlank = null;
    private boolean hasUnique = false;

    public DwsWriteRecorder(Connection connection, TapTable tapTable, String schema) {
        super(connection, tapTable, schema);
    }

    public DwsWriteRecorder replaceBlank(String replaceBlank) {
        this.replaceBlank = replaceBlank;
        return this;
    }

    public DwsWriteRecorder hasUnique(boolean hasUnique) {
        this.hasUnique = hasUnique;
        return this;
    }

    @Override
    protected void upsert(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (allColumn.size() == updatedColumn.size() && allColumn.size() == uniqueCondition.size()) {
            insertIgnore(after, listResult);
        } else {
            if (hasUnique) {
                super.upsert(after, listResult);
            } else {
                super.oldUpsert(after, listResult);
            }
        }
    }

    @Override
    protected Object filterValue(Object value, String dataType) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof String && EmptyKit.isEmpty((String) value)) {
            return replaceBlank;
        }
        return super.filterValue(value, dataType);
    }

}
