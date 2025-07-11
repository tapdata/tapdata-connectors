package io.tapdata.connector.tdengine;

import io.tapdata.common.dml.NormalWriteRecorder;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TDengineWriteRecorder extends NormalWriteRecorder {

    private final String timestampField;

    public TDengineWriteRecorder(Connection connection, TapTable tapTable, String schema, String timestampField) {
        super(connection, tapTable, schema);
        this.timestampField = timestampField;
    }

    @Override
    public void addInsertBatch(Map<String, Object> after, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(after)) {
            return;
        }
        justInsert(after);
        preparedStatement.addBatch();
    }

    //just insert
    protected void justInsert(Map<String, Object> after) throws SQLException {
        if (EmptyKit.isNull(preparedStatement)) {
            String insertSql = "INSERT INTO `" + schema + "`.`" + tapTable.getId() + "` ("
                    + allColumn.stream().map(k -> "`" + k + "`").collect(Collectors.joining(", ")) + ") " +
                    "VALUES(" + StringKit.copyString("?", allColumn.size(), ",") + ") ";
            preparedStatement = connection.prepareStatement(insertSql);
        }
        preparedStatement.clearParameters();
        int pos = 1;
        for (String key : allColumn) {
            preparedStatement.setObject(pos++, after.get(key));
        }
    }

    @Override
    public void addUpdateBatch(Map<String, Object> after, Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) {

    }

    @Override
    public void addDeleteBatch(Map<String, Object> before, WriteListResult<TapRecordEvent> listResult) throws SQLException {
        if (EmptyKit.isEmpty(before)) {
            return;
        }
        if (EmptyKit.isBlank(timestampField) || Objects.isNull(before.get(timestampField))) {
            return;
        }
        if (EmptyKit.isNull(preparedStatement)) {
            if (EmptyKit.isNotBlank(timestampField)) {
                preparedStatement = connection.prepareStatement(String.format("DELETE FROM `%s`.`%s` WHERE `%s`='%s'",
                        schema, tapTable.getId(), timestampField, before.get(timestampField)));
            }
        }
        preparedStatement.clearParameters();
//        dealNullBefore(before, 1);
        preparedStatement.addBatch();
    }
}
