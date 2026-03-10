package io.tapdata.connector.klustron.target;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Consumer;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/6 15:03 Create
 * @description
 */
public class KunLunMateViewWriter extends KunLunWriter<KunLunMateViewWriter> {
    String group;
    List<TapRecordEvent> cacheEvents;
    Map<String, List<String>> indexKey;

    public KunLunMateViewWriter indexKey(Map<String, List<String>> indexKey) {
        this.indexKey = indexKey;
        return this;
    }

    public void writeRecord(TapConnectorContext connectorContext,
                            List<TapRecordEvent> tapRecordEvents,
                            TapTable tapTable,
                            Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        cacheEvents = new ArrayList<>();
        for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
            accept(tapRecordEvent, tapTable, writeListResultConsumer);
        }
        commit(cacheEvents, tapTable, writeListResultConsumer);
    }

    public void accept(TapRecordEvent e, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        String tableId = e.getTableId();
        String type;
        if (e instanceof TapInsertRecordEvent) {
            type = "i";
        } else if (e instanceof TapUpdateRecordEvent) {
            type = "u";
        } else if (e instanceof TapDeleteRecordEvent) {
            type = "d";
        } else {
            return;
        }
        String groupType = String.format("%s:%s", tableId, type);
        if (null != this.group && !groupType.equals(this.group)) {
            commit(cacheEvents, tapTable, writeListResultConsumer);
            cacheEvents = new ArrayList<>();
            this.group = groupType;
        }
        cacheEvents.add(e);
    }

    public void commit(List<TapRecordEvent> events, TapTable mateView, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        if (events == null || events.isEmpty() || mateView == null) {
            return;
        }
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        String mvQualified = getSchemaAndTable != null ? getSchemaAndTable.apply(mateView.getId()) : null;
        if (mvQualified == null || mvQualified.isEmpty()) {
            String schema = ofPgConfig.getSchema();
            String name = "\"" + mateView.getId() + "\"";
            mvQualified = (schema != null && !schema.isEmpty()) ? "\"" + schema + "\"." + name : name;
        }
        String tableId = events.get(0).getTableId();
        String srcQualified = getSchemaAndTable != null ? getSchemaAndTable.apply(tableId) : null;
        if (srcQualified == null || srcQualified.isEmpty()) {
            String schema = ofPgConfig.getSchema();
            String name = "\"" + tableId + "\"";
            srcQualified = (schema != null && !schema.isEmpty()) ? "\"" + schema + "\"." + name : name;
        }
        List<String> keys = indexKey.get(tableId);
        String dataChangeType;
        if (events.get(0) instanceof TapInsertRecordEvent) {
            dataChangeType = "INSERT";
            listResult.setInsertedCount(events.size());
        } else if (events.get(0) instanceof TapDeleteRecordEvent) {
            dataChangeType = "DELETE";
            listResult.setRemovedCount(events.size());
        } else if (events.get(0) instanceof TapUpdateRecordEvent) {
            dataChangeType = "UPDATE";
            listResult.setModifiedCount(events.size());
        } else {
            return;
        }
        List<Object[]> rows = new ArrayList<>();
        for (TapRecordEvent e : events) {
            if ("INSERT".equals(dataChangeType) && e instanceof TapInsertRecordEvent ie) {
                Map<String, Object> after = ie.getAfter();
                Object[] vals = new Object[keys.size()];
                for (int i = 0; i < keys.size(); i++) {
                    vals[i] = after.get(keys.get(i));
                }
                rows.add(vals);
            } else if ("DELETE".equals(dataChangeType) && e instanceof TapDeleteRecordEvent de) {
                Map<String, Object> before = de.getBefore();
                Object[] vals = new Object[keys.size()];
                for (int i = 0; i < keys.size(); i++) {
                    vals[i] = before.get(keys.get(i));
                }
                rows.add(vals);
            } else if ("UPDATE".equals(dataChangeType) && e instanceof TapUpdateRecordEvent ue) {
                Map<String, Object> after = ue.getAfter();
                Map<String, Object> before = ue.getBefore();
                Map<String, Object> chosen = (after != null && !after.isEmpty()) ? after : before;
                Object[] vals = new Object[keys.size()];
                for (int i = 0; i < keys.size(); i++) {
                    vals[i] = chosen.get(keys.get(i));
                }
                rows.add(vals);
            }
        }
        if (rows.isEmpty()) {
            return;
        }
        StringBuilder colsBuilder = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                colsBuilder.append("\",\"");
            }
            colsBuilder.append(keys.get(i));
        }
        String cols = "\"" + colsBuilder + "\"";
        StringBuilder sql = new StringBuilder();
        sql.append("REFRESH MATERIALIZED VARYING VIEW ").append(mvQualified)
                .append(' ').append(dataChangeType).append(' ')
                .append(srcQualified).append(' ')
                .append('(').append(cols).append(')')
                .append(" VALUE ");
        StringBuilder tupleBuilder = new StringBuilder("(");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                tupleBuilder.append(',');
            }
            tupleBuilder.append('?');
        }
        tupleBuilder.append(')');
        for (int i = 0; i < rows.size(); i++) {
            Object[] value = rows.get(i);
            sql.append("(");
            StringJoiner v = new StringJoiner(",");
            for (int index = 0; index < value.length; index++) {
                if (value[index] instanceof String) {
                    v.add("'" + value[index] + "'");
                } else {
                    v.add(value[index] == null ? "null" : value[index].toString());
                }
            }
            sql.append(v).append(")");
            if (i < rows.size() - 1) {
                sql.append(',');
            }
        }
        try (Connection connection = pgJdbcContext.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql.toString());
            writeListResultConsumer.accept(listResult);
        } catch (SQLException e) {
            throw new CoreException("Refresh materialized varying view failed: {}", e.getMessage(), e);
        }
    }
}
