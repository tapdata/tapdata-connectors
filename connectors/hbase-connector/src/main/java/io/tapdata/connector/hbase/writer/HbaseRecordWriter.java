package io.tapdata.connector.hbase.writer;

import io.tapdata.connector.hbase.HbaseConfig;
import io.tapdata.connector.hbase.HbaseContext;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class HbaseRecordWriter {

    private static final String ROW_KEY_FIELD = "row_key";

    private final HbaseContext context;
    private final HbaseConfig config;

    public HbaseRecordWriter(HbaseContext context, HbaseConfig config) {
        this.context = context;
        this.config = config;
    }

    public void write(List<TapRecordEvent> events, TapTable table,
                      Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        long insertCount = 0, updateCount = 0, deleteCount = 0;

        List<String> columnFamilyNames = getColumnFamilyNames(table);

        try (Table hTable = context.getConnection().getTable(TableName.valueOf(table.getId()))) {
            for (TapRecordEvent event : events) {
                try {
                    if (event instanceof TapInsertRecordEvent) {
                        processInsert((TapInsertRecordEvent) event, hTable, columnFamilyNames);
                        insertCount++;
                    } else if (event instanceof TapUpdateRecordEvent) {
                        processUpdate((TapUpdateRecordEvent) event, hTable, columnFamilyNames);
                        updateCount++;
                    } else if (event instanceof TapDeleteRecordEvent) {
                        processDelete((TapDeleteRecordEvent) event, hTable);
                        deleteCount++;
                    }
                } catch (Exception e) {
                    listResult.addError(event, e);
                }
            }
        }

        listResult.insertedCount(insertCount)
                .modifiedCount(updateCount)
                .removedCount(deleteCount);
        consumer.accept(listResult);
    }

    private void processInsert(TapInsertRecordEvent event, Table hTable, List<String> columnFamilyNames) throws Exception {
        Map<String, Object> after = event.getAfter();
        if (after == null) {
            return;
        }
        String rowKey = String.valueOf(after.get(ROW_KEY_FIELD));
        if (rowKey == null || "null".equals(rowKey)) {
            return;
        }

        Put put = new Put(Bytes.toBytes(rowKey));
        for (String cf : columnFamilyNames) {
            Object cfValue = after.get(cf);
            if (cfValue instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> qualifiers = (Map<String, Object>) cfValue;
                for (Map.Entry<String, Object> entry : qualifiers.entrySet()) {
                    String value = entry.getValue() != null ? entry.getValue().toString() : "";
                    put.addColumn(
                            Bytes.toBytes(cf),
                            Bytes.toBytes(entry.getKey()),
                            Bytes.toBytes(value)
                    );
                }
            } else if (cfValue != null) {
                put.addColumn(
                        Bytes.toBytes(cf),
                        Bytes.toBytes("value"),
                        Bytes.toBytes(cfValue.toString())
                );
            }
        }
        hTable.put(put);
    }

    private void processUpdate(TapUpdateRecordEvent event, Table hTable, List<String> columnFamilyNames) throws Exception {
        // HBase Put acts as upsert, so update is the same as insert
        Map<String, Object> after = event.getAfter();
        if (after == null) {
            return;
        }
        String rowKey = String.valueOf(after.get(ROW_KEY_FIELD));
        if (rowKey == null || "null".equals(rowKey)) {
            return;
        }

        Put put = new Put(Bytes.toBytes(rowKey));
        for (String cf : columnFamilyNames) {
            Object cfValue = after.get(cf);
            if (cfValue instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> qualifiers = (Map<String, Object>) cfValue;
                for (Map.Entry<String, Object> entry : qualifiers.entrySet()) {
                    String value = entry.getValue() != null ? entry.getValue().toString() : "";
                    put.addColumn(
                            Bytes.toBytes(cf),
                            Bytes.toBytes(entry.getKey()),
                            Bytes.toBytes(value)
                    );
                }
            } else if (cfValue != null) {
                put.addColumn(
                        Bytes.toBytes(cf),
                        Bytes.toBytes("value"),
                        Bytes.toBytes(cfValue.toString())
                );
            }
        }
        hTable.put(put);
    }

    private void processDelete(TapDeleteRecordEvent event, Table hTable) throws Exception {
        Map<String, Object> before = event.getBefore();
        if (before == null) {
            return;
        }
        String rowKey = String.valueOf(before.get(ROW_KEY_FIELD));
        if (rowKey == null || "null".equals(rowKey)) {
            return;
        }

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        hTable.delete(delete);
    }

    private List<String> getColumnFamilyNames(TapTable table) {
        List<String> families = new ArrayList<>();
        if (table != null && table.getNameFieldMap() != null) {
            for (Map.Entry<String, TapField> entry : table.getNameFieldMap().entrySet()) {
                if (!ROW_KEY_FIELD.equals(entry.getKey())) {
                    families.add(entry.getKey());
                }
            }
        }
        return families;
    }
}
