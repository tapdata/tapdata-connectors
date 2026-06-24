package io.tapdata.connector.hbase.writer;

import io.tapdata.connector.hbase.HbaseConfig;
import io.tapdata.connector.hbase.HbaseContext;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class HbaseRecordWriter {

    private static final Logger logger = LoggerFactory.getLogger(HbaseRecordWriter.class);
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

        String columnFamily = config.getColumnFamily();
        byte[] cfBytes = Bytes.toBytes(columnFamily);

        // Parallel lists: batch.get(i) is the Row for batchEvents.get(i)
        List<Row> batch = new ArrayList<>();
        List<TapRecordEvent> batchEvents = new ArrayList<>();

        try (Table hTable = context.getConnection().getTable(TableName.valueOf(table.getId()))) {
            for (TapRecordEvent event : events) {
                try {
                    if (event instanceof TapInsertRecordEvent) {
                        Put put = buildPut(cfBytes, ((TapInsertRecordEvent) event).getAfter());
                        if (put != null) {
                            batch.add(put);
                            batchEvents.add(event);
                        }
                    } else if (event instanceof TapUpdateRecordEvent) {
                        Put put = buildPut(cfBytes, ((TapUpdateRecordEvent) event).getAfter());
                        if (put != null) {
                            batch.add(put);
                            batchEvents.add(event);
                        }
                    } else if (event instanceof TapDeleteRecordEvent) {
                        String rowKey = extractRowKey(((TapDeleteRecordEvent) event).getBefore());
                        if (rowKey != null) {
                            batch.add(new Delete(Bytes.toBytes(rowKey)));
                            batchEvents.add(event);
                        } else {
                            logger.warn("Skipping delete event with missing row_key");
                        }
                    }
                } catch (Exception e) {
                    listResult.addError(event, e);
                }

                if (batch.size() >= config.getWriteBatchSize()) {
                    BatchResult br = flushBatch(hTable, batch, batchEvents);
                    insertCount += br.inserted;
                    updateCount += br.updated;
                    deleteCount += br.deleted;
                    for (int i = 0; i < br.errors.size(); i++) {
                        listResult.addError(br.errors.get(i), br.errorCauses.get(i));
                    }
                    batch.clear();
                    batchEvents.clear();
                }
            }

            // Flush remaining
            if (!batch.isEmpty()) {
                BatchResult br = flushBatch(hTable, batch, batchEvents);
                insertCount += br.inserted;
                updateCount += br.updated;
                deleteCount += br.deleted;
                for (int i = 0; i < br.errors.size(); i++) {
                    listResult.addError(br.errors.get(i), br.errorCauses.get(i));
                }
            }
        }

        listResult.insertedCount(insertCount)
                .modifiedCount(updateCount)
                .removedCount(deleteCount);
        consumer.accept(listResult);
    }

    /**
     * Build a Put for the given row data. All non-row-key fields are stored as qualifiers
     * within the configured column family. Null values are skipped (no qualifier written)
     * to avoid incorrectly overwriting existing data with empty strings.
     */
    private Put buildPut(byte[] cfBytes, Map<String, Object> data) {
        if (data == null) {
            return null;
        }
        String rowKey = extractRowKey(data);
        if (rowKey == null) {
            logger.warn("Skipping insert/update event with missing row_key");
            return null;
        }

        Put put = new Put(Bytes.toBytes(rowKey));
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldName = entry.getKey();
            if (ROW_KEY_FIELD.equals(fieldName)) {
                continue;
            }
            if (entry.getValue() == null) {
                continue; // Skip null values — don't overwrite existing data with empty string
            }
            put.addColumn(cfBytes, Bytes.toBytes(fieldName), Bytes.toBytes(entry.getValue().toString()));
        }
        return put;
    }

    /**
     * Flush a batch of ordered Row actions via Table.batch(). Uses the results array to
     * determine per-row success/failure, so counts are accurate and errors carry the correct
     * event reference.
     */
    private BatchResult flushBatch(Table hTable, List<Row> batch, List<TapRecordEvent> batchEvents) {
        BatchResult result = new BatchResult();
        Object[] results = new Object[batch.size()];

        try {
            hTable.batch(batch, results);
        } catch (Exception e) {
            // Catastrophic batch failure — all rows in this batch are errors
            logger.error("Batch write failed for {} mutations: {}", batch.size(), e.getMessage());
            for (TapRecordEvent event : batchEvents) {
                result.addError(event, e);
            }
            return result;
        }

        // Per-row success/failure from the results array
        for (int i = 0; i < results.length; i++) {
            TapRecordEvent event = batchEvents.get(i);
            if (results[i] == null || results[i] instanceof org.apache.hadoop.hbase.client.Result) {
                // Success: count by original event type
                if (event instanceof TapInsertRecordEvent) {
                    result.inserted++;
                } else if (event instanceof TapUpdateRecordEvent) {
                    result.updated++;
                } else if (event instanceof TapDeleteRecordEvent) {
                    result.deleted++;
                }
            } else if (results[i] instanceof Throwable) {
                result.addError(event, (Throwable) results[i]);
            }
        }

        return result;
    }

    private String extractRowKey(Map<String, Object> data) {
        if (data == null) {
            return null;
        }
        Object rowKeyObj = data.get(ROW_KEY_FIELD);
        if (rowKeyObj == null) {
            return null;
        }
        String rowKey = String.valueOf(rowKeyObj);
        if (rowKey.isEmpty()) {
            return null;
        }
        return rowKey;
    }

    /**
     * Internal result holder for a single batch flush.
     */
    private static class BatchResult {
        long inserted;
        long updated;
        long deleted;
        final List<TapRecordEvent> errors = new ArrayList<>();
        final List<Throwable> errorCauses = new ArrayList<>();

        void addError(TapRecordEvent event, Throwable cause) {
            errors.add(event);
            errorCauses.add(cause);
        }
    }
}
