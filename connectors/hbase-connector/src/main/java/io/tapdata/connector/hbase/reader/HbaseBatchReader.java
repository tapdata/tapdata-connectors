package io.tapdata.connector.hbase.reader;

import io.tapdata.connector.hbase.HbaseConfig;
import io.tapdata.connector.hbase.HbaseContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.BiConsumer;

public class HbaseBatchReader {

    private static final Logger logger = LoggerFactory.getLogger(HbaseBatchReader.class);
    private static final String ROW_KEY_FIELD = "row_key";

    private final HbaseContext context;
    private final HbaseConfig config;

    public HbaseBatchReader(HbaseContext context, HbaseConfig config) {
        this.context = context;
        this.config = config;
    }

    public void read(TapConnectorContext connectorContext, TapTable table, Object offsetState,
                     int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        String defaultCF = config.getColumnFamily();
        List<String> columnFamilies = getColumnFamiliesFromHBase(table);
        List<TapEvent> eventList = new ArrayList<>();
        String lastRowKey = null;

        try (Table hTable = context.getConnection().getTable(TableName.valueOf(table.getId()))) {
            Scan scan = new Scan();
            scan.setCaching(config.getScanCaching());
            scan.setBatch(config.getScanBatch());
            scan.setCacheBlocks(false);

            // Resume from checkpoint if offset is provided
            if (offsetState instanceof String && !((String) offsetState).isEmpty()) {
                String offsetKey = (String) offsetState;
                scan.withStartRow(Bytes.toBytes(offsetKey), false);
                logger.debug("Resuming scan from row key: {}", offsetKey);
            }

            for (String family : columnFamilies) {
                scan.addFamily(Bytes.toBytes(family));
            }

            try (ResultScanner scanner = hTable.getScanner(scan)) {
                for (Result result : scanner) {
                    if (result.isEmpty()) {
                        continue;
                    }

                    String rowKey = Bytes.toString(result.getRow());
                    lastRowKey = rowKey;
                    DataMap after = new DataMap();
                    after.put(ROW_KEY_FIELD, rowKey);

                    for (String family : columnFamilies) {
                        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(family));
                        if (familyMap == null || familyMap.isEmpty()) {
                            continue;
                        }

                        if (isDefaultColumnFamily(family, defaultCF)) {
                            // Default CF: expose qualifiers as individual string fields
                            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                                after.put(
                                        Bytes.toString(entry.getKey()),
                                        Bytes.toString(entry.getValue())
                                );
                            }
                        } else {
                            // Non-default CF: expose as a nested map field (backward compatible)
                            DataMap qualifierMap = new DataMap();
                            for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                                qualifierMap.put(
                                        Bytes.toString(entry.getKey()),
                                        Bytes.toString(entry.getValue())
                                );
                            }
                            after.put(family, qualifierMap);
                        }
                    }

                    TapInsertRecordEvent recordEvent = new TapInsertRecordEvent()
                            .init()
                            .table(table.getId())
                            .after(after);
                    eventList.add(recordEvent);

                    if (eventList.size() >= eventBatchSize) {
                        eventsOffsetConsumer.accept(new ArrayList<>(eventList), lastRowKey);
                        eventList.clear();
                    }
                }
            }
        }

        if (!eventList.isEmpty()) {
            eventsOffsetConsumer.accept(eventList, lastRowKey);
        }
    }

    public long count(TapConnectorContext connectorContext, TapTable table) throws Throwable {
        long rowCount = 0;
        try (Table hTable = context.getConnection().getTable(TableName.valueOf(table.getId()))) {
            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter());
            scan.setCaching(500);

            try (ResultScanner scanner = hTable.getScanner(scan)) {
                for (Result ignored : scanner) {
                    rowCount++;
                }
            }
        }
        return rowCount;
    }

    /**
     * A column family is the "default" if it matches the configured CF name.
     * Qualifiers in the default CF are exposed as individual fields;
     * qualifiers in other CFs are exposed as nested map fields.
     */
    private boolean isDefaultColumnFamily(String family, String configuredCF) {
        return configuredCF.equals(family);
    }

    /**
     * Discover actual column families from HBase table metadata.
     * Falls back to TapTable field names if the descriptor cannot be read.
     */
    private List<String> getColumnFamiliesFromHBase(TapTable table) {
        try (Admin admin = context.getAdmin()) {
            TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(table.getId()));
            List<String> families = new ArrayList<>();
            for (ColumnFamilyDescriptor cf : descriptor.getColumnFamilies()) {
                families.add(cf.getNameAsString());
            }
            if (!families.isEmpty()) {
                return families;
            }
        } catch (IOException e) {
            logger.warn("Failed to get table descriptor for {}, falling back to TapTable fields: {}",
                    table.getId(), e.getMessage());
        }
        // Fallback: use TapTable field names (backward compatible)
        List<String> families = new ArrayList<>();
        if (table.getNameFieldMap() != null) {
            for (Map.Entry<String, TapField> entry : table.getNameFieldMap().entrySet()) {
                if (!ROW_KEY_FIELD.equals(entry.getKey())) {
                    families.add(entry.getKey());
                }
            }
        }
        return families;
    }
}
