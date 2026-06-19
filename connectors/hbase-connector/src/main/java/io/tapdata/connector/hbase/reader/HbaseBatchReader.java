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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.function.BiConsumer;

public class HbaseBatchReader {

    private static final String ROW_KEY_FIELD = "row_key";

    private final HbaseContext context;
    private final HbaseConfig config;

    public HbaseBatchReader(HbaseContext context, HbaseConfig config) {
        this.context = context;
        this.config = config;
    }

    public void read(TapConnectorContext connectorContext, TapTable table, int eventBatchSize,
                     BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        List<String> columnFamilies = getColumnFamiliesFromTable(table);
        List<TapEvent> eventList = new ArrayList<>();

        try (Table hTable = context.getConnection().getTable(TableName.valueOf(table.getId()))) {
            Scan scan = new Scan();
            scan.setCaching(config.getScanCaching());
            scan.setBatch(config.getScanBatch());
            scan.setCacheBlocks(false);

            for (String family : columnFamilies) {
                scan.addFamily(Bytes.toBytes(family));
            }

            try (ResultScanner scanner = hTable.getScanner(scan)) {
                for (Result result : scanner) {
                    if (result.isEmpty()) {
                        continue;
                    }

                    DataMap after = new DataMap();
                    after.put(ROW_KEY_FIELD, Bytes.toString(result.getRow()));

                    for (String family : columnFamilies) {
                        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(family));
                        if (familyMap != null && !familyMap.isEmpty()) {
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
                        eventsOffsetConsumer.accept(new ArrayList<>(eventList), null);
                        eventList.clear();
                    }
                }
            }
        }

        if (!eventList.isEmpty()) {
            eventsOffsetConsumer.accept(eventList, null);
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

    private List<String> getColumnFamiliesFromTable(TapTable table) {
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
