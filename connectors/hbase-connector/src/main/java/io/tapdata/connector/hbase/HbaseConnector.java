package io.tapdata.connector.hbase;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.hbase.reader.HbaseBatchReader;
import io.tapdata.connector.hbase.writer.HbaseRecordWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class HbaseConnector extends ConnectorBase {

    private static final Logger logger = LoggerFactory.getLogger(HbaseConnector.class);
    private static final String ROW_KEY_FIELD = "row_key";
    private static final int SCHEMA_SAMPLE_MAX_ROWS = 5;

    private HbaseConfig hbaseConfig;
    private HbaseContext hbaseContext;
    private final AtomicBoolean started = new AtomicBoolean(false);

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        if (started.compareAndSet(false, true)) {
            hbaseConfig = new HbaseConfig().load(connectionContext.getConnectionConfig());
            hbaseConfig.validate();
            hbaseContext = new HbaseContext(hbaseConfig);
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        if (started.compareAndSet(true, false)) {
            if (hbaseContext != null) {
                hbaseContext.close();
            }
        }
    }

    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTableV2);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);

        codecRegistry.registerFromTapValue(TapTimeValue.class, "string", tapTimeValue ->
                formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue ->
                tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, "string", tapDateValue ->
                formatTapDateTime(tapDateValue.getValue(), "yyyy-MM-dd"));
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize,
                               Consumer<List<TapTable>> consumer) throws Throwable {
        List<TableName> tableNames;
        try (Admin admin = hbaseContext.getAdmin()) {
            if (EmptyKit.isNotEmpty(tables)) {
                tableNames = new ArrayList<>();
                for (String table : tables) {
                    tableNames.add(TableName.valueOf(table));
                }
            } else {
                tableNames = Arrays.asList(admin.listTableNames());
            }

            String defaultCF = hbaseConfig.getColumnFamily();
            List<TapTable> tapTableList = new ArrayList<>();
            for (TableName tableName : tableNames) {
                TapTable tapTable = table(tableName.getNameAsString());
                tapTable.add(field(ROW_KEY_FIELD, "string").isPrimaryKey(true).pos(0));

                try {
                    TableDescriptor descriptor = admin.getDescriptor(tableName);
                    List<ColumnFamilyDescriptor> columnFamilies = Arrays.asList(descriptor.getColumnFamilies());
                    int pos = 1;

                    for (ColumnFamilyDescriptor columnFamily : columnFamilies) {
                        String cfName = columnFamily.getNameAsString();

                        if (cfName.equals(defaultCF)) {
                            // Default CF: sample rows to discover qualifier names as individual fields.
                            // This keeps discoverSchema consistent with batchRead, which flattens
                            // qualifiers from the default CF into top-level fields.
                            Set<String> qualifiers = sampleQualifiers(tableName, cfName);
                            if (!qualifiers.isEmpty()) {
                                for (String qual : qualifiers) {
                                    tapTable.add(field(qual, "string").pos(pos++));
                                }
                            }
                            // Empty table: no fallback field — schema will be discovered
                            // dynamically when data is first written
                        } else {
                            // Non-default CF: represented as a field containing a JSON map of qualifiers
                            tapTable.add(field(cfName, "string").pos(pos++));
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Failed to get table descriptor for {}, using default '{}' column family: {}",
                            tableName.getNameAsString(), defaultCF, e.getMessage());
                    tapTable.add(field(defaultCF, "string").pos(1));
                }

                tapTableList.add(tapTable);

                if (tapTableList.size() >= tableSize) {
                    consumer.accept(tapTableList);
                    tapTableList = new ArrayList<>();
                }
            }

            if (!tapTableList.isEmpty()) {
                consumer.accept(tapTableList);
            }
        }
    }

    /**
     * Sample a few rows from the given table/CF to discover qualifier names.
     * This is the standard weak-schema approach for NoSQL data sources like HBase.
     */
    private Set<String> sampleQualifiers(TableName tableName, String cfName) {
        Set<String> qualifiers = new LinkedHashSet<>();
        try (Table hTable = hbaseContext.getConnection().getTable(tableName)) {
            Scan scan = new Scan();
            scan.addFamily(Bytes.toBytes(cfName));
            scan.setCaching(1);
            try (ResultScanner scanner = hTable.getScanner(scan)) {
                int sampled = 0;
                for (Result result : scanner) {
                    if (sampled++ >= SCHEMA_SAMPLE_MAX_ROWS) break;
                    NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(cfName));
                    if (familyMap != null) {
                        for (byte[] qual : familyMap.keySet()) {
                            qualifiers.add(Bytes.toString(qual));
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Failed to sample qualifiers for table {}: {}", tableName.getNameAsString(), e.getMessage());
        }
        return qualifiers;
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        HbaseConfig testConfig = new HbaseConfig().load(connectionContext.getConnectionConfig());
        testConfig.validate();
        HbaseTest hbaseTest = new HbaseTest(testConfig);

        try {
            consumer.accept(hbaseTest.testHostPort());
            consumer.accept(hbaseTest.testConnect());
        } finally {
            hbaseTest.close();
        }

        return ConnectionOptions.create().connectionString(testConfig.getZookeeperQuorum());
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        try (Admin admin = hbaseContext.getAdmin()) {
            return admin.listTableNames().length;
        }
    }

    private void batchRead(TapConnectorContext connectorContext, TapTable table, Object offsetState,
                           int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        HbaseBatchReader batchReader = new HbaseBatchReader(hbaseContext, hbaseConfig);
        batchReader.read(connectorContext, table, offsetState, eventBatchSize, eventsOffsetConsumer);
    }

    private long batchCount(TapConnectorContext connectorContext, TapTable table) throws Throwable {
        HbaseBatchReader batchReader = new HbaseBatchReader(hbaseContext, hbaseConfig);
        return batchReader.count(connectorContext, table);
    }

    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents,
                             TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        HbaseRecordWriter recordWriter = new HbaseRecordWriter(hbaseContext, hbaseConfig);
        recordWriter.write(tapRecordEvents, tapTable, consumer);
    }

    private CreateTableOptions createTableV2(TapConnectorContext tapConnectorContext, TapCreateTableEvent tapCreateTableEvent) throws Throwable {
        TapTable table = tapCreateTableEvent.getTable();
        TableName tableName = TableName.valueOf(table.getId());

        try (Admin admin = hbaseContext.getAdmin()) {
            if (admin.tableExists(tableName)) {
                CreateTableOptions createTableOptions = new CreateTableOptions();
                createTableOptions.setTableExists(true);
                return createTableOptions;
            }

            // Create table with a single configurable column family.
            // Source fields will be stored as qualifiers within this CF,
            // following HBase best practice of 2-3 column families per table.
            String cfName = hbaseConfig.getColumnFamily();
            TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(
                            ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cfName))
                                    .setMaxVersions(1)
                                    .build())
                    .build();
            admin.createTable(descriptor);

            CreateTableOptions createTableOptions = new CreateTableOptions();
            createTableOptions.setTableExists(false);
            return createTableOptions;
        }
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
        TableName tableName = TableName.valueOf(tapDropTableEvent.getTableId());

        try (Admin admin = hbaseContext.getAdmin()) {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        }
    }

    private void getTableNames(TapConnectionContext connectionContext, int batchSize, Consumer<List<String>> listConsumer) throws Throwable {
        try (Admin admin = hbaseContext.getAdmin()) {
            List<String> tableNames = new ArrayList<>();
            for (TableName tableName : admin.listTableNames()) {
                tableNames.add(tableName.getNameAsString());
                if (tableNames.size() >= batchSize) {
                    listConsumer.accept(tableNames);
                    tableNames = new ArrayList<>();
                }
            }
            if (!tableNames.isEmpty()) {
                listConsumer.accept(tableNames);
            }
        }
    }
}
