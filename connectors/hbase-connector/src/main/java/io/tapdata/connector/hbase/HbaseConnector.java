package io.tapdata.connector.hbase;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.hbase.reader.HbaseBatchReader;
import io.tapdata.connector.hbase.writer.HbaseRecordWriter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

@TapConnectorClass("spec.json")
public class HbaseConnector extends ConnectorBase {

    private static final String ROW_KEY_FIELD = "row_key";

    private HbaseConfig hbaseConfig;
    private HbaseContext hbaseContext;
    private final AtomicBoolean started = new AtomicBoolean(false);

    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        if (started.compareAndSet(false, true)) {
            hbaseConfig = new HbaseConfig().load(connectionContext.getConnectionConfig());
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
        Admin admin = hbaseContext.getAdmin();
        List<TableName> tableNames;
        if (EmptyKit.isNotEmpty(tables)) {
            tableNames = new ArrayList<>();
            for (String table : tables) {
                tableNames.add(TableName.valueOf(table));
            }
        } else {
            tableNames = java.util.Arrays.asList(admin.listTableNames());
        }

        List<TapTable> tapTableList = new ArrayList<>();
        for (TableName tableName : tableNames) {
            TapTable tapTable = table(tableName.getNameAsString());
            tapTable.add(field(ROW_KEY_FIELD, "string").isPrimaryKey(true).pos(0));

            try {
                HTableDescriptor descriptor = admin.getTableDescriptor(tableName);
                int pos = 1;
                for (HColumnDescriptor columnFamily : descriptor.getColumnFamilies()) {
                    String cfName = columnFamily.getNameAsString();
                    tapTable.add(field(cfName, "string").pos(pos++));
                }
            } catch (Exception e) {
                tapTable.add(field("cf", "string").pos(1));
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

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        HbaseConfig testConfig = new HbaseConfig().load(connectionContext.getConnectionConfig());
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
        Admin admin = hbaseContext.getAdmin();
        return admin.listTableNames().length;
    }

    private void batchRead(TapConnectorContext connectorContext, TapTable table, Object offsetState,
                           int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        HbaseBatchReader batchReader = new HbaseBatchReader(hbaseContext, hbaseConfig);
        batchReader.read(connectorContext, table, eventBatchSize, eventsOffsetConsumer);
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
        Admin admin = hbaseContext.getAdmin();

        if (admin.tableExists(tableName)) {
            CreateTableOptions createTableOptions = new CreateTableOptions();
            createTableOptions.setTableExists(true);
            return createTableOptions;
        }

        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        Map<String, TapField> fieldMap = table.getNameFieldMap();
        if (fieldMap != null) {
            for (Map.Entry<String, TapField> entry : fieldMap.entrySet()) {
                if (!ROW_KEY_FIELD.equals(entry.getKey())) {
                    HColumnDescriptor columnDescriptor = new HColumnDescriptor(entry.getKey());
                    columnDescriptor.setMaxVersions(1);
                    descriptor.addFamily(columnDescriptor);
                }
            }
        }

        // If no column families defined, add a default one
        if (descriptor.getColumnFamilies().length == 0) {
            descriptor.addFamily(new HColumnDescriptor("cf"));
        }

        admin.createTable(descriptor);

        CreateTableOptions createTableOptions = new CreateTableOptions();
        createTableOptions.setTableExists(false);
        return createTableOptions;
    }

    private void dropTable(TapConnectorContext tapConnectorContext, TapDropTableEvent tapDropTableEvent) throws Throwable {
        TableName tableName = TableName.valueOf(tapDropTableEvent.getTableId());
        Admin admin = hbaseContext.getAdmin();

        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
    }

    private void getTableNames(TapConnectionContext connectionContext, int batchSize, Consumer<List<String>> listConsumer) throws Throwable {
        Admin admin = hbaseContext.getAdmin();
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
