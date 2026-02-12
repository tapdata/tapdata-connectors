package io.tapdata.connector.paimon;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.service.PaimonService;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.collections4.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Paimon Connector for Tapdata
 * Supports write operations to Apache Paimon tables
 *
 * @author Tapdata
 */
@TapConnectorClass("spec.json")
public class PaimonConnector extends ConnectorBase {

    private static final String TAG = PaimonConnector.class.getSimpleName();
    
    private PaimonConfig paimonConfig;
    private PaimonService paimonService;

    /**
     * Initialize connection when connector starts
     *
     * @param connectionContext connection context
     * @throws Throwable if initialization fails
     */
    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        DataMap connectionConfig = connectionContext.getConnectionConfig();
        DataMap nodeConfig = connectionContext.getNodeConfig();
        if (MapUtils.isEmpty(connectionConfig)) {
            throw new RuntimeException("Connection config cannot be empty");
        }
        
        // Load configuration
        paimonConfig = new PaimonConfig().load(connectionConfig);
        if (MapUtils.isNotEmpty(nodeConfig)) {
            nodeConfig.remove("database");
            paimonConfig.load(nodeConfig);
        }

        // Initialize Paimon service
        paimonService = new PaimonService(paimonConfig);
        paimonService.init();
        
        connectionContext.getLog().info("Paimon connector started successfully");
    }

    /**
     * Clean up resources when connector stops
     *
     * @param connectionContext connection context
     */
    @Override
    public void onStop(TapConnectionContext connectionContext) {
        if (paimonService != null) {
            try {
                paimonService.close();
                paimonService = null;
            } catch (Exception e) {
                connectionContext.getLog().warn("Error closing Paimon service: " + e.getMessage(), e);
            }
        }
    }

    /**
     * Test connection to Paimon
     *
     * @param connectionContext connection context
     * @param consumer test result consumer
     * @return connection options
     * @throws Throwable if test fails
     */
    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        
        try {
            onStart(connectionContext);
            
            // Test warehouse accessibility
            boolean warehouseAccessible = paimonService.testWarehouseAccess();
            if (warehouseAccessible) {
                consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, 
                    "Warehouse is accessible"));
            } else {
                consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, 
                    "Cannot access warehouse"));
                return connectionOptions;
            }
            
            // Test write permission
            boolean canWrite = paimonService.testWritePermission();
            if (canWrite) {
                consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, 
                    "Write permission verified"));
            } else {
                consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, 
                    "No write permission"));
            }
            
        } catch (Throwable throwable) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, 
                "Connection test failed: " + throwable.getMessage()));
            throw throwable;
        } finally {
            onStop(connectionContext);
        }
        
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return paimonService.getTableCount();
    }

    /**
     * Discover schema - load table definitions from Paimon
     *
     * @param connectionContext connection context
     * @param tables list of table names to discover (null for all tables)
     * @param tableSize batch size for table discovery
     * @param consumer consumer for discovered tables
     * @throws Throwable if discovery fails
     */
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, 
                               int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        List<TapTable> tapTables = paimonService.discoverTables(tables);
        consumer.accept(tapTables);
    }

    /**
     * Register connector capabilities
     *
     * @param connectorFunctions connector functions registry
     * @param codecRegistry codec registry
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        // Source capabilities
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);

        // Target capabilities
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportClearTable(this::clearTable);

        // Register codec for data type conversions
        registerCodecs(codecRegistry);
    }

    /**
     * Register data type codecs
     *
     * @param codecRegistry codec registry
     */
    private void registerCodecs(TapCodecsRegistry codecRegistry) {
        // Register codec for TapRawValue - convert to JSON string
        codecRegistry.registerFromTapValue(TapRawValue.class, "STRING", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) {
                return toJson(tapRawValue.getValue());
            }
            return null;
        });

        // Register codec for TapMapValue - convert to JSON string
        codecRegistry.registerFromTapValue(TapMapValue.class, "STRING", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) {
                return toJson(tapMapValue.getValue());
            }
            return null;
        });

        // Register codec for TapArrayValue - convert to JSON string
        codecRegistry.registerFromTapValue(TapArrayValue.class, "STRING", tapArrayValue -> {
            if (tapArrayValue != null && tapArrayValue.getValue() != null) {
                return toJson(tapArrayValue.getValue());
            }
            return null;
        });
        codecRegistry.registerFromTapValue(TapDateTimeValue.class, tapDateTimeValue -> tapDateTimeValue.getValue().toTimestamp());
        codecRegistry.registerFromTapValue(TapDateValue.class, tapDateValue -> (int) (tapDateValue.getValue().getSeconds() / 86400));
        codecRegistry.registerFromTapValue(TapTimeValue.class, "CHAR(8)", tapTimeValue -> formatTapDateTime(tapTimeValue.getValue(), "HH:mm:ss"));
        codecRegistry.registerFromTapValue(TapYearValue.class, "CHAR(4)", TapValue::getOriginValue);
    }

    /**
     * Create table in Paimon
     *
     * @param connectorContext connector context
     * @param createTableEvent create table event
     * @return create table options
     * @throws Throwable if creation fails
     */
    private CreateTableOptions createTable(TapConnectorContext connectorContext,
                                           TapCreateTableEvent createTableEvent) throws Throwable {
        final Log log = connectorContext.getLog();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        createTableOptions.setTableExists(false);
        
        TapTable table = createTableEvent.getTable();
        log.info("Creating Paimon table: " + table.getName());
        
        boolean created = paimonService.createTable(table, log);
        if (created) {
            log.info("Table created successfully: " + table.getName());
        } else {
            createTableOptions.setTableExists(true);
            log.info("Table already exists: " + table.getName());
        }
        
        return createTableOptions;
    }

    /**
     * Drop table from Paimon
     *
     * @param connectorContext connector context
     * @param dropTableEvent drop table event
     * @throws Throwable if drop fails
     */
    private void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Throwable {
        final Log log = connectorContext.getLog();
        String tableName = dropTableEvent.getTableId();
        
        log.info("Dropping Paimon table: " + tableName);
        paimonService.dropTable(tableName);
        log.info("Table dropped successfully: " + tableName);
    }

    /**
     * Clear all data from table
     *
     * @param connectorContext connector context
     * @param tapClearTableEvent clear table event
     * @throws Throwable if clear fails
     */
    private void clearTable(TapConnectorContext connectorContext, 
                           io.tapdata.entity.event.ddl.table.TapClearTableEvent tapClearTableEvent) throws Throwable {
        final Log log = connectorContext.getLog();
        String tableName = tapClearTableEvent.getTableId();
        
        log.info("Clearing Paimon table: " + tableName);
        paimonService.clearTable(tableName);
        log.info("Table cleared successfully: " + tableName);
    }

    /**
     * Create index on Paimon table
     *
     * @param connectorContext connector context
     * @param table table definition
     * @param createIndexEvent create index event
     */
    private void createIndex(TapConnectorContext connectorContext, TapTable table, 
                            TapCreateIndexEvent createIndexEvent) {
        final Log log = connectorContext.getLog();
        
        log.info("Creating index on table: " + table.getName());
        try {
            paimonService.createIndex(table, createIndexEvent.getIndexList());
            log.info("Index created successfully on table: " + table.getName());
        } catch (Exception e) {
            log.warn("Failed to create index on table " + table.getName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Batch read records from Paimon table
     *
     * @param connectorContext connector context
     * @param table table to read from
     * @param offsetState offset state for resuming read
     * @param eventBatchSize batch size for events
     * @param eventsOffsetConsumer consumer for events and offset
     * @throws Throwable if read fails
     */
    private void batchRead(TapConnectorContext connectorContext, TapTable table, Object offsetState,
                          int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        final Log log = connectorContext.getLog();

        try {
            log.info("Starting batch read from table: " + table.getName());

            // Read records using Paimon service
            paimonService.batchRead(table, offsetState, eventBatchSize, eventsOffsetConsumer, connectorContext);

            log.info("Batch read completed for table: " + table.getName());

        } catch (Exception e) {
            log.error("Error reading records from table " + table.getName() + ": " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Stream read records from Paimon table (CDC mode)
     *
     * @param connectorContext connector context
     * @param tables list of tables to read from
     * @param offsetState offset state for resuming read
     * @param eventBatchSize batch size for events
     * @param eventsOffsetConsumer consumer for events and offset
     * @throws Throwable if read fails
     */
    private void streamRead(TapConnectorContext connectorContext, List<String> tables, Object offsetState,
                           int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        final Log log = connectorContext.getLog();

        try {
            log.info("Starting stream read from tables: " + tables);

            // Stream read records using Paimon service
            paimonService.streamRead(tables, offsetState, eventBatchSize, eventsOffsetConsumer, connectorContext, this::isAlive);

            log.info("Stream read completed for tables: " + tables);

        } catch (Exception e) {
            log.error("Error in stream read from tables " + tables + ": " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Write records to Paimon table
     *
     * @param connectorContext connector context
     * @param tapRecordEvents list of record events to write
     * @param table target table
     * @param writeListResultConsumer consumer for write results
     * @throws Throwable if write fails
     */
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents,
                            TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        final Log log = connectorContext.getLog();

        try {
            // Write records using Paimon service
            WriteListResult<TapRecordEvent> result = paimonService.writeRecords(
                tapRecordEvents, table, connectorContext);

            writeListResultConsumer.accept(result);

        } catch (Exception e) {
            log.error("Error writing records to table " + table.getName() + ": " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Convert timestamp to stream offset (snapshot ID) for each table
     * This allows resuming stream read from a specific point in time
     *
     * @param connectorContext connector context
     * @param timestamp timestamp in milliseconds (null for current time)
     * @return offset object containing snapshot IDs for each table
     * @throws Throwable if conversion fails
     */
    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long timestamp) throws Throwable {
        final Log log = connectorContext.getLog();

        // Build table list from context
        List<String> tableList = new ArrayList<>();
        Iterator<Entry<TapTable>> iterator = connectorContext.getTableMap().iterator();
        while (iterator.hasNext()) {
            tableList.add(iterator.next().getKey());
        }

        try {
            // Use current time if timestamp is null
            Long effectiveTimestamp = timestamp != null ? timestamp : System.currentTimeMillis();
            log.info("Converting timestamp {} to stream offset for {} tables", effectiveTimestamp, tableList.size());

            // Get snapshot IDs for specified tables at the given timestamp
            return paimonService.timestampToStreamOffset(tableList, effectiveTimestamp, log);

        } catch (Exception e) {
            log.error("Error converting timestamp to stream offset: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Count records in a table
     *
     * @param connectorContext connector context
     * @param table table to count
     * @return record count
     * @throws Throwable if count fails
     */
    private long batchCount(TapConnectorContext connectorContext, TapTable table) throws Throwable {
        final Log log = connectorContext.getLog();

        try {
            log.info("Counting records in table: " + table.getName());

            // Count records using Paimon service
            long count = paimonService.batchCount(table, log);

            log.info("Table {} has {} records", table.getName(), count);
            return count;

        } catch (Exception e) {
            log.error("Error counting records in table " + table.getName() + ": " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Query records by advance filter
     *
     * @param connectorContext connector context
     * @param filter advance filter with conditions
     * @param table table to query
     * @param consumer consumer for filter results
     * @throws Throwable if query fails
     */
    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter filter,
                                     TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        final Log log = connectorContext.getLog();

        try {
            log.info("Querying table {} with advance filter", table.getName());

            // Query records using Paimon service
            paimonService.queryByAdvanceFilter(table, filter, consumer, log);

            log.info("Query completed for table: " + table.getName());

        } catch (Exception e) {
            log.error("Error querying table " + table.getName() + ": " + e.getMessage(), e);
            throw e;
        }
    }
}

