package io.tapdata.connector.paimon;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.service.PaimonService;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.collections4.MapUtils;

import java.util.List;
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
        
        boolean created = paimonService.createTable(table);
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
            // Get DML policies
            String insertPolicy = connectorContext.getConnectorCapabilities()
                .getCapabilityAlternative(ConnectionOptions.DML_INSERT_POLICY);
            if (insertPolicy == null) {
                insertPolicy = ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS;
            }
            
            String updatePolicy = connectorContext.getConnectorCapabilities()
                .getCapabilityAlternative(ConnectionOptions.DML_UPDATE_POLICY);
            if (updatePolicy == null) {
                updatePolicy = ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS;
            }
            
            // Write records using Paimon service
            WriteListResult<TapRecordEvent> result = paimonService.writeRecords(
                tapRecordEvents, table, insertPolicy, updatePolicy);
            
            writeListResultConsumer.accept(result);
            
        } catch (Exception e) {
            log.error("Error writing records to table " + table.getName() + ": " + e.getMessage(), e);
            throw e;
        }
    }
}

