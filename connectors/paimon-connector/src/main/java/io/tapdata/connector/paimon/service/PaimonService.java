package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import io.tapdata.entity.schema.value.DateTime;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.io.Closeable;
import java.util.*;

/**
 * Service class for Paimon operations
 *
 * @author Tapdata
 */
public class PaimonService implements Closeable {

    private final PaimonConfig config;
    private Catalog catalog;
    // Note: Both BatchTableWrite and BatchTableCommit only support one-time committing
    // We don't cache them and create new instances for each write operation

    public PaimonService(PaimonConfig config) {
        this.config = config;
    }

    /**
     * Initialize Paimon catalog
     *
     * @throws Exception if initialization fails
     */
    public void init() throws Exception {
        config.validate();
        
        Options options = new Options();
        options.set("warehouse", config.getFullWarehousePath());
        
        // Configure storage based on type
        configureStorage(options);
        
        // Create catalog context
        CatalogContext context = CatalogContext.create(options, new Configuration());
        
        // Create catalog
        catalog = CatalogFactory.createCatalog(context);
    }

    /**
     * Configure storage options based on storage type
     *
     * @param options Paimon options
     */
    private void configureStorage(Options options) {
        String storageType = config.getStorageType().toLowerCase();
        
        switch (storageType) {
            case "s3":
                options.set("s3.endpoint", config.getS3Endpoint());
                options.set("s3.access-key", config.getS3AccessKey());
                options.set("s3.secret-key", config.getS3SecretKey());
                if (config.getS3Region() != null && !config.getS3Region().isEmpty()) {
                    options.set("s3.region", config.getS3Region());
                }
                options.set("s3.path.style.access", "true");
                break;
            case "hdfs":
                options.set("fs.defaultFS", "hdfs://" + config.getHdfsHost() + ":" + config.getHdfsPort());
                if (config.getHdfsUser() != null && !config.getHdfsUser().isEmpty()) {
                    options.set("hadoop.user.name", config.getHdfsUser());
                }
                break;
            case "oss":
                options.set("fs.oss.endpoint", config.getOssEndpoint());
                options.set("fs.oss.accessKeyId", config.getOssAccessKey());
                options.set("fs.oss.accessKeySecret", config.getOssSecretKey());
                break;
            case "local":
                // No additional configuration needed for local storage
                break;
            default:
                throw new IllegalArgumentException("Unsupported storage type: " + storageType);
        }
    }

    /**
     * Test warehouse accessibility
     *
     * @return true if warehouse is accessible
     */
    public boolean testWarehouseAccess() {
        try {
            // Try to list databases
            catalog.listDatabases();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Test write permission
     *
     * @return true if write permission is available
     */
    public boolean testWritePermission() {
        try {
            // Try to create a test database if it doesn't exist
            String testDb = config.getDatabase();
            try {
                catalog.getDatabase(testDb);
                // Database exists
            } catch (Catalog.DatabaseNotExistException e) {
                // Database does not exist, create it
                catalog.createDatabase(testDb, true);
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get table count in the database
     *
     * @return number of tables
     * @throws Exception if query fails
     */
    public int getTableCount() throws Exception {
        String database = config.getDatabase();

        // Check if database exists
        try {
            catalog.getDatabase(database);
        } catch (Catalog.DatabaseNotExistException e) {
            // Database does not exist
            return 0;
        }

        // Get all tables in database
        List<String> tables = catalog.listTables(database);
        return tables != null ? tables.size() : 0;
    }

    /**
     * Discover tables in Paimon
     *
     * @param tableNames list of table names to discover (null for all)
     * @return list of discovered tables
     * @throws Exception if discovery fails
     */
    public List<TapTable> discoverTables(List<String> tableNames) throws Exception {
        List<TapTable> tables = new ArrayList<>();
        String database = config.getDatabase();

        // Ensure database exists
        try {
            catalog.getDatabase(database);
        } catch (Catalog.DatabaseNotExistException e) {
            // Database does not exist
            return tables;
        }

        // Get all tables in database
        List<String> allTables = catalog.listTables(database);
        
        // Filter tables if specific names provided
        if (tableNames != null && !tableNames.isEmpty()) {
            allTables.retainAll(tableNames);
        }
        
        // Load schema for each table
        for (String tableName : allTables) {
            try {
                Identifier identifier = Identifier.create(database, tableName);
                Table paimonTable = catalog.getTable(identifier);
                
                TapTable tapTable = convertToTapTable(tableName, paimonTable);
                tables.add(tapTable);
            } catch (Exception e) {
                // Skip tables that cannot be loaded
            }
        }
        
        return tables;
    }

    /**
     * Convert Paimon table to TapTable
     *
     * @param tableName table name
     * @param paimonTable Paimon table
     * @return TapTable
     */
    private TapTable convertToTapTable(String tableName, Table paimonTable) {
        TapTable tapTable = new TapTable(tableName);
        
        // Convert fields
        List<DataField> fields = paimonTable.rowType().getFields();
        for (DataField field : fields) {
            TapField tapField = new TapField(field.name(), convertDataType(field.type()));
            tapField.setNullable(field.type().isNullable());
            tapTable.add(tapField);
        }
        
        // Set primary keys
        List<String> primaryKeys = paimonTable.primaryKeys();
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            tapTable.add(new io.tapdata.entity.schema.TapIndex()
                .name("PRIMARY")
                .unique(true)
                .primary(true));
        }
        
        return tapTable;
    }

    /**
     * Convert Paimon data type to Tapdata type name
     *
     * @param dataType Paimon data type
     * @return Tapdata type name
     */
    private String convertDataType(DataType dataType) {
        String typeString = dataType.toString().toUpperCase();

        if (dataType.equals(DataTypes.BOOLEAN())) {
            return "BOOLEAN";
        } else if (dataType.equals(DataTypes.TINYINT())) {
            return "TINYINT";
        } else if (dataType.equals(DataTypes.SMALLINT())) {
            return "SMALLINT";
        } else if (dataType.equals(DataTypes.INT())) {
            return "INT";
        } else if (dataType.equals(DataTypes.BIGINT())) {
            return "BIGINT";
        } else if (dataType.equals(DataTypes.FLOAT())) {
            return "FLOAT";
        } else if (dataType.equals(DataTypes.DOUBLE())) {
            return "DOUBLE";
        } else if (dataType.equals(DataTypes.STRING())) {
            return "STRING";
        } else if (dataType.equals(DataTypes.DATE())) {
            return "DATE";
        } else if (dataType.equals(DataTypes.TIMESTAMP())) {
            return "TIMESTAMP";
        } else if (typeString.startsWith("ARRAY")) {
            return "ARRAY";
        } else if (typeString.startsWith("MAP")) {
            return "MAP";
        } else if (typeString.startsWith("ROW")) {
            return "ROW";
        } else {
            return "STRING"; // Default to STRING for unknown types
        }
    }

    /**
     * Create table in Paimon
     *
     * @param tapTable table definition
     * @return true if created, false if already exists
     * @throws Exception if creation fails
     */
    public boolean createTable(TapTable tapTable) throws Exception {
        String database = config.getDatabase();
        String tableName = tapTable.getName();

        // Ensure database exists
        try {
            catalog.getDatabase(database);
        } catch (Catalog.DatabaseNotExistException e) {
            // Database does not exist, create it
            catalog.createDatabase(database, true);
        }

        Identifier identifier = Identifier.create(database, tableName);

        // Check if table already exists
        try {
            catalog.getTable(identifier);
            // Table exists, check if bucket mode matches
            boolean existingIsDynamic = isTableDynamicBucket(identifier);
            boolean configIsDynamic = config.isDynamicBucketMode();

            if (existingIsDynamic != configIsDynamic) {
                // Bucket mode mismatch, need to recreate table
                // WARNING: This will delete all existing data
                catalog.dropTable(identifier, true);
                // Continue to create table with new bucket mode
            } else {
                // Table exists and bucket mode matches, no need to recreate
                return false;
            }
        } catch (Catalog.TableNotExistException e) {
            // Table does not exist, continue to create
        }

        // Build schema
        Schema.Builder schemaBuilder = Schema.newBuilder();

        // Add fields
        Map<String, TapField> fields = tapTable.getNameFieldMap();
        if (fields != null) {
            for (Map.Entry<String, TapField> entry : fields.entrySet()) {
                String fieldName = entry.getKey();
                TapField tapField = entry.getValue();
                DataType dataType = convertToPaimonDataType(tapField);
                schemaBuilder.column(fieldName, dataType);
            }
        }

        // Set primary keys
        Collection<String> primaryKeys = tapTable.primaryKeys();
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            schemaBuilder.primaryKey(new ArrayList<>(primaryKeys));
        }

        // Set bucket configuration based on bucket mode
        if (config.isDynamicBucketMode()) {
            // Dynamic bucket mode: set bucket to -1
            // This mode uses StreamTableWrite and provides better flexibility
            schemaBuilder.option("bucket", "-1");
        } else {
            // Fixed bucket mode: set specific bucket count
            // This mode uses BatchTableWrite and provides better performance
            Integer bucketCount = config.getBucketCount();
            if (bucketCount == null || bucketCount <= 0) {
                bucketCount = 4; // Default to 4 buckets if not configured
            }
            schemaBuilder.option("bucket", String.valueOf(bucketCount));
        }

        // Create table
        catalog.createTable(identifier, schemaBuilder.build(), false);

        return true;
    }

    /**
     * Convert TapField to Paimon DataType
     *
     * @param tapField TapField
     * @return Paimon DataType
     */
    private DataType convertToPaimonDataType(TapField tapField) {
        String dataType = tapField.getDataType();
        if (dataType == null) {
            return DataTypes.STRING();
        }

        dataType = dataType.toUpperCase();

        if (dataType.contains("BOOLEAN") || dataType.contains("BOOL")) {
            return DataTypes.BOOLEAN();
        } else if (dataType.contains("TINYINT") || dataType.contains("INT8")) {
            return DataTypes.TINYINT();
        } else if (dataType.contains("SMALLINT") || dataType.contains("INT16")) {
            return DataTypes.SMALLINT();
        } else if (dataType.contains("BIGINT") || dataType.contains("INT64") || dataType.contains("LONG")) {
            return DataTypes.BIGINT();
        } else if (dataType.contains("INT") || dataType.contains("INT32") || dataType.contains("INTEGER")) {
            return DataTypes.INT();
        } else if (dataType.contains("FLOAT")) {
            return DataTypes.FLOAT();
        } else if (dataType.contains("DOUBLE") || dataType.contains("NUMBER")) {
            return DataTypes.DOUBLE();
        } else if (dataType.contains("DECIMAL")) {
            return DataTypes.DECIMAL(38, 10);
        } else if (dataType.contains("DATE")) {
            return DataTypes.DATE();
        } else if (dataType.contains("TIMESTAMP") || dataType.contains("DATETIME")) {
            return DataTypes.TIMESTAMP();
        } else if (dataType.contains("BINARY") || dataType.contains("BYTES")) {
            return DataTypes.BYTES();
        } else if (dataType.contains("ARRAY")) {
            // For ARRAY type, use STRING to store JSON representation
            // Paimon ARRAY requires element type specification which we don't have here
            return DataTypes.STRING();
        } else if (dataType.contains("MAP") || dataType.contains("ROW")) {
            // For MAP/ROW type, use STRING to store JSON representation
            // Paimon MAP requires key/value type specification which we don't have here
            return DataTypes.STRING();
        } else {
            return DataTypes.STRING();
        }
    }

    /**
     * Drop table from Paimon
     *
     * @param tableName table name
     * @throws Exception if drop fails
     */
    public void dropTable(String tableName) throws Exception {
        String database = config.getDatabase();
        Identifier identifier = Identifier.create(database, tableName);

        try {
            catalog.getTable(identifier);
            // Table exists, proceed to drop
            catalog.dropTable(identifier, true);
        } catch (Catalog.TableNotExistException e) {
            // Table does not exist, do nothing
        }
    }

    /**
     * Clear all data from table
     *
     * @param tableName table name
     * @throws Exception if clear fails
     */
    public void clearTable(String tableName) throws Exception {
        String database = config.getDatabase();
        Identifier identifier = Identifier.create(database, tableName);

        // Get table, if not exists, return
        Table table;
        try {
            table = catalog.getTable(identifier);
        } catch (Catalog.TableNotExistException e) {
            // Table does not exist, nothing to clear
            return;
        }

        // Drop and recreate table to clear data

        // Rebuild schema from table
        Schema.Builder schemaBuilder = Schema.newBuilder();

        // Add fields from rowType
        List<DataField> fields = table.rowType().getFields();
        for (DataField field : fields) {
            schemaBuilder.column(field.name(), field.type());
        }

        // Add primary keys
        List<String> primaryKeys = table.primaryKeys();
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            schemaBuilder.primaryKey(primaryKeys);
        }

        // Preserve all table options (including bucket configuration)
        // But exclude options that cannot be used when creating table with FileSystemCatalog
        Map<String, String> options = table.options();
        if (options != null && !options.isEmpty()) {
            for (Map.Entry<String, String> entry : options.entrySet()) {
                String key = entry.getKey();
                // Skip 'path' option as FileSystemCatalog doesn't support custom table path
                if ("path".equals(key)) {
                    continue;
                }
                schemaBuilder.option(key, entry.getValue());
            }
        }

        Schema schema = schemaBuilder.build();

        catalog.dropTable(identifier, true);
        catalog.createTable(identifier, schema, false);
    }

    /**
     * Create index on table
     * Note: Paimon doesn't support traditional indexes, but we can log the request
     *
     * @param table table definition
     * @param indexList list of indexes to create
     */
    public void createIndex(TapTable table, List<TapIndex> indexList) {
        // Paimon doesn't support traditional indexes
        // Primary keys are already handled during table creation
        // This method is a no-op but required by the interface
    }

    /**
     * Write records to Paimon table
     *
     * @param recordEvents list of record events
     * @param table target table
     * @param insertPolicy insert policy
     * @param updatePolicy update policy
     * @return write result
     * @throws Exception if write fails
     */
    public WriteListResult<TapRecordEvent> writeRecords(List<TapRecordEvent> recordEvents,
                                                        TapTable table,
                                                        String insertPolicy,
                                                        String updatePolicy) throws Exception {
        // Detect actual bucket mode from the table
        String database = config.getDatabase();
        String tableName = table.getName();
        Identifier identifier = Identifier.create(database, tableName);

        boolean isDynamicBucket = isTableDynamicBucket(identifier);

        // Choose write method based on actual table bucket mode
        if (isDynamicBucket) {
            return writeRecordsWithStreamWrite(recordEvents, table, insertPolicy, updatePolicy);
        } else {
            return writeRecordsWithBatchWrite(recordEvents, table, insertPolicy, updatePolicy);
        }
    }

    /**
     * Check if table is using dynamic bucket mode
     *
     * @param identifier table identifier
     * @return true if dynamic bucket mode, false if fixed bucket mode
     * @throws Exception if check fails
     */
    private boolean isTableDynamicBucket(Identifier identifier) throws Exception {
        Table paimonTable = catalog.getTable(identifier);
        // Get bucket option from table options
        String bucketOption = paimonTable.options().get("bucket");

        // If bucket is -1 or not set, it's dynamic bucket mode
        if (bucketOption == null) {
            return true; // Default is dynamic
        }

        try {
            int bucket = Integer.parseInt(bucketOption);
            return bucket == -1;
        } catch (NumberFormatException e) {
            return true; // If parse fails, assume dynamic
        }
    }

    /**
     * Write records using BatchTableWrite (for fixed bucket mode)
     *
     * @param recordEvents list of record events
     * @param table target table
     * @param insertPolicy insert policy
     * @param updatePolicy update policy
     * @return write result
     * @throws Exception if write fails
     */
    private WriteListResult<TapRecordEvent> writeRecordsWithBatchWrite(List<TapRecordEvent> recordEvents,
                                                                        TapTable table,
                                                                        String insertPolicy,
                                                                        String updatePolicy) throws Exception {
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        String database = config.getDatabase();
        String tableName = table.getName();
        Identifier identifier = Identifier.create(database, tableName);

        // Create new writer and commit for each batch
        // Note: Both BatchTableWrite and BatchTableCommit only support one-time committing
        BatchTableWrite writer = createBatchWriter(identifier);
        BatchTableCommit commit = createBatchCommit(identifier);

        try {
            for (TapRecordEvent event : recordEvents) {
                if (event instanceof TapInsertRecordEvent) {
                    handleBatchInsert((TapInsertRecordEvent) event, writer, table);
                    result.incrementInserted(1);
                } else if (event instanceof TapUpdateRecordEvent) {
                    handleBatchUpdate((TapUpdateRecordEvent) event, writer, table, updatePolicy);
                    result.incrementModified(1);
                } else if (event instanceof TapDeleteRecordEvent) {
                    handleBatchDelete((TapDeleteRecordEvent) event, writer, table);
                    result.incrementRemove(1);
                }
            }

            // Commit the batch
            commit.commit(writer.prepareCommit());

        } catch (Exception e) {
            throw new RuntimeException("Failed to write records to table " + tableName, e);
        } finally {
            // Close writer and commit after use since they only support one-time committing
            try {
                writer.close();
            } catch (Exception e) {
                // Ignore close errors
            }
            try {
                commit.close();
            } catch (Exception e) {
                // Ignore close errors
            }
        }

        return result;
    }

    /**
     * Write records using StreamTableWrite (for dynamic bucket mode)
     *
     * @param recordEvents list of record events
     * @param table target table
     * @param insertPolicy insert policy
     * @param updatePolicy update policy
     * @return write result
     * @throws Exception if write fails
     */
    private WriteListResult<TapRecordEvent> writeRecordsWithStreamWrite(List<TapRecordEvent> recordEvents,
                                                                         TapTable table,
                                                                         String insertPolicy,
                                                                         String updatePolicy) throws Exception {
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        String database = config.getDatabase();
        String tableName = table.getName();
        Identifier identifier = Identifier.create(database, tableName);

        // Create stream writer and commit
        StreamTableWrite writer = createStreamWriter(identifier);
        StreamTableCommit commit = createStreamCommit(identifier);

        try {
            for (TapRecordEvent event : recordEvents) {
                if (event instanceof TapInsertRecordEvent) {
                    handleStreamInsert((TapInsertRecordEvent) event, writer, table);
                    result.incrementInserted(1);
                } else if (event instanceof TapUpdateRecordEvent) {
                    handleStreamUpdate((TapUpdateRecordEvent) event, writer, table, updatePolicy);
                    result.incrementModified(1);
                } else if (event instanceof TapDeleteRecordEvent) {
                    handleStreamDelete((TapDeleteRecordEvent) event, writer, table);
                    result.incrementRemove(1);
                }
            }

            // Prepare commit with commitIdentifier
            // Use current timestamp as commitIdentifier for simplicity
            long commitIdentifier = System.currentTimeMillis();
            List<org.apache.paimon.table.sink.CommitMessage> messages = writer.prepareCommit(false, commitIdentifier);

            // Commit the batch
            commit.commit(commitIdentifier, messages);

        } catch (Exception e) {
            throw new RuntimeException("Failed to write records to table " + tableName, e);
        } finally {
            // Close writer and commit after use
            try {
                writer.close();
            } catch (Exception e) {
                // Ignore close errors
            }
            try {
                commit.close();
            } catch (Exception e) {
                // Ignore close errors
            }
        }

        return result;
    }

    /**
     * Create a new batch writer for table
     * Note: BatchTableWrite only supports one-time committing, so we create a new writer each time
     *
     * @param identifier table identifier
     * @return batch table writer
     * @throws Exception if creation fails
     */
    private BatchTableWrite createBatchWriter(Identifier identifier) throws Exception {
        Table table = catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        return writeBuilder.newWrite();
    }

    /**
     * Create a new batch commit for table
     * Note: BatchTableCommit only supports one-time committing, so we create a new commit each time
     *
     * @param identifier table identifier
     * @return batch table commit
     * @throws Exception if creation fails
     */
    private BatchTableCommit createBatchCommit(Identifier identifier) throws Exception {
        Table table = catalog.getTable(identifier);
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
        return writeBuilder.newCommit();
    }

    /**
     * Create a new stream writer for table
     *
     * @param identifier table identifier
     * @return stream table writer
     * @throws Exception if creation fails
     */
    private StreamTableWrite createStreamWriter(Identifier identifier) throws Exception {
        Table table = catalog.getTable(identifier);
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        return writeBuilder.newWrite();
    }

    /**
     * Create a new stream commit for table
     *
     * @param identifier table identifier
     * @return stream table commit
     * @throws Exception if creation fails
     */
    private StreamTableCommit createStreamCommit(Identifier identifier) throws Exception {
        Table table = catalog.getTable(identifier);
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
        return writeBuilder.newCommit();
    }

    /**
     * Handle insert event with batch writer
     *
     * @param event insert event
     * @param writer batch writer
     * @param table table definition
     * @throws Exception if insert fails
     */
    private void handleBatchInsert(TapInsertRecordEvent event, BatchTableWrite writer, TapTable table) throws Exception {
        Map<String, Object> after = event.getAfter();
        String database = config.getDatabase();
        Identifier identifier = Identifier.create(database, table.getName());
        GenericRow row = convertToGenericRow(after, table, identifier);
        writer.write(row);
    }

    /**
     * Handle update event with batch writer
     *
     * @param event update event
     * @param writer batch writer
     * @param table table definition
     * @param updatePolicy update policy
     * @throws Exception if update fails
     */
    private void handleBatchUpdate(TapUpdateRecordEvent event, BatchTableWrite writer,
                                    TapTable table, String updatePolicy) throws Exception {
        Map<String, Object> after = event.getAfter();
        String database = config.getDatabase();
        Identifier identifier = Identifier.create(database, table.getName());
        GenericRow row = convertToGenericRow(after, table, identifier);
        writer.write(row);
    }

    /**
     * Handle delete event with batch writer
     *
     * @param event delete event
     * @param writer batch writer
     * @param table table definition
     * @throws Exception if delete fails
     */
    private void handleBatchDelete(TapDeleteRecordEvent event, BatchTableWrite writer, TapTable table) throws Exception {
        Map<String, Object> before = event.getBefore();
        String database = config.getDatabase();
        Identifier identifier = Identifier.create(database, table.getName());
        GenericRow row = convertToGenericRow(before, table, identifier);
        // Set row kind to DELETE
        row.setRowKind(org.apache.paimon.types.RowKind.DELETE);
        writer.write(row);
    }

    /**
     * Handle insert event with stream writer
     *
     * @param event insert event
     * @param writer stream writer
     * @param table table definition
     * @throws Exception if insert fails
     */
    private void handleStreamInsert(TapInsertRecordEvent event, StreamTableWrite writer, TapTable table) throws Exception {
        Map<String, Object> after = event.getAfter();
        String database = config.getDatabase();
        Identifier identifier = Identifier.create(database, table.getName());
        GenericRow row = convertToGenericRow(after, table, identifier);
        writer.write(row);
    }

    /**
     * Handle update event with stream writer
     *
     * @param event update event
     * @param writer stream writer
     * @param table table definition
     * @param updatePolicy update policy
     * @throws Exception if update fails
     */
    private void handleStreamUpdate(TapUpdateRecordEvent event, StreamTableWrite writer,
                                     TapTable table, String updatePolicy) throws Exception {
        Map<String, Object> after = event.getAfter();
        String database = config.getDatabase();
        Identifier identifier = Identifier.create(database, table.getName());
        GenericRow row = convertToGenericRow(after, table, identifier);
        writer.write(row);
    }

    /**
     * Handle delete event with stream writer
     *
     * @param event delete event
     * @param writer stream writer
     * @param table table definition
     * @throws Exception if delete fails
     */
    private void handleStreamDelete(TapDeleteRecordEvent event, StreamTableWrite writer, TapTable table) throws Exception {
        Map<String, Object> before = event.getBefore();
        String database = config.getDatabase();
        Identifier identifier = Identifier.create(database, table.getName());
        GenericRow row = convertToGenericRow(before, table, identifier);
        // Set row kind to DELETE
        row.setRowKind(org.apache.paimon.types.RowKind.DELETE);
        writer.write(row);
    }

    /**
     * Convert map to GenericRow
     *
     * @param data data map
     * @param table table definition
     * @param identifier table identifier
     * @return GenericRow
     * @throws Exception if conversion fails
     */
    private GenericRow convertToGenericRow(Map<String, Object> data, TapTable table, Identifier identifier) throws Exception {
        // Get Paimon table to access actual field types
        Table paimonTable = catalog.getTable(identifier);
        List<DataField> paimonFields = paimonTable.rowType().getFields();

        Map<String, TapField> tapFields = table.getNameFieldMap();
        int fieldCount = tapFields.size();
        Object[] values = new Object[fieldCount];

        int index = 0;
        for (Map.Entry<String, TapField> entry : tapFields.entrySet()) {
            String fieldName = entry.getKey();
            Object value = data.get(fieldName);

            // Get corresponding Paimon field type
            DataType paimonType = null;
            for (DataField paimonField : paimonFields) {
                if (paimonField.name().equals(fieldName)) {
                    paimonType = paimonField.type();
                    break;
                }
            }

            // Convert value to Paimon-compatible type
            values[index++] = convertValueToPaimonType(value, paimonType);
        }

        return GenericRow.of(values);
    }

    /**
     * Convert value to Paimon-compatible type
     *
     * @param value original value
     * @param paimonType target Paimon data type
     * @return converted value
     */
    private Object convertValueToPaimonType(Object value, DataType paimonType) {
        if (value == null || paimonType == null) {
            return null;
        }

        // Get the type root for comparison (ignores nullable attribute)
        String typeString = paimonType.toString().toUpperCase();

        // Handle STRING type - convert to BinaryString
        if (typeString.contains("STRING") || typeString.contains("VARCHAR") || typeString.contains("CHAR")) {
            if (value instanceof String) {
                return BinaryString.fromString((String) value);
            } else {
                return BinaryString.fromString(String.valueOf(value));
            }
        }

        // Handle TIMESTAMP type
        if (typeString.contains("TIMESTAMP")) {
            if (value instanceof DateTime) {
                DateTime dateTime = (DateTime) value;
                // Convert DateTime to Paimon Timestamp
                // DateTime.getSeconds() returns seconds since epoch (can be negative for dates before 1970)
                // DateTime.getNano() returns nanoseconds part
                long epochSecond = dateTime.getSeconds();
                int nanoSecond = dateTime.getNano();

                // Convert to milliseconds and nanos-of-millisecond
                // Similar to Timestamp.fromInstant() implementation
                long millisecond = epochSecond * 1000L + nanoSecond / 1_000_000;
                int nanoOfMillisecond = nanoSecond % 1_000_000;

                // Ensure nanoOfMillisecond is always positive (0-999,999)
                if (nanoOfMillisecond < 0) {
                    millisecond -= 1;
                    nanoOfMillisecond += 1_000_000;
                }

                return Timestamp.fromEpochMillis(millisecond, nanoOfMillisecond);
            } else if (value instanceof java.sql.Timestamp) {
                java.sql.Timestamp ts = (java.sql.Timestamp) value;
                return Timestamp.fromEpochMillis(ts.getTime());
            } else if (value instanceof java.util.Date) {
                java.util.Date date = (java.util.Date) value;
                return Timestamp.fromEpochMillis(date.getTime());
            } else if (value instanceof Long) {
                return Timestamp.fromEpochMillis((Long) value);
            }
        }

        // Handle DATE type
        if (typeString.contains("DATE") && !typeString.contains("TIMESTAMP")) {
            if (value instanceof DateTime) {
                DateTime dateTime = (DateTime) value;
                // Convert to days since epoch (1970-01-01)
                long millis = dateTime.getSeconds() * 1000L;
                return (int) (millis / (1000 * 60 * 60 * 24));
            } else if (value instanceof java.sql.Date) {
                java.sql.Date date = (java.sql.Date) value;
                // Convert to days since epoch (1970-01-01)
                return (int) (date.getTime() / (1000 * 60 * 60 * 24));
            } else if (value instanceof java.util.Date) {
                java.util.Date date = (java.util.Date) value;
                return (int) (date.getTime() / (1000 * 60 * 60 * 24));
            }
        }

        // Handle numeric types - ensure correct Java type
        if (typeString.contains("TINYINT")) {
            if (value instanceof Number) {
                return ((Number) value).byteValue();
            }
        }

        if (typeString.contains("SMALLINT")) {
            if (value instanceof Number) {
                return ((Number) value).shortValue();
            }
        }

        if (typeString.contains("INT") && !typeString.contains("BIGINT") && !typeString.contains("SMALLINT") && !typeString.contains("TINYINT")) {
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
        }

        if (typeString.contains("BIGINT")) {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
        }

        if (typeString.contains("FLOAT")) {
            if (value instanceof Number) {
                return ((Number) value).floatValue();
            }
        }

        if (typeString.contains("DOUBLE")) {
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
        }

        if (typeString.contains("BOOLEAN")) {
            if (value instanceof Boolean) {
                return value;
            } else if (value instanceof Number) {
                return ((Number) value).intValue() != 0;
            } else if (value instanceof String) {
                return Boolean.parseBoolean((String) value);
            }
        }

        // For other types, return as-is
        return value;
    }

    @Override
    public void close() {
        // Close catalog
        if (catalog != null) {
            try {
                catalog.close();
            } catch (Exception e) {
                // Ignore close errors
            }
        }
    }
}

