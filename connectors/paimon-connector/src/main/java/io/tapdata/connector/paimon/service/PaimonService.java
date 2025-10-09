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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
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
    private final Map<String, BatchTableWrite> writerCache = new HashMap<>();
    private final Map<String, BatchTableCommit> commitCache = new HashMap<>();

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
            if (!catalog.databaseExists(testDb)) {
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
        if (!catalog.databaseExists(database)) {
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
        if (!catalog.databaseExists(database)) {
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
        if (!catalog.databaseExists(database)) {
            catalog.createDatabase(database, true);
        }
        
        Identifier identifier = Identifier.create(database, tableName);
        
        // Check if table already exists
        if (catalog.tableExists(identifier)) {
            return false;
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
        
        // Create table
        catalog.createTable(identifier, schemaBuilder.build(), true);
        
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

        if (catalog.tableExists(identifier)) {
            catalog.dropTable(identifier, true);
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

        if (!catalog.tableExists(identifier)) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }

        // Drop and recreate table to clear data
        Table table = catalog.getTable(identifier);

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

        Schema schema = schemaBuilder.build();

        catalog.dropTable(identifier, true);
        catalog.createTable(identifier, schema, true);
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
        WriteListResult<TapRecordEvent> result = new WriteListResult<>();
        String database = config.getDatabase();
        String tableName = table.getName();
        Identifier identifier = Identifier.create(database, tableName);

        // Get or create writer
        BatchTableWrite writer = getOrCreateWriter(identifier);
        BatchTableCommit commit = getOrCreateCommit(identifier);

        try {
            for (TapRecordEvent event : recordEvents) {
                try {
                    if (event instanceof TapInsertRecordEvent) {
                        handleInsert((TapInsertRecordEvent) event, writer, table);
                        result.incrementInserted(1);
                    } else if (event instanceof TapUpdateRecordEvent) {
                        handleUpdate((TapUpdateRecordEvent) event, writer, table, updatePolicy);
                        result.incrementModified(1);
                    } else if (event instanceof TapDeleteRecordEvent) {
                        handleDelete((TapDeleteRecordEvent) event, writer, table);
                        result.incrementRemove(1);
                    }
                } catch (Exception e) {
                    result.addError(event, e);
                }
            }

            // Commit the batch
            commit.commit(writer.prepareCommit());

        } catch (Exception e) {
            throw new RuntimeException("Failed to write records to table " + tableName, e);
        }

        return result;
    }

    /**
     * Get or create writer for table
     *
     * @param identifier table identifier
     * @return batch table writer
     * @throws Exception if creation fails
     */
    private BatchTableWrite getOrCreateWriter(Identifier identifier) throws Exception {
        String key = identifier.getFullName();
        if (!writerCache.containsKey(key)) {
            Table table = catalog.getTable(identifier);
            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            BatchTableWrite writer = writeBuilder.newWrite();
            writerCache.put(key, writer);
        }
        return writerCache.get(key);
    }

    /**
     * Get or create commit for table
     *
     * @param identifier table identifier
     * @return batch table commit
     * @throws Exception if creation fails
     */
    private BatchTableCommit getOrCreateCommit(Identifier identifier) throws Exception {
        String key = identifier.getFullName();
        if (!commitCache.containsKey(key)) {
            Table table = catalog.getTable(identifier);
            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
            BatchTableCommit commit = writeBuilder.newCommit();
            commitCache.put(key, commit);
        }
        return commitCache.get(key);
    }

    /**
     * Handle insert event
     *
     * @param event insert event
     * @param writer batch writer
     * @param table table definition
     * @throws Exception if insert fails
     */
    private void handleInsert(TapInsertRecordEvent event, BatchTableWrite writer, TapTable table) throws Exception {
        Map<String, Object> after = event.getAfter();
        GenericRow row = convertToGenericRow(after, table);
        writer.write(row);
    }

    /**
     * Handle update event
     *
     * @param event update event
     * @param writer batch writer
     * @param table table definition
     * @param updatePolicy update policy
     * @throws Exception if update fails
     */
    private void handleUpdate(TapUpdateRecordEvent event, BatchTableWrite writer,
                             TapTable table, String updatePolicy) throws Exception {
        Map<String, Object> after = event.getAfter();
        GenericRow row = convertToGenericRow(after, table);
        writer.write(row);
    }

    /**
     * Handle delete event
     *
     * @param event delete event
     * @param writer batch writer
     * @param table table definition
     * @throws Exception if delete fails
     */
    private void handleDelete(TapDeleteRecordEvent event, BatchTableWrite writer, TapTable table) throws Exception {
        Map<String, Object> before = event.getBefore();
        GenericRow row = convertToGenericRow(before, table);
        // Set row kind to DELETE
        row.setRowKind(org.apache.paimon.types.RowKind.DELETE);
        writer.write(row);
    }

    /**
     * Convert map to GenericRow
     *
     * @param data data map
     * @param table table definition
     * @return GenericRow
     */
    private GenericRow convertToGenericRow(Map<String, Object> data, TapTable table) {
        Map<String, TapField> fields = table.getNameFieldMap();
        int fieldCount = fields.size();
        Object[] values = new Object[fieldCount];

        int index = 0;
        for (Map.Entry<String, TapField> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            Object value = data.get(fieldName);
            values[index++] = value;
        }

        return GenericRow.of(values);
    }

    @Override
    public void close() {
        // Close all writers
        for (BatchTableWrite writer : writerCache.values()) {
            try {
                writer.close();
            } catch (Exception e) {
                // Ignore close errors
            }
        }
        writerCache.clear();

        // Close all commits
        for (BatchTableCommit commit : commitCache.values()) {
            try {
                commit.close();
            } catch (Exception e) {
                // Ignore close errors
            }
        }
        commitCache.clear();

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

