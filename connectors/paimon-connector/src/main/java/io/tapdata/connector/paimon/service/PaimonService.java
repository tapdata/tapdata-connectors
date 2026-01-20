package io.tapdata.connector.paimon.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.*;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;

import java.io.Closeable;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service class for Paimon operations
 *
 * @author Tapdata
 */
public class PaimonService implements Closeable {

	private final PaimonConfig config;
	private Catalog catalog;

	// Cache writers and commits per table for long lifecycle
	// Key: database.tableName
	private final Map<String, StreamTableWrite> streamWriterCache = new ConcurrentHashMap<>();
	private final Map<String, StreamTableCommit> streamCommitCache = new ConcurrentHashMap<>();

	// Atomic counter for generating unique, incrementing commit identifiers
	// This ensures no duplicate commit identifiers even in high-concurrency scenarios
	private final AtomicLong commitIdentifierGenerator = new AtomicLong(0);

	// ===== Batch Accumulation for Performance =====
	// Track accumulated records per table before commit
	private final Map<String, AtomicInteger> accumulatedRecordCount = new ConcurrentHashMap<>();
	// Track last commit time per table
	private final Map<String, AtomicLong> lastCommitTime = new ConcurrentHashMap<>();
	// Lock for commit operations per table
	private final Map<String, Object> commitLocks = new ConcurrentHashMap<>();

	// ===== Async Commit Support =====
	// Background thread for async commits
	private ScheduledExecutorService asyncCommitExecutor;
	// Flag to track if async commit is enabled
	private volatile boolean asyncCommitEnabled = false;

	// ===== Paimon Field Cache for Performance =====
	// LRU cache for Paimon field mappings: Key = "database.tableName", Value = Map<fieldName, DataType>
	// Limit to 5 tables to avoid excessive memory usage
	private final Map<String, Map<String, DataType>> paimonFieldCache = Collections.synchronizedMap(
			new LinkedHashMap<String, Map<String, DataType>>(5, 0.75f, true) {
				private static final long serialVersionUID = 1L;

				@Override
				protected boolean removeEldestEntry(Map.Entry<String, Map<String, DataType>> eldest) {
					return size() > 5;
				}
			}
	);

	// LRU cache for field index mappings: Key = "database.tableName", Value = Map<fieldName, index>
	// Limit to 5 tables to avoid excessive memory usage
	private final Map<String, Map<String, Integer>> fieldIndexCache = Collections.synchronizedMap(
			new LinkedHashMap<String, Map<String, Integer>>(5, 0.75f, true) {
				private static final long serialVersionUID = 1L;

				@Override
				protected boolean removeEldestEntry(Map.Entry<String, Map<String, Integer>> eldest) {
					return size() > 5;
				}
			}
	);

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

		// Create catalog context with Hadoop configuration (for S3A, etc.)
		Configuration hadoopConf = buildHadoopConfiguration();
		CatalogContext context = CatalogContext.create(options, hadoopConf);

		// Create catalog
		catalog = CatalogFactory.createCatalog(context);

		// Initialize async commit if enabled
		initAsyncCommit();
	}

	/**
	 * Initialize async commit executor if enabled in config
	 */
	private void initAsyncCommit() {
		Boolean enableAsync = config.getEnableAsyncCommit();
		Integer commitInterval = config.getCommitIntervalMs();

		if (enableAsync != null && enableAsync && commitInterval != null && commitInterval > 0) {
			asyncCommitEnabled = true;

			// Create scheduled executor with single thread
			asyncCommitExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
				Thread t = new Thread(r, "paimon-async-commit");
				t.setDaemon(true); // Daemon thread won't prevent JVM shutdown
				return t;
			});

			// Schedule periodic commit task
			asyncCommitExecutor.scheduleAtFixedRate(() -> {
				try {
					// Commit all tables that have accumulated data
					for (String tableKey : new ArrayList<>(accumulatedRecordCount.keySet())) {
						AtomicInteger count = accumulatedRecordCount.get(tableKey);
						if (count != null && count.get() > 0) {
							// Check if enough time has passed since last commit
							AtomicLong lastCommit = lastCommitTime.get(tableKey);
							if (lastCommit != null) {
								long timeSinceLastCommit = System.currentTimeMillis() - lastCommit.get();
								if (timeSinceLastCommit >= commitInterval) {
									flushTable(tableKey);
								}
							}
						}
					}
				} catch (Exception e) {
					// Log error but don't stop the scheduler
					System.err.println("Error in async commit: " + e.getMessage());
				}
			}, commitInterval, commitInterval, TimeUnit.MILLISECONDS);
		}
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
				options.set("s3.upload.max-concurrency", "20");
				options.set("s3.upload.part-size", "16mb");
				options.set("s3.fast-upload", "true");
				options.set("s3.accelerate-mode", "true");
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
	 * Build Hadoop Configuration when needed (e.g., S3A)
	 */
	private Configuration buildHadoopConfiguration() {
		Configuration conf = new Configuration();
		String storageType = config.getStorageType() == null ? "" : config.getStorageType().toLowerCase();
		if ("s3".equals(storageType)) {
			String endpoint = config.getS3Endpoint();
			String accessKey = config.getS3AccessKey();
			String secretKey = config.getS3SecretKey();
			String region = config.getS3Region();

			if (endpoint != null && !endpoint.isEmpty()) {
				// Strip scheme for fs.s3a.endpoint, and set SSL flag accordingly
				String ep = endpoint.trim();
				boolean https = false;
				if (ep.startsWith("http://")) {
					ep = ep.substring("http://".length());
				} else if (ep.startsWith("https://")) {
					ep = ep.substring("https://".length());
					https = true;
				}
				conf.set("fs.s3a.endpoint", ep);
				conf.setBoolean("fs.s3a.connection.ssl.enabled", https);
			}
			if (accessKey != null) {
				conf.set("fs.s3a.access.key", accessKey);
			}
			if (secretKey != null) {
				conf.set("fs.s3a.secret.key", secretKey);
			}
			if (region != null && !region.isEmpty()) {
				conf.set("fs.s3a.region", region);
			}
			// Path-style access is typically needed for MinIO
			conf.setBoolean("fs.s3a.path.style.access", true);
			// Use simple static credentials to avoid picking up instance profiles accidentally
			conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
			// Do NOT force-map s3 scheme to S3A here. Paimon S3 plugin shades Hadoop classes
			// and handles scheme registration internally. Forcing mappings can cause
			// NoClassDefFoundError due to classloader/version conflicts.
			// Ensure S3A filesystem is used when scheme is s3a
			conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
			conf.set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A");

		}
		return conf;
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
	 * @param tableName   table name
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
	public boolean createTable(TapTable tapTable, Log log) throws Exception {
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
				// Bucket mode mismatch, log warning and continue with existing table
				String existingMode = existingIsDynamic ? "dynamic" : "fixed";
				String configMode = configIsDynamic ? "dynamic" : "fixed";
				log.warn("Table {} already exists with {} bucket mode, but config specifies {} bucket mode. " +
								"Cannot switch bucket mode for existing table. Using existing table configuration.",
						tableName, existingMode, configMode);
			}
			// Table exists, no need to recreate
			return false;
		} catch (Catalog.TableNotExistException e) {
			// Table does not exist, continue to create
		}

		// Build schema
		Schema.Builder schemaBuilder = Schema.newBuilder();
		Map<String, Object> schemaBuilderVariableMap = new HashMap<>();

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
		Collection<String> primaryKeys = tapTable.primaryKeys(true);
		if (primaryKeys != null && !primaryKeys.isEmpty()) {
			schemaBuilder.primaryKey(new ArrayList<>(primaryKeys));
		}

		// Set bucket configuration based on bucket mode
		if (config.isDynamicBucketMode()) {
			// Dynamic bucket mode: set bucket to -1
			// This mode provides better flexibility
			schemaBuilder.option("bucket", "-1");
			schemaBuilderVariableMap.put("bucket", -1);
		} else {
			// Fixed bucket mode: set specific bucket count
			Integer bucketCount = config.getBucketCount();
			if (bucketCount == null || bucketCount <= 0) {
				bucketCount = 4; // Default to 4 buckets if not configured
			}
			schemaBuilder.option("bucket", String.valueOf(bucketCount));
			schemaBuilderVariableMap.put("bucket", String.valueOf(bucketCount));
		}
		if (EmptyKit.isNotBlank(config.getFileFormat())) {
			schemaBuilder.option("file.format", config.getFileFormat());
			schemaBuilderVariableMap.put("file.format", config.getFileFormat());
        }
		if (EmptyKit.isNotBlank(config.getCompression())) {
			schemaBuilder.option("compression", config.getCompression());
			schemaBuilderVariableMap.put("compression", config.getCompression());
		}

		// ===== Performance Optimization Options =====

		// 1. Write buffer size - controls memory buffer for writes
		// Larger buffer = better performance but more memory usage
		if (config.getWriteBufferSize() != null && config.getWriteBufferSize() > 0) {
			schemaBuilder.option("write-buffer-size", config.getWriteBufferSize() + "mb");
			schemaBuilderVariableMap.put("write-buffer-size", config.getWriteBufferSize() + "mb");
		}

		// 2. Target file size - Paimon will try to create files of this size
		// Larger files = fewer files but slower compaction
		if (config.getTargetFileSize() != null && config.getTargetFileSize() > 0) {
			schemaBuilder.option("target-file-size", config.getTargetFileSize() + "mb");
			schemaBuilderVariableMap.put("target-file-size", config.getTargetFileSize() + "mb");
		}

		// 3. Compaction settings
		if (config.getEnableAutoCompaction() != null) {
			if (config.getEnableAutoCompaction()) {
				// Enable full compaction for better query performance
				schemaBuilder.option("compaction.async.enabled", "true");
				schemaBuilder.option("compaction.optimization-interval", config.getCompactionIntervalMinutes() + "min");
				schemaBuilderVariableMap.put("compaction.async.enabled", "true");
				schemaBuilderVariableMap.put("compaction.optimization-interval", config.getCompactionIntervalMinutes() + "min");

				// Set compaction strategy
				schemaBuilder.option("changelog-producer", "input");
				schemaBuilderVariableMap.put("changelog-producer", "input");

				// Compact small files more aggressively
				schemaBuilder.option("num-sorted-run.compaction-trigger", "3");
				schemaBuilderVariableMap.put("num-sorted-run.compaction-trigger", "3");
				schemaBuilder.option("num-sorted-run.stop-trigger", "5");
				schemaBuilderVariableMap.put("num-sorted-run.stop-trigger", "5");
			} else {
				// Disable auto compaction
				schemaBuilder.option("compaction.optimization-interval", "0");
				schemaBuilderVariableMap.put("compaction.optimization-interval", "0");
			}
		}

		// 4. Snapshot settings for better performance
		// Keep more snapshots in memory for faster access
		schemaBuilder.option("snapshot.num-retained.min", "5");
		schemaBuilder.option("snapshot.num-retained.max", "50");
		schemaBuilder.option("snapshot.time-retained", "30min");
		schemaBuilderVariableMap.put("snapshot.num-retained.min", "5");
		schemaBuilderVariableMap.put("snapshot.num-retained.max", "50");
		schemaBuilderVariableMap.put("snapshot.time-retained", "30min");

		// 5. Commit settings
		// Force compact on commit for better read performance
		schemaBuilder.option("commit.force-compact", "false"); // Don't force compact on every commit
		schemaBuilderVariableMap.put("commit.force-compact", "false");

		// 6. Scan settings for better read performance
		schemaBuilder.option("scan.plan-sort-partition", "true");
		schemaBuilderVariableMap.put("scan.plan-sort-partition", "true");

		// 7. Changelog settings for CDC scenarios
		schemaBuilder.option("changelog-producer.lookup-wait", "false"); // Don't wait for lookup
		schemaBuilderVariableMap.put("changelog-producer.lookup-wait", "false");

		// 8. Memory settings
		schemaBuilder.option("sink.parallelism", String.valueOf(config.getWriteThreads()));
		schemaBuilderVariableMap.put("sink.parallelism", String.valueOf(config.getWriteThreads()));

		// Create table
		catalog.createTable(identifier, schemaBuilder.build(), false);

		// log schema builder variables
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		log.info("Created table {} with schema: {}", identifier.getFullName(), gson.toJson(schemaBuilder.build()));

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
	 * @param table     table definition
	 * @param indexList list of indexes to create
	 */
	public void createIndex(TapTable table, List<TapIndex> indexList) {
		// Paimon doesn't support traditional indexes
		// Primary keys are already handled during table creation
		// This method is a no-op but required by the interface
	}

	/**
	 * Write records to Paimon table using stream write
	 *
	 * @param recordEvents list of record events
	 * @param table        target table
	 * @return write result
	 * @throws Exception if write fails
	 */
	public WriteListResult<TapRecordEvent> writeRecords(List<TapRecordEvent> recordEvents,
														TapTable table,
														TapConnectorContext connectorContext) throws Exception {
		return writeRecordsWithStreamWriteInternal(recordEvents, table, connectorContext);
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
	 * Internal implementation of stream write with retry support
	 *
	 * @param recordEvents list of record events
	 * @param table        target table
	 * @return write result
	 * @throws Exception if write fails
	 */
	private WriteListResult<TapRecordEvent> writeRecordsWithStreamWriteInternal(List<TapRecordEvent> recordEvents,
																				TapTable table,
																				TapConnectorContext connectorContext) throws Exception {
		String database = config.getDatabase();
		String tableName = table.getName();
		String tableKey = database + "." + tableName;

		// Use loop instead of recursion for retry
		int maxRetries = 3;
		int retryCount = 0;

		while (true) {
			WriteListResult<TapRecordEvent> result = new WriteListResult<>();
			Identifier identifier = Identifier.create(database, tableName);

			// Get or create cached writer and commit
			StreamTableWrite writer = getOrCreateStreamWriter(tableKey, identifier);
			StreamTableCommit commit = getOrCreateStreamCommit(tableKey, identifier);

			try {
				// Write all records to the writer
				for (TapRecordEvent event : recordEvents) {
					if (event instanceof TapInsertRecordEvent) {
						handleStreamInsert((TapInsertRecordEvent) event, writer, table);
						result.incrementInserted(1);
					} else if (event instanceof TapUpdateRecordEvent) {
						handleStreamUpdate((TapUpdateRecordEvent) event, writer, table);
						result.incrementModified(1);
					} else if (event instanceof TapDeleteRecordEvent) {
						handleStreamDelete((TapDeleteRecordEvent) event, writer, table);
						result.incrementRemove(1);
					}
				}

				// Update accumulated record count
				AtomicInteger recordCount = accumulatedRecordCount.computeIfAbsent(tableKey, k -> new AtomicInteger(0));
				int currentCount = recordCount.addAndGet(recordEvents.size());

				// Initialize last commit time if not exists
				AtomicLong lastCommit = lastCommitTime.computeIfAbsent(tableKey, k -> new AtomicLong(System.currentTimeMillis()));

				// Determine if we should commit based on:
				// 1. Accumulated record count exceeds threshold
				// 2. Time since last commit exceeds interval
				// 3. Batch accumulation is disabled (size = 0)
				boolean shouldCommit = false;
				Integer batchSize = config.getBatchAccumulationSize();
				Integer commitInterval = config.getCommitIntervalMs();

				if (batchSize == null || batchSize <= 0) {
					// Batch accumulation disabled, commit immediately
					shouldCommit = true;
				} else if (currentCount >= batchSize) {
					// Record count threshold reached
					shouldCommit = true;
				} else if (commitInterval != null && commitInterval > 0) {
					// Check time-based commit
					long timeSinceLastCommit = System.currentTimeMillis() - lastCommit.get();
					if (timeSinceLastCommit >= commitInterval) {
						shouldCommit = true;
					}
				}

				// Perform commit if needed
				if (shouldCommit) {
					// Use lock to ensure only one thread commits at a time for this table
					Object lock = commitLocks.computeIfAbsent(tableKey, k -> new Object());
					synchronized (lock) {
						// Double-check if we still need to commit (another thread might have committed)
						int finalCount = recordCount.get();
						if (finalCount > 0) {
							// Prepare commit with commitIdentifier
							// Use atomic counter to generate unique, incrementing commit identifier
							long commitIdentifier = commitIdentifierGenerator.incrementAndGet();
							List<CommitMessage> messages = writer.prepareCommit(false, commitIdentifier);

							// Commit the batch
							commit.commit(commitIdentifier, messages);

							// Reset counters after successful commit
							recordCount.set(0);
							lastCommit.set(System.currentTimeMillis());

							connectorContext.getLog().debug("Committed {} accumulated records for table {}",
								finalCount, tableKey);
						}
					}
				}

				// StreamTableWrite can be reused, so we don't clean up here
				return result;

			} catch (Exception e) {
				// Check if it's ThreadGroup destroyed error and we can retry
				if (retryCount < maxRetries && isThreadGroupDestroyedError(e)) {
					connectorContext.getLog().warn("ThreadGroup destroyed in stream write, retrying... (attempt " + (retryCount + 1) + "/" + maxRetries + ")");
					retryCount++;
					// Completely rebuild all resources
					reinitCatalog();
					// Continue to next iteration for retry
					TimeUnit.SECONDS.sleep(1L);
					continue;
				}
				// Don't clean up on error for stream write, let it be reused or cleaned up on close
				throw new RuntimeException("Failed to write records to table " + tableName, e);
			}
		}
	}

	/**
	 * Reinitialize the Paimon catalog.
	 * This is used to recover from ThreadGroup destroyed errors caused by classloader unloading.
	 * This method completely rebuilds all resources including catalog and all cached writers/commits.
	 *
	 * @throws Exception if reinitialization fails
	 */
	private synchronized void reinitCatalog() throws Exception {
		// Clean up all resources
		cleanupAllResources();

		// Reinitialize catalog
		init();
	}

	/**
	 * Clean up all cached resources including writers, commits, and catalog.
	 * This method ensures proper cleanup with delays to allow internal threads to terminate.
	 */
	private void cleanupAllResources() {
		// Shutdown async commit executor first
		if (asyncCommitExecutor != null) {
			asyncCommitEnabled = false;
			asyncCommitExecutor.shutdown();
			try {
				if (!asyncCommitExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
					asyncCommitExecutor.shutdownNow();
				}
			} catch (InterruptedException e) {
				asyncCommitExecutor.shutdownNow();
				Thread.currentThread().interrupt();
			}
			asyncCommitExecutor = null;
		}

		// Close all cached writers and commits first
		for (String tableKey : new ArrayList<>(streamWriterCache.keySet())) {
			cleanupTableResources(tableKey);
		}

		// Clear all caches
		streamWriterCache.clear();
		streamCommitCache.clear();

		// Clear batch accumulation tracking
		accumulatedRecordCount.clear();
		lastCommitTime.clear();
		commitLocks.clear();

		// Clear Paimon field cache
		paimonFieldCache.clear();
		fieldIndexCache.clear();

		// Close old catalog if exists
		if (catalog != null) {
			try {
				catalog.close();
			} catch (Exception e) {
				// Ignore close errors
			}
			catalog = null;
		}

		// Wait a bit to ensure all internal threads are cleaned up
		// This is critical to avoid ThreadGroup destroyed errors
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Check if the exception is caused by ThreadGroup being destroyed.
	 * This typically happens when the classloader that created Paimon's thread factory
	 * has been unloaded, causing the captured ThreadGroup to be destroyed.
	 *
	 * @param e the exception to check
	 * @return true if it's a ThreadGroup destroyed error
	 */
	private boolean isThreadGroupDestroyedError(Throwable e) {
		Throwable cause = e;
		while (cause != null) {
			if (cause instanceof IllegalThreadStateException) {
				return true;
			}
			cause = cause.getCause();
		}
		return false;
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
	 * Get or create cached stream writer for table
	 * ConcurrentHashMap.computeIfAbsent() ensures thread-safe creation without additional locking
	 *
	 * @param tableKey   table key (database.tableName)
	 * @param identifier table identifier
	 * @return stream table writer
	 * @throws Exception if creation fails
	 */
	private StreamTableWrite getOrCreateStreamWriter(String tableKey, Identifier identifier) throws Exception {
		return streamWriterCache.computeIfAbsent(tableKey, k -> {
			try {
				return createStreamWriter(identifier);
			} catch (Exception e) {
				throw new RuntimeException("Failed to create stream writer for table " + tableKey, e);
			}
		});
	}

	/**
	 * Get or create cached stream commit for table
	 * ConcurrentHashMap.computeIfAbsent() ensures thread-safe creation without additional locking
	 *
	 * @param tableKey   table key (database.tableName)
	 * @param identifier table identifier
	 * @return stream table commit
	 * @throws Exception if creation fails
	 */
	private StreamTableCommit getOrCreateStreamCommit(String tableKey, Identifier identifier) throws Exception {
		return streamCommitCache.computeIfAbsent(tableKey, k -> {
			try {
				return createStreamCommit(identifier);
			} catch (Exception e) {
				throw new RuntimeException("Failed to create stream commit for table " + tableKey, e);
			}
		});
	}



	/**
	 * Clean up all cached resources for a specific table
	 *
	 * @param tableKey table key (database.tableName)
	 */
	private void cleanupTableResources(String tableKey) {
		// Close and remove stream commit first (before writer)
		StreamTableCommit streamCommit = streamCommitCache.remove(tableKey);
		if (streamCommit != null) {
			try {
				streamCommit.close();
			} catch (Exception e) {
				// Ignore close errors
			}
		}

		// Close and remove stream writer
		StreamTableWrite streamWriter = streamWriterCache.remove(tableKey);
		if (streamWriter != null) {
			try {
				streamWriter.close();
			} catch (Exception e) {
				// Ignore close errors, especially IllegalThreadStateException
				// which can occur if ThreadGroup is already destroyed
			}
			// Force wait a bit to ensure internal threads are cleaned up
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}



	/**
	 * Handle insert event with stream writer
	 *
	 * @param event  insert event
	 * @param writer stream writer
	 * @param table  table definition
	 * @throws Exception if insert fails
	 */
	private void handleStreamInsert(TapInsertRecordEvent event, StreamTableWrite writer, TapTable table) throws Exception {
		Map<String, Object> after = event.getAfter();
		String database = config.getDatabase();
		Identifier identifier = Identifier.create(database, table.getName());
		GenericRow row = convertToGenericRow(after, table, identifier);
		if (config.getBucketMode().equals("fixed")) {
			writer.write(row);
		} else {
			int bucket = selectBucketForDynamic(row, table);
			writer.write(row, bucket);
		}
	}

	/**
	 * Handle update event with stream writer
	 *
	 * @param event  update event
	 * @param writer stream writer
	 * @param table  table definition
	 * @throws Exception if update fails
	 */
	private void handleStreamUpdate(TapUpdateRecordEvent event, StreamTableWrite writer, TapTable table) throws Exception {
		Map<String, Object> after = event.getAfter();
		String database = config.getDatabase();
		Identifier identifier = Identifier.create(database, table.getName());
		GenericRow row = convertToGenericRow(after, table, identifier);
		if (config.getBucketMode().equals("fixed")) {
			writer.write(row);
		} else {
			int bucket = selectBucketForDynamic(row, table);
			writer.write(row, bucket);
		}
	}

	/**
	 * Handle delete event with stream writer
	 *
	 * @param event  delete event
	 * @param writer stream writer
	 * @param table  table definition
	 * @throws Exception if delete fails
	 */
	private void handleStreamDelete(TapDeleteRecordEvent event, StreamTableWrite writer, TapTable table) throws Exception {
		Map<String, Object> before = event.getBefore();
		String database = config.getDatabase();
		Identifier identifier = Identifier.create(database, table.getName());
		GenericRow row = convertToGenericRow(before, table, identifier);
		// Set row kind to DELETE
		row.setRowKind(org.apache.paimon.types.RowKind.DELETE);
		if (config.getBucketMode().equals("fixed")) {
			writer.write(row);
		} else {
			int bucket = selectBucketForDynamic(row, table);
			writer.write(row, bucket);
		}
	}

	/**
	 * Select deterministic bucket for dynamic-bucket tables.
	 * Use primary keys if present; otherwise hash all fields (sorted by name).
	 *
	 * Note: This method uses the converted GenericRow values to ensure consistent
	 * bucket selection across insert/update/delete operations, especially for
	 * Date/DateTime types that are converted to int/long values.
	 *
	 * @param row converted GenericRow with Paimon-compatible values
	 * @param table table definition
	 * @return bucket number
	 */
	private int selectBucketForDynamic(GenericRow row, TapTable table) {
		int hint = (config.getBucketCount() != null && config.getBucketCount() > 0) ? config.getBucketCount() : 4;
		int hash = 0;
		Collection<String> pks = table.primaryKeys(true);
		Map<String, TapField> fields = table.getNameFieldMap();

		// Get or build field index mapping from cache
		String cacheKey = table.getId();
		Map<String, Integer> indexMap = getFieldIndexMap(cacheKey, fields);

		if (pks != null && !pks.isEmpty()) {
			// Use primary key fields for hashing
			for (String key : pks) {
				Integer fieldIndex = indexMap.get(key);
				if (fieldIndex != null && fieldIndex >= 0 && fieldIndex < row.getFieldCount()) {
					Object v = row.getField(fieldIndex);
					hash = 31 * hash + (v == null ? 0 : v.hashCode());
				}
			}
		} else {
			// Use all fields for hashing (sorted by name)
			if (fields != null && !fields.isEmpty()) {
				List<String> names = new ArrayList<>(fields.keySet());
				Collections.sort(names);
				for (String name : names) {
					Integer fieldIndex = indexMap.get(name);
					if (fieldIndex != null && fieldIndex >= 0 && fieldIndex < row.getFieldCount()) {
						Object v = row.getField(fieldIndex);
						hash = 31 * hash + (v == null ? 0 : v.hashCode());
					}
				}
			} else {
				// Fallback: hash all fields in order
				for (int i = 0; i < row.getFieldCount(); i++) {
					Object v = row.getField(i);
					hash = 31 * hash + (v == null ? 0 : v.hashCode());
				}
			}
		}
		return Math.floorMod(hash, hint);
	}

	/**
	 * Get or build field index mapping from cache
	 *
	 * @param cacheKey cache key (table ID)
	 * @param fields field map
	 * @return map of field name to index
	 */
	private Map<String, Integer> getFieldIndexMap(String cacheKey, Map<String, TapField> fields) {
		Map<String, Integer> indexMap = fieldIndexCache.get(cacheKey);

		if (indexMap == null) {
			// Cache miss - build field index mapping
			indexMap = new HashMap<>(fields.size());
			int index = 0;
			for (String name : fields.keySet()) {
				indexMap.put(name, index++);
			}

			// Store in cache
			fieldIndexCache.put(cacheKey, indexMap);
		}

		return indexMap;
	}

	/**
	 * Get field index by field name (deprecated - use getFieldIndexMap instead)
	 *
	 * @param fieldName field name
	 * @param fields field map
	 * @return field index, or -1 if not found
	 * @deprecated Use getFieldIndexMap for better performance with caching
	 */
	@Deprecated
	private int getFieldIndex(String fieldName, Map<String, TapField> fields) {
		int index = 0;
		for (String name : fields.keySet()) {
			if (name.equals(fieldName)) {
				return index;
			}
			index++;
		}
		return -1;
	}

	/**
	 * Convert map to GenericRow
	 *
	 * @param data       data map
	 * @param table      table definition
	 * @param identifier table identifier
	 * @return GenericRow
	 * @throws Exception if conversion fails
	 */
	private GenericRow convertToGenericRow(Map<String, Object> data, TapTable table, Identifier identifier) throws Exception {
		// Get or build field type mapping from cache
		String cacheKey = identifier.getFullName();
		Map<String, DataType> fieldTypeMap = paimonFieldCache.get(cacheKey);

		if (fieldTypeMap == null) {
			// Cache miss - build field type mapping
			Table paimonTable = catalog.getTable(identifier);
			List<DataField> paimonFields = paimonTable.rowType().getFields();

			fieldTypeMap = new HashMap<>(paimonFields.size());
			for (DataField paimonField : paimonFields) {
				fieldTypeMap.put(paimonField.name(), paimonField.type());
			}

			// Store in cache
			paimonFieldCache.put(cacheKey, fieldTypeMap);
		}

		Map<String, TapField> tapFields = table.getNameFieldMap();
		int fieldCount = tapFields.size();
		Object[] values = new Object[fieldCount];

		int index = 0;
		for (Map.Entry<String, TapField> entry : tapFields.entrySet()) {
			String fieldName = entry.getKey();
			Object value = data.get(fieldName);

			// Get corresponding Paimon field type from cache
			DataType paimonType = fieldTypeMap.get(fieldName);

			// Convert value to Paimon-compatible type
			values[index++] = convertValueToPaimonType(value, paimonType);
		}

		return GenericRow.of(values);
	}

	/**
	 * Convert value to Paimon-compatible type
	 *
	 * @param value      original value
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

		// Handle DECIMAL type
		if (typeString.contains("DECIMAL")) {
			if (value instanceof BigDecimal) {
				java.math.BigDecimal bigDecimal = (BigDecimal) value;
				// Extract precision and scale from the type string
				// Format: DECIMAL(precision, scale)
				int precision = 38; // default precision
				int scale = 10; // default scale

				try {
					int startIdx = typeString.indexOf("(");
					int commaIdx = typeString.indexOf(",");
					int endIdx = typeString.indexOf(")");

					if (startIdx > 0 && commaIdx > 0 && endIdx > 0) {
						precision = Integer.parseInt(typeString.substring(startIdx + 1, commaIdx).trim());
						scale = Integer.parseInt(typeString.substring(commaIdx + 1, endIdx).trim());
					}
				} catch (Exception e) {
					// Use default values if parsing fails
				}

				return Decimal.fromBigDecimal(bigDecimal, precision, scale);
			} else if (value instanceof Number) {
				// Convert other numeric types to BigDecimal first
				BigDecimal bigDecimal = new BigDecimal(value.toString());
				return Decimal.fromBigDecimal(bigDecimal, 38, 10);
			} else if (value instanceof String) {
				// Convert string to BigDecimal
				BigDecimal bigDecimal = new BigDecimal((String) value);
				return Decimal.fromBigDecimal(bigDecimal, 38, 10);
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

	/**
	 * Flush all accumulated records for all tables
	 * This should be called before closing the connector to ensure all data is committed
	 */
	public void flushAll() throws Exception {
		for (String tableKey : new ArrayList<>(streamWriterCache.keySet())) {
			flushTable(tableKey);
		}
	}

	/**
	 * Flush accumulated records for a specific table
	 *
	 * @param tableKey table key (database.tableName)
	 */
	public void flushTable(String tableKey) throws Exception {
		AtomicInteger recordCount = accumulatedRecordCount.get(tableKey);
		if (recordCount == null || recordCount.get() <= 0) {
			return; // Nothing to flush
		}

		StreamTableWrite writer = streamWriterCache.get(tableKey);
		StreamTableCommit commit = streamCommitCache.get(tableKey);

		if (writer == null || commit == null) {
			return; // Writer or commit not initialized
		}

		// Use lock to ensure thread safety
		Object lock = commitLocks.computeIfAbsent(tableKey, k -> new Object());
		synchronized (lock) {
			int finalCount = recordCount.get();
			if (finalCount > 0) {
				// Prepare and commit
				long commitIdentifier = commitIdentifierGenerator.incrementAndGet();
				List<CommitMessage> messages = writer.prepareCommit(false, commitIdentifier);
				commit.commit(commitIdentifier, messages);

				// Reset counters
				recordCount.set(0);
				AtomicLong lastCommit = lastCommitTime.get(tableKey);
				if (lastCommit != null) {
					lastCommit.set(System.currentTimeMillis());
				}
			}
		}
	}

	@Override
	public void close() {
		// Flush all accumulated data before closing
		try {
			flushAll();
		} catch (Exception e) {
			// Log error but continue with cleanup
			System.err.println("Error flushing accumulated data: " + e.getMessage());
		}

		cleanupAllResources();
	}
}
