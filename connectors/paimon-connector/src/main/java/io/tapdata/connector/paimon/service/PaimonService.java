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
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.*;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.*;
import org.apache.paimon.table.source.TableScan.Plan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.Snapshot;
import org.apache.paimon.utils.SnapshotManager;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
	private final Map<String, List<DataField>> paimonFieldCache = Collections.synchronizedMap(
			new LinkedHashMap<String, List<DataField>>(5, 0.75f, true) {
				private static final long serialVersionUID = 1L;

				@Override
				protected boolean removeEldestEntry(Map.Entry<String, List<DataField>> eldest) {
					return size() > 10;
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
					return size() > 10;
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
			conf.set("fs.s3a.impl.disable.cache", "true");
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
			TapField tapField = new TapField(field.name(), field.type().getTypeRoot().name());
			tapField.setNullable(field.type().isNullable());
			tapTable.add(tapField);
		}

		// Set primary keys
		List<String> primaryKeys = paimonTable.primaryKeys();
		if (primaryKeys != null && !primaryKeys.isEmpty()) {
			TapIndex tapIndex = new TapIndex().name("PRIMARY")
					.unique(true)
					.primary(true);
			tapIndex.setIndexFields(primaryKeys.stream().map(key -> new TapIndexField().name(key).fieldAsc(true)).collect(Collectors.toList()));
			tapTable.add(tapIndex);
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
		String pureDataType = StringKit.removeParentheses(dataType);
		switch (pureDataType) {
			case "BOOLEAN":
				return DataTypes.BOOLEAN();
			case "TINYINT":
				return DataTypes.TINYINT();
			case "SMALLINT":
				return DataTypes.SMALLINT();
			case "INT":
				return DataTypes.INT();
			case "BIGINT":
				return DataTypes.BIGINT();
			case "FLOAT":
				return DataTypes.FLOAT();
			case "DOUBLE":
				return DataTypes.DOUBLE();
			case "DECIMAL":
				return DataTypes.DECIMAL(tapField.getPrecision(), tapField.getScale());
			case "DATE":
				return DataTypes.DATE();
			case "TIME":
				return DataTypes.TIME(tapField.getScale());
			case "TIMESTAMP":
				return DataTypes.TIMESTAMP(tapField.getScale());
			case "TIMESTAMP_LTZ":
				return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(tapField.getScale());
			case "BINARY":
				return DataTypes.BINARY(tapField.getLength());
			case "VARBINARY":
				return DataTypes.VARBINARY(tapField.getLength());
			case "CHAR":
				return DataTypes.CHAR(tapField.getLength());
			case "VARCHAR":
				return DataTypes.VARCHAR(tapField.getLength());
			case "ARRAY":
				return DataTypes.ARRAY(DataTypes.STRING());
			case "MAP":
				return DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING());
			case "ROW":
				return DataTypes.ROW(DataTypes.STRING());
			case "MULTISET":
				return DataTypes.MULTISET(DataTypes.STRING());
			case "VARIANT":
				return DataTypes.VARIANT();
			default:
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

			try {
				// Get or create cached writer and commit
				StreamTableWrite writer = getOrCreateStreamWriter(tableKey, identifier);
				StreamTableCommit commit = getOrCreateStreamCommit(tableKey, identifier);

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
				Throwable illegalThreadStateException = CommonUtils.matchThrowable(e, IllegalThreadStateException.class);
				if (null != illegalThreadStateException) {
					String message = String.format("Failed to write records to table %s occurred illegal thread state exception, current thread name: %s, thread group: %s",
							tableName, Thread.currentThread().getName(), Thread.currentThread().getThreadGroup() != null ? Thread.currentThread().getThreadGroup().getName() : "null");
					throw new RuntimeException(message, e);
				} else {
					throw new RuntimeException("Failed to write records to table " + tableName, e);
				}
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
				if (catalog instanceof CachingCatalog) {
					CachingCatalog cachingCatalog = (CachingCatalog) catalog;
					Catalog wrapped = cachingCatalog.wrapped();
					if (wrapped instanceof FileSystemCatalog) {
						FileSystemCatalog fileSystemCatalog = (FileSystemCatalog) wrapped;
						FileIO fileIO = null;
						try {
							fileIO = fileSystemCatalog.fileIO();
						} catch (Throwable ignore) {
							// Ignore fileIO lookup errors
						}

						// Best-effort close: proactively close FileSystem instances cached by HadoopFileIO
						closeHadoopFileIOCachedFileSystems(fileIO);
						closeQuietly(fileIO);
					}
				}

				catalog.close();
			} catch (Throwable e) {
				// Ignore close errors
			} finally {
				catalog = null;
			}
		}

		// Wait a bit to ensure all internal threads are cleaned up
		// This is critical to avoid ThreadGroup destroyed errors
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private void closeQuietly(Closeable closeable) {
		if (closeable == null) {
			return;
		}
		try {
			closeable.close();
		} catch (Exception ignore) {
			// Ignore close errors
		}
	}

	/**
	 * Best-effort close for cached Hadoop FileSystem instances inside Paimon HadoopFileIO.
	 * <p>
	 * HadoopFileIO may cache FileSystem instances (e.g., in a field named "fsMap"). Even if
	 * Hadoop global FileSystem cache is disabled, this internal cache can still keep an S3A
	 * FileSystem whose thread factory captured a Task ThreadGroup that will be destroyed later.
	 */
	private void closeHadoopFileIOCachedFileSystems(Object fileIO) {
		if (!(fileIO instanceof HadoopFileIO)) {
			return;
		}

		try {
			Field fsMapField = fileIO.getClass().getDeclaredField("fsMap");
			fsMapField.setAccessible(true);
			Object fsMapObject = fsMapField.get(fileIO);
			if (!(fsMapObject instanceof Map)) {
				return;
			}

			Map<?, ?> fsMap = (Map<?, ?>) fsMapObject;
			if (fsMap.isEmpty()) {
				return;
			}

			// Copy values first to avoid ConcurrentModificationException in case close triggers internal updates.
			List<Object> fileSystems = new ArrayList<>(fsMap.values());
			for (Object fs : fileSystems) {
				if (fs instanceof FileSystem) {
					try {
						((FileSystem) fs).close();
					} catch (Exception ignore) {
						// Ignore close errors
					}
				}
			}

			try {
				fsMap.clear();
			} catch (Exception ignore) {
				// Ignore clear errors
			}
		} catch (NoSuchFieldException ignore) {
			// HadoopFileIO implementation differs; ignore.
		} catch (Throwable ignore) {
			// Best-effort only
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
			Throwable illegalThreadStateException = CommonUtils.matchThrowable(e, IllegalThreadStateException.class);
			if (illegalThreadStateException != null) {
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
	 * Uses RowKind.UPDATE_BEFORE (U-) and RowKind.UPDATE_AFTER (U+) to implement update
	 *
	 * @param event  update event
	 * @param writer stream writer
	 * @param table  table definition
	 * @throws Exception if update fails
	 */
	private void handleStreamUpdate(TapUpdateRecordEvent event, StreamTableWrite writer, TapTable table) throws Exception {
		String database = config.getDatabase();
		Identifier identifier = Identifier.create(database, table.getName());

		Map<String, Object> before = event.getBefore();
		Map<String, Object> after = event.getAfter();

		// Convert before and after data to GenericRow first to avoid duplicate conversion
		GenericRow beforeRow = null;
		if (before != null && !before.isEmpty()) {
			beforeRow = convertToGenericRow(before, table, identifier);
		}
		GenericRow afterRow = convertToGenericRow(after, table, identifier);

		// Check if primary key update detection is enabled
		Boolean enablePkUpdate = config.getEnablePrimaryKeyUpdate();
		if (enablePkUpdate != null && enablePkUpdate) {
			// Validate that before data is available when primary key update detection is enabled
			if (beforeRow == null) {
				throw new RuntimeException("Primary key update detection is enabled but before data is not available. " +
						"Please ensure the source database can provide before-update data or disable this feature.");
			}

			// Check if primary key has changed
			if (isPrimaryKeyChanged(beforeRow, afterRow, table)) {
				// Convert update to delete + insert
				// First, write DELETE using before data
				beforeRow.setRowKind(RowKind.DELETE);
				if (config.getBucketMode().equals("fixed")) {
					writer.write(beforeRow);
				} else {
					int bucket = selectBucketForDynamic(beforeRow, table);
					writer.write(beforeRow, bucket);
				}

				// Then, write INSERT using after data
				afterRow.setRowKind(RowKind.INSERT);
				if (config.getBucketMode().equals("fixed")) {
					writer.write(afterRow);
				} else {
					int bucket = selectBucketForDynamic(afterRow, table);
					writer.write(afterRow, bucket);
				}
				return;
			}
		}

		// Normal update logic: Write U- (UPDATE_BEFORE) if before data exists
		if (beforeRow != null) {
			beforeRow.setRowKind(RowKind.UPDATE_BEFORE);
			if (config.getBucketMode().equals("fixed")) {
				writer.write(beforeRow);
			} else {
				int bucket = selectBucketForDynamic(beforeRow, table);
				writer.write(beforeRow, bucket);
			}
		}

		// Write U+ (UPDATE_AFTER) using after data
		afterRow.setRowKind(RowKind.UPDATE_AFTER);
		if (config.getBucketMode().equals("fixed")) {
			writer.write(afterRow);
		} else {
			int bucket = selectBucketForDynamic(afterRow, table);
			writer.write(afterRow, bucket);
		}
	}

	/**
	 * Check if primary key values have changed between before and after GenericRow
	 * Uses converted GenericRow values to ensure consistent comparison
	 *
	 * @param beforeRow before GenericRow (must not be null)
	 * @param afterRow  after GenericRow (must not be null)
	 * @param table     table definition
	 * @return true if primary key has changed, false otherwise
	 */
	private boolean isPrimaryKeyChanged(GenericRow beforeRow, GenericRow afterRow, TapTable table) {
		// Get primary key fields
		Collection<String> primaryKeys = table.primaryKeys(true);
		if (primaryKeys == null || primaryKeys.isEmpty()) {
			// No primary key defined, no change detection needed
			return false;
		}

		// Get field index mapping
		Map<String, TapField> fields = table.getNameFieldMap();
		String cacheKey = table.getId();
		Map<String, Integer> indexMap = getFieldIndexMap(cacheKey, fields);

		// Build concatenated string of primary key values from before and after
		// Use same order for comparison
		List<String> pkList = new ArrayList<>(primaryKeys);
		StringBuilder beforePkStr = new StringBuilder();
		StringBuilder afterPkStr = new StringBuilder();

		for (String pkField : pkList) {
			Integer fieldIndex = indexMap.get(pkField);
			if (fieldIndex == null || fieldIndex < 0 || fieldIndex >= beforeRow.getFieldCount()) {
				continue;
			}

			Object beforeValue = beforeRow.getField(fieldIndex);
			Object afterValue = afterRow.getField(fieldIndex);

			// Convert to string for comparison
			String beforeStr = beforeValue == null ? "NULL" : String.valueOf(beforeValue);
			String afterStr = afterValue == null ? "NULL" : String.valueOf(afterValue);

			beforePkStr.append(beforeStr).append("|");
			afterPkStr.append(afterStr).append("|");
		}

		// Compare concatenated primary key strings
		return !beforePkStr.toString().contentEquals(afterPkStr);
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
		row.setRowKind(RowKind.DELETE);
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
	 * <p>
	 * Note: This method uses the converted GenericRow values to ensure consistent
	 * bucket selection across insert/update/delete operations, especially for
	 * Date/DateTime types that are converted to int/long values.
	 *
	 * @param row   converted GenericRow with Paimon-compatible values
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
	 * @param fields   field map
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
	 * @param fields    field map
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
		List<DataField> paimonFields = paimonFieldCache.get(cacheKey);

		if (paimonFields == null) {
			// Cache miss - build field type mapping
			Table paimonTable = catalog.getTable(identifier);
			paimonFields = paimonTable.rowType().getFields();

			// Store in cache
			paimonFieldCache.put(cacheKey, paimonFields);
		}

		GenericRow genericRow = new GenericRow(paimonFields.size());
		for (int i = 0; i < paimonFields.size(); i++) {
			DataField dataField = paimonFields.get(i);
			String fieldName = dataField.name();
			Object value = data.get(fieldName);

			// Get corresponding Paimon field type from cache
			DataType paimonType = dataField.type();

			genericRow.setField(i, convertValueToPaimonType(value, paimonType));
		}

		return genericRow;
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

	/**
	 * Convert timestamp to stream offset (snapshot IDs) for specified tables
	 *
	 * This method finds the snapshot ID that is earlier than or equal to the given timestamp
	 * for each table. The snapshot ID can be used to resume stream reading from that point.
	 *
	 * @param tables    list of table names to get offset for
	 * @param timestamp timestamp in milliseconds
	 * @param log       logger
	 * @return map of table name to snapshot ID
	 * @throws Exception if conversion fails
	 */
	public Object timestampToStreamOffset(List<String> tables, Long timestamp, Log log) throws Exception {
		log.info("Converting timestamp {} to stream offset for {} tables", timestamp, tables.size());

		Map<String, Object> offsetMap = new HashMap<>();
		String database = config.getDatabase();

		// For each table, find the snapshot ID at or before the given timestamp
		for (String tableName : tables) {
			try {
				Identifier identifier = Identifier.create(database, tableName);
				Table paimonTable = catalog.getTable(identifier);

				// Use reflection to access snapshotManager() method
				// AbstractFileStoreTable is not public, so we need to use reflection
				try {
					java.lang.reflect.Method snapshotManagerMethod = paimonTable.getClass().getMethod("snapshotManager");
					SnapshotManager snapshotManager = (SnapshotManager) snapshotManagerMethod.invoke(paimonTable);

					// Find snapshot at or before the given timestamp
					Snapshot snapshot = snapshotManager.earlierOrEqualTimeMills(timestamp);

					if (snapshot != null) {
						long snapshotId = snapshot.id();
						offsetMap.put(tableName, snapshotId);
						log.info("Table {} - found snapshot {} at timestamp {}", tableName, snapshotId, snapshot.timeMillis());
					} else {
						// No snapshot found at or before the timestamp, use null (will start from beginning)
						offsetMap.put(tableName, null);
						log.warn("Table {} - no snapshot found at or before timestamp {}, will start from beginning", tableName, timestamp);
					}
				} catch (NoSuchMethodException e) {
					log.warn("Table {} does not have snapshotManager() method, cannot find snapshot by timestamp", tableName);
					offsetMap.put(tableName, null);
				}
			} catch (Catalog.TableNotExistException e) {
				log.warn("Table {} does not exist, skipping", tableName);
			} catch (Exception e) {
				log.error("Error finding snapshot for table {}: {}", tableName, e.getMessage(), e);
				// Put null to start from beginning for this table
				offsetMap.put(tableName, null);
			}
		}

		log.info("Timestamp to offset conversion result: {}", offsetMap);
		return offsetMap;
	}

	/**
	 * Stream read records from Paimon table (CDC mode)
	 *
	 * @param tables            list of tables to read from
	 * @param offsetState       offset state for resuming read
	 * @param eventBatchSize    batch size for events
	 * @param eventsOffsetConsumer consumer for events and offset
	 * @param connectorContext  connector context
	 * @throws Exception if read fails
	 */
	public void streamRead(List<String> tables, Object offsetState, int eventBatchSize,
						   java.util.function.BiConsumer<List<io.tapdata.entity.event.TapEvent>, Object> eventsOffsetConsumer,
						   TapConnectorContext connectorContext, Supplier<Boolean> running) throws Exception {
		Log log = connectorContext.getLog();
		log.info("Starting stream read from tables: {}", tables);

		String database = config.getDatabase();

		// Parse offset state - each table has its own snapshot ID
		Map<String, Long> tableSnapshots = new HashMap<>();
		if (offsetState instanceof Map) {
			Map<String, Object> offsetMap = (Map<String, Object>) offsetState;
			for (Map.Entry<String, Object> entry : offsetMap.entrySet()) {
				if (entry.getValue() != null) {
					tableSnapshots.put(entry.getKey(), Long.parseLong(entry.getValue().toString()) + 1);
				}
			}
			log.info("Resuming stream read from snapshots: {}", tableSnapshots);
		} else if (offsetState == null) {
			// First time stream read - start from AFTER latest snapshot to avoid reading historical data
			log.info("No offset state found, initializing stream read from after latest snapshots");

			// For each table, get the latest snapshot ID
			for (String tableName : tables) {
				try {
					Identifier identifier = Identifier.create(database, tableName);
					Table paimonTable = catalog.getTable(identifier);

					// Use reflection to access snapshotManager() method
					try {
						java.lang.reflect.Method snapshotManagerMethod = paimonTable.getClass().getMethod("snapshotManager");
						SnapshotManager snapshotManager = (SnapshotManager) snapshotManagerMethod.invoke(paimonTable);

						// Get the latest snapshot
						Snapshot latestSnapshot = snapshotManager.latestSnapshot();

						if (latestSnapshot != null) {
							long snapshotId = latestSnapshot.id();
							// IMPORTANT: restore(snapshotId) will INCLUDE that snapshot's data
							// To start from AFTER the latest snapshot, we need to use snapshotId + 1
							// This way, only NEW data after current snapshot will be read
							long nextSnapshotId = snapshotId + 1;
							tableSnapshots.put(tableName, nextSnapshotId);
							log.info("Table {} - initialized to start AFTER latest snapshot {} (will start from snapshot {})",
									tableName, snapshotId, nextSnapshotId);
						} else {
							// No snapshot exists yet, stream read will start from the first snapshot when it's created
							log.info("Table {} - no snapshots exist yet, will start from first snapshot", tableName);
							// Don't put anything in tableSnapshots, let it start naturally
						}
					} catch (NoSuchMethodException e) {
						log.warn("Table {} does not have snapshotManager() method", tableName);
					}
				} catch (Catalog.TableNotExistException e) {
					log.warn("Table {} does not exist, skipping", tableName);
				} catch (Exception e) {
					log.error("Error getting latest snapshot for table {}: {}", tableName, e.getMessage(), e);
				}
			}

			log.info("Initialized stream read to start after latest snapshots: {}", tableSnapshots);
		}

		// Initialize stream scans for all tables
		Map<String, StreamTableScan> streamScans = new HashMap<>();
		Map<String, TableRead> tableReads = new HashMap<>();
		Map<String, List<DataField>> paimonFieldsMap = new HashMap<>();
		Map<String, Map<String, TapField>> tapFieldsMap = new HashMap<>();

		for (String tableName : tables) {
			Identifier identifier = Identifier.create(database, tableName);

			// Get Paimon table
			Table paimonTable;
			try {
				paimonTable = catalog.getTable(identifier);
			} catch (Catalog.TableNotExistException e) {
				log.warn("Table {} does not exist, skipping stream read", tableName);
				continue;
			}

			// Get TapTable definition
			TapTable tapTable = connectorContext.getTableMap().get(tableName);
			if (tapTable == null) {
				log.warn("TapTable definition not found for table: {}, skipping", tableName);
				continue;
			}

			// Create read builder
			ReadBuilder readBuilder = paimonTable.newReadBuilder();

			// Create stream scan
			StreamTableScan streamScan = readBuilder.newStreamScan();

			// Restore from offset if available
			Long startSnapshotId = tableSnapshots.get(tableName);
			if (startSnapshotId != null) {
				streamScan.restore(startSnapshotId);
				log.info("Restored table {} from snapshot: {}", tableName, startSnapshotId);
			}

			// Get field names and types for conversion
			List<DataField> paimonFields = paimonTable.rowType().getFields();
			Map<String, TapField> tapFields = tapTable.getNameFieldMap();

			// Create table read
			TableRead tableRead = readBuilder.newRead();

			// Store in maps
			streamScans.put(tableName, streamScan);
			tableReads.put(tableName, tableRead);
			paimonFieldsMap.put(tableName, paimonFields);
			tapFieldsMap.put(tableName, tapFields);

			log.info("Initialized stream scan for table: {}", tableName);
		}

		if (streamScans.isEmpty()) {
			log.warn("No valid tables to stream read");
			return;
		}

		log.info("Starting continuous stream read for {} tables with multi-threading", streamScans.size());

		// Create thread pool for table scanning - one thread per table
		int threadCount = Math.min(streamScans.size(), Runtime.getRuntime().availableProcessors());
		ExecutorService executorService = new ThreadPoolExecutor(
				threadCount,
				threadCount,
				60L,
				TimeUnit.SECONDS,
				new LinkedBlockingQueue<>(),
				r -> {
					Thread t = new Thread(r);
					t.setName("Paimon-StreamRead-" + t.getId());
					t.setDaemon(true);
					return t;
				}
		);

		AtomicReference<Throwable> threadException = new AtomicReference<>();
		BlockingQueue<io.tapdata.entity.event.TapEvent> eventQueue = new LinkedBlockingQueue<>(eventBatchSize * 10);
		Map<String, Long> currentOffsets = new ConcurrentHashMap<>();

		// Initialize offsets
		for (String tableName : streamScans.keySet()) {
			Long snapshot = tableSnapshots.get(tableName);
			if (snapshot != null) {
				currentOffsets.put(tableName, snapshot);
			}
		}

		// Start consumer thread to collect events and send to downstream
		Thread consumerThread = new Thread(() -> {
			List<io.tapdata.entity.event.TapEvent> batch = new ArrayList<>();
			try {
				while (running.get() || !eventQueue.isEmpty()) {
					io.tapdata.entity.event.TapEvent event = eventQueue.poll(100, TimeUnit.MILLISECONDS);
					if (event != null) {
						batch.add(event);

						// Send batch when size reached
						if (batch.size() >= eventBatchSize) {
							Map<String, Object> offsets = new HashMap<>(currentOffsets);
							eventsOffsetConsumer.accept(batch, offsets);
							batch = new ArrayList<>();
						}
					}

					// When stopping, send remaining batch even if not full
					// This ensures no data loss when running becomes false
					if (!running.get() && !batch.isEmpty()) {
						Map<String, Object> offsets = new HashMap<>(currentOffsets);
						eventsOffsetConsumer.accept(batch, offsets);
						batch = new ArrayList<>();
					}
				}

				// Send any remaining events (final safety check)
				if (!batch.isEmpty()) {
					Map<String, Object> offsets = new HashMap<>(currentOffsets);
					eventsOffsetConsumer.accept(batch, offsets);
				}
			} catch (Exception e) {
				log.error("Error in consumer thread: {}", e.getMessage(), e);
				threadException.set(e);
            }
		});
		consumerThread.setName("Paimon-StreamRead-Consumer");
		consumerThread.setDaemon(true);
		consumerThread.start();

		// Submit scanning tasks for each table
		for (String tableName : streamScans.keySet()) {
			StreamTableScan streamScan = streamScans.get(tableName);
			TableRead tableRead = tableReads.get(tableName);
			List<DataField> paimonFields = paimonFieldsMap.get(tableName);
			Map<String, TapField> tapFields = tapFieldsMap.get(tableName);

			executorService.submit(() -> {
				log.info("Started stream read thread for table: {}", tableName);
				try {
					while (running.get()) {
						// Check for exceptions in other threads
						if (threadException.get() != null) {
							break;
						}

						// Plan next batch of splits
						Plan plan = streamScan.plan();
						List<Split> splits = plan.splits();

						if (splits.isEmpty()) {
							// No new data, update checkpoint and wait
							Long currentSnapshot = streamScan.checkpoint();
							currentOffsets.put(tableName, currentSnapshot);
							Thread.sleep(1000);
							continue;
						}

						log.debug("Table {} has {} new splits to read", tableName, splits.size());

						// Read data from each split
						long totalRecords = 0;
						for (Split split : splits) {
							if (!running.get()) {
								break;
							}

							// Create record reader for this split
							RecordReader<InternalRow> reader = tableRead.createReader(split);

							try {
								// Read records from this split
								RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();

								while (iterator != null && running.get()) {
									InternalRow row;
									while ((row = iterator.next()) != null) {
										// Convert InternalRow to Map
										Map<String, Object> data = convertInternalRowToMap(row, paimonFields, tapFields);

										// Determine event type based on RowKind
										io.tapdata.entity.event.TapEvent event = createEventFromRowKind(row, data, tableName);

										if (event != null) {
											// Add to queue, block if queue is full
											eventQueue.put(event);
											totalRecords++;
										}
									}

									// Release current batch
									iterator.releaseBatch();

									// Read next batch
									iterator = reader.readBatch();
								}

							} finally {
								// Close reader
								try {
									reader.close();
								} catch (Exception e) {
									log.warn("Error closing reader for table {}: {}", tableName, e.getMessage());
								}
							}
						}

						// Save checkpoint for this table
						Long currentSnapshot = streamScan.checkpoint();
						currentOffsets.put(tableName, currentSnapshot);

						log.debug("Stream read batch completed for table: {}, records: {}", tableName, totalRecords);
					}
				} catch (InterruptedException e) {
					log.warn("Stream read thread interrupted for table: {}", tableName);
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					log.error("Error in stream read thread for table {}: {}", tableName, e.getMessage(), e);
					threadException.set(e);
					return;
				}
				log.info("Stream read thread stopped for table: {}", tableName);
			});
		}

		// Wait for threads to complete or exception to occur
		try {
			while (running.get()) {
				if (threadException.get() != null) {
					throw new RuntimeException("Stream read failed", threadException.get());
				}
				Thread.sleep(1000);
			}
		} finally {
			executorService.shutdown();
			try {
				if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
					executorService.shutdownNow();
				}
			} catch (InterruptedException e) {
				executorService.shutdownNow();
				Thread.currentThread().interrupt();
			}

			// Wait for consumer thread
			try {
				consumerThread.join(5000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		log.info("Stream read completed for all tables");
	}

	/**
	 * Create TapEvent from InternalRow based on RowKind
	 *
	 * @param row       internal row
	 * @param data      converted data map
	 * @param tableName table name
	 * @return TapEvent (Insert, Update, or Delete)
	 */
	private io.tapdata.entity.event.TapEvent createEventFromRowKind(InternalRow row, Map<String, Object> data, String tableName) {
		RowKind rowKind = row.getRowKind();

		switch (rowKind) {
			case INSERT:
			case UPDATE_AFTER:
				// For INSERT and UPDATE_AFTER, create insert event
				io.tapdata.entity.event.dml.TapInsertRecordEvent insertEvent =
						new io.tapdata.entity.event.dml.TapInsertRecordEvent().init();
				insertEvent.setTableId(tableName);
				insertEvent.setAfter(data);
				insertEvent.setReferenceTime(System.currentTimeMillis());
				return insertEvent;

			case DELETE:
			case UPDATE_BEFORE:
				// For DELETE and UPDATE_BEFORE, create delete event
				io.tapdata.entity.event.dml.TapDeleteRecordEvent deleteEvent =
						new io.tapdata.entity.event.dml.TapDeleteRecordEvent().init();
				deleteEvent.setTableId(tableName);
				deleteEvent.setBefore(data);
				deleteEvent.setReferenceTime(System.currentTimeMillis());
				return deleteEvent;

			default:
				// Unknown row kind, skip
				return null;
		}
	}

	/**
	 * Batch read records from Paimon table
	 *
	 * @param table             table definition
	 * @param offsetState       offset state for resuming read (not used for now)
	 * @param eventBatchSize    batch size for events
	 * @param eventsOffsetConsumer consumer for events and offset
	 * @param connectorContext  connector context
	 * @throws Exception if read fails
	 */
	public void batchRead(TapTable table, Object offsetState, int eventBatchSize,
						  java.util.function.BiConsumer<List<io.tapdata.entity.event.TapEvent>, Object> eventsOffsetConsumer,
						  TapConnectorContext connectorContext) throws Exception {
		String database = config.getDatabase();
		String tableName = table.getName();
		Identifier identifier = Identifier.create(database, tableName);

		Log log = connectorContext.getLog();
		log.info("Starting batch read from table: {}", tableName);

		// Get Paimon table
		Table paimonTable;
		try {
			paimonTable = catalog.getTable(identifier);
		} catch (Catalog.TableNotExistException e) {
			log.warn("Table {} does not exist, skipping batch read", tableName);
			return;
		}

		// Create read builder
		ReadBuilder readBuilder = paimonTable.newReadBuilder();

		// Create table scan to get splits
		TableScan tableScan = readBuilder.newScan();
		TableScan.Plan plan = tableScan.plan();
		List<Split> splits = plan.splits();

		log.info("Table {} has {} splits to read", tableName, splits.size());

		// Get field names and types for conversion
		List<DataField> paimonFields = paimonTable.rowType().getFields();
		Map<String, TapField> tapFields = table.getNameFieldMap();

		// Read data from each split
		long totalRecords = 0;
		for (Split split : splits) {
			// Create record reader for this split
			RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split);

			List<io.tapdata.entity.event.TapEvent> events = new ArrayList<>();

			try {
				// Read records from this split using RecordReader.RecordIterator
				RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();

				while (iterator != null) {
					InternalRow row;
					while ((row = iterator.next()) != null) {
						// Check RowKind to filter out intermediate states
						// Only read final state data (INSERT and UPDATE_AFTER)
						RowKind rowKind = row.getRowKind();
						if (rowKind == RowKind.UPDATE_BEFORE || rowKind == RowKind.DELETE) {
							// Skip intermediate states (UPDATE_BEFORE and DELETE)
							// These are not final state data
							continue;
						}

						// Convert InternalRow to Map
						Map<String, Object> data = convertInternalRowToMap(row, paimonFields, tapFields);

						// Create TapInsertRecordEvent
						io.tapdata.entity.event.dml.TapInsertRecordEvent event =
								new io.tapdata.entity.event.dml.TapInsertRecordEvent().init();
						event.setTableId(tableName);
						event.setAfter(data);

						events.add(event);
						totalRecords++;

						// Send batch when size reached
						if (events.size() >= eventBatchSize) {
							eventsOffsetConsumer.accept(events, null);
							events = new ArrayList<>();
						}
					}

					// Release current batch
					iterator.releaseBatch();

					// Read next batch
					iterator = reader.readBatch();
				}

				// Send remaining events
				if (!events.isEmpty()) {
					eventsOffsetConsumer.accept(events, null);
				}

			} finally {
				// Close reader
				try {
					reader.close();
				} catch (Exception e) {
					log.warn("Error closing reader: {}", e.getMessage());
				}
			}
		}

		log.info("Batch read completed for table: {}, total records: {}", tableName, totalRecords);
	}

	/**
	 * Convert Paimon InternalRow to Map
	 *
	 * @param row          Paimon internal row
	 * @param paimonFields Paimon field definitions
	 * @param tapFields    TapData field definitions
	 * @return data map
	 */
	private Map<String, Object> convertInternalRowToMap(InternalRow row, List<DataField> paimonFields,
														 Map<String, TapField> tapFields) {
		Map<String, Object> data = new LinkedHashMap<>();

		for (int i = 0; i < paimonFields.size(); i++) {
			DataField paimonField = paimonFields.get(i);
			String fieldName = paimonField.name();
			DataType dataType = paimonField.type();

			// Check if field is null
			if (row.isNullAt(i)) {
				data.put(fieldName, null);
				continue;
			}

			// Convert value based on data type
			Object value = convertPaimonValueToJava(row, i, dataType);
			data.put(fieldName, value);
		}

		return data;
	}

	/**
	 * Convert Paimon value to Java object
	 *
	 * @param row      internal row
	 * @param pos      field position
	 * @param dataType Paimon data type
	 * @return Java object
	 */
	private Object convertPaimonValueToJava(InternalRow row, int pos, DataType dataType) {
		String typeString = dataType.toString().toUpperCase();

		if (typeString.contains("BOOLEAN")) {
			return row.getBoolean(pos);
		} else if (typeString.contains("TINYINT")) {
			return row.getByte(pos);
		} else if (typeString.contains("SMALLINT")) {
			return row.getShort(pos);
		} else if (typeString.contains("INT") && !typeString.contains("BIGINT")) {
			return row.getInt(pos);
		} else if (typeString.contains("BIGINT")) {
			return row.getLong(pos);
		} else if (typeString.contains("FLOAT")) {
			return row.getFloat(pos);
		} else if (typeString.contains("DOUBLE")) {
			return row.getDouble(pos);
		} else if (typeString.contains("DECIMAL")) {
			Decimal decimal = row.getDecimal(pos, dataType.asSQLString().length(), 10);
			return decimal != null ? decimal.toBigDecimal() : null;
		} else if (typeString.contains("STRING") || typeString.contains("VARCHAR") || typeString.contains("CHAR")) {
			BinaryString binaryString = row.getString(pos);
			return binaryString != null ? binaryString.toString() : null;
		} else if (typeString.contains("DATE")) {
			int days = row.getInt(pos);
			// Convert days since epoch to java.sql.Date
			return new java.sql.Date(days * 86400000L);
		} else if (typeString.contains("TIMESTAMP")) {
			Timestamp timestamp = row.getTimestamp(pos, 6);
			if (timestamp != null) {
				// Convert Paimon Timestamp to java.sql.Timestamp
				long millis = timestamp.getMillisecond();
				int nanos = timestamp.getNanoOfMillisecond();
				java.sql.Timestamp sqlTimestamp = new java.sql.Timestamp(millis);
				sqlTimestamp.setNanos(nanos);
				return sqlTimestamp;
			}
			return null;
		} else if (typeString.contains("BINARY") || typeString.contains("BYTES")) {
			return row.getBinary(pos);
		} else {
			// For unknown types, try to get as string
			BinaryString binaryString = row.getString(pos);
			return binaryString != null ? binaryString.toString() : null;
		}
	}

	/**
	 * Count records in Paimon table
	 *
	 * @param table table definition
	 * @param log   logger
	 * @return record count
	 * @throws Exception if count fails
	 */
	public long batchCount(TapTable table, Log log) throws Exception {
		String database = config.getDatabase();
		String tableName = table.getName();
		Identifier identifier = Identifier.create(database, tableName);

		log.info("Counting records in table: {}", tableName);

		// Get Paimon table
		Table paimonTable;
		try {
			paimonTable = catalog.getTable(identifier);
		} catch (Catalog.TableNotExistException e) {
			log.warn("Table {} does not exist, returning count 0", tableName);
			return 0;
		}

		// Create read builder
		ReadBuilder readBuilder = paimonTable.newReadBuilder();

		// Create table scan to get splits
		TableScan tableScan = readBuilder.newScan();
		TableScan.Plan plan = tableScan.plan();
		List<Split> splits = plan.splits();

		log.debug("Table {} has {} splits to count", tableName, splits.size());

		// Count records from all splits
		long totalCount = 0;
		for (Split split : splits) {
			// Create record reader for this split
			RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split);

			try {
				// Read records from this split
				RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();

				while (iterator != null) {
					InternalRow row;
					while ((row = iterator.next()) != null) {
						// Check RowKind to filter out intermediate states
						// Only count final state data (INSERT and UPDATE_AFTER)
						RowKind rowKind = row.getRowKind();
						if (rowKind == RowKind.INSERT || rowKind == RowKind.UPDATE_AFTER) {
							totalCount++;
						}
					}

					// Release current batch
					iterator.releaseBatch();

					// Read next batch
					iterator = reader.readBatch();
				}

			} finally {
				// Close reader
				try {
					reader.close();
				} catch (Exception e) {
					log.warn("Error closing reader: {}", e.getMessage());
				}
			}
		}

		log.info("Table {} has {} records", tableName, totalCount);
		return totalCount;
	}

	/**
	 * Query records by advance filter
	 *
	 * @param table    table definition
	 * @param filter   advance filter with conditions
	 * @param consumer consumer for filter results
	 * @param log      logger
	 * @throws Exception if query fails
	 */
	public void queryByAdvanceFilter(TapTable table, io.tapdata.pdk.apis.entity.TapAdvanceFilter filter,
									 java.util.function.Consumer<io.tapdata.pdk.apis.entity.FilterResults> consumer,
									 Log log) throws Exception {
		String database = config.getDatabase();
		String tableName = table.getName();
		Identifier identifier = Identifier.create(database, tableName);

		log.info("Querying table {} with advance filter", tableName);

		// Get Paimon table
		Table paimonTable;
		try {
			paimonTable = catalog.getTable(identifier);
		} catch (Catalog.TableNotExistException e) {
			log.warn("Table {} does not exist, skipping query", tableName);
			return;
		}

		// Get field names and types for conversion
		List<DataField> paimonFields = paimonTable.rowType().getFields();
		Map<String, TapField> tapFields = table.getNameFieldMap();

		// Create read builder
		ReadBuilder readBuilder = paimonTable.newReadBuilder();

		// Create batch scan
		TableScan tableScan = readBuilder.newScan();
		TableRead tableRead = readBuilder.newRead();

		// Plan all splits
		Plan plan = tableScan.plan();
		List<Split> splits = plan.splits();

		log.debug("Table {} has {} splits to query", tableName, splits.size());

		// Determine batch size
		int batchSize = filter != null && filter.getBatchSize() != null && filter.getBatchSize() > 0
			? filter.getBatchSize() : 1000;

		// Determine limit and skip
		int limit = filter != null && filter.getLimit() != null ? filter.getLimit() : Integer.MAX_VALUE;
		int skip = filter != null && filter.getSkip() != null ? filter.getSkip() : 0;

		io.tapdata.pdk.apis.entity.FilterResults filterResults = new io.tapdata.pdk.apis.entity.FilterResults();
		int skippedCount = 0;
		int returnedCount = 0;

		// Read records from all splits
		outerLoop:
		for (Split split : splits) {
			// Create record reader for this split
			RecordReader<InternalRow> reader = tableRead.createReader(split);

			try {
				// Read records from this split
				RecordReader.RecordIterator<InternalRow> iterator = reader.readBatch();

				while (iterator != null) {
					InternalRow row;
					while ((row = iterator.next()) != null) {
						// Check if we've returned enough records
						if (returnedCount >= limit) {
							break outerLoop;
						}

						// Check RowKind to filter out intermediate states
						// Only read final state data (INSERT and UPDATE_AFTER)
						RowKind rowKind = row.getRowKind();
						if (rowKind == RowKind.UPDATE_BEFORE || rowKind == RowKind.DELETE) {
							// Skip intermediate states
							continue;
						}

						// Convert InternalRow to Map
						Map<String, Object> data = convertInternalRowToMap(row, paimonFields, tapFields);

						// Apply filter conditions
						if (matchesFilter(data, filter)) {
							// Handle skip
							if (skippedCount < skip) {
								skippedCount++;
								continue;
							}

							// Add to results
							filterResults.add(data);
							returnedCount++;

							// Send batch when size reached
							if (filterResults.resultSize() >= batchSize) {
								consumer.accept(filterResults);
								filterResults = new io.tapdata.pdk.apis.entity.FilterResults();
							}
						}
					}

					// Release current batch
					iterator.releaseBatch();

					// Read next batch
					iterator = reader.readBatch();
				}

			} finally {
				// Close reader
				try {
					reader.close();
				} catch (Exception e) {
					log.warn("Error closing reader: {}", e.getMessage());
				}
			}
		}

		// Send remaining results
		if (filterResults.resultSize() > 0) {
			consumer.accept(filterResults);
		}

		log.info("Query completed for table: {}, returned {} records", tableName, returnedCount);
	}

	/**
	 * Check if data matches filter conditions
	 * For now, we only support basic filtering (skip/limit)
	 * Advanced filtering (where conditions) can be added later
	 *
	 * @param data   data map
	 * @param filter advance filter
	 * @return true if matches
	 */
	private boolean matchesFilter(Map<String, Object> data, io.tapdata.pdk.apis.entity.TapAdvanceFilter filter) {
		// For now, we don't support where conditions in Paimon
		// All records match (filtering is done by skip/limit)
		return true;
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

