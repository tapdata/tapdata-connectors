package io.tapdata.connector.paimon.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.TapCallbackOffset;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.exception.TapPdkRetryableEx;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.*;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.*;
import org.apache.paimon.table.source.*;
import org.apache.paimon.table.source.TableScan.Plan;
import org.apache.paimon.types.*;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.disk.IOManagerImpl.splitPaths;

/**
 * Service class for Paimon operations
 *
 * @author Tapdata
 */
public class PaimonService implements AutoCloseable {

	/** 日志和异常信息中使用的当前 Service 类标识。 */
	private static final String TAG = PaimonService.class.getName();
	/** 原始主键字段过多且启用 hashKey 时，在目标表中使用的合成主键字段名。 */
	private static final String HASH_KEY = "_hash_key";
	/**
	 * 当前 JVM 内物理表写入所有权注册表。
	 *
	 * <p>Key 为规范化物理表路径的摘要，Value 为 Service 实例和逻辑表组成的 owner；用于阻止同一
	 * JVM 中多个 writer 同时写动态桶表。该变量不提供跨 JVM 的分布式锁能力。
	 */
	private static final Map<String, String> ACTIVE_PHYSICAL_TABLE_OWNERS = new ConcurrentHashMap<>();
	/** legacy 合成主键使用的摘要算法；保留 MD5 是为了兼容已有表的主键编码。 */
	public static final String HASH_ALGORITHM = "MD5";
	/** legacy 合成主键编码多个原始主键值时使用的分隔符。 */
	public static final byte SPLIT_CHAR = ',';
	/** Key 为表名，Value 表示该表写入时是否需要计算 {@link #HASH_KEY} 合成主键。 */
	private final Map<String, Boolean> computeHashKey = new ConcurrentHashMap<>();
	/** Key 为表名，Value 为生成合成主键时参与计算的原始主键字段集合。 */
	private final Map<String, Collection<String>> primaryKeyMap = new ConcurrentHashMap<>();
	/** 当前连接解析后的 Paimon 配置，提供仓库、Catalog、写入和提交相关参数。 */
	private final PaimonConfig config;
	/** Paimon Catalog 实例，负责数据库、表元数据和 FileStoreTable 的访问。 */
	private Catalog catalog;

	/**
	 * 表级规范写上下文，Key 为 {@code database.tableName}。
	 *
	 * <p>每个物理表只允许一个上下文持有 writer、committer、动态桶 router 和 commit identifier，
	 * 避免同表不同写入对象的路由索引或提交状态相互分离。多表任务因此是“一表一写入、一表一提交器”，
	 * 但各表 snapshot 依次提交，并不构成跨表原子事务；Paimon Flink 多表提交器也按表分组并逐表提交：
	 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/StoreMultiCommitter.java#L150-L176
	 */
	private final Map<String, PaimonTableWriteContext> tableWriteContexts = new ConcurrentHashMap<>();
	/** Key 为逻辑表标识，Value 为其物理表路径摘要，用于释放 JVM 内的物理表 owner。 */
	private final Map<String, String> physicalTableByLogicalTable = new ConcurrentHashMap<>();
	/** 当前 Service 实例的唯一写入 owner 标识，用于区分同 JVM 内的不同连接或任务。 */
	private final String serviceWriterOwner = UUID.randomUUID().toString();

	/**
	 * 表级写入生命周期锁；串行化同一表的写入、prepare/commit、DDL drain 和资源关闭操作。
	 */
	private final Map<String, Object> commitLocks = new ConcurrentHashMap<>();
	/** 正在执行 DDL drain 的逻辑表集合；集合中的表禁止创建或继续使用写上下文。 */
	private final Set<String> drainingTables = ConcurrentHashMap.newKeySet();
	// ===== Micro-batch, offset barrier, scheduler, and lifecycle =====
	private final PaimonMicroBatchCoordinator microBatchCoordinator;
	private final PaimonServiceLifecycle lifecycle = new PaimonServiceLifecycle();
	private final PaimonAsyncCommitScheduler asyncCommitScheduler;
	private final LongSupplier clock;
	private final RetryWaiter retryWaiter;
	private final Object callbackExecutionLock = new Object();
	private final Object closeLock = new Object();
	private final AtomicReference<InterruptedException> cleanupInterruption =
			new AtomicReference<>();
	/** Connector 注入的 offset 回调；只允许在 NEW 状态绑定，运行后不可替换。 */
	private Consumer<Object> flushOffsetCallback;
	/** 最近一次执行写入的任务上下文，供兼容的异步线程取得任务日志；跨线程读取需要 volatile。 */
	private volatile TapConnectorContext activeConnectorContext;
	/**
	 * 与当前 Service 绑定的任务状态 Map，用于持久化稳定 commitUser 和下一个 commit identifier。
	 * 一个 Service 生命周期内禁止切换到另一个任务状态 Map。
	 */
	private volatile KVMap<Object> boundTaskStateMap;
	/**
	 * 写入侧粘滞故障栅栏。首次不可安全继续的异常会保存在这里，后续写入统一失败并要求重启，
	 * 防止复用已经部分推进的 writer、router 或提交状态。
	 */
	private final AtomicReference<Throwable> stickyWriteFailure = new AtomicReference<>();
	/**
	 * 动态桶表的源事件入口保护器，Key 为逻辑表标识。
	 *
	 * <p>PDK 2.0.8 不提供可排序的 source sequence，因此禁止同一动态桶表的重叠入口把锁竞争顺序
	 * 误当成事件顺序；不同表以及 fixed/append 表仍可保持原有并发能力。注意后者只表示桶路由允许
	 * 并发，不证明无 sequence.field 的主键更新可以乱序到达；同一主键若被重叠 callback 更新，
	 * commitLocks 的抢锁顺序仍可能不同于源事件顺序。
	 */
	private final Map<String, DynamicIngressGuard> dynamicSourceIngressGuards = new ConcurrentHashMap<>();

	// ===== Paimon Field Cache for Performance =====
	/**
	 * Paimon 目标字段顺序缓存：Key 为 {@code database.tableName}，Value 为目标 RowType 的
	 * {@link DataField} 列表。使用同步 LRU Map，最多保留 10 张表，减少重复读取 Catalog 元数据。
	 */
	private final Map<String, List<DataField>> paimonFieldCache = Collections.synchronizedMap(
			new LinkedHashMap<String, List<DataField>>(5, 0.75f, true) {
				/** 匿名 LRU Map 的序列化版本标识。 */
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
	/**
	 * save tapContext log
	 */
	private Log log;

	public PaimonService(PaimonConfig config, Log log) {
		this(config, log, System::currentTimeMillis, RetryWaiter.INTERRUPTIBLE_ONE_SECOND);
	}

	PaimonService(
			PaimonConfig config,
			Log log,
			LongSupplier clock,
			RetryWaiter retryWaiter) {
		this(
				config,
				log,
				clock,
				retryWaiter,
				PaimonAsyncCommitScheduler::newDaemonExecutor);
	}

	PaimonService(
			PaimonConfig config,
			Log log,
			LongSupplier clock,
			RetryWaiter retryWaiter,
			PaimonAsyncCommitScheduler.ExecutorFactory schedulerExecutorFactory) {
		this.log = log;
		this.config = config;
		this.clock = Objects.requireNonNull(clock, "clock");
		this.retryWaiter = Objects.requireNonNull(retryWaiter, "retryWaiter");
		this.microBatchCoordinator = new PaimonMicroBatchCoordinator(
				config.getBatchAccumulationSize(), config.getCommitIntervalMs());
		this.asyncCommitScheduler = new PaimonAsyncCommitScheduler(
				Boolean.TRUE.equals(config.getEnableAsyncCommit()) && config.getCommitIntervalMs() > 0,
				microBatchCoordinator,
				clock::getAsLong,
				schedulerExecutorFactory,
				this::flushScheduledTable,
				this::recordStickyFailure);
	}

	/**
	 * Initialize Paimon catalog
	 *
	 * @throws Exception if initialization fails
	 */
	public synchronized void init() throws Exception {
		if (lifecycle.state() != PaimonServiceLifecycle.State.NEW) {
			throw new IllegalStateException(
					"Paimon service cannot initialize while lifecycle is "
							+ lifecycle.state());
		}
		try {
			config.validate();

		// Clean up stale paimon-io-* spill dirs left by abnormally terminated JVMs (OOM/crash/SIGKILL),
		// which would otherwise accumulate and exhaust local disk. Live dirs owned by active sibling
		// tasks in this JVM are protected and never deleted.
		cleanupStaleSpillDirs();

		Options options = new Options();
		options.set("warehouse", config.getFullWarehousePath());

		// Configure storage based on type
		configureStorage(options);

		// Create catalog context with Hadoop configuration (for S3A, etc.)
		Configuration hadoopConf = buildHadoopConfiguration();
		CatalogContext context = CatalogContext.create(options, hadoopConf);

		// Create catalog
			catalog = CatalogFactory.createCatalog(context);
			lifecycle.publishRunning();
		} catch (Throwable failure) {
			recordStickyFailure(failure);
			cleanupAfterInitializationFailure(failure);
			rethrow(failure);
		}
	}

	/** Package-private lifecycle publication for unit tests with injected table contexts. */
	void startForTest() {
		lifecycle.publishRunning();
	}

	/**
	 * Remove stale {@code paimon-io-*} spill directories under the configured temp roots that were
	 * left behind by abnormally terminated JVMs. Best-effort: failures are logged, never thrown.
	 */
	private void cleanupStaleSpillDirs() {
		try {
			String tmpDirs = config.getDiskTmpDir();
			if (StringUtils.isBlank(tmpDirs)) {
				tmpDirs = System.getProperty("java.io.tmpdir", new File(".").getAbsolutePath());
			}
			String[] roots = splitPaths(tmpDirs);
			int deleted = PaimonSpillDirCleaner.cleanupStaleSpillDirs(
					roots,
					PaimonSpillDirCleaner.DEFAULT_STALE_GRACE_MS,
					(path, bytes) -> log.info("Removed stale Paimon spill dir {} ({} bytes)", path, bytes));
			if (deleted > 0) {
				log.info("Cleaned up {} stale Paimon spill dir(s) under {}", deleted, tmpDirs);
			}
		} catch (Exception e) {
			log.warn("Failed to clean up stale Paimon spill dirs: {}", e.getMessage());
		}
	}

	private Log getAsyncCommitLog() {
		TapConnectorContext connectorContext = activeConnectorContext;
		if (connectorContext != null) {
			connectorContext.configContext();
			Log currentLog = connectorContext.getLog();
			if (currentLog != null) {
				return currentLog;
			}
		}
		return log;
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
				// 解决连接重置：调低并发、增大超时
//				options.set("fs.s3a.connection.maximum", "32");
//				options.set("fs.s3a.connection.timeout", "300000");
//				options.set("fs.s3a.socket.timeout", "300000");
//				// 重试机制（解决临时连接失败）
//				options.set("fs.s3a.retry.limit", "5");
//				options.set("fs.s3a.retry.interval", "1000");

				break;
			case "hdfs":
				options.set("fs.defaultFS", "hdfs://" + config.getHdfsHost() + ":" + config.getHdfsPort());
				if (config.getHdfsUser() != null && !config.getHdfsUser().isEmpty()) {
					options.set("hadoop.user.name", config.getHdfsUser());
				}
				break;
			case "oss":
				// REVIEW: these keys match Paimon OSSLoader, but this connector module does not
				// package paimon-oss 1.3.1, so options alone cannot register the oss:// FileIO.
				// Add the matching loader artifact (or remove the advertised storage type).
				// Source:
				// https://github.com/apache/paimon/blob/release-1.3.1/paimon-filesystems/paimon-oss/src/main/java/org/apache/paimon/oss/OSSLoader.java#L52-L68
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
			// S3A fast-upload buffers each upload block to a local directory before sending to S3.
			// Defaults to ${hadoop.tmp.dir}/s3a under /tmp, which can run out of space and fail with
			// "Could not find any valid local directory for s3ablock-...". Redirect the buffer to the
			// configured scratch dir (the same disk used for Paimon spill) and ensure it exists.
			String s3aBufferDir = config.getDiskTmpDir();
			if (StringUtils.isBlank(s3aBufferDir)) {
				s3aBufferDir = System.getProperty("java.io.tmpdir", "/tmp");
			}
			for (String p : s3aBufferDir.split(",")) {
				String dir = p.trim();
				if (!dir.isEmpty()) {
					try {
						new File(dir).mkdirs();
					} catch (Exception ignore) {
						// best-effort directory creation
					}
				}
			}
			conf.set("fs.s3a.buffer.dir", s3aBufferDir);
			if (EmptyKit.isNotEmpty(config.getS3Properties())) {
				config.getS3Properties().forEach(v -> conf.set(v.get("propKey"), v.get("propValue")));
			}
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
		List<String> primaryKeys = paimonTable.primaryKeys();
		for (DataField field : fields) {
			TapField tapField = new TapField(field.name(), field.type().asSQLString().replace("NOT NULL", "").trim());
			tapField.setNullable(field.type().isNullable());
			if (primaryKeys.contains(field.name())) {
				tapField.setPrimaryKey(true);
				tapField.setPrimaryKeyPos(primaryKeys.indexOf(field.name()) + 1);
			}
			tapTable.add(tapField);
		}

		// Set primary keys
		if (primaryKeys != null && !primaryKeys.isEmpty()) {
			TapIndex tapIndex = new TapIndex().name("PRIMARY").unique(true).coreUnique(true).primary(true);
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
	public boolean createTable(TapTable tapTable) throws Exception {
		try (PaimonServiceLifecycle.Ingress ignored = lifecycle.enter("createTable")) {
			return createTableInternal(tapTable);
		}
	}

	private boolean createTableInternal(TapTable tapTable) throws Exception {
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
			Table existingTable = catalog.getTable(identifier);
			if (existingTable instanceof FileStoreTable) {
				FileStoreTable existingFileStoreTable = (FileStoreTable) existingTable;
				BucketMode existingMode = existingFileStoreTable.bucketMode();
				BucketMode configuredMode =
						PaimonWriteSemanticContractResolver.deriveBucketMode(
								existingFileStoreTable.schema(), resolveEffectiveBucket(tableName));

				if (existingMode != configuredMode) {
					// Bucket mode mismatch is informational only. Paimon does not support changing
					// the physical bucket model by recreating an already existing table here.
					log.warn("Table {} already exists with Paimon bucket mode {}, but " +
									"effective config resolves to {}. Cannot switch bucket mode " +
									"for existing table. Using existing table configuration.",
							tableName, existingMode, configuredMode);
				}
			}
			// Table exists, no need to recreate
			return false;
		} catch (Catalog.TableNotExistException e) {
			// Table does not exist, continue to create
		}

		// Build schema
		Schema.Builder schemaBuilder = Schema.newBuilder();

		// Set primary keys
		Collection<String> primaryKeys = tapTable.primaryKeys(true);
		if (primaryKeys != null && !primaryKeys.isEmpty()) {
			if (config.getHashKey(tableName) && primaryKeys.size() > 5) {
				schemaBuilder.primaryKey(Collections.singletonList(HASH_KEY));
			} else {
				schemaBuilder.primaryKey(new ArrayList<>(primaryKeys));
			}
		}

		// Add fields
		Map<String, TapField> fields = tapTable.getNameFieldMap();
		if (fields != null) {
			if (config.getHashKey(tableName) && EmptyKit.isNotEmpty(primaryKeys) && primaryKeys.size() > 5) {
				schemaBuilder.column(HASH_KEY, DataTypes.VARCHAR(32));
			}
			for (Map.Entry<String, TapField> entry : fields.entrySet()) {
				String fieldName = entry.getKey();
				TapField tapField = entry.getValue();
				DataType dataType = convertToPaimonDataType(tapField);
				schemaBuilder.column(fieldName, dataType);
			}
		}

		if (EmptyKit.isNotEmpty(config.getPartitionKey(tableName))) {
			schemaBuilder.partitionKeys(config.getPartitionKey(tableName));
		}

		// Resolve the Connector's first-class dynamic/postpone/fixed choice plus any later native
		// tableProperties.bucket override to Paimon's final bucket value. Paimon defines -1 as
		// dynamic, -2 as postpone, and positive values as fixed; PaimonConfig rejects every other
		// first-class combination instead of silently falling back. The generic tableProperties loop
		// below persists the same final override together with the remaining native options.
		// Sources:
		// https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L100-L112
		// https://github.com/apache/paimon/blob/release-1.3.1/paimon-common/src/main/java/org/apache/paimon/table/BucketMode.java#L63-L73
		// https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java#L99-L109
		// https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/AppendOnlyFileStore.java#L72-L75
		schemaBuilder.option(
				CoreOptions.BUCKET.key(), Integer.toString(resolveEffectiveBucket(tableName)));
		if (EmptyKit.isNotBlank(config.getFileFormat(tableName))) {
			// The final Schema is preflighted with Paimon's FileFormat provider discovery before
			// Catalog#createTable. Use the canonical option constant so first-class and
			// tableProperties values share the same final key.
			// Sources:
			// https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/schema/SchemaValidation.java#L160-L162
			// https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L229-L238
			schemaBuilder.option(
					CoreOptions.FILE_FORMAT.key(), config.getFileFormat(tableName));
		}
		if (EmptyKit.isNotBlank(config.getCompression(tableName))) {
			// Paimon 1.3.1 CoreOptions#fileCompression reads only FILE_COMPRESSION. Using the
			// canonical constant prevents the Connector field name "compression" from becoming an
			// unknown Schema option that Paimon silently ignores.
			// Sources:
			// https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L259-L264
			// https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L2237-L2240
			schemaBuilder.option(
					CoreOptions.FILE_COMPRESSION.key(), config.getCompression(tableName));
		}

		// ===== Performance Optimization Options =====

		// 1. Write buffer size - controls memory buffer for writes
		// Larger buffer = better performance but more memory usage
		if (config.getWriteBufferSize() != null && config.getWriteBufferSize() > 0) {
			schemaBuilder.option("write-buffer-size", config.getWriteBufferSize() + "mb");
		}

		if (Boolean.TRUE.equals(config.getDiskOverflowWrite())) {
			schemaBuilder.option("write-buffer-spillable", "true");
			schemaBuilder.option("write-buffer-spill.max-disk-size", config.getDiskMaxSize() + "gb");
		}

		// 2. Target file size - Paimon will try to create files of this size
		// Larger files = fewer files but slower compaction
		if (config.getTargetFileSize(tableName) != null && config.getTargetFileSize(tableName) > 0) {
			schemaBuilder.option("target-file-size", config.getTargetFileSize(tableName) + "mb");
		}

		// 3. Compaction settings
		if (config.getEnableAutoCompaction(tableName) != null) {
			if (config.getEnableAutoCompaction(tableName)) {
				// Enable full compaction for better query performance
				schemaBuilder.option("compaction.optimization-interval", config.getCompactionIntervalMinutes(tableName) + "min");

				// REVIEW: Paimon 1.3.1 rejects any non-NONE changelog producer on a table without
				// primary keys. With the current default enableAutoCompaction=true, creation of an
				// append-only/BUCKET_UNAWARE table reaches this option and fails schema validation.
				// Source:
				// https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/schema/SchemaValidation.java#L120-L126
				schemaBuilder.option("changelog-producer", "input");

				// Compact small files more aggressively
				schemaBuilder.option("num-sorted-run.compaction-trigger", "30");
				schemaBuilder.option("num-sorted-run.stop-trigger", "2147483647");
			} else {
				// Disable auto compaction
				schemaBuilder.option("write-only", "true");
			}
		}

		// These are on-disk snapshot-expiration bounds, not an in-memory cache. The hard-coded
		// 2..5 / 30-minute window also bounds how long commit-user snapshot reconciliation and
		// streaming-read offsets can rely on old snapshots after a prolonged outage.
		schemaBuilder.option("snapshot.num-retained.min", "2");
		schemaBuilder.option("snapshot.num-retained.max", "5");
		schemaBuilder.option("snapshot.time-retained", "30min");

		// 5. Commit settings
		// Force compact on commit for better read performance
		schemaBuilder.option("commit.force-compact", "false");

		// 6. Scan settings for better read performance
		schemaBuilder.option("scan.plan-sort-partition", "true");

		// 7. Changelog settings for CDC scenarios
		schemaBuilder.option("changelog-producer.lookup-wait", "false"); // Don't wait for lookup

		if (EmptyKit.isNotEmpty(config.getTableProperties(tableName))) {
			config.getTableProperties(tableName).forEach(v -> {
				if (StringUtils.isEmpty(v.get("propKey"))
					|| StringUtils.isEmpty(v.get("propValue"))
				) {
					log.warn("tapdata paimon config error", "key or value exists null in tableProperties");
				} else {
					schemaBuilder.option(v.get("propKey"), v.get("propValue"));
				}
			});
		}
		Schema finalSchema = schemaBuilder.build();
		// Paimon 1.3.1 AbstractCatalog#createTable copies Catalog table-default options into
		// Schema#options with putIfAbsent immediately before creating the table. Mirror that exact
		// mutation order so the connector validates the same effective options, while preserving
		// explicit tableProperties overrides and rejecting before Catalog state is changed.
		// Sources:
		// https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/catalog/CatalogUtils.java#L99-L101
		// https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/catalog/AbstractCatalog.java#L380-L405
		// https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/catalog/AbstractCatalog.java#L649-L650
		CatalogUtils.tableDefaultOptions(catalog.options())
				.forEach(finalSchema.options()::putIfAbsent);
		PaimonWriteSemanticContractResolver.validateNewTable(
				identifier.getFullName(), finalSchema);

		// Create only after the final Paimon write contract has passed validation.
		catalog.createTable(identifier, finalSchema, false);

		// log schema builder variables
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		log.info("Created table {} with schema: {}", identifier.getFullName(), gson.toJson(finalSchema));

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
			case "INTEGER":
				return DataTypes.INT();
			case "BIGINT":
				return DataTypes.BIGINT();
			case "FLOAT":
				return DataTypes.FLOAT();
			case "DOUBLE":
				return DataTypes.DOUBLE();
			case "DECIMAL":
				return DataTypes.DECIMAL(getFieldPrecisionAndScale(dataType).getLeft(), getFieldPrecisionAndScale(dataType).getRight());
			case "DATE":
				return DataTypes.DATE();
			case "TIME":
				return DataTypes.TIME(getFieldFraction(dataType));
			case "TIMESTAMP":
				return DataTypes.TIMESTAMP(getFieldFraction(dataType));
			case "TIMESTAMP WITH LOCAL TIME ZONE":
				return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(getFieldFraction(dataType));
			case "BINARY":
				return DataTypes.BINARY(getFieldLength(dataType));
			case "VARBINARY":
				return DataTypes.VARBINARY(getFieldLength(dataType));
			case "BYTES":
				return DataTypes.BYTES();
			case "CHAR":
				return DataTypes.CHAR(getFieldLength(dataType));
			case "VARCHAR":
				return DataTypes.VARCHAR(getFieldLength(dataType));
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

	public Integer getFieldLength(String dataType) {
		//提取括号里的值
		Pattern pattern = Pattern.compile("\\(([^)]+)\\)");
		Matcher matcher = pattern.matcher(dataType);
		if (matcher.find()) {
			long length = Long.parseLong(matcher.group(1));
			if (length > Integer.MAX_VALUE) {
				return Integer.MAX_VALUE;
			} else {
				return (int) length;
			}
		}
		return Integer.MAX_VALUE;
	}

	public Integer getFieldFraction(String dataType) {
		//提取括号里的值
		Pattern pattern = Pattern.compile("\\(([^)]+)\\)");
		Matcher matcher = pattern.matcher(dataType);
		if (matcher.find()) {
			return Integer.parseInt(matcher.group(1));
		}
		return 6;
	}

	public Pair<Integer, Integer> getFieldPrecisionAndScale(String dataType) {
		//提取括号里的值,逗号的前一个和后一个
		Pattern pattern = Pattern.compile("\\(([^,]+),([^)]+)\\)");
		Matcher matcher = pattern.matcher(dataType);
		if (matcher.find()) {
			return Pair.of(Integer.parseInt(matcher.group(1).trim()), Integer.parseInt(matcher.group(2).trim()));
		}
		return Pair.of(38, 10);
	}

	/**
	 * Drop table from Paimon
	 *
	 * @param tableName table name
	 * @throws Exception if drop fails
	 */
	public void dropTable(String tableName) throws Exception {
		String database = config.getDatabase();
		String tableKey = database + "." + tableName;
		Identifier identifier = Identifier.create(database, tableName);

		try {
			runTableDdl(tableKey, tableName, () -> catalog.dropTable(identifier, true));
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
		String tableKey = database + "." + tableName;
		Identifier identifier = Identifier.create(database, tableName);

		try {
			// Enter the lifecycle gate before touching Catalog state. Native truncate keeps
			// partition keys, options, UUID and physical location intact.
			runTableDdl(tableKey, tableName, () -> {
				Table currentTable = catalog.getTable(identifier);
				try (BatchTableCommit commit = currentTable.newBatchWriteBuilder().newCommit()) {
					commit.truncateTable();
				}
			});
		} catch (Catalog.TableNotExistException e) {
			// Table does not exist, nothing to clear
		}
	}

	private void runTableDdl(String tableKey, String tableName, TableDdlAction action)
			throws Exception {
		try (PaimonServiceLifecycle.Ingress ignored = lifecycle.enter("DDL")) {
			throwIfStickyWriteFailure();
			List<PaimonMicroBatchCoordinator.CallbackReservation> ready = new ArrayList<>();
			Exception ddlFailure = null;
			boolean drainSucceeded = false;
			Object lock = commitLocks.computeIfAbsent(tableKey, key -> new Object());
			synchronized (lock) {
				if (!drainingTables.add(tableKey)) {
					throw new IllegalStateException("Table DDL is already in progress for " + tableKey);
				}
				try {
					ready.addAll(flushTableLocked(tableKey, "DDL", false));
					drainSucceeded = true;
					PaimonTableWriteContext context = tableWriteContexts.remove(tableKey);
					if (context != null) {
						context.close();
					}
					action.run();
				} catch (Exception failure) {
					ddlFailure = failure;
					recordStickyFailure(failure);
				} finally {
					invalidateTableDerivedCaches(tableKey, tableName);
					unregisterPhysicalTableOwner(tableKey);
					if (drainSucceeded) {
						microBatchCoordinator.clearWriterDerivedStateAfterDdl(tableKey);
					}
					dynamicSourceIngressGuards.remove(tableKey);
					drainingTables.remove(tableKey);
				}
			}
			if (drainSucceeded) {
				executeCallbacks(ready, false);
			}
			asyncCommitScheduler.stateChanged();
			if (ddlFailure != null) {
				throw ddlFailure;
			}
		}
	}

	/**
	 * Invalidate Connector-owned metadata derived from one logical table generation.
	 *
	 * <p>Paimon 1.3.1 {@code CachingCatalog#dropTable} invalidates Paimon's own table and
	 * partition caches after a successful drop. Connector policy additionally invalidates these
	 * independent row-layout and legacy-hash caches after every DDL attempt, including failures,
	 * so a later table generation cannot reuse stale field positions or source primary keys.
	 * This helper runs inside the existing per-table lifecycle lock.
	 *
	 * <p>Source:
	 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/catalog/CachingCatalog.java#L184-L197
	 */
	private void invalidateTableDerivedCaches(String tableKey, String tableName) {
		paimonFieldCache.remove(tableKey);
		fieldIndexCache.remove(tableKey);
		computeHashKey.remove(tableName);
		primaryKeyMap.remove(tableName);
	}

	/** Clear the same Connector-owned derived-cache set during whole-service cleanup. */
	private void clearAllTableDerivedCaches() {
		paimonFieldCache.clear();
		fieldIndexCache.clear();
		computeHashKey.clear();
		primaryKeyMap.clear();
	}

	@FunctionalInterface
	private interface TableDdlAction {
		void run() throws Exception;
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
		throwIfConcurrentIngressStickyFailure();
		try (PaimonServiceLifecycle.Ingress ignored = lifecycle.enter("writeRecords")) {
			if (recordEvents == null || recordEvents.isEmpty()) {
				return new WriteListResult<>();
			}
			DmlMetadata metadata;
			try {
				metadata = validateDmlMetadata(recordEvents);
			} catch (PaimonFatalWriteException failure) {
				recordStickyFailure(failure);
				throw failure;
			}
			String tableName = table.getName();
			String tableKey = config.getDatabase() + "." + tableName;
			DynamicIngressGuard ingressGuard = dynamicSourceIngressGuard(tableKey, tableName);
			beginSourceIngress(ingressGuard, "writeRecords", tableKey);
			boolean successful = false;
			try {
				WriteListResult<TapRecordEvent> result =
						writeRecordsWithStreamWriteInternal(
								recordEvents, table, connectorContext, metadata);
				successful = true;
				return result;
			} finally {
				endSourceIngress(ingressGuard, successful);
			}
		}
	}

	/**
	 * Resolve the effective native bucket option after applying the same tableProperties precedence
	 * used by new-table Schema construction.
	 *
	 * <p>The Connector always writes a first-class bucket value, so Catalog table defaults cannot
	 * override it. A later explicit {@code tableProperties.bucket} entry wins, matching repeated
	 * {@link Schema.Builder#option(String, String)} calls.
	 *
	 * <p>Sources:
	 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L100-L112
	 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/schema/Schema.java#L367-L376
	 */
	private int resolveEffectiveBucket(String tableName) {
		Map<String, String> bucketOption = new HashMap<>();
		bucketOption.put(
				CoreOptions.BUCKET.key(), Integer.toString(config.resolveBucket(tableName)));
		if (EmptyKit.isNotEmpty(config.getTableProperties(tableName))) {
			for (Map<String, String> property : config.getTableProperties(tableName)) {
				if (CoreOptions.BUCKET.key().equals(property.get("propKey"))
						&& StringUtils.isNotEmpty(property.get("propValue"))) {
					bucketOption.put(CoreOptions.BUCKET.key(), property.get("propValue"));
				}
			}
		}
		return CoreOptions.fromMap(bucketOption).bucket();
	}

	public void afterInitialSync(TapConnectorContext connectorContext, TapTable tapTable) throws Exception {
		try (PaimonServiceLifecycle.Ingress ignored = lifecycle.enter("afterInitialSync")) {
			String tableName = tapTable.getName();
			String database = config.getDatabase();
			String tableKey = database + "." + tableName;
			DynamicIngressGuard ingressGuard = dynamicSourceIngressGuard(tableKey, tableName);
			beginSourceIngress(ingressGuard, "afterInitialSync", tableKey);
			boolean successful = false;
			try {
				bindTaskState(connectorContext);
				Identifier identifier = Identifier.create(database, tableName);
				Object lock = commitLocks.computeIfAbsent(tableKey, k -> new Object());
				synchronized (lock) {
					PaimonTableWriteContext writeContext =
							getOrCreateTableWriteContext(tableKey, tableName, identifier, connectorContext);
					if (writeContext.hasPendingCommit()) {
						confirmPendingCommitLocked(writeContext, tableKey, "initial-pending");
					} else {
						// afterInitialSync is a per-table completion boundary. Preserve the
						// existing forced commit even when this table received no rows.
						commitTableLocked(writeContext, tableKey, "initial");
					}
				}
				successful = true;
			} catch (Exception failure) {
				recordStickyFailure(failure);
				throw failure;
			} finally {
				endSourceIngress(ingressGuard, successful);
			}
			asyncCommitScheduler.stateChanged();
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
	private WriteListResult<TapRecordEvent> writeRecordsWithStreamWriteInternal(
			List<TapRecordEvent> recordEvents,
			TapTable table,
			TapConnectorContext connectorContext,
			DmlMetadata metadata) throws Exception {
		throwIfStickyWriteFailure();
		bindTaskState(connectorContext);
		connectorContext.configContext();
		activeConnectorContext = connectorContext;
		Log currentLog = connectorContext.getLog();
		String database = config.getDatabase();
		String tableName = table.getName();
		String tableKey = database + "." + tableName;
		WriteListResult<TapRecordEvent> result = new WriteListResult<>();
		Identifier identifier = Identifier.create(database, tableName);
		List<PaimonMicroBatchCoordinator.CallbackReservation> readyCallbacks = new ArrayList<>();
		boolean writerIngressStarted = false;
		try {
			Object lock = commitLocks.computeIfAbsent(tableKey, k -> new Object());
			synchronized (lock) {
				PaimonTableWriteContext writeContext = tableWriteContexts.get(tableKey);
				if (writeContext != null && writeContext.hasPendingCommit()) {
					readyCallbacks.addAll(confirmPendingCommitLocked(
							writeContext, tableKey, "pending-retry"));
				}
				cacheSourceDerivedState(tableName, table);
				DmlBatchPreflight preflight =
						resolveAndValidateDmlBatch(tableKey, identifier, table, recordEvents);
				if (writeContext == null) {
					writeContext = getOrCreateTableWriteContext(
							tableKey,
							tableName,
							identifier,
							connectorContext,
							preflight.fileStoreTable,
							preflight.writeSemanticContract);
				}
				writerIngressStarted = true;
				for (TapRecordEvent event : recordEvents) {
					if (event instanceof TapInsertRecordEvent) {
						handleStreamInsert((TapInsertRecordEvent) event, writeContext, table, currentLog);
						result.incrementInserted(1);
					} else if (event instanceof TapUpdateRecordEvent) {
						handleStreamUpdate((TapUpdateRecordEvent) event, writeContext, table, currentLog);
						result.incrementModified(1);
					} else if (event instanceof TapDeleteRecordEvent) {
						handleStreamDelete((TapDeleteRecordEvent) event, writeContext, table, currentLog);
						result.incrementRemove(1);
					}
				}

				if (metadata.cdc) {
					PaimonMicroBatchCoordinator.BatchDecision decision =
							microBatchCoordinator.acceptCdc(
									tableKey,
									recordEvents.size(),
									metadata.sourceLanes,
									clock.getAsLong());
					if (decision.shouldCommit()) {
						readyCallbacks.addAll(commitTableLocked(
								writeContext,
								tableKey,
								decision.shouldCommitBySize() ? "size" : "time"));
					}
				} else {
					microBatchCoordinator.acceptInitial(tableKey, recordEvents.size());
				}
			}
		} catch (Exception failure) {
			if (failure instanceof PaimonDynamicBucketPollutedException
					|| failure instanceof PaimonFatalWriteException
					|| writerIngressStarted) {
				recordStickyFailure(failure);
			}
			throw failure;
		}

		executeCallbacks(readyCallbacks, false);
		asyncCommitScheduler.stateChanged();
		return result;
	}

	private DmlMetadata validateDmlMetadata(List<TapRecordEvent> recordEvents) {
		Boolean cdc = null;
		Set<String> sourceLanes = new LinkedHashSet<>();
		for (TapRecordEvent event : recordEvents) {
			Map<String, Object> info = event.getInfo();
			Object rawStage = info == null ? null : info.get(TapRecordEvent.INFO_KEY_SYNC_STAGE);
			String stage = normalizeSyncStage(rawStage);
			boolean eventCdc = "CDC".equals(stage);
			if (cdc != null && cdc != eventCdc) {
				throw new PaimonFatalWriteException(
						"Mixed INITIAL_SYNC and CDC events are not allowed in one Paimon batch");
			}
			cdc = eventCdc;
			if (!eventCdc) {
				continue;
			}

			Object nodeIds = info.get(TapCallbackOffset.KEY_NODE_IDS);
			String sourceLane = firstSourceLane(nodeIds, "CDC DML");
			sourceLanes.add(sourceLane);
			Object streamOffset = info.get(TapCallbackOffset.KEY_STREAM_OFFSET);
			if (streamOffset != null) {
				if (!(info.get(TapCallbackOffset.KEY_SOURCE_TIME) instanceof Long)) {
					throw new PaimonFatalWriteException(
							"Offset-bearing CDC DML requires a Long sourceTime");
				}
				if (flushOffsetCallback == null) {
					throw new PaimonFatalWriteException(
							"Offset-bearing CDC DML requires flushOffsetCallback before writing");
				}
			}
		}
		return new DmlMetadata(Boolean.TRUE.equals(cdc), sourceLanes);
	}

	private static String normalizeSyncStage(Object rawStage) {
		return normalizeSyncStage(rawStage, "Paimon DML");
	}

	private static String normalizeSyncStage(Object rawStage, String payloadType) {
		String stage;
		if (rawStage instanceof String) {
			stage = (String) rawStage;
		} else if (rawStage instanceof Enum) {
			stage = ((Enum<?>) rawStage).name();
		} else {
			throw new PaimonFatalWriteException(
					payloadType + " syncStage must be INITIAL_SYNC or CDC");
		}
		if (!"INITIAL_SYNC".equals(stage) && !"CDC".equals(stage)) {
			throw new PaimonFatalWriteException(
					payloadType + " syncStage must be INITIAL_SYNC or CDC: " + stage);
		}
		return stage;
	}

	private static String firstSourceLane(Object nodeIds, String payloadType) {
		if (!(nodeIds instanceof List) || ((List<?>) nodeIds).isEmpty()) {
			throw new PaimonFatalWriteException(
					payloadType + " requires a non-empty nodeIds list");
		}
		Object first = ((List<?>) nodeIds).get(0);
		if (!(first instanceof String) || StringUtils.isBlank((String) first)) {
			throw new PaimonFatalWriteException(
					payloadType + " requires a non-blank nodeIds[0]");
		}
		return (String) first;
	}

	public void processHeartbeat(HeartbeatEvent heartbeat) throws Exception {
		try (PaimonServiceLifecycle.Ingress ignored = lifecycle.enter("heartbeat")) {
			try {
				if (heartbeat == null) {
					throw new PaimonFatalWriteException("Paimon Heartbeat must not be null");
				}
				Map<String, Object> info = heartbeat.getInfo();
				Object rawStage = info == null
						? null
						: info.get(TapCallbackOffset.KEY_SYNC_STAGE);
				String stage = normalizeSyncStage(rawStage, "Paimon Heartbeat");
				if ("INITIAL_SYNC".equals(stage)) {
					return;
				}
				if (flushOffsetCallback == null) {
					throw new PaimonFatalWriteException(
							"CDC Heartbeat requires flushOffsetCallback");
				}
				Object streamOffset = info.get(TapCallbackOffset.KEY_STREAM_OFFSET);
				if (streamOffset == null) {
					throw new PaimonFatalWriteException(
							"CDC Heartbeat requires streamOffset");
				}
				Object sourceTime = info.get(TapCallbackOffset.KEY_SOURCE_TIME);
				if (!(sourceTime instanceof Long)) {
					throw new PaimonFatalWriteException(
							"CDC Heartbeat requires a Long sourceTime");
				}
				Object nodeIds = info.get(TapCallbackOffset.KEY_NODE_IDS);
				String sourceLane = firstSourceLane(nodeIds, "CDC Heartbeat");
				TapCallbackOffset payload = new TapCallbackOffset()
						.streamOffset(streamOffset)
						.syncStage("CDC")
						.sourceTime((Long) sourceTime)
						.nodeIds(nodeIds);
				payload.put(TapCallbackOffset.KEY_EVENT_TIME, heartbeat.getReferenceTime());
				PaimonMicroBatchCoordinator.CallbackReservation reservation =
						microBatchCoordinator.registerHeartbeat(sourceLane, payload);
				if (reservation != null) {
					executeCallbacks(Collections.singletonList(reservation), false);
				}
			} catch (PaimonFatalWriteException failure) {
				recordStickyFailure(failure);
				throw failure;
			}
		}
	}

	private List<PaimonMicroBatchCoordinator.CallbackReservation> commitTableLocked(
			PaimonTableWriteContext writeContext, String tableKey, String trigger) throws Exception {
		return commitTableLocked(writeContext, tableKey, trigger, false);
	}

	private List<PaimonMicroBatchCoordinator.CallbackReservation> commitTableLocked(
			PaimonTableWriteContext writeContext,
			String tableKey,
			String trigger,
			boolean uninterruptibleCleanup) throws Exception {
		PaimonMicroBatchCoordinator.CommitTarget target =
				microBatchCoordinator.captureCommitTarget(tableKey);
		return commitOrConfirmLocked(
				writeContext, target, true, trigger, uninterruptibleCleanup);
	}

	private List<PaimonMicroBatchCoordinator.CallbackReservation> confirmPendingCommitLocked(
			PaimonTableWriteContext writeContext, String tableKey, String trigger) throws Exception {
		return confirmPendingCommitLocked(writeContext, tableKey, trigger, false);
	}

	private List<PaimonMicroBatchCoordinator.CallbackReservation> confirmPendingCommitLocked(
			PaimonTableWriteContext writeContext,
			String tableKey,
			String trigger,
			boolean uninterruptibleCleanup) throws Exception {
		if (!writeContext.hasPendingCommit()) {
			return Collections.emptyList();
		}
		PaimonMicroBatchCoordinator.CommitTarget target =
				microBatchCoordinator.pendingCommitTarget(tableKey);
		if (target == null) {
			target = microBatchCoordinator.captureCommitTarget(tableKey);
			microBatchCoordinator.markPendingCommit(target);
		}
		return commitOrConfirmLocked(
				writeContext, target, false, trigger, uninterruptibleCleanup);
	}

	private List<PaimonMicroBatchCoordinator.CallbackReservation> commitOrConfirmLocked(
			PaimonTableWriteContext writeContext,
			PaimonMicroBatchCoordinator.CommitTarget target,
			boolean newCommit,
			String trigger,
			boolean uninterruptibleCleanup) throws Exception {
		long startedAt = clock.getAsLong();
		try {
			if (newCommit) {
				writeContext.commit();
			} else {
				writeContext.retryPendingCommit();
			}
			return publishSuccessfulCommit(target, trigger, startedAt);
		} catch (Exception firstFailure) {
			if (!writeContext.hasPendingCommit()) {
				recordStickyFailure(firstFailure);
				throw firstFailure;
			}
			microBatchCoordinator.markPendingCommit(target);
			int retry = 1;
			while (retry <= 3) {
				try {
					while (true) {
						try {
							retryWaiter.awaitRetry();
							break;
						} catch (InterruptedException interrupted) {
							if (!uninterruptibleCleanup) {
								Thread.currentThread().interrupt();
								interrupted.addSuppressed(firstFailure);
								recordStickyFailure(interrupted);
								throw interrupted;
							}
							cleanupInterruption.compareAndSet(null, interrupted);
							Thread.interrupted();
						}
					}
					if (!writeContext.hasPendingCommit()) {
						IllegalStateException missingPending = new IllegalStateException(
								"Paimon pending commit disappeared before confirmation for " + target.tableKey());
						firstFailure.addSuppressed(missingPending);
						break;
					}
					writeContext.retryPendingCommit();
					return publishSuccessfulCommit(target, trigger + "-retry-" + retry, startedAt);
				} catch (InterruptedException interrupted) {
					throw interrupted;
				} catch (Exception retryFailure) {
					firstFailure.addSuppressed(retryFailure);
					retry++;
				}
			}
			recordStickyFailure(firstFailure);
			throw firstFailure;
		}
	}

	private List<PaimonMicroBatchCoordinator.CallbackReservation> publishSuccessfulCommit(
			PaimonMicroBatchCoordinator.CommitTarget target, String trigger, long startedAt) {
		long completedAt = clock.getAsLong();
		getAsyncCommitLog().debug(
				"Committed Paimon table {} trigger={} records={} generation={} durationMs={}",
				target.tableKey(),
				trigger,
				target.bufferedRecordCount(),
				target.acceptedGeneration(),
				Math.max(0L, completedAt - startedAt));
		return microBatchCoordinator.publishCommit(target, completedAt);
	}

	private void executeCallbacks(
			Collection<PaimonMicroBatchCoordinator.CallbackReservation> reservations,
			boolean stopDrain) throws Exception {
		Deque<PaimonMicroBatchCoordinator.CallbackReservation> ready =
				new ArrayDeque<>(reservations);
		while (!ready.isEmpty()) {
			PaimonMicroBatchCoordinator.CallbackReservation reservation = ready.removeFirst();
			synchronized (callbackExecutionLock) {
				PaimonServiceLifecycle.ConsumerPermit permit = lifecycle.tryStartConsumer(
						stopDrain,
						() -> {
							if (!microBatchCoordinator.markConsumerStarted(reservation)) {
								throw new IllegalStateException(
										"Paimon callback reservation is no longer active");
							}
						});
				if (permit == null) {
					return;
				}
				try (PaimonServiceLifecycle.ConsumerPermit ignored = permit) {
					try {
						Consumer<Object> callback = flushOffsetCallback;
						if (callback == null) {
							throw new IllegalStateException(
									"flushOffsetCallback is unavailable for a ready CDC Heartbeat");
						}
						callback.accept(reservation.payload());
						PaimonMicroBatchCoordinator.CallbackReservation next =
								microBatchCoordinator.completeCallback(reservation);
						if (next != null) {
							ready.addFirst(next);
						}
					} catch (Throwable callbackFailure) {
						microBatchCoordinator.failCallback(reservation);
						recordStickyFailure(callbackFailure);
						rethrow(callbackFailure);
					}
				}
			}
		}
	}

	private void flushScheduledTable(String tableKey) throws Exception {
		try {
			try (PaimonServiceLifecycle.Ingress ignored = lifecycle.enter("scheduler")) {
				List<PaimonMicroBatchCoordinator.CallbackReservation> ready =
						Collections.emptyList();
				Object lock = commitLocks.computeIfAbsent(tableKey, key -> new Object());
				synchronized (lock) {
					if (microBatchCoordinator.isDue(tableKey, clock.getAsLong())) {
						ready = flushTableLocked(tableKey, "scheduler", false);
					}
				}
				executeCallbacks(ready, false);
			}
		} catch (IllegalStateException rejection) {
			PaimonServiceLifecycle.State state = lifecycle.state();
			if (state == PaimonServiceLifecycle.State.STOPPING
					|| state == PaimonServiceLifecycle.State.CLOSED) {
				// close won the lifecycle race after this scheduler task was dispatched.
				// Stop drain owns any remaining buffer, so this is normal cancellation.
				return;
			}
			throw rejection;
		}
	}

	private void recordStickyFailure(Throwable failure) {
		if (failure == null) {
			return;
		}
		stickyWriteFailure.compareAndSet(null, failure);
		lifecycle.fail(stickyWriteFailure.get());
	}

	private static void rethrow(Throwable failure) throws Exception {
		if (failure instanceof Exception) {
			throw (Exception) failure;
		}
		if (failure instanceof Error) {
			throw (Error) failure;
		}
		throw new IllegalStateException("Paimon service failed", failure);
	}

	private void cleanupAfterInitializationFailure(Throwable primary) {
		try {
			cleanupAllResources();
		} catch (Throwable cleanupFailure) {
			primary.addSuppressed(cleanupFailure);
		} finally {
			flushOffsetCallback = null;
			lifecycle.publishClosed(primary);
		}
	}

	private static final class DmlMetadata {
		private final boolean cdc;
		private final Set<String> sourceLanes;

		private DmlMetadata(boolean cdc, Set<String> sourceLanes) {
			this.cdc = cdc;
			this.sourceLanes = Collections.unmodifiableSet(new LinkedHashSet<>(sourceLanes));
		}
	}

	@FunctionalInterface
	interface RetryWaiter {
		RetryWaiter INTERRUPTIBLE_ONE_SECOND = () -> TimeUnit.SECONDS.sleep(1L);

		void awaitRetry() throws InterruptedException;
	}

	/**
	 * Cache source metadata used by the Connector's legacy synthetic-key path.
	 *
	 * <p>The caller holds the same per-table lifecycle lock as {@link #runTableDdl}. Keeping
	 * population under that lock prevents an ingress waiting behind DDL from publishing metadata
	 * for the old table generation after the DDL finally block has invalidated it. This changes
	 * only cache timing; the legacy MD5 encoding and {@code VARCHAR(32)} contract are unchanged.
	 */
	private void cacheSourceDerivedState(String tableName, TapTable table) {
		computeHashKey.computeIfAbsent(tableName,
				ignored -> Boolean.TRUE.equals(config.getHashKey(tableName))
						&& EmptyKit.isNotEmpty(table.primaryKeys(true))
						&& table.primaryKeys(true).size() > 5);
		primaryKeyMap.putIfAbsent(tableName, table.primaryKeys(true));
	}

	private boolean isPaimonConflict(Throwable e) {
		Throwable t = e;
		while (t != null) {
			String msg = t.getMessage();
			if (msg != null) {
				if (msg.contains("File deletion conflicts detected")
						|| msg.contains("Trying to delete file")
						|| msg.contains("noConflictsOrFail")
						|| msg.contains("assertNoDelete")) {
					return true;
				}
			}
			if (t instanceof IllegalStateException
					&& msg != null
					&& msg.contains("not previously added")) {
				return true;
			}
			t = t.getCause();
		}
		return false;
	}

	/**
	 * Clean up all cached resources including writers, commits, and catalog.
	 * Material cleanup failures are aggregated instead of being downgraded to log-only success.
	 */
	private void cleanupAllResources() throws Exception {
		Throwable failure = null;
		boolean interrupted = false;

		// Never forcibly interrupt a Paimon commit running on the scheduler worker.
		try {
			if (!asyncCommitScheduler.shutdownAndAwait(5L, TimeUnit.SECONDS)) {
				failure = appendFailure(
						failure,
						new IllegalStateException(
								"Timed out waiting for the Paimon commit scheduler to terminate"));
			}
		} catch (InterruptedException interruption) {
			failure = appendFailure(failure, interruption);
			interrupted = true;
			Thread.interrupted();
		}

		// Close all canonical table write contexts first.
		for (String tableKey : new ArrayList<>(tableWriteContexts.keySet())) {
			PaimonTableWriteContext context = tableWriteContexts.remove(tableKey);
			try {
				if (context != null) {
					context.close();
				}
			} catch (Throwable contextFailure) {
				failure = appendFailure(failure, contextFailure);
			} finally {
				unregisterPhysicalTableOwner(tableKey);
				dynamicSourceIngressGuards.remove(tableKey);
			}
		}

		tableWriteContexts.clear();
		for (String tableKey : new ArrayList<>(physicalTableByLogicalTable.keySet())) {
			unregisterPhysicalTableOwner(tableKey);
		}

		commitLocks.clear();
		drainingTables.clear();
		microBatchCoordinator.clear();

		// Clear every Connector-owned table-derived cache, matching the single-table DDL path.
		clearAllTableDerivedCaches();

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
						} catch (Throwable fileIoLookupFailure) {
							failure = appendFailure(failure, fileIoLookupFailure);
						}

						// Proactively close FileSystem instances cached by HadoopFileIO before FileIO.
						closeHadoopFileIOCachedFileSystems(fileIO);
						if (fileIO != null) {
							try {
								fileIO.close();
							} catch (Throwable fileIoCloseFailure) {
								failure = appendFailure(failure, fileIoCloseFailure);
							}
						}
					}
				}

				catalog.close();
			} catch (Throwable catalogCloseFailure) {
				failure = appendFailure(failure, catalogCloseFailure);
			} finally {
				catalog = null;
			}
		}

		// Wait a bit to ensure all internal threads are cleaned up
		// This is critical to avoid ThreadGroup destroyed errors
		try {
			Thread.sleep(500);
		} catch (InterruptedException interruption) {
			failure = appendFailure(failure, interruption);
			interrupted = true;
			Thread.interrupted();
		}

		if (interrupted) {
			Thread.currentThread().interrupt();
		}
		if (failure != null) {
			rethrow(failure);
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


	private PaimonTableWriteContext getOrCreateTableWriteContext(
			String tableKey,
			String tableName,
			Identifier identifier,
			TapConnectorContext connectorContext) throws Exception {
		return getOrCreateTableWriteContext(
				tableKey, tableName, identifier, connectorContext, null, null);
	}

	private PaimonTableWriteContext getOrCreateTableWriteContext(
			String tableKey,
			String tableName,
			Identifier identifier,
			TapConnectorContext connectorContext,
			FileStoreTable preResolvedTable,
			PaimonWriteSemanticContract preResolvedContract) throws Exception {
		Object lifecycleLock = commitLocks.computeIfAbsent(tableKey, ignored -> new Object());
		synchronized (lifecycleLock) {
			if (drainingTables.contains(tableKey)) {
				throw new IllegalStateException("Paimon table is draining for DDL: " + tableKey);
			}
			try {
				return tableWriteContexts.computeIfAbsent(tableKey, ignored -> {
				try {
					if (drainingTables.contains(tableKey)) {
						throw new IllegalStateException("Paimon table is draining for DDL: " + tableKey);
					}
					Table table =
							preResolvedTable == null
									? catalog.getTable(identifier)
									: preResolvedTable;
					if (!(table instanceof FileStoreTable)) {
						throw new IllegalArgumentException(
								"Only FileStoreTable supports connector writes for " + tableKey);
					}
					FileStoreTable fileStoreTable = (FileStoreTable) table;
					// Contract resolution must precede commit-state binding and the HASH_DYNAMIC
					// RocksDB pollution preflight, not merely raw writer construction.
					PaimonWriteSemanticContract writeSemanticContract =
							preResolvedContract == null
									? PaimonWriteSemanticContractResolver.resolve(
											tableKey, fileStoreTable)
									: preResolvedContract;
					registerPhysicalTableOwner(tableKey, fileStoreTable);
					try {
						PaimonCommitStateStore.Binding binding = PaimonCommitStateStore.bind(
								boundTaskStateMap,
								config.getFullWarehousePath(),
								fileStoreTable);
						if (fileStoreTable.bucketMode() == BucketMode.HASH_DYNAMIC) {
							PaimonDynamicBucketPreflight.ensureHashDynamicValidated(
									boundTaskStateMap,
									config.getFullWarehousePath(),
									tableKey,
									fileStoreTable,
									config.getDiskTmpDir(tableName));
						}
						if (fileStoreTable.bucketMode() == BucketMode.KEY_DYNAMIC
								&& fileStoreTable.options().containsKey("cross-partition-upsert.index-ttl")) {
							log.warn(
									"Table {} configures cross-partition-upsert.index-ttl; "
											+ "Paimon may produce duplicate primary keys after old index entries expire",
									tableKey);
						}
						return PaimonTableWriteContext.create(
								tableKey,
								tableName,
								fileStoreTable,
								binding.commitUser(),
								config.getDiskTmpDir(tableName),
								binding.nextCommitIdentifier(),
								binding.store(),
								writeSemanticContract);
					} catch (Exception e) {
						unregisterPhysicalTableOwner(tableKey);
						throw e;
					}
				} catch (Exception e) {
					throw new TableWriteContextCreationException(tableKey, e);
				}
				});
			} catch (TableWriteContextCreationException e) {
				Throwable cause = e.getCause();
				if (cause instanceof Exception) {
					throw (Exception) cause;
				}
				throw e;
			}
		}
	}

	private DmlBatchPreflight resolveAndValidateDmlBatch(
			String tableKey,
			Identifier identifier,
			TapTable tapTable,
			List<TapRecordEvent> recordEvents) throws Exception {
		PaimonTableWriteContext existingContext = tableWriteContexts.get(tableKey);
		PaimonWriteSemanticContract contract;
		FileStoreTable fileStoreTable = null;
		if (existingContext != null) {
			contract = existingContext.writeSemanticContract();
		} else {
			Table targetTable = catalog.getTable(identifier);
			if (!(targetTable instanceof FileStoreTable)) {
				throw new IllegalArgumentException(
						"Only FileStoreTable supports connector writes for " + tableKey);
			}
			fileStoreTable = (FileStoreTable) targetTable;
			contract =
					PaimonWriteSemanticContractResolver.resolve(
							tableKey, fileStoreTable);
		}

		PaimonGeneratedFieldDependencies generatedFields =
				generatedFieldDependencies(tapTable);
		PaimonDmlImageValidator.validateBatch(
				tableKey, contract, generatedFields, tapTable, recordEvents);
		return new DmlBatchPreflight(fileStoreTable, contract);
	}

	private static final class DmlBatchPreflight {
		private final FileStoreTable fileStoreTable;
		private final PaimonWriteSemanticContract writeSemanticContract;

		private DmlBatchPreflight(
				FileStoreTable fileStoreTable,
				PaimonWriteSemanticContract writeSemanticContract) {
			this.fileStoreTable = fileStoreTable;
			this.writeSemanticContract = writeSemanticContract;
		}
	}

	private synchronized void bindTaskState(TapConnectorContext connectorContext) {
		if (connectorContext == null || connectorContext.getStateMap() == null) {
			throw new IllegalStateException("Tap task state map is required for Paimon writes");
		}
		KVMap<Object> stateMap = connectorContext.getStateMap();
		if (boundTaskStateMap == null) {
			boundTaskStateMap = stateMap;
			return;
		}
		if (boundTaskStateMap != stateMap) {
			throw new IllegalStateException(
					"Paimon service cannot be shared by multiple Tap task state maps; restart the connector");
		}
	}

	private void registerPhysicalTableOwner(String tableKey, FileStoreTable table) {
		// Paimon explicitly disallows concurrent HASH_DYNAMIC writers and KEY_DYNAMIC owns a local
		// full-key index. This registry enforces the connector's 1/1/0 topology only inside this
		// JVM; deployment must provide an external single-writer lease to cover other processes.
		// Source:
		// https://github.com/apache/paimon/blob/release-1.3.1/paimon-common/src/main/java/org/apache/paimon/table/BucketMode.java#L40-L55
		String physicalHash = PaimonCommitStateStore.physicalTableHash(
				table.location().toUri().toString());
		String owner = serviceWriterOwner + ':' + tableKey;
		String existing = ACTIVE_PHYSICAL_TABLE_OWNERS.putIfAbsent(physicalHash, owner);
		if (existing != null && !existing.equals(owner)) {
			throw new IllegalStateException(
					"Another Paimon writer context already owns the target physical table");
		}
		physicalTableByLogicalTable.put(tableKey, physicalHash);
	}

	private void unregisterPhysicalTableOwner(String tableKey) {
		String physicalHash = physicalTableByLogicalTable.remove(tableKey);
		if (physicalHash != null) {
			ACTIVE_PHYSICAL_TABLE_OWNERS.remove(
					physicalHash, serviceWriterOwner + ':' + tableKey);
		}
	}

	private static final class TableWriteContextCreationException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		private TableWriteContextCreationException(String tableKey, Throwable cause) {
			super("Failed to create Paimon write context for table " + tableKey, cause);
		}
	}


	/**
	 * Handle insert event with stream writer
	 *
	 * @param event  insert event
	 * @param writeContext connector table write context
	 * @param table  table definition
	 * @throws Exception if insert fails
	 */
	private void handleStreamInsert(TapInsertRecordEvent event, PaimonTableWriteContext writeContext, TapTable table, Log currentLog) throws Exception {
		Map<String, Object> after = event.getAfter();
		String database = config.getDatabase();
		Identifier identifier = Identifier.create(database, table.getName());
		PaimonWriteSemanticContract contract = writeContext.writeSemanticContract();
		GenericRow row = convertToGenericRow(after, table, identifier);
		PaimonRowKindField.apply(contract, row, RowKind.INSERT);
		writeContext.validateRoutingRow(row, "INSERT");
		writeRow(writeContext, event, row, table, after, currentLog);
	}

	/**
	 * Handle update event with stream writer
	 * Uses RowKind.UPDATE_BEFORE (U-) and RowKind.UPDATE_AFTER (U+) to implement update
	 *
	 * @param event  update event
	 * @param writeContext connector table write context
	 * @param table  table definition
	 * @throws Exception if update fails
	 */
	private void handleStreamUpdate(TapUpdateRecordEvent event, PaimonTableWriteContext writeContext, TapTable table, Log currentLog) throws Exception {
		String database = config.getDatabase();
		Identifier identifier = Identifier.create(database, table.getName());

		Map<String, Object> before = event.getBefore();
		Map<String, Object> after = event.getAfter();
		PaimonWriteSemanticContract contract = writeContext.writeSemanticContract();

		// Convert before and after data to GenericRow first to avoid duplicate conversion
		GenericRow beforeRow = null;
		if (before != null && !before.isEmpty()) {
			beforeRow = convertToGenericRow(before, table, identifier);
		}
		GenericRow afterRow = convertToGenericRow(after, table, identifier);

		// Check if primary key update detection is enabled
		Boolean enablePkUpdate = config.getEnablePrimaryKeyUpdate(table.getName());
		if (enablePkUpdate != null && enablePkUpdate) {
			// Validate that before data is available when primary key update detection is enabled
			if (beforeRow == null) {
				throw new RuntimeException("Primary key update detection is enabled but before data is not available. " +
						"Please ensure the source database can provide before-update data or disable this feature.");
			}

			// Check if primary key has changed
			if (isPrimaryKeyChanged(beforeRow, afterRow, table)) {
				// Validate both effective rows before the first writer call so a malformed
				// after image cannot leave a half-written DELETE + INSERT pair.
				PaimonRowKindField.apply(contract, beforeRow, RowKind.DELETE);
				PaimonRowKindField.apply(contract, afterRow, RowKind.INSERT);
				writeContext.validateRoutingRow(beforeRow, "DELETE");
				writeContext.validateRoutingRow(afterRow, "INSERT");
				writeRow(writeContext, event, beforeRow, table, before, currentLog);
				writeRow(writeContext, event, afterRow, table, after, currentLog);
				return;
			}
		}

		// Apply and validate the complete logical operation before the first write.
		if (beforeRow != null) {
			PaimonRowKindField.apply(contract, beforeRow, RowKind.UPDATE_BEFORE);
			writeContext.validateRoutingRow(beforeRow, "UPDATE_BEFORE");
		}
		PaimonRowKindField.apply(contract, afterRow, RowKind.UPDATE_AFTER);
		writeContext.validateRoutingRow(afterRow, "UPDATE_AFTER");

		// Normal update logic: Write U- (UPDATE_BEFORE) if before data exists.
		if (beforeRow != null) {
			writeRow(writeContext, event, beforeRow, table, before, currentLog);
		}

		// Write U+ (UPDATE_AFTER) using after data
		writeRow(writeContext, event, afterRow, table, after, currentLog);
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
	boolean isPrimaryKeyChanged(GenericRow beforeRow, GenericRow afterRow, TapTable table) {
		// Get primary key fields
		Collection<String> primaryKeys = table.primaryKeys(true);
		if (primaryKeys == null || primaryKeys.isEmpty()) {
			// No primary key defined, no change detection needed
			return false;
		}

		String tableKey = config.getDatabase() + "." + table.getName();
		List<DataField> targetFields = paimonFieldCache.get(tableKey);
		if (targetFields == null) {
			throw new PaimonFatalWriteException(
					"Paimon target RowType is not cached for primary-key comparison on " + tableKey);
		}

		Map<String, Integer> targetIndexes = new HashMap<>();
		for (int i = 0; i < targetFields.size(); i++) {
			targetIndexes.put(targetFields.get(i).name(), i);
		}
		for (String primaryKey : primaryKeys) {
			Integer index = targetIndexes.get(primaryKey);
			if (index == null || index < 0
					|| index >= beforeRow.getFieldCount()
					|| index >= afterRow.getFieldCount()) {
				throw new PaimonFatalWriteException(
						"Primary-key field is absent from Paimon target RowType: " + primaryKey);
			}
			// GenericRow contains already converted Paimon values. deepEquals preserves typed
			// equality for primitive/object arrays and avoids delimiter/null-literal collisions.
			if (!Objects.deepEquals(beforeRow.getField(index), afterRow.getField(index))) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Handle delete event with stream writer
	 *
	 * @param event  delete event
	 * @param writeContext connector table write context
	 * @param table  table definition
	 * @throws Exception if delete fails
	 */
	private void handleStreamDelete(TapDeleteRecordEvent event, PaimonTableWriteContext writeContext, TapTable table, Log currentLog) throws Exception {
		Map<String, Object> before = event.getBefore();
		String database = config.getDatabase();
		Identifier identifier = Identifier.create(database, table.getName());
		PaimonWriteSemanticContract contract = writeContext.writeSemanticContract();
		GenericRow row = convertToGenericRow(before, table, identifier);
		PaimonRowKindField.apply(contract, row, RowKind.DELETE);
		writeContext.validateRoutingRow(row, "DELETE");
		writeRow(writeContext, event, row, table, before, currentLog);
	}

	private PaimonGeneratedFieldDependencies generatedFieldDependencies(TapTable table) {
		if (!Boolean.TRUE.equals(computeHashKey.get(table.getName()))) {
			return PaimonGeneratedFieldDependencies.none();
		}
		Collection<String> sourcePrimaryKeys = primaryKeyMap.get(table.getName());
		if (sourcePrimaryKeys == null || sourcePrimaryKeys.isEmpty()) {
			throw new PaimonFatalWriteException(
					"PAIMON_FULL_CHANGELOG_REQUIRED table="
							+ config.getDatabase()
							+ "."
							+ table.getName()
							+ ", reason=synthetic hash key has no source primary-key dependencies");
		}
		return PaimonGeneratedFieldDependencies.of(
				Collections.singletonMap(HASH_KEY, sourcePrimaryKeys));
	}

	/**
	 * Unified write entry with exception capture and row logging.
	 *
	 * @param writeContext connector table write context
	 * @param event CDC event being written
	 * @param row row to write
	 * @param table table definition
	 * @throws Exception if write fails
	 */
	private void writeRow(PaimonTableWriteContext writeContext, TapRecordEvent event, GenericRow row, TapTable table, Map<String, Object> sourceData, Log currentLog) throws Exception {
		try {
			writeContext.write(row);
		} catch (Exception e) {
			currentLog.warn("Failed to write row to Paimon. table={}, bucket=connector-managed, sourceData={}, event={}, row={}",
					table == null ? null : table.getName(), formatSourceDataForLog(sourceData), formatEventForLog(event), formatRowForLog(row, table), e);
			throw e;
		}
	}

	/**
	 * Format row content for human-friendly error logging.
	 *
	 * @param row   Paimon row
	 * @param table table definition
	 * @return readable row string
	 */
	private String formatRowForLog(GenericRow row, TapTable table) {
		if (row == null) {
			return "null";
		}

		StringBuilder builder = new StringBuilder("GenericRow{");
		builder.append("rowKind=").append(row.getRowKind());
		builder.append(", fieldCount=").append(row.getFieldCount());
		builder.append(", fieldMapping=").append(formatFieldMappingForLog(table, row.getFieldCount()));
		builder.append(", valueMetadata=");

		List<String> fieldNames = resolveRowFieldNames(table, row.getFieldCount());
		builder.append('{');
		for (int i = 0; i < row.getFieldCount(); i++) {
			if (i > 0) {
				builder.append(", ");
			}
			String fieldName = i < fieldNames.size() ? fieldNames.get(i) : "field_" + i;
			builder.append(i).append(':').append(fieldName).append('=').append(formatLogValueMetadata(row.getField(i)));
		}
		builder.append('}');
		builder.append('}');
		return builder.toString();
	}

	/**
	 * Format field index to field name mapping for human-friendly error logging.
	 *
	 * @param table table definition
	 * @param fieldCount row field count
	 * @return readable field mapping string
	 */
	private String formatFieldMappingForLog(TapTable table, int fieldCount) {
		List<String> fieldNames = resolveRowFieldNames(table, fieldCount);
		if (fieldNames.isEmpty()) {
			return "[]";
		}

		StringBuilder builder = new StringBuilder("[");
		for (int i = 0; i < fieldNames.size(); i++) {
			if (i > 0) {
				builder.append(", ");
			}
			builder.append(i).append(':').append(fieldNames.get(i));
		}
		builder.append(']');
		return builder.toString();
	}

	/**
	 * Resolve row field names from cached Paimon schema if available.
	 *
	 * @param table table definition
	 * @param fieldCount row field count
	 * @return field names aligned with row order
	 */
	private List<String> resolveRowFieldNames(TapTable table, int fieldCount) {
		if (table == null || fieldCount <= 0) {
			return Collections.emptyList();
		}

		String cacheKey = config.getDatabase() + "." + table.getName();
		List<DataField> paimonFields = paimonFieldCache.get(cacheKey);
		if (paimonFields == null || paimonFields.isEmpty()) {
			return Collections.emptyList();
		}

		List<String> fieldNames = new ArrayList<>(paimonFields.size());
		for (DataField field : paimonFields) {
			fieldNames.add(field.name());
		}
		return fieldNames;
	}

	/**
	 * Format log value to a compact readable string.
	 *
	 * @param value field value
	 * @return formatted string
	 */
	private String formatLogValueMetadata(Object value) {
		if (value == null) {
			return "null";
		}
		if (value instanceof BinaryString) {
			return "BinaryString(len=" + value.toString().length() + ')';
		}
		if (value instanceof CharSequence) {
			return value.getClass().getSimpleName() + "(len=" + ((CharSequence) value).length() + ')';
		}
		if (value instanceof byte[]) {
			return "byte[" + ((byte[]) value).length + "]";
		}
		if (value instanceof Collection) {
			return value.getClass().getSimpleName() + "(size=" + ((Collection<?>) value).size() + ')';
		}
		if (value instanceof Map) {
			return value.getClass().getSimpleName() + "(size=" + ((Map<?, ?>) value).size() + ')';
		}
		if (value.getClass().isArray()) {
			return value.getClass().getComponentType().getSimpleName()
					+ "[" + java.lang.reflect.Array.getLength(value) + ']';
		}
		return value.getClass().getSimpleName();
	}

	private String formatSourceDataForLog(Map<String, Object> sourceData) {
		if (sourceData == null) {
			return "null";
		}
		StringBuilder builder = new StringBuilder("{");
		int index = 0;
		for (Map.Entry<String, Object> entry : sourceData.entrySet()) {
			if (index++ > 0) {
				builder.append(", ");
			}
			builder.append(entry.getKey()).append('=').append(formatLogValueMetadata(entry.getValue()));
		}
		builder.append('}');
		return builder.toString();
	}

	private String formatEventForLog(TapRecordEvent event) {
		if (event == null) {
			return "null";
		}
		return event.getClass().getSimpleName()
				+ "{referenceTime=" + event.getReferenceTime() + '}';
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
		boolean useHashKey = Boolean.TRUE.equals(computeHashKey.get(table.getName()));
		for (int i = 0; i < paimonFields.size(); i++) {
			DataField dataField = paimonFields.get(i);
			String fieldName = dataField.name();
			Object value;
			if (useHashKey && HASH_KEY.equals(fieldName)) {
				value = toHash(primaryKeyMap.get(table.getName()), data);
			} else {
				value = data.get(fieldName);
			}

			// Get corresponding Paimon field type from cache
			DataType paimonType = dataField.type();

			genericRow.setField(i, convertValueToPaimonType(value, paimonType));
		}

		return genericRow;
	}

	protected String toHash(Collection<String> keys, Map<String, Object> data) {
		try {
			MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
				boolean isFirst = true;
				baos.write('[');
				for (String key : keys) {
					if (isFirst) {
						isFirst = false;
					} else {
						baos.write(SPLIT_CHAR);
					}

					Object val = data.get(key);
					byte[] bytes = toBytes(val);
					baos.write(bytes);
				}
				baos.write(']');

				byte[] hashBytes = md.digest(baos.toByteArray());
				StringBuilder hashHex = new StringBuilder();
				for (byte b : hashBytes) {
					hashHex.append(String.format("%02x", b));
				}
				return hashHex.toString(); // 返回 128 位（32 个字符）的哈希值
			}
		} catch (Exception e) {
			int fieldCount = keys == null ? 0 : keys.size();
			throw new RuntimeException(
					"Failed to compute synthetic Paimon hash key for " + fieldCount
							+ " field(s); causeType=" + e.getClass().getSimpleName());
		}
	}

	protected byte[] toBytes(Object data) throws IOException {
		if (null == data) return new byte[0];
		if (data instanceof byte[]) return (byte[]) data;
		if (data.getClass().isArray()) return arrayToBytes(Arrays.asList((Object[]) data));
		if (data instanceof Collection) return arrayToBytes((Collection<?>) data);
		if (data instanceof Map) return mapToBytes((Map<?, ?>) data);
		return data.toString().getBytes();
	}

	protected byte[] arrayToBytes(Collection<?> collection) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			boolean isFirst = true;
			baos.write('[');
			for (Object o : collection) {
				if (isFirst) {
					isFirst = false;
				} else {
					baos.write(SPLIT_CHAR);
				}
				baos.write(toBytes(o));
			}
			baos.write(']');
			return baos.toByteArray();
		}
	}

	protected byte[] mapToBytes(Map<?, ?> map) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			baos.write('{');
			for (Map.Entry<?, ?> en : map.entrySet()) {
				baos.write(toBytes(en.getKey()));
				baos.write(':');
				baos.write(toBytes(en.getValue()));
			}
			baos.write('}');
			return baos.toByteArray();
		}
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
		String rooType = paimonType.getTypeRoot().name();
		switch (rooType) {
			case "CHAR":
			case "VARCHAR":
			case "STRING":
				return BinaryString.fromString(String.valueOf(value));
			case "TINYINT":
				return ((Number) value).byteValue();
			case "SMALLINT":
				return ((Number) value).shortValue();
			case "BIGINT":
				return ((Number) value).longValue();
			case "DOUBLE":
				return ((Number) value).doubleValue();
			case "FLOAT":
				return ((Number) value).floatValue();
			case "DECIMAL":
				Pair<Integer, Integer> fieldPrecisionAndScale = getFieldPrecisionAndScale(paimonType.asSQLString());
				return Decimal.fromBigDecimal((BigDecimal) value, fieldPrecisionAndScale.getLeft(), fieldPrecisionAndScale.getRight());
			case "TIMESTAMP_WITHOUT_TIME_ZONE":
			case "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
				java.sql.Timestamp sqlTimestamp = (java.sql.Timestamp) value;
				return Timestamp.fromEpochMillis(sqlTimestamp.getTime(), (sqlTimestamp.getNanos() % 1000000));
		}
		return value;
	}

	/**
	 * Flush all accumulated records for all tables
	 * This should be called before closing the connector to ensure all data is committed
	 */
	public void flushAll() throws Exception {
		for (String tableKey : new ArrayList<>(tableWriteContexts.keySet())) {
			flushTable(tableKey);
		}
	}

	/**
	 * Flush accumulated records for a specific table
	 *
	 * @param tableKey table key (database.tableName)
	 */
	public void flushTable(String tableKey) throws Exception {
		try (PaimonServiceLifecycle.Ingress ignored = lifecycle.enter("flushTable")) {
			List<PaimonMicroBatchCoordinator.CallbackReservation> ready =
					flushTableInternal(tableKey, "manual");
			executeCallbacks(ready, false);
			asyncCommitScheduler.stateChanged();
		}
	}

	private List<PaimonMicroBatchCoordinator.CallbackReservation> flushTableInternal(
			String tableKey, String trigger) throws Exception {
		return flushTableInternal(tableKey, trigger, false);
	}

	private List<PaimonMicroBatchCoordinator.CallbackReservation> flushTableInternal(
			String tableKey, String trigger, boolean uninterruptibleCleanup) throws Exception {
		Object lock = commitLocks.computeIfAbsent(tableKey, k -> new Object());
		synchronized (lock) {
			return flushTableLocked(tableKey, trigger, uninterruptibleCleanup);
		}
	}

	private List<PaimonMicroBatchCoordinator.CallbackReservation> flushTableLocked(
			String tableKey, String trigger, boolean uninterruptibleCleanup) throws Exception {
		Object lock = commitLocks.get(tableKey);
		if (lock == null || !Thread.holdsLock(lock)) {
			throw new IllegalStateException(
					"Paimon table flush requires the table lock for " + tableKey);
		}

		PaimonMicroBatchCoordinator.TableSnapshot snapshot =
				microBatchCoordinator.tableSnapshot(tableKey);
		PaimonTableWriteContext writeContext = tableWriteContexts.get(tableKey);
		if (writeContext == null) {
			if (snapshot.bufferedRecordCount() == 0 && !snapshot.hasPendingCommit()) {
				return Collections.emptyList();
			}
			throw new IllegalStateException(
					"Buffered records exist but Paimon write context is missing for table " + tableKey);
		}

		List<PaimonMicroBatchCoordinator.CallbackReservation> ready = new ArrayList<>();
		if (writeContext.hasPendingCommit()) {
			ready.addAll(confirmPendingCommitLocked(
					writeContext,
					tableKey,
					trigger + "-pending",
					uninterruptibleCleanup));
		}
		snapshot = microBatchCoordinator.tableSnapshot(tableKey);
		if (snapshot.bufferedRecordCount() > 0) {
			ready.addAll(commitTableLocked(
					writeContext, tableKey, trigger, uninterruptibleCleanup));
		}
		return ready;
	}

	private void throwIfStickyWriteFailure() {
		Throwable failure = stickyWriteFailure.get();
		if (failure != null) {
			throw new IllegalStateException(
					"Paimon write service is fenced after an ingress failure; restart the task before retrying",
					failure);
		}
	}

	private void throwIfConcurrentIngressStickyFailure() {
		if (stickyWriteFailure.get() instanceof ConcurrentSourceIngressException) {
			throwIfStickyWriteFailure();
		}
	}

	private DynamicIngressGuard dynamicSourceIngressGuard(String tableKey, String tableName) throws Exception {
		PaimonTableWriteContext existing = tableWriteContexts.get(tableKey);
		BucketMode mode;
		if (existing != null) {
			mode = existing.bucketMode();
		} else {
			Table table = catalog.getTable(Identifier.create(config.getDatabase(), tableName));
			mode = table instanceof FileStoreTable
					? ((FileStoreTable) table).bucketMode()
					: null;
		}
		if (mode == null || !PaimonBucketWriterStrategyFactory.requiresOrderedSingleWriterIngress(mode)) {
			return null;
		}
		return dynamicSourceIngressGuards.computeIfAbsent(tableKey, ignored -> new DynamicIngressGuard());
	}

	private void beginSourceIngress(
			DynamicIngressGuard ingressGuard, String operation, String tableKey) {
		throwIfStickyWriteFailure();
		if (ingressGuard != null) {
			synchronized (ingressGuard) {
				throwIfStickyWriteFailure();
				if (ingressGuard.active) {
					IllegalStateException failure = new ConcurrentSourceIngressException(
							"Concurrent Paimon source ingress is unsupported without an ordered PDK "
									+ "source sequence: " + operation + " on " + tableKey);
					recordStickyFailure(failure);
					throw failure;
				}
				ingressGuard.active = true;
			}
		}
	}

	private void endSourceIngress(DynamicIngressGuard ingressGuard, boolean successful) {
		if (ingressGuard != null) {
			synchronized (ingressGuard) {
				ingressGuard.active = false;
				if (successful) {
					throwIfStickyWriteFailure();
				}
			}
		} else if (successful) {
			throwIfStickyWriteFailure();
		}
	}

	private static final class DynamicIngressGuard {
		private boolean active;
	}

	private static final class ConcurrentSourceIngressException extends IllegalStateException {
		private ConcurrentSourceIngressException(String message) {
			super(message);
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
		String typeRoot = dataType.getTypeRoot().name();
		switch (typeRoot) {
			case "BOOLEAN":
				return row.getBoolean(pos);
			case "TINYINT":
				return row.getByte(pos);
			case "SMALLINT":
				return row.getShort(pos);
			case "INTEGER":
				return row.getInt(pos);
			case "BIGINT":
				return row.getLong(pos);
			case "FLOAT":
				return row.getFloat(pos);
			case "DOUBLE":
				return row.getDouble(pos);
			case "DECIMAL":
				Decimal decimal;
				if (dataType instanceof DecimalType) {
					decimal = row.getDecimal(pos, ((DecimalType) dataType).getPrecision(), ((DecimalType) dataType).getScale());
				} else {
					Pair<Integer, Integer> fieldPrecisionAndScale = getFieldPrecisionAndScale(dataType.asSQLString());
					decimal = row.getDecimal(pos, fieldPrecisionAndScale.getLeft(), fieldPrecisionAndScale.getRight());
				}
				return decimal != null ? decimal.toBigDecimal() : null;
			case "DATE":
				int days = row.getInt(pos);
				return new java.sql.Date(days * 86400000L);
			case "TIMESTAMP_WITHOUT_TIME_ZONE": {
				Timestamp timestamp;
				if (dataType instanceof TimestampType) {
					timestamp = row.getTimestamp(pos, ((TimestampType) dataType).getPrecision());
				} else {
					Integer fraction = getFieldFraction(dataType.asSQLString());
					timestamp = row.getTimestamp(pos, fraction);
				}
				if (timestamp != null) {
					return timestamp.toLocalDateTime();
				}
				return null;
			}
			case "TIMESTAMP_WITH_LOCAL_TIME_ZONE": {
				Timestamp timestamp;
				if (dataType instanceof TimestampType) {
					timestamp = row.getTimestamp(pos, ((TimestampType) dataType).getPrecision());
				} else {
					Integer fraction = getFieldFraction(dataType.asSQLString());
					timestamp = row.getTimestamp(pos, fraction);
				}
				if (timestamp != null) {
					return timestamp.toLocalDateTime().atZone(ZoneOffset.UTC);
				}
				return null;
			}
			case "TIME_WITHOUT_TIME_ZONE":
				return LocalTime.ofNanoOfDay(row.getInt(pos) * 1000_000L).atDate(LocalDate.ofYearDay(1970, 1));
			case "BINARY":
			case "VARBINARY":
			case "BYTES":
				return row.getBinary(pos);
			default:
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
	public synchronized void close() throws Exception {
		synchronized (closeLock) {
			if (lifecycle.state() == PaimonServiceLifecycle.State.CLOSED) {
				Throwable previous = lifecycle.terminalOutcome();
				if (previous != null) {
					rethrow(previous);
				}
				return;
			}

			Throwable failure = lifecycle.firstFailure();
			boolean interrupted = false;
			if (lifecycle.state() == PaimonServiceLifecycle.State.RUNNING) {
				lifecycle.beginStopping();
			}

			while (true) {
				try {
					if (asyncCommitScheduler.shutdownAndAwait(5L, TimeUnit.SECONDS)) {
						break;
					}
				} catch (InterruptedException interruption) {
					failure = appendFailure(failure, interruption);
					interrupted = true;
					Thread.interrupted();
				}
			}

			while (!lifecycle.isQuiescent()) {
				try {
					lifecycle.awaitQuiescence();
				} catch (InterruptedException interruption) {
					failure = appendFailure(failure, interruption);
					interrupted = true;
					Thread.interrupted();
				}
			}

			boolean allTablesDrained = failure == null;
			for (String tableKey : new ArrayList<>(tableWriteContexts.keySet())) {
				try {
					flushTableInternal(tableKey, "stop", true);
				} catch (Throwable drainFailure) {
					failure = appendFailure(failure, drainFailure);
					allTablesDrained = false;
				}
			}
			InterruptedException retryInterruption = cleanupInterruption.getAndSet(null);
			if (retryInterruption != null) {
				failure = appendFailure(failure, retryInterruption);
				interrupted = true;
			}

			Throwable stickyAfterDrain = lifecycle.firstFailure();
			if (stickyAfterDrain != null) {
				failure = appendFailure(failure, stickyAfterDrain);
				allTablesDrained = false;
			}
			if (allTablesDrained) {
				try {
					List<PaimonMicroBatchCoordinator.CallbackReservation> ready =
							new ArrayList<>(
									microBatchCoordinator.reservedButNotStartedCallbacks());
					ready.addAll(microBatchCoordinator.reserveReadyCallbacks());
					executeCallbacks(ready, true);
				} catch (Throwable callbackFailure) {
					failure = appendFailure(failure, callbackFailure);
				}
			}

			for (String tableKey : new ArrayList<>(tableWriteContexts.keySet())) {
				PaimonTableWriteContext context = tableWriteContexts.remove(tableKey);
				try {
					if (context != null) {
						context.close();
					}
				} catch (Throwable contextFailure) {
					failure = appendFailure(failure, contextFailure);
				} finally {
					unregisterPhysicalTableOwner(tableKey);
				}
			}

			try {
				cleanupAllResources();
			} catch (Throwable cleanupFailure) {
				failure = appendFailure(failure, cleanupFailure);
			}
			dynamicSourceIngressGuards.clear();
			activeConnectorContext = null;
			boundTaskStateMap = null;
			flushOffsetCallback = null;
			lifecycle.publishClosed(failure);
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
			if (failure != null) {
				rethrow(failure);
			}
		}
	}

	private static Throwable appendFailure(Throwable primary, Throwable additional) {
		if (additional == null || additional == primary) {
			return primary;
		}
		if (primary == null) {
			return additional;
		}
		for (Throwable suppressed : primary.getSuppressed()) {
			if (suppressed == additional) {
				return primary;
			}
		}
		primary.addSuppressed(additional);
		return primary;
	}

	public synchronized void setFlushOffsetCallback(Consumer<Object> flushOffsetCallback) {
		PaimonServiceLifecycle.State state = lifecycle.state();
		if (state != PaimonServiceLifecycle.State.NEW) {
			if (this.flushOffsetCallback == flushOffsetCallback) {
				return;
			}
			throw new IllegalStateException(
					"flushOffsetCallback cannot be replaced while Paimon service is " + state);
		}
		this.flushOffsetCallback = flushOffsetCallback;
	}

}
