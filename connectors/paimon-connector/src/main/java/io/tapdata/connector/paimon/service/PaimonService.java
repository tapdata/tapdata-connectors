package io.tapdata.connector.paimon.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.TapCallbackOffset;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
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
import java.io.Closeable;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
	 * PDK 异步 offset 协议开关。
	 *
	 * <p>PDK 2.0.8 的 callback 没有持久化确认和源事件顺序契约，因此当前固定为 false，
	 * 强制 CDC 在返回前同步提交，避免数据尚未落入 Paimon 时 offset 已经前移。
	 */
	private static final boolean ASYNC_OFFSET_CONTRACT_VERIFIED = false;
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
	 * 避免同表不同写入对象的路由索引或提交状态相互分离。
	 */
	private final Map<String, PaimonTableWriteContext> tableWriteContexts = new ConcurrentHashMap<>();
	/** Key 为逻辑表标识，Value 为其物理表路径摘要，用于释放 JVM 内的物理表 owner。 */
	private final Map<String, String> physicalTableByLogicalTable = new ConcurrentHashMap<>();
	/** 当前 Service 实例的唯一写入 owner 标识，用于区分同 JVM 内的不同连接或任务。 */
	private final String serviceWriterOwner = UUID.randomUUID().toString();

	// ===== Batch Accumulation for Performance =====
	/** Key 为逻辑表标识，Value 为该表自上次成功提交后累计的记录数。 */
	private final Map<String, AtomicInteger> accumulatedRecordCount = new ConcurrentHashMap<>();
	/** Key 为逻辑表标识，Value 为该表最近一次成功提交或初始化提交计时的毫秒时间戳。 */
	private final Map<String, AtomicLong> lastCommitTime = new ConcurrentHashMap<>();
	/**
	 * 表级写入生命周期锁；串行化同一表的写入、prepare/commit、DDL drain 和资源关闭操作。
	 */
	private final Map<String, Object> commitLocks = new ConcurrentHashMap<>();
	/** 正在执行 DDL drain 的逻辑表集合；集合中的表禁止创建或继续使用写上下文。 */
	private final Set<String> drainingTables = ConcurrentHashMap.newKeySet();
	/**
	 * 已进入 CDC 阶段、理论上可参与异步 flush 的表集合。
	 *
	 * <p>当前 {@link #ASYNC_OFFSET_CONTRACT_VERIFIED} 为 false，因此仅保留该状态供未来协议验证后使用。
	 */
	private final Set<String> asyncCommitEligibleTables = ConcurrentHashMap.newKeySet();

	// ===== Async Commit Support =====
	/** 定时扫描并提交已累计数据的单线程执行器；当前同步 offset 模式下不会创建。 */
	private ScheduledExecutorService asyncCommitExecutor;
	/**
	 * 按到达顺序保存每张表尚未回调的首个 offset；同步 Map 维持多表回调队列的可见性。
	 * 当前平台托管 offset 模式下不再填充，仅保留兼容逻辑。
	 */
	private final Map<String, TapCallbackOffset> firstOffsetByTable;
	/** 已完成 Paimon 提交、允许从全局 offset 队列头部回调的平台表集合。 */
	private final Set<String> committedOffsetTables = new HashSet<>();
	/** 串行化多表 offset 回调及队列头部推进，防止回调乱序。 */
	private final Object offsetCallbackLock = new Object();
	/** Connector 管理 offset 时使用的回调；当前 setter 会忽略传入值并保持为 null。 */
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
	 * 误当成事件顺序；不同表以及 fixed/append 表仍可保持原有并发能力。
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
		this.log = log;
		this.config = config;
		this.firstOffsetByTable = Collections.synchronizedMap(new LinkedHashMap<>());
	}

	/**
	 * Initialize Paimon catalog
	 *
	 * @throws Exception if initialization fails
	 */
	public void init() throws Exception {
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

		// Initialize async commit if enabled
		initAsyncCommit();
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

	/**
	 * Initialize async commit executor if enabled in config
	 */
	private void initAsyncCommit() {
		Boolean enableAsync = config.getEnableAsyncCommit();
		Integer commitInterval = config.getCommitIntervalMs();
		if (!ASYNC_OFFSET_CONTRACT_VERIFIED) {
			if (Boolean.TRUE.equals(enableAsync)) {
				log.warn(
						"Paimon async commit is disabled because the current PDK does not expose "
								+ "a durable offset acknowledgement/source-order contract; CDC writes will commit synchronously");
			}
			return;
		}

		if (flushOffsetCallback != null
				&& enableAsync != null && enableAsync && commitInterval != null && commitInterval > 0) {
			// Create scheduled executor with single thread
			asyncCommitExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
				Thread t = new Thread(r, "paimon-async-commit");
				t.setDaemon(true); // Daemon thread won't prevent JVM shutdown
				return t;
			});

			// Schedule periodic commit task
			asyncCommitExecutor.scheduleAtFixedRate(() -> {
				if (stickyWriteFailure.get() != null) {
					return;
				}
				try {
					// Commit all tables that have accumulated data
					for (String tableKey : new ArrayList<>(accumulatedRecordCount.keySet())) {
						if (!asyncCommitEligibleTables.contains(tableKey)) {
							continue;
						}
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
					stickyWriteFailure.compareAndSet(null, e);
					getAsyncCommitLog().warn("Error in async commit: {}", e.getMessage(), e);
				}
			}, commitInterval, commitInterval, TimeUnit.MILLISECONDS);
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
			boolean configIsDynamic = "dynamic".equalsIgnoreCase(config.getBucketMode(tableName));

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

		// Set bucket configuration based on bucket mode
		if ("dynamic".equalsIgnoreCase(config.getBucketMode(tableName))) {
			// Dynamic bucket mode: set bucket to -1
			// This mode provides better flexibility
			schemaBuilder.option("bucket", "-1");
		} else {
			// Fixed bucket mode: set specific bucket count
			Integer bucketCount = config.getBucketCount(tableName);
			if (bucketCount == null || bucketCount <= 0) {
				bucketCount = 4; // Default to 4 buckets if not configured
			}
			schemaBuilder.option("bucket", String.valueOf(bucketCount));
		}
		if (EmptyKit.isNotBlank(config.getFileFormat(tableName))) {
			schemaBuilder.option("file.format", config.getFileFormat(tableName));
		}
		if (EmptyKit.isNotBlank(config.getCompression(tableName))) {
			schemaBuilder.option("compression", config.getCompression(tableName));
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

				// Set compaction strategy
				schemaBuilder.option("changelog-producer", "input");

				// Compact small files more aggressively
				schemaBuilder.option("num-sorted-run.compaction-trigger", "30");
				schemaBuilder.option("num-sorted-run.stop-trigger", "2147483647");
			} else {
				// Disable auto compaction
				schemaBuilder.option("write-only", "true");
			}
		}

		// 4. Snapshot settings for better performance
		// Keep more snapshots in memory for faster access
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

		// 8. Memory settings
		schemaBuilder.option("sink.parallelism", String.valueOf(config.getWriteThreads()));

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
			runTableDdl(tableKey, () -> catalog.dropTable(identifier, true));
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

		// Get table, if not exists, return
		Table table;
		try {
			table = catalog.getTable(identifier);
		} catch (Catalog.TableNotExistException e) {
			// Table does not exist, nothing to clear
			return;
		}

		// Drain and invalidate the old writer before changing table contents. Native truncate keeps
		// partition keys, options, UUID and physical location intact.
		runTableDdl(tableKey, () -> {
			Table currentTable = catalog.getTable(identifier);
			try (BatchTableCommit commit = currentTable.newBatchWriteBuilder().newCommit()) {
				commit.truncateTable();
			}
		});
	}

	private void runTableDdl(String tableKey, TableDdlAction action) throws Exception {
		throwIfStickyWriteFailure();
		Object lock = commitLocks.computeIfAbsent(tableKey, ignored -> new Object());
		synchronized (lock) {
			if (!drainingTables.add(tableKey)) {
				throw new IllegalStateException("Table DDL is already in progress for " + tableKey);
			}
			try {
				AtomicInteger count = accumulatedRecordCount.get(tableKey);
				if (count != null && count.get() > 0) {
					flushTable(tableKey);
				}
				PaimonTableWriteContext context = tableWriteContexts.remove(tableKey);
				if (context != null) {
					context.close();
				}
				action.run();
			} finally {
				// Keep ownership through the Catalog DDL itself. Releasing it before action.run()
				// would let a second local service create a writer against a half-mutated table.
				unregisterPhysicalTableOwner(tableKey);
				accumulatedRecordCount.remove(tableKey);
				lastCommitTime.remove(tableKey);
				asyncCommitEligibleTables.remove(tableKey);
				dynamicSourceIngressGuards.remove(tableKey);
				drainingTables.remove(tableKey);
			}
		}
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
		String tableName = table.getName();
		String tableKey = config.getDatabase() + "." + tableName;
		DynamicIngressGuard ingressGuard = dynamicSourceIngressGuard(tableKey, tableName);
		beginSourceIngress(ingressGuard, "writeRecords", tableKey);
		boolean successful = false;
		try {
			computeHashKey.computeIfAbsent(tableName,
					ignored -> Boolean.TRUE.equals(config.getHashKey(tableName))
							&& EmptyKit.isNotEmpty(table.primaryKeys(true))
							&& table.primaryKeys(true).size() > 5);
			primaryKeyMap.putIfAbsent(tableName, table.primaryKeys(true));
			WriteListResult<TapRecordEvent> result =
					writeRecordsWithStreamWriteInternal(recordEvents, table, connectorContext);
			successful = true;
			return result;
		} finally {
			endSourceIngress(ingressGuard, successful);
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
		if (!(paimonTable instanceof FileStoreTable)) {
			return false;
		}
		BucketMode mode = ((FileStoreTable) paimonTable).bucketMode();
		return PaimonBucketWriterStrategyFactory.requiresOrderedSingleWriterIngress(mode);
	}

	public void afterInitialSync(TapConnectorContext connectorContext, TapTable tapTable) throws Exception {
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
			try {
				synchronized (lock) {
					PaimonTableWriteContext writeContext =
							getOrCreateTableWriteContext(tableKey, tableName, identifier, connectorContext);
					writeContext.commit();
					commitCallback(tableName);
					AtomicInteger count = accumulatedRecordCount.get(tableKey);
					if (count != null) {
						count.set(0);
					}
					lastCommitTime.computeIfAbsent(tableKey, k -> new AtomicLong()).set(System.currentTimeMillis());
				}
			} catch (Exception e) {
				stickyWriteFailure.compareAndSet(null, e);
				throw e;
			}
			successful = true;
		} finally {
			endSourceIngress(ingressGuard, successful);
		}
//		initAsyncCommit();
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
		if (recordEvents == null || recordEvents.isEmpty()) {
			return new WriteListResult<>();
		}
		throwIfStickyWriteFailure();
		bindTaskState(connectorContext);
		connectorContext.configContext();
		activeConnectorContext = connectorContext;
		Log currentLog = connectorContext.getLog();
		String database = config.getDatabase();
		String tableName = table.getName();
		String tableKey = database + "." + tableName;
		Map<String, Object> firstEventInfo = recordEvents.get(0).getInfo();
		boolean cdcStage = "CDC".equals(
				firstEventInfo == null ? null : firstEventInfo.get(TapRecordEvent.INFO_KEY_SYNC_STAGE));
		if (cdcStage) {
			asyncCommitEligibleTables.add(tableKey);
		}

		// Use loop instead of recursion for retry
		int maxRetries = 3;

		writeAttempt:
		while (true) {
			WriteListResult<TapRecordEvent> result = new WriteListResult<>();
			Identifier identifier = Identifier.create(database, tableName);
			PaimonTableWriteContext writeContext = null;
			boolean pendingBeforeBatch = false;
			boolean ingressStarted = false;

			try {
				// One context owns writer, committer, dynamic bucket state and commit identifiers.
				Object lock = commitLocks.computeIfAbsent(tableKey, k -> new Object());
				synchronized (lock) {
					writeContext = getOrCreateTableWriteContext(
							tableKey, tableName, identifier, connectorContext);
					pendingBeforeBatch = writeContext.hasPendingCommit();
					if (pendingBeforeBatch) {
						writeContext.retryPendingCommit();
						commitCallback(tableName);
						AtomicInteger previousCount = accumulatedRecordCount.get(tableKey);
						if (previousCount != null) {
							previousCount.set(0);
						}
						lastCommitTime.computeIfAbsent(tableKey, k -> new AtomicLong())
								.set(System.currentTimeMillis());
					}
					ingressStarted = true;
					for (TapRecordEvent event : recordEvents) {
						if (flushOffsetCallback != null && !firstOffsetByTable.containsKey(tableName)) {
							TapCallbackOffset tapOffset = new TapCallbackOffset();
							// 从 TapRecordEvent.info 中提取 offset 信息
							// 这些信息由 HazelcastTargetPdkBaseNode.handleTapdataEventDML 方法添加
							Object batchOffset = event.getInfo("batchOffset");
							Object streamOffset = event.getInfo("streamOffset");
							Object syncStage = event.getInfo("syncStage");
							Object sourceTime = event.getInfo("sourceTime");
							Object nodeIds = event.getInfo("nodeIds");

							// 填充 TapOffset
							tapOffset.batchOffset(batchOffset)
									.streamOffset(streamOffset)
									.tableId(event.getTableId())
									.syncStage(syncStage != null ? syncStage.toString() : null)
									.sourceTime(sourceTime instanceof Long ? (Long) sourceTime : null)
									.eventTime(event.getReferenceTime())
									.nodeIds(nodeIds);
							if (tapOffset.hasValidOffset()) {
								firstOffsetByTable.put(tableName, tapOffset);
							}
						}
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

					if (cdcStage && !ASYNC_OFFSET_CONTRACT_VERIFIED) {
						// Current PDK does not define a durable callback acknowledgement or a sequence
						// for concurrent calls. Confirm the Paimon snapshot before every CDC return.
						shouldCommit = true;
					} else if (flushOffsetCallback == null && cdcStage) {
						shouldCommit = true;
					} else if (batchSize == null || batchSize <= 0) {
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
						// init sync stage just commit once, for batch commit + spill disk
						if (!cdcStage) {
							// Any historical pending commit was confirmed before the event loop. The
							// current initial-sync batch is already buffered exactly once and is committed
							// by afterInitialSync; re-entering here would duplicate the whole batch.
							return result;
						}
						// Double-check if we still need to commit (another thread might have committed)
						int finalCount = recordCount.get();
						if (finalCount > 0) {
							long commitStartTime = System.currentTimeMillis();
							writeContext.commit();
							commitCallback(tableName);
							// Reset counters after successful commit
							recordCount.set(0);
							lastCommit.set(System.currentTimeMillis());

							long commitDuration = System.currentTimeMillis() - commitStartTime;
							currentLog.debug("Committed {} accumulated records for table {} in {} ms",
									finalCount, tableKey, commitDuration);
						}
					}
				}

				// The canonical table write context is reused until table drain or service close.
				return result;

			} catch (Exception e) {
				if (e instanceof PaimonDynamicBucketPollutedException
						|| e instanceof PaimonFatalWriteException) {
					if (ingressStarted) {
						stickyWriteFailure.compareAndSet(null, e);
					}
					throw e;
				}
				// If commit outcome is ambiguous, retry the prepared messages before considering
				// catalog recreation or source-batch replay. This is the Paimon idempotent path.
				if (writeContext != null && writeContext.hasPendingCommit()) {
					Exception pendingError = e;
					for (int pendingRetry = 0; pendingRetry < maxRetries; pendingRetry++) {
						try {
							writeContext.retryPendingCommit();
							commitCallback(tableName);
							AtomicInteger count = accumulatedRecordCount.get(tableKey);
							if (count != null) {
								count.set(0);
							}
							lastCommitTime.computeIfAbsent(tableKey, k -> new AtomicLong())
									.set(System.currentTimeMillis());
							// Re-enter only when the failure happened while confirming the historical
							// pending commit, before this source batch reached the writer. If the
							// current batch already entered (and its own ambiguous commit was just
							// confirmed), replaying the loop would write the whole batch twice.
							if (pendingBeforeBatch && !ingressStarted) {
								continue writeAttempt;
							}
							return result;
						} catch (Exception retryError) {
							pendingError.addSuppressed(retryError);
							CommonUtils.ignoreAnyError(() -> TimeUnit.SECONDS.sleep(1L), TAG);
						}
					}
					if (ingressStarted) {
						stickyWriteFailure.compareAndSet(null, pendingError);
					}
					throw new TapPdkRetryableEx("paimon", ErrorKit.getLastCause(pendingError));
				}
				// A row-routing/write failure may have advanced an in-memory dynamic index. Do not
				// replay in-place or rebuild every table: fail this task so its durable source offset
				// controls replay and every router is reconstructed from a committed snapshot.
				if (ingressStarted) {
					stickyWriteFailure.compareAndSet(null, e);
				}
				throw new TapPdkRetryableEx("paimon", ErrorKit.getLastCause(e));
			}
		}
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

		// Close all canonical table write contexts first.
		for (String tableKey : new ArrayList<>(tableWriteContexts.keySet())) {
			cleanupTableResources(tableKey);
		}

		tableWriteContexts.clear();
		for (String tableKey : new ArrayList<>(physicalTableByLogicalTable.keySet())) {
			unregisterPhysicalTableOwner(tableKey);
		}

		// Clear batch accumulation tracking
		accumulatedRecordCount.clear();
		lastCommitTime.clear();
		commitLocks.clear();
		drainingTables.clear();
		asyncCommitEligibleTables.clear();

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


	private PaimonTableWriteContext getOrCreateTableWriteContext(
			String tableKey,
			String tableName,
			Identifier identifier,
			TapConnectorContext connectorContext) throws Exception {
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
					Table table = catalog.getTable(identifier);
					if (!(table instanceof FileStoreTable)) {
						throw new IllegalArgumentException(
								"Only FileStoreTable supports connector writes for " + tableKey);
					}
					FileStoreTable fileStoreTable = (FileStoreTable) table;
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
								binding.store());
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

	/** Remove and close the one canonical context for a physical table. */
	private void cleanupTableResources(String tableKey) {
		PaimonTableWriteContext context = tableWriteContexts.remove(tableKey);
		try {
			if (context == null) {
				return;
			}
			try {
				context.close();
			} catch (Exception e) {
				log.warn("Failed to close Paimon write context for table {}", tableKey, e);
			}
		} finally {
			unregisterPhysicalTableOwner(tableKey);
			dynamicSourceIngressGuards.remove(tableKey);
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
		writeContext.validateRequiredRoutingFields(after, "INSERT");
		String database = config.getDatabase();
		Identifier identifier = Identifier.create(database, table.getName());
		GenericRow row = convertToGenericRow(after, table, identifier);
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

		// Convert before and after data to GenericRow first to avoid duplicate conversion
		GenericRow beforeRow = null;
		if (before != null && !before.isEmpty()) {
			writeContext.validateRequiredRoutingFields(before, "UPDATE_BEFORE");
			beforeRow = convertToGenericRow(before, table, identifier);
		}
		writeContext.validateRequiredRoutingFields(after, "UPDATE_AFTER");
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
				// Convert update to delete + insert
				// First, write DELETE using before data
				beforeRow.setRowKind(RowKind.DELETE);
				writeRow(writeContext, event, beforeRow, table, before, currentLog);

				// Then, write INSERT using after data
				afterRow.setRowKind(RowKind.INSERT);
				writeRow(writeContext, event, afterRow, table, after, currentLog);
				return;
			}
		}

		// Normal update logic: Write U- (UPDATE_BEFORE) if before data exists
		if (beforeRow != null) {
			beforeRow.setRowKind(RowKind.UPDATE_BEFORE);
			writeRow(writeContext, event, beforeRow, table, before, currentLog);
		}

		// Write U+ (UPDATE_AFTER) using after data
		afterRow.setRowKind(RowKind.UPDATE_AFTER);
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
		writeContext.validateRequiredRoutingFields(before, "DELETE");
		String database = config.getDatabase();
		Identifier identifier = Identifier.create(database, table.getName());
		GenericRow row = convertToGenericRow(before, table, identifier);
		// Set row kind to DELETE
		row.setRowKind(RowKind.DELETE);
		writeRow(writeContext, event, row, table, before, currentLog);
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
		throwIfStickyWriteFailure();
		AtomicInteger recordCount = accumulatedRecordCount.get(tableKey);
		if (recordCount == null || recordCount.get() <= 0) {
			return; // Nothing to flush
		}

		PaimonTableWriteContext writeContext = tableWriteContexts.get(tableKey);

		if (writeContext == null) {
			throw new IllegalStateException(
					"Accumulated records exist but Paimon write context is missing for table " + tableKey);
		}

		// Use lock to ensure thread safety
		Object lock = commitLocks.computeIfAbsent(tableKey, k -> new Object());
		try {
			synchronized (lock) {
				int finalCount = recordCount.get();
				if (finalCount > 0) {
					writeContext.commit();
					commitCallback(writeContext.tableName());
					// Reset counters
					recordCount.set(0);
					AtomicLong lastCommit = lastCommitTime.get(tableKey);
					if (lastCommit != null) {
						lastCommit.set(System.currentTimeMillis());
					}
				}
			}
		} catch (Exception e) {
			stickyWriteFailure.compareAndSet(null, e);
			throw e;
		}
	}

	private void throwIfStickyWriteFailure() {
		Throwable failure = stickyWriteFailure.get();
		if (failure != null) {
			throw new IllegalStateException(
					"Paimon write service is fenced after an ingress failure; restart the task before retrying",
					failure);
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
					IllegalStateException failure = new IllegalStateException(
							"Concurrent Paimon source ingress is unsupported without an ordered PDK "
									+ "source sequence: " + operation + " on " + tableKey);
					stickyWriteFailure.compareAndSet(null, failure);
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
	public void close() throws Exception {
		Exception failure = null;
		try {
			flushAll();
		} catch (Exception e) {
			failure = e;
		}

		// Close contexts explicitly so resource failures are not hidden by best-effort catalog cleanup.
		for (String tableKey : new ArrayList<>(tableWriteContexts.keySet())) {
			PaimonTableWriteContext context = tableWriteContexts.remove(tableKey);
			try {
				if (context == null) {
					continue;
				}
				try {
					context.close();
				} catch (Exception e) {
					if (failure == null) {
						failure = e;
					} else {
						failure.addSuppressed(e);
					}
				}
			} finally {
				unregisterPhysicalTableOwner(tableKey);
			}
		}

		cleanupAllResources();
		dynamicSourceIngressGuards.clear();
		activeConnectorContext = null;
		boundTaskStateMap = null;
		if (failure != null) {
			throw failure;
		}
	}

	public void setFlushOffsetCallback(Consumer<Object> flushOffsetCallback) {
		if (flushOffsetCallback != null) {
			log.warn(
					"Ignoring connector-managed offset callback: Paimon runs in synchronous PLATFORM_MANAGED mode");
		}
		this.flushOffsetCallback = null;
	}

	public Map<String, TapCallbackOffset> getFirstOffsetByTable() {
		return firstOffsetByTable;
	}

	private void commitCallback(String tableName) throws Exception {
		if (flushOffsetCallback == null) {
			return;
		}

		// Serialize callbacks and drain only the committed prefix of the global arrival order.
		// A callback failure keeps the offset at the head for a later retry.
		synchronized (offsetCallbackLock) {
			synchronized (firstOffsetByTable) {
				committedOffsetTables.add(tableName);
			}
			while (true) {
				Map.Entry<String, TapCallbackOffset> firstEntry;
				synchronized (firstOffsetByTable) {
					firstEntry = firstOffsetByTable.entrySet().stream().findFirst().orElse(null);
					if (firstEntry == null || !committedOffsetTables.contains(firstEntry.getKey())) {
						return;
					}
				}

				TapCallbackOffset offset = firstEntry.getValue();
				if (offset != null && offset.hasValidOffset()) {
					try {
						TapCallbackOffset callbackOffset = new TapCallbackOffset();
						callbackOffset.putAll(offset);
						flushOffsetCallback.accept(callbackOffset);
					} catch (Exception e) {
						IllegalStateException callbackFailure = new IllegalStateException(
								"Failed to flush committed Paimon offset", e);
						stickyWriteFailure.compareAndSet(null, callbackFailure);
						throw callbackFailure;
					}
				}

				synchronized (firstOffsetByTable) {
					TapCallbackOffset current = firstOffsetByTable.get(firstEntry.getKey());
					if (current == firstEntry.getValue()) {
						firstOffsetByTable.remove(firstEntry.getKey());
						committedOffsetTables.remove(firstEntry.getKey());
					}
				}
			}
		}
	}
}
