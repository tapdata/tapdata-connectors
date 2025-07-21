package io.tapdata.connector.starrocks.streamload;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.connector.starrocks.StarrocksJdbcContext;
import io.tapdata.connector.starrocks.bean.StarrocksConfig;
import io.tapdata.connector.starrocks.streamload.exception.StarrocksRetryableException;
import io.tapdata.connector.starrocks.streamload.exception.StarrocksRuntimeException;
import io.tapdata.connector.starrocks.streamload.exception.StreamLoadException;
import io.tapdata.connector.starrocks.streamload.rest.models.RespContent;
import io.tapdata.connector.starrocks.util.MinuteWriteLimiter;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.writeListResult;

/**
 * @author jarad
 * @date 7/14/22
 */
public class StarrocksStreamLoader {
    private static final String TAG = StarrocksStreamLoader.class.getSimpleName();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String HTTPS_LOAD_URL_PATTERN = "https://%s/api/%s/%s/_stream_load";
    private static final String HTTP_LOAD_URL_PATTERN = "http://%s/api/%s/%s/_stream_load";
    private static final String LABEL_PREFIX_PATTERN = "tapdata_%s_%s";

    private final StarrocksConfig StarrocksConfig;
    private final CloseableHttpClient httpClient;
    private final RecordStream recordStream;

    private boolean loadBatchFirstRecord;
    // 改为按表存储dataColumns
    private final Map<String, Set<String>> dataColumnsByTable;
    private MessageSerializer messageSerializer;
    private TapTable tapTable;
    private final Metrics metrics;

    // 新增字段：时间和大小控制
    private long lastFlushTime;
    private final Map<String, Long> currentBatchSizeByTable; // 改为按表管理
    private final Map<String, Long> lastFlushTimeByTable; // 按表管理刷新时间
    private final MinuteWriteLimiter minuteWriteLimiter;

    // 文件缓存相关字段 - 改为按表管理
    private final Map<String, Path> tempCacheFilesByTable;
    private final Map<String, FileOutputStream> cacheFileStreamsByTable;
    private final Map<String, Boolean> isFirstRecordByTable;
    private final Set<String> pendingFlushTables; // 记录还没有flush的表

    // 日志打印控制
    private long lastLogTime;
    private static final long LOG_INTERVAL_MS = 30 * 1000; // 30秒

    // 线程安全和定时刷新
    private final Object writeLock = new Object();
    private ScheduledExecutorService flushScheduler;
    private ScheduledFuture<?> flushTask;

    // 内存保护：限制同时处理的表数量
    private static final int MAX_CONCURRENT_TABLES = 50;
    private long lastMemoryCheckTime = 0;
    private static final long MEMORY_CHECK_INTERVAL = 30000; // 30秒检查一次内存

    public StarrocksStreamLoader(StarrocksJdbcContext StarrocksJdbcContext, CloseableHttpClient httpClient) {
        this.StarrocksConfig = (StarrocksConfig) StarrocksJdbcContext.getConfig();
        this.httpClient = httpClient;
        Integer writeByteBufferCapacity = StarrocksConfig.getWriteByteBufferCapacity();
        if (null == writeByteBufferCapacity) {
            writeByteBufferCapacity = Constants.CACHE_BUFFER_SIZE;
        } else {
            writeByteBufferCapacity = writeByteBufferCapacity * 1024;
        }
        this.recordStream = new RecordStream(writeByteBufferCapacity, Constants.CACHE_BUFFER_COUNT);
        this.loadBatchFirstRecord = true;
        this.dataColumnsByTable = new ConcurrentHashMap<>();
        initMessageSerializer();
        this.metrics = new Metrics();

        // 初始化新的字段
        this.lastFlushTime = System.currentTimeMillis();
        this.currentBatchSizeByTable = new ConcurrentHashMap<>();
        this.lastFlushTimeByTable = new ConcurrentHashMap<>();
        this.minuteWriteLimiter = new MinuteWriteLimiter(StarrocksConfig.getMinuteLimitMB());
        this.lastLogTime = System.currentTimeMillis();

        // 初始化文件缓存相关的Map
        this.tempCacheFilesByTable = new ConcurrentHashMap<>();
        this.cacheFileStreamsByTable = new ConcurrentHashMap<>();
        this.isFirstRecordByTable = new ConcurrentHashMap<>();
        this.pendingFlushTables = ConcurrentHashMap.newKeySet();

        // 初始化定时刷新
        initializeFlushScheduler();
    }

    private void initMessageSerializer() {
        StarrocksConfig.WriteFormat writeFormat = StarrocksConfig.getWriteFormatEnum();
        TapLogger.info(TAG, "Starrocks stream load run with {} format", writeFormat);
        switch (writeFormat) {
            case csv:
                messageSerializer = new CsvSerializer();
                break;
            case json:
                messageSerializer = new JsonSerializer();
                break;
        }
    }

    /**
     * 为指定表初始化缓存文件
     */
    private void initializeCacheFileForTable(String tableName) {
        try {
            // 内存保护：检查表数量限制
            checkMemoryAndTableLimits(tableName);

            // 先清理该表的现有资源（如果存在）
            cleanupCacheFileForTable(tableName);

            // 创建临时目录
            Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"), "starrocks-cache");
            if (!Files.exists(tempDir)) {
                Files.createDirectories(tempDir);
            }

            // 创建临时缓存文件，使用简化的文件名避免过多对象创建
            long timestamp = System.currentTimeMillis();
            long threadId = Thread.currentThread().getId();
            String fileName = String.format("starrocks-cache-%s-%d-%d.tmp", tableName, threadId, timestamp);
            Path tempCacheFile = tempDir.resolve(fileName);

            // 初始化文件输出流
            FileOutputStream cacheFileStream = new FileOutputStream(tempCacheFile.toFile());

            // 存储到Map中
            tempCacheFilesByTable.put(tableName, tempCacheFile);
            cacheFileStreamsByTable.put(tableName, cacheFileStream);
            isFirstRecordByTable.put(tableName, true);

            TapLogger.debug(TAG, "Initialized cache file for table {}: {}", tableName, tempCacheFile.toString());
        } catch (IOException e) {
            TapLogger.error(TAG, "Failed to initialize cache file for table {}: {}", tableName, e.getMessage());
            throw new StarrocksRuntimeException("Failed to initialize cache file for table " + tableName, e);
        }
    }

    /**
     * 获取总的批次大小
     */
    private long getTotalBatchSize() {
        return currentBatchSizeByTable.values().stream().mapToLong(Long::longValue).sum();
    }

    /**
     * 获取指定表的批次大小
     */
    private long getTableBatchSize(String tableName) {
        return currentBatchSizeByTable.getOrDefault(tableName, 0L);
    }

    /**
     * 获取指定表的上次刷新时间
     */
    private long getTableLastFlushTime(String tableName) {
        return lastFlushTimeByTable.getOrDefault(tableName, System.currentTimeMillis());
    }

    /**
     * 检查内存使用情况和表数量限制
     */
    private void checkMemoryAndTableLimits(String tableName) {
        // 检查表数量限制
        if (tempCacheFilesByTable.size() >= MAX_CONCURRENT_TABLES) {
            TapLogger.warn(TAG, "Too many concurrent tables ({}), forcing cleanup of oldest tables",
                tempCacheFilesByTable.size());

            // 强制清理一些最老的表（简单策略：清理前10个）
            int cleanupCount = 0;
            for (String table : new HashSet<>(tempCacheFilesByTable.keySet())) {
                if (cleanupCount >= 10) break;
                if (!table.equals(tableName)) {
                    cleanupCacheFileForTable(table);
                    pendingFlushTables.remove(table);
                    cleanupCount++;
                }
            }
        }

        // 定期检查内存使用情况
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastMemoryCheckTime > MEMORY_CHECK_INTERVAL) {
            lastMemoryCheckTime = currentTime;

            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            long maxMemory = runtime.maxMemory();

            double memoryUsagePercent = (double) usedMemory / maxMemory * 100;

            TapLogger.info(TAG, "Memory usage: {}% ({}/{} MB), active tables: {}",
                String.format("%.1f", memoryUsagePercent),
                usedMemory / 1024 / 1024,
                maxMemory / 1024 / 1024,
                tempCacheFilesByTable.size());

            // 如果内存使用率超过80%，强制垃圾回收
            if (memoryUsagePercent > 80) {
                TapLogger.warn(TAG, "High memory usage detected ({}%), forcing garbage collection",
                    String.format("%.1f", memoryUsagePercent));
                System.gc();
            }
        }
    }

    /**
     * 初始化定时刷新调度器
     */
    private void initializeFlushScheduler() {
        flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "StarRocks-Flush-Scheduler-" + Thread.currentThread().getId());
            t.setDaemon(true);
            return t;
        });

        // 启动定时检查任务，每10秒检查一次是否需要刷新
        flushTask = flushScheduler.scheduleWithFixedDelay(() -> {
            try {
                checkAndFlushIfNeeded();
            } catch (Exception e) {
                TapLogger.warn(TAG, "Error in scheduled flush check: {}", e.getMessage());
            }
        }, 10, 10, TimeUnit.SECONDS);

        TapLogger.debug(TAG, "Initialized flush scheduler with 10-second interval");
    }

    /**
     * 检查并在需要时执行刷新 - 检查所有表的未刷新文件
     */
    private void checkAndFlushIfNeeded() {
        synchronized (writeLock) {
            if (pendingFlushTables.isEmpty()) {
                return; // 没有数据需要刷新
            }

            long currentTime = System.currentTimeMillis();
            long flushTimeoutMs = StarrocksConfig.getFlushTimeoutSeconds() * 1000L;
            long flushSizeBytes = StarrocksConfig.getFlushSizeMB() * 1024L * 1024L;

            // 检查每个表是否需要刷新
            Set<String> tablesToFlush = new HashSet<>();

            for (String tableName : new HashSet<>(pendingFlushTables)) {
                long tableLastFlushTime = getTableLastFlushTime(tableName);
                long timeSinceLastFlush = currentTime - tableLastFlushTime;
                long tableSize = getTableBatchSize(tableName);

                boolean timeoutReached = timeSinceLastFlush >= flushTimeoutMs;
                boolean sizeReached = tableSize >= flushSizeBytes;

                if (timeoutReached || sizeReached) {
                    tablesToFlush.add(tableName);
                    String reason = sizeReached ? "size_threshold" : "timeout";
                    TapLogger.info(TAG, "Table {} scheduled flush triggered by {}: table_size={}, " +
                        "waiting_time={} ms, timeout_threshold={} ms",
                        tableName, reason, formatBytes(tableSize), timeSinceLastFlush, flushTimeoutMs);
                }
            }

            // 刷新需要刷新的表
            if (!tablesToFlush.isEmpty()) {
                try {
                    TapLogger.info(TAG, "Scheduled flush: {} tables to flush, total_size={}, {}",
                        tablesToFlush.size(), formatBytes(getTotalBatchSize()), metrics.getCachedInfo());

                    flushSpecificTables(tablesToFlush);
                } catch (Exception e) {
                    TapLogger.error(TAG, "Failed to execute scheduled flush: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * 刷新指定的表
     */
    private void flushSpecificTables(Set<String> tablesToFlush) throws StarrocksRetryableException {
        for (String tableName : tablesToFlush) {
            try {
                // 为每个表创建临时的TapTable对象进行刷新
                if (tapTable != null && tapTable.getId().equals(tableName)) {
                    RespContent respContent = flushTable(tableName, tapTable);
                    if (respContent != null) {
                        TapLogger.debug(TAG, "Successfully flushed table {} during scheduled flush", tableName);
                    }
                } else {
                    TapLogger.warn(TAG, "Cannot flush table {} - no matching TapTable found", tableName);
                }
            } catch (Exception e) {
                TapLogger.error(TAG, "Failed to flush table {} during scheduled flush: {}", tableName, e.getMessage());
            }
        }

        // 清理缓存的 metrics，因为数据已成功刷新
        if (!tablesToFlush.isEmpty()) {
            metrics.clearCache();
            TapLogger.debug(TAG, "Cleared cached metrics after scheduled flush");
        }
    }

    /**
     * 刷新所有待刷新的表
     */
    private void flushAllPendingTables() throws StarrocksRetryableException {
        // 创建待刷新表的副本，避免在迭代过程中修改集合
        Set<String> tablesToFlush = new HashSet<>(pendingFlushTables);

        for (String tableName : tablesToFlush) {
            try {
                // 为每个表创建临时的TapTable对象进行刷新
                if (tapTable != null && tapTable.getId().equals(tableName)) {
                    RespContent respContent = flushTable(tableName, tapTable);
                    if (respContent != null) {
                        TapLogger.debug(TAG, "Successfully flushed table {} during scheduled flush", tableName);
                    }
                } else {
                    TapLogger.warn(TAG, "Cannot flush table {} - no matching TapTable found", tableName);
                }
            } catch (Exception e) {
                TapLogger.error(TAG, "Failed to flush table {} during scheduled flush: {}", tableName, e.getMessage());
            }
        }

        // 清理缓存的 metrics，因为数据已成功刷新
        if (!pendingFlushTables.isEmpty()) {
            metrics.clearCache();
            TapLogger.debug(TAG, "Cleared cached metrics after scheduled flush");
        }
    }

    public void writeRecord(final List<TapRecordEvent> tapRecordEvents, final TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        synchronized (writeLock) {
            try {
                WriteListResult<TapRecordEvent> listResult = writeListResult();
                this.tapTable = table;
                String tableName = table.getId();
                boolean isAgg = StarrocksTableType.Aggregate.toString().equals(StarrocksConfig.getUniqueKeyType());

                // 确保该表有缓存文件
                if (!tempCacheFilesByTable.containsKey(tableName)) {
                    TapLogger.info(TAG, "Initializing cache file for new table: {}", tableName);
                    initializeCacheFileForTable(tableName);
                }

            long batchStartSize = getTotalBatchSize();
            int processedEvents = 0;
            long batchDataSize = 0;

            for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
                byte[] bytes = messageSerializer.serialize(table, tapRecordEvent, isAgg);
                batchDataSize += bytes.length;

                // 检查每分钟写入限制
                if (minuteWriteLimiter.isLimitEnabled() && !minuteWriteLimiter.canWrite(bytes.length)) {
                    long secondsToWait = minuteWriteLimiter.getSecondsToNextMinute();
                    TapLogger.warn(TAG, "Per-minute write limit exceeded. Current minute written: {}, limit: {}. Will wait {} seconds until next minute.",
                        formatBytes(minuteWriteLimiter.getCurrentMinuteWritten()), formatBytes(minuteWriteLimiter.getMinuteLimitBytes()), secondsToWait);

                    // 先刷新当前缓冲的数据
                    if (!pendingFlushTables.isEmpty()) {
                        flush(table);
                    }

                    // 等待到下一分钟
                    try {
                        Thread.sleep(secondsToWait * 1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new StarrocksRuntimeException("Interrupted while waiting for next minute", ie);
                    }
                }

                if (needFlush(tapRecordEvent, bytes.length, isAgg, tableName)) {
                    flushTable(tableName, table);
                }

                // 检查该表是否需要初始化startLoad（每个表第一次处理时都需要）
                boolean needStartLoad = !dataColumnsByTable.containsKey(tableName);

                if (needStartLoad) {
                    startLoad(tapRecordEvent, tableName);
                }
                writeToCacheFile(bytes, tableName);

                // 更新该表的批次大小
                currentBatchSizeByTable.put(tableName, getTableBatchSize(tableName) + bytes.length);

                // 将该表标记为待刷新
                pendingFlushTables.add(tableName);

                // 直接统计到listResult
                if (tapRecordEvent instanceof TapInsertRecordEvent) {
                    listResult.incrementInserted(1);
                } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                    listResult.incrementModified(1);
                } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                    listResult.incrementRemove(1);
                }
                processedEvents++;
            }

            // 检查是否需要打印状态日志（每30秒一次）
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastLogTime >= LOG_INTERVAL_MS) {
                logCurrentStatus(processedEvents, batchDataSize, currentTime);
                lastLogTime = currentTime;
            }

            // 检查是否需要刷新（基于时间或大小阈值）
            if (shouldFlushAfterBatch()) {
                // 刷新时也打印一次状态
                logCurrentStatus(processedEvents, batchDataSize, currentTime);
                lastLogTime = currentTime;
                flushTable(tableName, table);
            }
                writeListResultConsumer.accept(listResult);
            } catch (Throwable e) {
                recordStream.init();
                throw e;
            }
        }
    }

    public void writeRecord(byte[] record) throws IOException {
        if (loadBatchFirstRecord) {
            loadBatchFirstRecord = false;
        } else {
            recordStream.write(messageSerializer.lineEnd());
        }
        recordStream.write(record);
    }

    /**
     * 将数据写入指定表的缓存文件
     */
    private void writeToCacheFile(byte[] data, String tableName) throws IOException {
        try {
            FileOutputStream cacheFileStream = cacheFileStreamsByTable.get(tableName);
            if (cacheFileStream == null) {
                // 缓存文件流不存在，可能是被清理了，重新初始化
                TapLogger.warn(TAG, "Cache file stream not found for table {}, reinitializing...", tableName);
                initializeCacheFileForTable(tableName);
                cacheFileStream = cacheFileStreamsByTable.get(tableName);

                if (cacheFileStream == null) {
                    throw new IOException("Failed to reinitialize cache file stream for table: " + tableName);
                }
            }

            Boolean isFirstRecord = isFirstRecordByTable.get(tableName);
            if (isFirstRecord == null) {
                isFirstRecord = true;
            }

            if (!isFirstRecord) {
                // 写入分隔符
                cacheFileStream.write(messageSerializer.lineEnd());
            } else {
                // 写入批次开始标记
                cacheFileStream.write(messageSerializer.batchStart());
                isFirstRecordByTable.put(tableName, false);
            }
            // 写入实际数据
            cacheFileStream.write(data);
            cacheFileStream.flush(); // 确保数据写入磁盘

            TapLogger.debug(TAG, "Written {} bytes to cache file for table {}", data.length, tableName);
        } catch (IOException e) {
            TapLogger.error(TAG, "Failed to write to cache file for table {}: {}", tableName, e.getMessage());
            throw e;
        }
    }

    public void startLoad(final TapRecordEvent recordEvent, String tableName) throws IOException {
        recordStream.startInput();

        // 确保该表有缓存文件
        if (!tempCacheFilesByTable.containsKey(tableName)) {
            TapLogger.info(TAG, "Initializing cache file for table {} in startLoad", tableName);
            initializeCacheFileForTable(tableName);
        }

        // 为指定表设置dataColumns
        Set<String> newDataColumns = getDataColumns(recordEvent);
        dataColumnsByTable.put(tableName, newDataColumns);

        loadBatchFirstRecord = true;
        isFirstRecordByTable.put(tableName, true); // 重置该表的文件记录标志

        TapLogger.info(TAG, "Started new load batch for table {} with operation: {}, dataColumns: {}",
            tableName, OperationType.getOperationFlag(recordEvent), newDataColumns);
    }

    private Set<String> getDataColumns(TapRecordEvent recordEvent) {
        Set<String> columns = Collections.emptySet();
        String eventType = "unknown";

        if (recordEvent instanceof TapInsertRecordEvent) {
            TapInsertRecordEvent insertEvent = (TapInsertRecordEvent) recordEvent;
            columns = insertEvent.getAfter() != null ? insertEvent.getAfter().keySet() : Collections.emptySet();
            eventType = "INSERT";
        } else if (recordEvent instanceof TapUpdateRecordEvent) {
            TapUpdateRecordEvent updateEvent = (TapUpdateRecordEvent) recordEvent;
            columns = updateEvent.getAfter() != null ? updateEvent.getAfter().keySet() : Collections.emptySet();
            eventType = "UPDATE";
        } else if (recordEvent instanceof TapDeleteRecordEvent) {
            TapDeleteRecordEvent deleteEvent = (TapDeleteRecordEvent) recordEvent;
            columns = deleteEvent.getBefore() != null ? deleteEvent.getBefore().keySet() : Collections.emptySet();
            eventType = "DELETE";
        }

        return columns;
    }

    public RespContent put(final TapTable table) throws StreamLoadException, StarrocksRetryableException {
        StarrocksConfig.WriteFormat writeFormat = StarrocksConfig.getWriteFormatEnum();
        try {
            final String loadUrl = buildLoadUrl(StarrocksConfig.getStarrocksHttp(), StarrocksConfig.getDatabase(), table.getId());
            final String prefix = buildPrefix(table.getId());
            String tableName = table.getId();

            String label = prefix + "-" + UUID.randomUUID();
            List<String> columns = new ArrayList<>();

            // 获取该表的dataColumns
            Set<String> tableDataColumns = dataColumnsByTable.get(tableName);
            if (tableDataColumns == null) {
                tableDataColumns = Collections.emptySet();
            }

            // 打印调试信息 - put方法
            TapLogger.info(TAG, "[PUT] Building columns for table {}: tableDataColumns={}, tapTable.getNameFieldMap().keySet()={}, uniqueKeyType={}",
                tableName, tableDataColumns, tapTable.getNameFieldMap().keySet(), StarrocksConfig.getUniqueKeyType());

            for (String col : tapTable.getNameFieldMap().keySet()) {
                boolean isInDataColumns = tableDataColumns.contains(col);
                boolean isAggregateType = StarrocksTableType.Aggregate.toString().equals(StarrocksConfig.getUniqueKeyType());
                boolean shouldInclude = isInDataColumns || isAggregateType;

                TapLogger.debug(TAG, "[PUT] Column {}: isInDataColumns={}, isAggregateType={}, shouldInclude={}",
                    col, isInDataColumns, isAggregateType, shouldInclude);

                if (shouldInclude) {
                    columns.add("`" + col + "`");
                }
            }

            TapLogger.info(TAG, "[PUT] Final columns list for table {}: {}", tableName, columns);
            // add the Starrocks_DELETE_SIGN at the end of the column
            columns.add(Constants.Starrocks_DELETE_SIGN);
            HttpPutBuilder putBuilder = new HttpPutBuilder();
            InputStreamEntity entity = new InputStreamEntity(recordStream, recordStream.getContentLength());
            entity.setContentType("application/json");
            putBuilder.setUrl(loadUrl)
                    // 前端表单传出来的值和tdd json加载的值可能有差别，如前端传的pwd可能是null，tdd的是空字符串
                    .baseAuth(StarrocksConfig.getUser(), StarrocksConfig.getPassword())
                    .addCommonHeader()
                    .addFormat(writeFormat)
                    .addColumns(columns)
                    .setLabel(label)
                    .setEntity(entity);
            Collection<String> primaryKeys = table.primaryKeys(true);
            if (CollectionUtils.isEmpty(primaryKeys)) {
                putBuilder.enableAppend();
            } else {
                if (StarrocksTableType.Primary.toString().equals(StarrocksConfig.getUniqueKeyType()) || StarrocksTableType.Unique.toString().equals(StarrocksConfig.getUniqueKeyType())) {
                    putBuilder.enableDelete();
                    putBuilder.addPartialHeader();
                } else {
                    putBuilder.enableAppend();
                }
            }
            HttpPut httpPut = putBuilder.build();
            TapLogger.debug(TAG, "Call stream load http api, url: {}, headers: {}", loadUrl, putBuilder.header);
            try (CloseableHttpResponse execute = httpClient.execute(httpPut)) {
                return handlePreCommitResponse(execute);
            }
        } catch (StarrocksRetryableException e) {
            throw e;
        } catch (Exception e) {
            throw new StreamLoadException(String.format("Call stream load error: %s", e.getMessage()), e);
        }
    }

    /**
     * 直接从缓存文件发送数据，模仿 curl 的行为
     */
    public RespContent putFromFile(final TapTable table) throws StreamLoadException, StarrocksRetryableException {
        StarrocksConfig.WriteFormat writeFormat = StarrocksConfig.getWriteFormatEnum();
        try {
            final String loadUrl = buildLoadUrl(StarrocksConfig.getStarrocksHttp(), StarrocksConfig.getDatabase(), table.getId());
            final String prefix = buildPrefix(table.getId());
            String tableName = table.getId();

            String label = prefix + "-" + UUID.randomUUID();
            List<String> columns = new ArrayList<>();

            // 获取该表的dataColumns
            Set<String> tableDataColumns = dataColumnsByTable.get(tableName);
            if (tableDataColumns == null) {
                tableDataColumns = Collections.emptySet();
            }

            // 打印调试信息 - putFromFile方法
            TapLogger.info(TAG, "[PUT_FROM_FILE] Building columns for table {}: tableDataColumns={}, tapTable.getNameFieldMap().keySet()={}, uniqueKeyType={}",
                tableName, tableDataColumns, tapTable.getNameFieldMap().keySet(), StarrocksConfig.getUniqueKeyType());

            for (String col : tapTable.getNameFieldMap().keySet()) {
                boolean isInDataColumns = tableDataColumns.contains(col);
                boolean isAggregateType = StarrocksTableType.Aggregate.toString().equals(StarrocksConfig.getUniqueKeyType());
                boolean shouldInclude = isInDataColumns || isAggregateType;

                TapLogger.debug(TAG, "[PUT_FROM_FILE] Column {}: isInDataColumns={}, isAggregateType={}, shouldInclude={}",
                    col, isInDataColumns, isAggregateType, shouldInclude);

                if (shouldInclude) {
                    columns.add("`" + col + "`");
                }
            }

            TapLogger.info(TAG, "[PUT_FROM_FILE] Columns before adding DELETE_SIGN for table {}: {}", tableName, columns);
            // add the Starrocks_DELETE_SIGN at the end of the column
            columns.add(Constants.Starrocks_DELETE_SIGN);

            TapLogger.info(TAG, "[PUT_FROM_FILE] Final columns list for table {} (with DELETE_SIGN): {}", tableName, columns);

            // 直接从指定表的缓存文件创建 InputStreamEntity，模仿 curl -T 的行为
            Path tableCacheFile = tempCacheFilesByTable.get(tableName);
            if (tableCacheFile == null || !Files.exists(tableCacheFile)) {
                throw new StreamLoadException("Cache file not found for table: " + tableName);
            }

            long fileSize = Files.size(tableCacheFile);
            FileInputStream fileInputStream = new FileInputStream(tableCacheFile.toFile());
            InputStreamEntity entity = new InputStreamEntity(fileInputStream, fileSize);
            entity.setContentType("application/json");

            HttpPutBuilder putBuilder = new HttpPutBuilder();
            putBuilder.setUrl(loadUrl)
                    .baseAuth(StarrocksConfig.getUser(), StarrocksConfig.getPassword())
                    .addCommonHeader()
                    .addFormat(writeFormat)
                    .addColumns(columns)
                    .setLabel(label)
                    .setEntity(entity);

            Collection<String> primaryKeys = table.primaryKeys(true);
            if (CollectionUtils.isEmpty(primaryKeys)) {
                putBuilder.enableAppend();
            } else {
                if (StarrocksTableType.Primary.toString().equals(StarrocksConfig.getUniqueKeyType()) || StarrocksTableType.Unique.toString().equals(StarrocksConfig.getUniqueKeyType())) {
                    putBuilder.enableDelete();
                    putBuilder.addPartialHeader();
                } else {
                    putBuilder.enableAppend();
                }
            }

            HttpPut httpPut = putBuilder.build();
            TapLogger.debug(TAG, "Call stream load http api from file, url: {}, headers: {}, file_size: {}",
                loadUrl, putBuilder.header, formatBytes(fileSize));

            try (CloseableHttpResponse execute = httpClient.execute(httpPut)) {
                return handlePreCommitResponse(execute);
            } finally {
                // 确保文件流被关闭
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    TapLogger.warn(TAG, "Failed to close file input stream: {}", e.getMessage());
                }
            }
        } catch (StarrocksRetryableException e) {
            throw e;
        } catch (Exception e) {
            throw new StreamLoadException(String.format("Call stream load from file error: %s", e.getMessage()), e);
        }
    }

    public RespContent handlePreCommitResponse(CloseableHttpResponse response) throws Exception {
        final int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200 || response.getEntity() == null) {
            throw new StarrocksRetryableException("Stream load error: " + response.getStatusLine().toString());
        }
        String loadResult = EntityUtils.toString(response.getEntity());

        TapLogger.debug(TAG, "Stream load Result {}", loadResult);
        RespContent respContent = OBJECT_MAPPER.readValue(loadResult, RespContent.class);
        if (!respContent.isSuccess() && !"Publish Timeout".equals(respContent.getStatus())) {
            if (respContent.getMessage().toLowerCase().contains("too many filtered rows")
                    || respContent.getMessage().toLowerCase().contains("access denied")) {
                throw new StreamLoadException("Stream load failed | Error: " + loadResult);
            }
            throw new StarrocksRetryableException(loadResult);
        }
        return respContent;
    }

    /**
     * 刷新指定表的数据
     */
    public RespContent flushTable(String tableName, TapTable table) throws StarrocksRetryableException {
        // 检查该表是否有数据需要刷新
        if (!pendingFlushTables.contains(tableName)) {
            return null;
        }

        // 记录刷新开始时间和状态
        long flushStartTime = System.currentTimeMillis();
        long waitTime = flushStartTime - lastFlushTime;
        long tableDataSize = getTableBatchSize(tableName);

        try {
            // 完成该表的缓存文件写入
            finalizeCacheFileForTable(tableName);

            // 记录写入的数据量到每分钟限制器
            if (minuteWriteLimiter.isLimitEnabled()) {
                minuteWriteLimiter.recordWrite(tableDataSize);
                TapLogger.debug(TAG, "Recorded {} bytes to minute limiter. Current minute total: {} bytes",
                    tableDataSize, minuteWriteLimiter.getCurrentMinuteWritten());
            }

            // 直接从文件发送，模仿 curl 的行为
            RespContent respContent = putFromFile(table);

            // 计算刷新耗时
            long flushEndTime = System.currentTimeMillis();
            long flushDuration = flushEndTime - flushStartTime;

            // 记录刷新详细信息
            TapLogger.info(TAG, "Table {} flush completed: flushed_size={}, waiting_time={} ms, " +
                "flush_duration={} ms, response={}",
                tableName, formatBytes(tableDataSize), waitTime, flushDuration, respContent);

            return respContent;
        } catch (StarrocksRetryableException e) {
            long flushEndTime = System.currentTimeMillis();
            long flushDuration = flushEndTime - flushStartTime;
            TapLogger.error(TAG, "Table {} flush failed: flushed_size={}, waiting_time={} ms, " +
                "flush_duration={} ms, error={}",
                tableName, formatBytes(tableDataSize), waitTime, flushDuration, e.getMessage());
            throw e;
        } catch (Exception e) {
            long flushEndTime = System.currentTimeMillis();
            long flushDuration = flushEndTime - flushStartTime;
            TapLogger.error(TAG, "Table {} flush failed: flushed_size={}, waiting_time={} ms, " +
                "flush_duration={} ms, error={}",
                tableName, formatBytes(tableDataSize), waitTime, flushDuration, e.getMessage());
            throw new StarrocksRuntimeException(e);
        } finally {
            // 清理该表的缓存文件
            cleanupCacheFileForTable(tableName);
            // 从待刷新列表中移除该表
            pendingFlushTables.remove(tableName);
            // 清理该表的批次大小
            currentBatchSizeByTable.remove(tableName);
            // 更新该表的刷新时间
            lastFlushTimeByTable.put(tableName, System.currentTimeMillis());

            // 如果所有表都已刷新，重置全局状态
            if (pendingFlushTables.isEmpty()) {
                recordStream.setContentLength(0L);
                lastFlushTime = System.currentTimeMillis();
            }
        }
    }

    public RespContent flush(TapTable table) throws StarrocksRetryableException {
        // 兼容性方法，调用新的flushTable方法
        return flushTable(table.getId(), table);
    }

    /**
     * 在停止时刷新剩余数据 - 刷新所有待刷新的表
     */
    public void flushOnStop() throws StarrocksRetryableException {
        synchronized (writeLock) {
            if (!pendingFlushTables.isEmpty()) {
                TapLogger.info(TAG, "Flushing remaining data on stop: accumulated_size={}, pending_tables={}, {}",
                    formatBytes(getTotalBatchSize()), pendingFlushTables.size(), metrics.getCachedInfo());

                // 刷新所有待刷新的表
                flushAllPendingTables();

                // 清理缓存的 metrics，因为数据已成功刷新
                metrics.clearCache();
                TapLogger.info(TAG, "Cleared cached metrics after stop flush");
            }
        }
    }

    public void shutdown() {
        try {
            TapLogger.info(TAG, "Shutting down StarrocksStreamLoader, active tables: {}", tempCacheFilesByTable.size());

            // 停止定时刷新调度器
            if (flushTask != null) {
                flushTask.cancel(false);
                flushTask = null;
            }
            if (flushScheduler != null) {
                flushScheduler.shutdown();
                try {
                    if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        flushScheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    flushScheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                flushScheduler = null;
            }

            // 关闭HTTP客户端
            if (this.httpClient != null) {
                this.httpClient.close();
            }

            // 清理所有表的缓存文件
            cleanupAllCacheFiles();

            // 清理所有Map，释放内存
            tempCacheFilesByTable.clear();
            cacheFileStreamsByTable.clear();
            isFirstRecordByTable.clear();
            dataColumnsByTable.clear();
            pendingFlushTables.clear();
            currentBatchSizeByTable.clear();
            lastFlushTimeByTable.clear();

            // 强制垃圾回收
            System.gc();

            TapLogger.info(TAG, "StarrocksStreamLoader shutdown completed");
        } catch (Exception e) {
            TapLogger.error(TAG, "Error during shutdown: {}", e.getMessage());
        }
    }

    protected String buildLoadUrl(final String StarrocksHttp, final String database, final String tableName) {
        if (Boolean.TRUE.equals(StarrocksConfig.getUseHTTPS()))
            return String.format(HTTPS_LOAD_URL_PATTERN, StarrocksHttp, database, tableName);
        else
            return String.format(HTTP_LOAD_URL_PATTERN, StarrocksHttp, database, tableName);
    }

    private String buildPrefix(final String tableName) {
        return String.format(LABEL_PREFIX_PATTERN, Thread.currentThread().getId(), tableName);
    }

    /**
     * 格式化字节大小为易读格式
     */
    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
        }
    }

    /**
     * 记录当前状态日志
     */
    private void logCurrentStatus(int processedEvents, long batchDataSize, long currentTime) {
        long waitTime = currentTime - lastFlushTime;
        long flushSizeMB = StarrocksConfig.getFlushSizeMB();
        long flushTimeoutSeconds = StarrocksConfig.getFlushTimeoutSeconds();

        TapLogger.info(TAG, "Status: events_in_batch={}, batch_data_size={}, " +
            "accumulated_buffer_size={}, flush_size_config={} MB, " +
            "flush_timeout_config={} seconds, waiting_time={} ms, {}",
            processedEvents, formatBytes(batchDataSize), formatBytes(getTotalBatchSize()),
            flushSizeMB, flushTimeoutSeconds, waitTime, metrics.getCachedInfo());
    }

    /**
     * 获取当前缓存的 metrics 信息
     */
    public String getCachedMetricsInfo() {
        return metrics.getCachedInfo();
    }

    /**
     * 获取当前缓存的 metrics 总数
     */
    public long getCachedMetricsTotal() {
        return metrics.getCachedTotal();
    }

    /**
     * 完成指定表的缓存文件写入
     */
    private void finalizeCacheFileForTable(String tableName) throws IOException {
        try {
            FileOutputStream cacheFileStream = cacheFileStreamsByTable.get(tableName);
            Path tempCacheFile = tempCacheFilesByTable.get(tableName);

            if (cacheFileStream != null && tempCacheFile != null) {
                // 写入批次结束标记
                cacheFileStream.write(messageSerializer.batchEnd());
                cacheFileStream.flush();
                cacheFileStream.close();

                TapLogger.debug(TAG, "Finalized cache file for table {}: {}, size: {}",
                    tableName, tempCacheFile.toString(), formatBytes(Files.size(tempCacheFile)));
            }
        } catch (IOException e) {
            TapLogger.error(TAG, "Failed to finalize cache file for table {}: {}", tableName, e.getMessage());
            throw e;
        }
    }

    /**
     * 清理指定表的缓存文件
     */
    private void cleanupCacheFileForTable(String tableName) {
        try {
            FileOutputStream cacheFileStream = cacheFileStreamsByTable.get(tableName);
            Path tempCacheFile = tempCacheFilesByTable.get(tableName);

            if (cacheFileStream != null) {
                try {
                    cacheFileStream.close();
                } catch (IOException e) {
                    TapLogger.warn(TAG, "Failed to close cache file stream for table {}: {}", tableName, e.getMessage());
                }
                cacheFileStreamsByTable.remove(tableName);
            }

            if (tempCacheFile != null && Files.exists(tempCacheFile)) {
                try {
                    long fileSize = Files.size(tempCacheFile);
                    Files.deleteIfExists(tempCacheFile);
                    TapLogger.debug(TAG, "Cleaned up cache file for table {}: {}, was {} in size",
                        tableName, tempCacheFile.toString(), formatBytes(fileSize));
                } catch (IOException e) {
                    TapLogger.warn(TAG, "Failed to delete cache file for table {}: {}", tableName, e.getMessage());
                }
                tempCacheFilesByTable.remove(tableName);
            }

            // 清理相关状态
            isFirstRecordByTable.remove(tableName);
            dataColumnsByTable.remove(tableName);
            currentBatchSizeByTable.remove(tableName);
            lastFlushTimeByTable.remove(tableName);
        } catch (Exception e) {
            TapLogger.error(TAG, "Failed to cleanup cache file for table {}: {}", tableName, e.getMessage());
        }
    }

    /**
     * 清理所有表的缓存文件
     */
    private void cleanupAllCacheFiles() {
        // 创建表名列表的副本，避免在迭代过程中修改集合
        Set<String> tableNames = new HashSet<>(tempCacheFilesByTable.keySet());

        for (String tableName : tableNames) {
            cleanupCacheFileForTable(tableName);
        }

        TapLogger.info(TAG, "Cleaned up cache files for {} tables during shutdown", tableNames.size());
    }







    protected boolean needFlush(TapRecordEvent recordEvent, int length, boolean noNeed, String tableName) {
        // 检查该表是否有数据（通过pendingFlushTables判断）
        boolean hasData = pendingFlushTables.contains(tableName);

        // 按表独立检查刷新条件
        long currentTime = System.currentTimeMillis();
        long tableLastFlushTime = getTableLastFlushTime(tableName);
        long timeSinceLastFlush = currentTime - tableLastFlushTime;
        long flushTimeoutMs = StarrocksConfig.getFlushTimeoutSeconds() * 1000L;
        long flushSizeBytes = StarrocksConfig.getFlushSizeMB() * 1024L * 1024L;

        // 检查该表是否达到大小阈值
        long tableCurrentSize = getTableBatchSize(tableName);
        boolean sizeThresholdReached = tableCurrentSize + length >= flushSizeBytes;

        // 检查该表是否达到时间阈值
        boolean timeThresholdReached = timeSinceLastFlush >= flushTimeoutMs;

        // 按表独立判断是否需要刷新
        if (hasData && (sizeThresholdReached || timeThresholdReached)) {
            String reason = sizeThresholdReached ? "size_threshold" : "time_threshold";
            TapLogger.info(TAG, "Table {} flush triggered by {}: table_size={}, size_threshold={}, " +
                "waiting_time={} ms, time_threshold={} ms, total_size={}",
                tableName, reason, formatBytes(tableCurrentSize + length), formatBytes(flushSizeBytes),
                timeSinceLastFlush, flushTimeoutMs, formatBytes(getTotalBatchSize()));

            // 更新日志时间，避免重复打印
            lastLogTime = System.currentTimeMillis();
            return true;
        }

        // 检查原有的刷新逻辑 - 按表检查dataColumns
        Set<String> currentDataColumns = dataColumnsByTable.get(tableName);
        boolean dataColumnsChanged = hasData && currentDataColumns != null &&
            !getDataColumns(recordEvent).equals(currentDataColumns) && !noNeed;
        boolean bufferFull = !recordStream.canWrite(length);

        if (dataColumnsChanged || bufferFull) {
            String reason = dataColumnsChanged ? "data_columns_changed" : "buffer_full";
            TapLogger.info(TAG, "Flush triggered by {}: current_size={}", reason, formatBytes(getTotalBatchSize() + length));

            // 更新日志时间，避免重复打印
            lastLogTime = System.currentTimeMillis();
            return true;
        }

        return false;
    }

    /**
     * 检查是否应该在批次处理后刷新
     * 只有在达到时间阈值时才刷新，大小阈值在单个记录处理时已经检查过了
     */
    private boolean shouldFlushAfterBatch() {
        if (pendingFlushTables.isEmpty()) {
            return false; // 没有数据需要刷新
        }

        long currentTime = System.currentTimeMillis();
        long timeSinceLastFlush = currentTime - lastFlushTime;
        long flushTimeoutMs = StarrocksConfig.getFlushTimeoutSeconds() * 1000L;

        // 只检查时间阈值，因为大小阈值在 needFlush 中已经检查过了
        if (timeSinceLastFlush >= flushTimeoutMs) {
            TapLogger.info(TAG, "Batch flush triggered by time_threshold: " +
                "waiting_time={} ms, time_threshold={} ms, accumulated_size={}",
                timeSinceLastFlush, flushTimeoutMs, formatBytes(getTotalBatchSize()));
            return true;
        }

        TapLogger.debug(TAG, "Batch flush not needed: waiting_time={} ms, time_threshold={} ms, accumulated_size={}",
            timeSinceLastFlush, flushTimeoutMs, formatBytes(getTotalBatchSize()));
        return false;
    }

    private void stopLoad() throws StarrocksRetryableException {
        if (null != tapTable && !pendingFlushTables.isEmpty()) {
            TapLogger.info(TAG, "Flushing remaining cached data on stop: accumulated_size={}",
                formatBytes(getTotalBatchSize()));
            flush(tapTable);
        }
    }

    public enum OperationType {
        INSERT(1, "insert"),
        UPDATE(2, "update"),
        DELETE(3, "delete");

        private final int code;
        private final String desc;

        OperationType(int code, String desc) {
            this.code = code;
            this.desc = desc;
        }

        static int getOperationFlag(TapRecordEvent recordEvent) {
            if (recordEvent instanceof TapInsertRecordEvent) {
                return INSERT.code;
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                return UPDATE.code;
            } else if (recordEvent instanceof TapDeleteRecordEvent) {
                return DELETE.code;
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    private static class Metrics {
        private long insert = 0L;
        private long update = 0L;
        private long delete = 0L;

        // 缓存的 metrics，用于未刷新时的累积
        private long cachedInsert = 0L;
        private long cachedUpdate = 0L;
        private long cachedDelete = 0L;

        public Metrics() {
        }

        public void increase(TapRecordEvent tapRecordEvent) {
            if (tapRecordEvent instanceof TapInsertRecordEvent) {
                insert++;
                cachedInsert++;
            } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                update++;
                cachedUpdate++;
            } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                delete++;
                cachedDelete++;
            }
        }

        public void clear() {
            insert = 0L;
            update = 0L;
            delete = 0L;
        }

        /**
         * 清理缓存的 metrics（在成功刷新后调用）
         */
        public void clearCache() {
            cachedInsert = 0L;
            cachedUpdate = 0L;
            cachedDelete = 0L;
        }

        /**
         * 获取当前缓存的 metrics 总数
         */
        public long getCachedTotal() {
            return cachedInsert + cachedUpdate + cachedDelete;
        }

        /**
         * 获取缓存的详细信息
         */
        public String getCachedInfo() {
            return String.format("cached[insert=%d, update=%d, delete=%d, total=%d]",
                cachedInsert, cachedUpdate, cachedDelete, getCachedTotal());
        }

        public void writeIntoResultList(WriteListResult<TapRecordEvent> listResult) {
            listResult.incrementInserted(insert);
            listResult.incrementModified(update);
            listResult.incrementRemove(delete);
        }

        /**
         * 创建一个包含当前 metrics 的 WriteListResult
         */
        public WriteListResult<TapRecordEvent> createResultList() {
            WriteListResult<TapRecordEvent> result = new WriteListResult<>();
            writeIntoResultList(result);
            return result;
        }
    }
}
