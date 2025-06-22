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
    private AtomicInteger lastEventFlag;
    private final AtomicReference<Set<String>> dataColumns;
    private MessageSerializer messageSerializer;
    private TapTable tapTable;
    private final Metrics metrics;

    // 新增字段：时间和大小控制
    private long lastFlushTime;
    private long currentBatchSize;
    private final MinuteWriteLimiter minuteWriteLimiter;

    // 文件缓存相关字段
    private Path tempCacheFile;
    private FileOutputStream cacheFileStream;
    private boolean isFirstRecord = true;

    // 日志打印控制
    private long lastLogTime;
    private static final long LOG_INTERVAL_MS = 30 * 1000; // 30秒

    // 线程安全和定时刷新
    private final Object writeLock = new Object();
    private ScheduledExecutorService flushScheduler;
    private ScheduledFuture<?> flushTask;

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
        this.lastEventFlag = new AtomicInteger(0);
        this.dataColumns = new AtomicReference<>();
        initMessageSerializer();
        this.metrics = new Metrics();

        // 初始化新的字段
        this.lastFlushTime = System.currentTimeMillis();
        this.currentBatchSize = 0;
        this.minuteWriteLimiter = new MinuteWriteLimiter(StarrocksConfig.getMinuteLimitMB());
        this.lastLogTime = System.currentTimeMillis();

        // 初始化文件缓存
        initializeCacheFile();

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
     * 初始化缓存文件
     */
    private void initializeCacheFile() {
        try {
            // 创建临时目录
            Path tempDir = Paths.get(System.getProperty("java.io.tmpdir"), "starrocks-cache");
            if (!Files.exists(tempDir)) {
                Files.createDirectories(tempDir);
            }

            // 创建临时缓存文件
            String fileName = String.format("starrocks-cache-%d-%d.tmp",
                Thread.currentThread().getId(), System.currentTimeMillis());
            tempCacheFile = tempDir.resolve(fileName);

            // 初始化文件输出流
            cacheFileStream = new FileOutputStream(tempCacheFile.toFile());

            //TapLogger.info(TAG, "Initialized cache file: {}", tempCacheFile.toString());
        } catch (IOException e) {
            TapLogger.error(TAG, "Failed to initialize cache file: {}", e.getMessage());
            throw new StarrocksRuntimeException("Failed to initialize cache file", e);
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
     * 检查并在需要时执行刷新
     */
    private void checkAndFlushIfNeeded() {
        synchronized (writeLock) {
            if (lastEventFlag.get() == 0) {
                return; // 没有数据需要刷新
            }

            long currentTime = System.currentTimeMillis();
            long timeSinceLastFlush = currentTime - lastFlushTime;
            long flushTimeoutMs = StarrocksConfig.getFlushTimeoutSeconds() * 1000L;

            if (timeSinceLastFlush >= flushTimeoutMs) {
                try {
                    TapLogger.info(TAG, "Scheduled flush triggered by timeout: waiting_time={} ms, " +
                        "timeout_threshold={} ms, accumulated_size={}",
                        timeSinceLastFlush, flushTimeoutMs, formatBytes(currentBatchSize));

                    flush(tapTable);
                } catch (Exception e) {
                    TapLogger.error(TAG, "Failed to execute scheduled flush: {}", e.getMessage());
                }
            }
        }
    }

    public void writeRecord(final List<TapRecordEvent> tapRecordEvents, final TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        synchronized (writeLock) {
            try {
                TapLogger.debug(TAG, "Batch events length is: {}", tapRecordEvents.size());
                WriteListResult<TapRecordEvent> listResult = writeListResult();
                this.tapTable = table;
                boolean isAgg = StarrocksTableType.Aggregate.toString().equals(StarrocksConfig.getUniqueKeyType());

            long batchStartSize = currentBatchSize;
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
                    if (lastEventFlag.get() != 0) {
                        flush(table, listResult);
                    }

                    // 等待到下一分钟
                    try {
                        Thread.sleep(secondsToWait * 1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new StarrocksRuntimeException("Interrupted while waiting for next minute", ie);
                    }
                }

                if (needFlush(tapRecordEvent, bytes.length, isAgg)) {
                    flush(table, listResult);
                }
                if (lastEventFlag.get() == 0) {
                    startLoad(tapRecordEvent);
                }
                writeToCacheFile(bytes);
                currentBatchSize += bytes.length;
                metrics.increase(tapRecordEvent);
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
                flush(table, listResult);
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
     * 将数据写入缓存文件
     */
    private void writeToCacheFile(byte[] data) throws IOException {
        try {
            if (!isFirstRecord) {
                // 写入分隔符
                cacheFileStream.write(messageSerializer.lineEnd());
            } else {
                // 写入批次开始标记
                cacheFileStream.write(messageSerializer.batchStart());
                isFirstRecord = false;
            }
            // 写入实际数据
            cacheFileStream.write(data);
            cacheFileStream.flush(); // 确保数据写入磁盘

            TapLogger.debug(TAG, "Written {} bytes to cache file", data.length);
        } catch (IOException e) {
            TapLogger.error(TAG, "Failed to write to cache file: {}", e.getMessage());
            throw e;
        }
    }

    public void startLoad(final TapRecordEvent recordEvent) throws IOException {
        recordStream.startInput();
        lastEventFlag.set(OperationType.getOperationFlag(recordEvent));
        dataColumns.set(getDataColumns(recordEvent));
        loadBatchFirstRecord = true;
        isFirstRecord = true; // 重置文件记录标志

        TapLogger.debug(TAG, "Started new load batch for operation: {}",
            OperationType.getOperationFlag(recordEvent));
    }

    private Set<String> getDataColumns(TapRecordEvent recordEvent) {
        if (recordEvent instanceof TapInsertRecordEvent) {
            return ((TapInsertRecordEvent) recordEvent).getAfter().keySet();
        } else if (recordEvent instanceof TapUpdateRecordEvent) {
            return ((TapUpdateRecordEvent) recordEvent).getAfter().keySet();
        } else if (recordEvent instanceof TapDeleteRecordEvent) {
            return ((TapDeleteRecordEvent) recordEvent).getBefore().keySet();
        }
        return Collections.emptySet();
    }

    public RespContent put(final TapTable table) throws StreamLoadException, StarrocksRetryableException {
        StarrocksConfig.WriteFormat writeFormat = StarrocksConfig.getWriteFormatEnum();
        try {
            final String loadUrl = buildLoadUrl(StarrocksConfig.getStarrocksHttp(), StarrocksConfig.getDatabase(), table.getId());
            final String prefix = buildPrefix(table.getId());

            String label = prefix + "-" + UUID.randomUUID();
            List<String> columns = new ArrayList<>();
            for (String col : tapTable.getNameFieldMap().keySet()) {
                if (dataColumns.get().contains(col) || StarrocksTableType.Aggregate.toString().equals(StarrocksConfig.getUniqueKeyType())) {
                    columns.add("`" + col + "`");
                }
            }
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

            String label = prefix + "-" + UUID.randomUUID();
            List<String> columns = new ArrayList<>();
            for (String col : tapTable.getNameFieldMap().keySet()) {
                if (dataColumns.get().contains(col) || StarrocksTableType.Aggregate.toString().equals(StarrocksConfig.getUniqueKeyType())) {
                    columns.add("`" + col + "`");
                }
            }
            // add the Starrocks_DELETE_SIGN at the end of the column
            columns.add(Constants.Starrocks_DELETE_SIGN);

            // 直接从文件创建 InputStreamEntity，模仿 curl -T 的行为
            long fileSize = Files.size(tempCacheFile);
            FileInputStream fileInputStream = new FileInputStream(tempCacheFile.toFile());
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

    public RespContent flush(TapTable table) throws StarrocksRetryableException {
        return flush(table, null);
    }

    public RespContent flush(TapTable table, WriteListResult<TapRecordEvent> listResult) throws StarrocksRetryableException {
        // the stream is not started yet, no response to get
        if (lastEventFlag.get() == 0) {
            return null;
        }

        // 记录刷新开始时间和状态
        long flushStartTime = System.currentTimeMillis();
        long waitTime = flushStartTime - lastFlushTime;
        long flushDataSize = currentBatchSize;

        try {
            // 完成缓存文件写入
            finalizeCacheFile();

            // 记录写入的数据量到每分钟限制器
            if (minuteWriteLimiter.isLimitEnabled()) {
                minuteWriteLimiter.recordWrite(currentBatchSize);
                TapLogger.debug(TAG, "Recorded {} bytes to minute limiter. Current minute total: {} bytes",
                    currentBatchSize, minuteWriteLimiter.getCurrentMinuteWritten());
            }

            // 直接从文件发送，模仿 curl 的行为
            RespContent respContent = putFromFile(table);

            // 计算刷新耗时
            long flushEndTime = System.currentTimeMillis();
            long flushDuration = flushEndTime - flushStartTime;

            // 记录刷新详细信息
            TapLogger.info(TAG, "Flush completed: flushed_size={}, waiting_time={} ms, " +
                "flush_duration={} ms, response={}",
                formatBytes(flushDataSize), waitTime, flushDuration, respContent);

            if (null != listResult) {
                metrics.writeIntoResultList(listResult);
                metrics.clear();
            }
            return respContent;
        } catch (StarrocksRetryableException e) {
            long flushEndTime = System.currentTimeMillis();
            long flushDuration = flushEndTime - flushStartTime;
            TapLogger.error(TAG, "Flush failed: flushed_size={}, waiting_time={} ms, " +
                "flush_duration={} ms, error={}",
                formatBytes(flushDataSize), waitTime, flushDuration, e.getMessage());
            throw e;
        } catch (Exception e) {
            long flushEndTime = System.currentTimeMillis();
            long flushDuration = flushEndTime - flushStartTime;
            TapLogger.error(TAG, "Flush failed: flushed_size={}, waiting_time={} ms, " +
                "flush_duration={} ms, error={}",
                formatBytes(flushDataSize), waitTime, flushDuration, e.getMessage());
            throw new StarrocksRuntimeException(e);
        } finally {
            lastEventFlag.set(0);
            recordStream.setContentLength(0L);
            // 重置批次大小和刷新时间
            currentBatchSize = 0;
            lastFlushTime = System.currentTimeMillis();
            // 清理缓存文件
            cleanupCacheFile();
        }
    }

    /**
     * 在停止时刷新剩余数据
     */
    public void flushOnStop() throws StarrocksRetryableException {
        synchronized (writeLock) {
            if (lastEventFlag.get() > 0 && tapTable != null) {
                TapLogger.info(TAG, "Flushing remaining data on stop: accumulated_size={}",
                    formatBytes(currentBatchSize));
                flush(tapTable);
            }
        }
    }

    public void shutdown() {
        try {
//            this.stopLoad();

            // 停止定时刷新调度器
            if (flushTask != null) {
                flushTask.cancel(false);
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
            }

            this.httpClient.close();

            // 清理缓存文件
            if (cacheFileStream != null) {
                try {
                    cacheFileStream.close();
                } catch (IOException e) {
                    TapLogger.warn(TAG, "Failed to close cache file stream during shutdown: {}", e.getMessage());
                }
            }

            if (tempCacheFile != null && Files.exists(tempCacheFile)) {
                try {
                    Files.deleteIfExists(tempCacheFile);
                    TapLogger.info(TAG, "Cleaned up cache file during shutdown: {}", tempCacheFile.toString());
                } catch (IOException e) {
                    TapLogger.warn(TAG, "Failed to delete cache file during shutdown: {}", e.getMessage());
                }
            }
        } catch (Exception ignored) {
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
            "flush_timeout_config={} seconds, waiting_time={} ms",
            processedEvents, formatBytes(batchDataSize), formatBytes(currentBatchSize),
            flushSizeMB, flushTimeoutSeconds, waitTime);
    }

    /**
     * 完成缓存文件写入
     */
    private void finalizeCacheFile() throws IOException {
        try {
            if (cacheFileStream != null) {
                // 写入批次结束标记
                cacheFileStream.write(messageSerializer.batchEnd());
                cacheFileStream.flush();
                cacheFileStream.close();

                TapLogger.debug(TAG, "Finalized cache file: {}, size: {}",
                    tempCacheFile.toString(), formatBytes(Files.size(tempCacheFile)));
            }
        } catch (IOException e) {
            TapLogger.error(TAG, "Failed to finalize cache file: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * 从缓存文件加载数据到 recordStream
     */
    private void loadCacheFileToStream() throws IOException {
        try {
            if (tempCacheFile != null && Files.exists(tempCacheFile)) {
                recordStream.startInput();

                // 分块读取文件内容并写入到 recordStream，避免一次性加载大文件导致内存问题
                long fileSize = Files.size(tempCacheFile);
                recordStream.setContentLength(fileSize);

                // 使用较小的缓冲区分块读取，避免阻塞
                int bufferSize = (int) (Constants.CACHE_BUFFER_SIZE / 1.5);
                byte[] buffer = new byte[bufferSize];

                try (FileInputStream fis = new FileInputStream(tempCacheFile.toFile())) {
                    int bytesRead;
                    long totalRead = 0;
                    while ((bytesRead = fis.read(buffer)) != -1) {
                        if (bytesRead == buffer.length) {
                            recordStream.write(buffer);
                        } else {
                            // 最后一块可能不满，需要创建正确大小的数组
                            byte[] lastChunk = new byte[bytesRead];
                            System.arraycopy(buffer, 0, lastChunk, 0, bytesRead);
                            recordStream.write(lastChunk);
                        }
                        totalRead += bytesRead;
                    }

                    TapLogger.debug(TAG, "Loaded cache file to stream in chunks: {}, total_size={}, chunks_size={}, total_read={}",
                        tempCacheFile.toString(), formatBytes(fileSize), formatBytes(bufferSize), formatBytes(totalRead));
                }

                recordStream.endInput();
            }
        } catch (IOException e) {
            TapLogger.error(TAG, "Failed to load cache file to stream: {}", e.getMessage());
            throw e;
        }
    }

    /**
     * 清理缓存文件
     */
    private void cleanupCacheFile() {
        try {
            if (cacheFileStream != null) {
                try {
                    cacheFileStream.close();
                } catch (IOException e) {
                    TapLogger.warn(TAG, "Failed to close cache file stream: {}", e.getMessage());
                }
                cacheFileStream = null;
            }

            if (tempCacheFile != null && Files.exists(tempCacheFile)) {
                long fileSize = Files.size(tempCacheFile);
                Files.deleteIfExists(tempCacheFile);
                TapLogger.debug(TAG, "Cleaned up cache file: {}, was {} in size",
                    tempCacheFile.toString(), formatBytes(fileSize));
            }

            // 重新初始化缓存文件以备下次使用
            initializeCacheFile();
        } catch (IOException e) {
            TapLogger.error(TAG, "Failed to cleanup cache file: {}", e.getMessage());
        }
    }

    protected boolean needFlush(TapRecordEvent recordEvent, int length, boolean noNeed) {
        int lastEventType = lastEventFlag.get();

        // 优先检查新的配置：大小和时间阈值
        long currentTime = System.currentTimeMillis();
        long timeSinceLastFlush = currentTime - lastFlushTime;
        long flushTimeoutMs = StarrocksConfig.getFlushTimeoutSeconds() * 1000L;
        long flushSizeBytes = StarrocksConfig.getFlushSizeMB() * 1024L * 1024L;

        // 检查是否达到大小阈值
        boolean sizeThresholdReached = currentBatchSize + length >= flushSizeBytes;

        // 检查是否达到时间阈值
        boolean timeThresholdReached = timeSinceLastFlush >= flushTimeoutMs;

        // 新配置的优先级高于通用配置
        if (lastEventType > 0 && (sizeThresholdReached || timeThresholdReached)) {
            String reason = sizeThresholdReached ? "size_threshold" : "time_threshold";
            TapLogger.info(TAG, "Flush triggered by {}: current_size={}, size_threshold={}, " +
                "waiting_time={} ms, time_threshold={} ms",
                reason, formatBytes(currentBatchSize + length), formatBytes(flushSizeBytes),
                timeSinceLastFlush, flushTimeoutMs);

            // 更新日志时间，避免重复打印
            lastLogTime = System.currentTimeMillis();
            return true;
        }

        // 检查原有的刷新逻辑
        boolean dataColumnsChanged = lastEventType > 0 && !getDataColumns(recordEvent).equals(dataColumns.get()) && !noNeed;
        boolean bufferFull = !recordStream.canWrite(length);

        if (dataColumnsChanged || bufferFull) {
            String reason = dataColumnsChanged ? "data_columns_changed" : "buffer_full";
            TapLogger.info(TAG, "Flush triggered by {}: current_size={}", reason, formatBytes(currentBatchSize + length));

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
        if (lastEventFlag.get() == 0) {
            return false; // 没有数据需要刷新
        }

        long currentTime = System.currentTimeMillis();
        long timeSinceLastFlush = currentTime - lastFlushTime;
        long flushTimeoutMs = StarrocksConfig.getFlushTimeoutSeconds() * 1000L;

        // 只检查时间阈值，因为大小阈值在 needFlush 中已经检查过了
        if (timeSinceLastFlush >= flushTimeoutMs) {
            TapLogger.info(TAG, "Batch flush triggered by time_threshold: " +
                "waiting_time={} ms, time_threshold={} ms, accumulated_size={}",
                timeSinceLastFlush, flushTimeoutMs, formatBytes(currentBatchSize));
            return true;
        }

        TapLogger.debug(TAG, "Batch flush not needed: waiting_time={} ms, time_threshold={} ms, accumulated_size={}",
            timeSinceLastFlush, flushTimeoutMs, formatBytes(currentBatchSize));
        return false;
    }

    private void stopLoad() throws StarrocksRetryableException {
        if (null != tapTable && lastEventFlag.get() > 0) {
            TapLogger.info(TAG, "Flushing remaining cached data on stop: accumulated_size={}",
                formatBytes(currentBatchSize));
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

        public Metrics() {
        }

        public void increase(TapRecordEvent tapRecordEvent) {
            if (tapRecordEvent instanceof TapInsertRecordEvent) {
                insert++;
            } else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
                update++;
            } else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
                delete++;
            }
        }

        public void clear() {
            insert = 0L;
            update = 0L;
            delete = 0L;
        }

        public void writeIntoResultList(WriteListResult<TapRecordEvent> listResult) {
            listResult.incrementInserted(insert);
            listResult.incrementModified(update);
            listResult.incrementRemove(delete);
        }
    }
}
