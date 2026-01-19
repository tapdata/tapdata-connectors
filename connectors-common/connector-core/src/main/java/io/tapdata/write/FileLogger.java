package io.tapdata.write;

import io.tapdata.entity.logger.TapLogger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

/**
 * High-performance file logger for string messages with multi-threaded write support.
 * Uses a lock-free queue and dedicated writer thread for optimal performance.
 * Automatically adds timestamps to each log line.
 *
 * <p>Example usage:
 * <pre>
 * // Initialize in connector's onStart method
 * FileLogger logger = FileLogger.builder()
 *     .logDirectory("logs/connector")
 *     .logFilePrefix("my-connector")
 *     .build();
 *
 * // Write logs from any thread
 * logger.write("Processing record: " + recordId);
 * logger.write("Batch completed, count: " + count);
 *
 * // Close in connector's onStop method
 * logger.close();
 * </pre>
 *
 * @author TapData
 */
public class FileLogger implements AutoCloseable {
    private static final String TAG = FileLogger.class.getSimpleName();

    // Configuration constants
    private static final int DEFAULT_QUEUE_CAPACITY = 100000;
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final int DEFAULT_FLUSH_INTERVAL_MS = 1000;
    private static final int DEFAULT_MAX_FILE_SIZE_MB = 100;
    private static final String LOG_FILE_EXTENSION = ".log";
    private static final String COMPRESSED_EXTENSION = ".gz";
    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final long DEFAULT_COMPRESS_INTERVAL_MS = 24 * 60 * 60 * 1000L; // 24 hours
    private static final int DEFAULT_RETAIN_DAYS = 7; // Keep compressed files for 7 days

    // Instance fields
    private final String logDirectory;
    private final String logFilePrefix;
    private final int queueCapacity;
    private final int batchSize;
    private final int flushIntervalMs;
    private final long maxFileSizeBytes;
    private final boolean autoTimestamp;
    private final boolean enableCompression;
    private final long compressIntervalMs;
    private final int retainDays;

    // Runtime state
    private final BlockingQueue<LogEntry> logQueue;
    private final AtomicBoolean running;
    private final AtomicBoolean closed;
    private final AtomicLong totalLinesLogged;
    private final AtomicLong totalLinesDropped;
    private final AtomicLong totalFilesCompressed;
    private final Thread writerThread;
    private final ScheduledExecutorService compressExecutor;
    private final ThreadLocal<SimpleDateFormat> dateFormat;

    // File handling
    private volatile BufferedWriter currentWriter;
    private volatile Path currentLogFile;
    private volatile long currentFileSize;
    private volatile int fileSequence;

    /**
     * Log entry wrapper for queue
     */
    private static class LogEntry {
        final String message;
        final long timestamp;

        LogEntry(String message) {
            this.message = message;
            this.timestamp = System.currentTimeMillis();
        }
    }

    /**
     * Builder for FileLogger
     */
    public static class Builder {
        private String logDirectory = "logs";
        private String logFilePrefix = "app";
        private int queueCapacity = DEFAULT_QUEUE_CAPACITY;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private int flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS;
        private int maxFileSizeMB = DEFAULT_MAX_FILE_SIZE_MB;
        private boolean autoTimestamp = true;
        private boolean enableCompression = false;
        private long compressIntervalMs = DEFAULT_COMPRESS_INTERVAL_MS;
        private int retainDays = DEFAULT_RETAIN_DAYS;

        public Builder logDirectory(String logDirectory) {
            this.logDirectory = logDirectory;
            return this;
        }

        public Builder logFilePrefix(String logFilePrefix) {
            this.logFilePrefix = logFilePrefix;
            return this;
        }

        public Builder queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder flushIntervalMs(int flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
            return this;
        }

        public Builder maxFileSizeMB(int maxFileSizeMB) {
            this.maxFileSizeMB = maxFileSizeMB;
            return this;
        }

        public Builder autoTimestamp(boolean autoTimestamp) {
            this.autoTimestamp = autoTimestamp;
            return this;
        }

        /**
         * Enable automatic compression of old log files
         * @param enable true to enable compression
         * @return this builder
         */
        public Builder enableCompression(boolean enable) {
            this.enableCompression = enable;
            return this;
        }

        /**
         * Set compression interval in milliseconds
         * @param intervalMs interval in milliseconds (default: 24 hours)
         * @return this builder
         */
        public Builder compressIntervalMs(long intervalMs) {
            this.compressIntervalMs = intervalMs;
            return this;
        }

        /**
         * Set compression interval in hours
         * @param hours interval in hours
         * @return this builder
         */
        public Builder compressIntervalHours(int hours) {
            this.compressIntervalMs = hours * 60 * 60 * 1000L;
            return this;
        }

        /**
         * Set how many days to retain compressed files
         * @param days number of days to retain (default: 7)
         * @return this builder
         */
        public Builder retainDays(int days) {
            this.retainDays = days;
            return this;
        }

        public FileLogger build() throws IOException {
            return new FileLogger(this);
        }
    }

    /**
     * Create a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Private constructor - use Builder
     */
    private FileLogger(Builder builder) throws IOException {
        this.logDirectory = builder.logDirectory;
        this.logFilePrefix = builder.logFilePrefix;
        this.queueCapacity = builder.queueCapacity;
        this.batchSize = builder.batchSize;
        this.flushIntervalMs = builder.flushIntervalMs;
        this.maxFileSizeBytes = builder.maxFileSizeMB * 1024L * 1024L;
        this.autoTimestamp = builder.autoTimestamp;
        this.enableCompression = builder.enableCompression;
        this.compressIntervalMs = builder.compressIntervalMs;
        this.retainDays = builder.retainDays;

        this.logQueue = new LinkedBlockingQueue<>(queueCapacity);
        this.running = new AtomicBoolean(false);
        this.closed = new AtomicBoolean(false);
        this.totalLinesLogged = new AtomicLong(0);
        this.totalLinesDropped = new AtomicLong(0);
        this.totalFilesCompressed = new AtomicLong(0);
        this.fileSequence = 0;

        this.dateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat(DATE_PATTERN));

        // Create log directory
        Path logDir = Paths.get(logDirectory);
        if (!Files.exists(logDir)) {
            Files.createDirectories(logDir);
        }

        // Initialize first log file
        rotateLogFile();

        // Start writer thread
        this.writerThread = new Thread(this::writerLoop, "FileLogger-Writer-" + logFilePrefix);
        this.writerThread.setDaemon(true);
        this.running.set(true);
        this.writerThread.start();

        // Start compression scheduler if enabled
        if (enableCompression) {
            this.compressExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "FileLogger-Compressor-" + logFilePrefix);
                t.setDaemon(true);
                return t;
            });
            this.compressExecutor.scheduleAtFixedRate(
                this::compressOldLogs,
                compressIntervalMs,
                compressIntervalMs,
                TimeUnit.MILLISECONDS
            );
            TapLogger.info(TAG, "FileLogger compression enabled: interval={}ms, retainDays={}",
                compressIntervalMs, retainDays);
        } else {
            this.compressExecutor = null;
        }

        TapLogger.info(TAG, "FileLogger initialized: directory={}, prefix={}, queueCapacity={}, batchSize={}, flushIntervalMs={}, maxFileSizeMB={}, compression={}",
            logDirectory, logFilePrefix, queueCapacity, batchSize, flushIntervalMs, builder.maxFileSizeMB, enableCompression);
    }

    /**
     * Write a log message (blocking until queued, guaranteed no data loss)
     *
     * @param message the message to log
     * @return true if message was queued, false if logger is closed or message is null
     */
    public boolean write(String message) {
        if (closed.get()) {
            return false;
        }

        if (message == null) {
            return false;
        }

        try {
            // Use put() instead of offer() to block until space is available
            // This guarantees no data loss
            logQueue.put(new LogEntry(message));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            TapLogger.warn(TAG, "Interrupted while writing log message");
            return false;
        }
    }

    /**
     * Write a log message (blocking with timeout, guaranteed no data loss unless timeout)
     *
     * @param message the message to log
     * @param timeoutMs timeout in milliseconds (0 means wait forever)
     * @return true if message was queued, false if timeout, logger is closed, or message is null
     */
    public boolean write(String message, long timeoutMs) {
        if (closed.get()) {
            return false;
        }

        if (message == null) {
            return false;
        }

        try {
            if (timeoutMs <= 0) {
                // Wait forever - guaranteed no data loss
                logQueue.put(new LogEntry(message));
                return true;
            } else {
                // Wait with timeout
                boolean offered = logQueue.offer(new LogEntry(message), timeoutMs, TimeUnit.MILLISECONDS);
                if (!offered) {
                    totalLinesDropped.incrementAndGet();
                    TapLogger.warn(TAG, "Failed to write log message within {}ms, message dropped", timeoutMs);
                }
                return offered;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            TapLogger.warn(TAG, "Interrupted while writing log message");
            return false;
        }
    }

    /**
     * Write a formatted log message (blocking until queued, guaranteed no data loss)
     *
     * @param format format string
     * @param args arguments
     * @return true if message was queued, false if logger is closed or format error
     */
    public boolean writef(String format, Object... args) {
        try {
            String message = String.format(format, args);
            return write(message);
        } catch (Exception e) {
            TapLogger.warn(TAG, "Error formatting log message: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Writer thread main loop
     */
    private void writerLoop() {
        TapLogger.info(TAG, "FileLogger writer thread started");

        long lastFlushTime = System.currentTimeMillis();
        int batchCount = 0;

        try {
            while (running.get() || !logQueue.isEmpty()) {
                try {
                    // Poll with timeout to allow periodic flushing
                    LogEntry entry = logQueue.poll(flushIntervalMs, TimeUnit.MILLISECONDS);

                    if (entry != null) {
                        writeLogEntry(entry);
                        batchCount++;
                        totalLinesLogged.incrementAndGet();
                    }

                    // Flush conditions: batch size reached or flush interval elapsed
                    long now = System.currentTimeMillis();
                    boolean shouldFlush = batchCount >= batchSize ||
                                         (now - lastFlushTime >= flushIntervalMs && batchCount > 0);

                    if (shouldFlush) {
                        flush();
                        lastFlushTime = now;
                        batchCount = 0;
                    }

                    // Check if file rotation is needed
                    if (currentFileSize >= maxFileSizeBytes) {
                        rotateLogFile();
                    }

                } catch (InterruptedException e) {
                    if (!running.get()) {
                        break;
                    }
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    TapLogger.error(TAG, "Error in writer loop: {}", e.getMessage(), e);
                }
            }

            // Final flush
            flush();

        } catch (Exception e) {
            TapLogger.error(TAG, "Fatal error in writer thread: {}", e.getMessage(), e);
        } finally {
            TapLogger.info(TAG, "FileLogger writer thread stopped. Total lines logged: {}, dropped: {}",
                totalLinesLogged.get(), totalLinesDropped.get());
        }
    }

    /**
     * Write a single log entry to file
     */
    private void writeLogEntry(LogEntry entry) throws IOException {
        if (currentWriter == null) {
            return;
        }

        StringBuilder line = new StringBuilder();

        // Add timestamp if enabled
        if (autoTimestamp) {
            String timestamp = dateFormat.get().format(new Date(entry.timestamp));
            line.append(timestamp).append(" | ");
        }

        // Add message
        line.append(entry.message);

        // Write line
        String lineStr = line.toString();
        currentWriter.write(lineStr);
        currentWriter.newLine();

        // Update file size (approximate)
        currentFileSize += lineStr.getBytes(StandardCharsets.UTF_8).length + 1; // +1 for newline
    }

    /**
     * Flush buffered data to disk
     */
    private void flush() {
        try {
            if (currentWriter != null) {
                currentWriter.flush();
            }
        } catch (IOException e) {
            TapLogger.error(TAG, "Error flushing log file: {}", e.getMessage(), e);
        }
    }

    /**
     * Force flush all buffered data to disk
     */
    public void forceFlush() {
        flush();
    }

    /**
     * Compress old log files and delete expired compressed files
     * This method is called periodically by the compression scheduler
     */
    private void compressOldLogs() {
        compressOldLogs(false);
    }

    /**
     * Compress old log files and delete expired compressed files
     * @param forceAll if true, compress all non-current log files regardless of age
     */
    private void compressOldLogs(boolean forceAll) {
        try {
            Path logDir = Paths.get(logDirectory);
            if (!Files.exists(logDir)) {
                return;
            }

            long now = System.currentTimeMillis();
            long compressThreshold = now - compressIntervalMs;
            long deleteThreshold = now - (retainDays * 24L * 60L * 60L * 1000L);

            try (Stream<Path> files = Files.list(logDir)) {
                files.filter(path -> {
                    String fileName = path.getFileName().toString();
                    // Only process log files with our prefix
                    return fileName.startsWith(logFilePrefix) &&
                           fileName.endsWith(LOG_FILE_EXTENSION) &&
                           !path.equals(currentLogFile); // Don't compress current file
                })
                .forEach(logFile -> {
                    try {
                        long lastModified = Files.getLastModifiedTime(logFile).toMillis();

                        // Compress old log files
                        if (forceAll || lastModified < compressThreshold) {
                            compressLogFile(logFile);
                        }
                    } catch (Exception e) {
                        TapLogger.error(TAG, "Error processing log file {}: {}",
                            logFile.getFileName(), e.getMessage(), e);
                    }
                });
            }

            // Delete expired compressed files
            try (Stream<Path> files = Files.list(logDir)) {
                files.filter(path -> {
                    String fileName = path.getFileName().toString();
                    return fileName.startsWith(logFilePrefix) &&
                           fileName.endsWith(COMPRESSED_EXTENSION);
                })
                .forEach(compressedFile -> {
                    try {
                        long lastModified = Files.getLastModifiedTime(compressedFile).toMillis();

                        if (lastModified < deleteThreshold) {
                            Files.delete(compressedFile);
                            TapLogger.info(TAG, "Deleted expired compressed file: {}",
                                compressedFile.getFileName());
                        }
                    } catch (Exception e) {
                        TapLogger.error(TAG, "Error deleting compressed file {}: {}",
                            compressedFile.getFileName(), e.getMessage(), e);
                    }
                });
            }

        } catch (Exception e) {
            TapLogger.error(TAG, "Error in compressOldLogs: {}", e.getMessage(), e);
        }
    }

    /**
     * Compress a single log file using GZIP
     */
    private void compressLogFile(Path logFile) {
        Path compressedFile = Paths.get(logFile.toString() + COMPRESSED_EXTENSION);

        try {
            // Skip if already compressed
            if (Files.exists(compressedFile)) {
                TapLogger.debug(TAG, "Compressed file already exists: {}", compressedFile.getFileName());
                return;
            }

            long originalSize = Files.size(logFile);

            // Compress the file
            try (InputStream in = Files.newInputStream(logFile);
                 OutputStream out = Files.newOutputStream(compressedFile);
                 GZIPOutputStream gzipOut = new GZIPOutputStream(out)) {

                byte[] buffer = new byte[8192];
                int len;
                while ((len = in.read(buffer)) > 0) {
                    gzipOut.write(buffer, 0, len);
                }
            }

            long compressedSize = Files.size(compressedFile);
            double ratio = (1.0 - (double) compressedSize / originalSize) * 100;

            // Delete original file after successful compression
            Files.delete(logFile);

            totalFilesCompressed.incrementAndGet();

            TapLogger.info(TAG, "Compressed log file: {} -> {} (saved {:.1f}%, {} -> {} bytes)",
                logFile.getFileName(), compressedFile.getFileName(),
                String.format("%.1f", ratio), originalSize, compressedSize);

        } catch (Exception e) {
            TapLogger.error(TAG, "Error compressing log file {}: {}",
                logFile.getFileName(), e.getMessage(), e);

            // Clean up partial compressed file on error
            try {
                if (Files.exists(compressedFile)) {
                    Files.delete(compressedFile);
                }
            } catch (IOException cleanupError) {
                TapLogger.error(TAG, "Error cleaning up partial compressed file: {}",
                    cleanupError.getMessage());
            }
        }
    }

    /**
     * Manually trigger compression of old log files
     * This can be called by users to compress logs on demand
     * Only compresses files older than the compression interval
     */
    public void compressNow() {
        compressNow(false);
    }

    /**
     * Manually trigger compression of log files
     * @param forceAll if true, compress all non-current log files regardless of age
     */
    public void compressNow(boolean forceAll) {
        if (!enableCompression) {
            TapLogger.warn(TAG, "Compression is not enabled");
            return;
        }

        TapLogger.info(TAG, "Manual compression triggered (forceAll={})", forceAll);
        compressOldLogs(forceAll);
    }

    /**
     * Rotate to a new log file
     */
    private void rotateLogFile() throws IOException {
        // Close current writer
        if (currentWriter != null) {
            try {
                currentWriter.flush();
                currentWriter.close();
            } catch (IOException e) {
                TapLogger.error(TAG, "Error closing current log file: {}", e.getMessage(), e);
            }
        }

        // Generate new file name
        SimpleDateFormat fileNameFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");
        String timestamp = fileNameFormat.format(new Date());
        String fileName = String.format("%s-%s-%04d%s", logFilePrefix, timestamp, fileSequence++, LOG_FILE_EXTENSION);

        currentLogFile = Paths.get(logDirectory, fileName);
        currentFileSize = 0;

        // Create new writer
        currentWriter = Files.newBufferedWriter(
            currentLogFile,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND
        );

        TapLogger.info(TAG, "Rotated to new log file: {}", currentLogFile);
    }

    /**
     * Get current statistics
     */
    public LoggerStats getStats() {
        return new LoggerStats(
            totalLinesLogged.get(),
            totalLinesDropped.get(),
            totalFilesCompressed.get(),
            logQueue.size(),
            currentFileSize,
            currentLogFile != null ? currentLogFile.toString() : null
        );
    }

    /**
     * Statistics holder
     */
    public static class LoggerStats {
        private final long totalLinesLogged;
        private final long totalLinesDropped;
        private final long totalFilesCompressed;
        private final int queueSize;
        private final long currentFileSize;
        private final String currentLogFile;

        public LoggerStats(long totalLinesLogged, long totalLinesDropped, long totalFilesCompressed,
                          int queueSize, long currentFileSize, String currentLogFile) {
            this.totalLinesLogged = totalLinesLogged;
            this.totalLinesDropped = totalLinesDropped;
            this.totalFilesCompressed = totalFilesCompressed;
            this.queueSize = queueSize;
            this.currentFileSize = currentFileSize;
            this.currentLogFile = currentLogFile;
        }

        public long getTotalLinesLogged() {
            return totalLinesLogged;
        }

        public long getTotalLinesDropped() {
            return totalLinesDropped;
        }

        public long getTotalFilesCompressed() {
            return totalFilesCompressed;
        }

        public int getQueueSize() {
            return queueSize;
        }

        public long getCurrentFileSize() {
            return currentFileSize;
        }

        public String getCurrentLogFile() {
            return currentLogFile;
        }

        @Override
        public String toString() {
            return String.format("LoggerStats{logged=%d, dropped=%d, compressed=%d, queueSize=%d, fileSize=%d, file=%s}",
                totalLinesLogged, totalLinesDropped, totalFilesCompressed, queueSize, currentFileSize, currentLogFile);
        }
    }

    /**
     * Close the logger and wait for all pending logs to be written
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            TapLogger.info(TAG, "Closing FileLogger...");

            // Stop the compression executor if enabled
            if (compressExecutor != null) {
                try {
                    compressExecutor.shutdown();
                    if (!compressExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        compressExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    compressExecutor.shutdownNow();
                    TapLogger.warn(TAG, "Interrupted while waiting for compression executor to finish");
                }
            }

            // Stop the writer thread
            running.set(false);

            // Wait for writer thread to finish
            try {
                writerThread.join(10000); // Wait up to 10 seconds
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                TapLogger.warn(TAG, "Interrupted while waiting for writer thread to finish");
            }

            // Close the current writer
            if (currentWriter != null) {
                try {
                    currentWriter.flush();
                    currentWriter.close();
                } catch (IOException e) {
                    TapLogger.error(TAG, "Error closing log file: {}", e.getMessage(), e);
                }
            }

            LoggerStats stats = getStats();
            TapLogger.info(TAG, "FileLogger closed. Final stats: {}", stats);
        }
    }
}
