package io.tapdata.common.log;

import io.tapdata.write.FileLogger;
import net.sf.log4jdbc.log.SpyLogDelegator;
import net.sf.log4jdbc.sql.Spy;
import net.sf.log4jdbc.sql.resultsetcollector.ResultSetCollector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Custom log delegator for postgres connector that allows custom logger name per instance.
 * This class uses ThreadLocal to support multiple concurrent tasks with different logger names.
 */
public class CustomLogDelegator implements SpyLogDelegator {

    static {
        System.out.println("[CustomLogDelegator] Class loaded successfully!");
    }

    // ThreadLocal to store logger name for each thread/task
    private static final ThreadLocal<String> threadLocalLoggerName = new ThreadLocal<String>() {
        @Override
        protected String initialValue() {
            return "jdbc.none.name";
        }
    };

    // Cache for FileLogger instances, keyed by logger name
    private static final ConcurrentHashMap<String, FileLogger> fileLoggerCache = new ConcurrentHashMap<>();

    // Log directory
    private static final String LOG_DIR = "logs" + File.separator + "connector";

    private final Logger sqlOnlyLogger;
    private final Logger sqlTimingLogger;
    private final Logger connectionLogger;
    private final Logger resultSetLogger;
    private final Logger auditLogger;

    public CustomLogDelegator() {
        // Get logger name from ThreadLocal when instance is created
        String loggerName = threadLocalLoggerName.get();
        sqlOnlyLogger = LogManager.getLogger(loggerName + ".sqlonly");
        sqlTimingLogger = LogManager.getLogger(loggerName + ".sqltiming");
        connectionLogger = LogManager.getLogger(loggerName + ".connection");
        resultSetLogger = LogManager.getLogger(loggerName + ".resultset");
        auditLogger = LogManager.getLogger(loggerName + ".audit");
    }

    /**
     * Set custom logger name prefix for current thread.
     * This should be called before creating JDBC connections in each task.
     */
    public static void setLoggerName(String name) {
        threadLocalLoggerName.set(name);
    }

    /**
     * Clear the logger name for current thread to prevent memory leaks.
     * This should be called when task is finished.
     */
    public static void clearLoggerName() {
        threadLocalLoggerName.remove();
    }

    @Override
    public boolean isJdbcLoggingEnabled() {
        return sqlOnlyLogger.isErrorEnabled() || sqlTimingLogger.isErrorEnabled();
    }

    @Override
    public void exceptionOccured(Spy spy, String methodCall, Exception e, String sql, long execTime) {
        String classType = spy == null ? "null" : spy.getClass().getName();
        String message = classType + "." + methodCall + " threw " + e.getClass().getName() + ": " + e.getMessage();
        if (sql != null && sql.length() > 0) {
            message += " SQL: " + sql;
        }
        sqlOnlyLogger.error(message, e);
        sqlTimingLogger.error(message + " {" + execTime + " msec}", e);
    }

    @Override
    public void methodReturned(Spy spy, String methodCall, String returnMsg) {

    }

    @Override
    public void constructorReturned(Spy spy, String constructionInfo) {

    }

    @Override
    public void sqlOccurred(Spy spy, String methodCall, String sql) {
        sqlOnlyLogger.debug(sql);
        // Write to file
        writeToFile(threadLocalLoggerName.get(), "[DEBUG] " + sql);
    }

    @Override
    public void sqlTimingOccurred(Spy spy, long execTime, String methodCall, String sql) {
        String message = sql + " {" + execTime + " msec}";
        sqlTimingLogger.info(message);
        // Write to file
        writeToFile(threadLocalLoggerName.get(), "[INFO] " + message);
    }

    @Override
    public void connectionOpened(Spy spy, long execTime) {
        // Disabled - don't log connection opened
    }

    @Override
    public void connectionClosed(Spy spy, long execTime) {
        // Disabled - don't log connection closed
    }

    @Override
    public void connectionAborted(Spy spy, long execTime) {

    }

    @Override
    public boolean isResultSetCollectionEnabled() {
        return resultSetLogger.isInfoEnabled();
    }

    @Override
    public boolean isResultSetCollectionEnabledWithUnreadValueFillIn() {
        return resultSetLogger.isInfoEnabled();
    }

    @Override
    public void resultSetCollected(ResultSetCollector resultSetCollector) {

    }

    @Override
    public void debug(String msg) {
        auditLogger.debug(msg);
    }

    /**
     * Write log message to file using FileLogger
     * File path: logs/connector/{loggerName}_*.log
     *
     * @param loggerName Logger name (used as file prefix)
     * @param message Log message
     */
    private void writeToFile(String loggerName, String message) {
        if (loggerName == null || message == null) {
            return;
        }

        try {
            FileLogger fileLogger = getOrCreateFileLogger(loggerName);
            if (fileLogger != null) {
                // FileLogger already adds timestamp, so just write the message
                fileLogger.write(message);
            }
        } catch (Exception e) {
            // Log error but don't throw exception to avoid breaking the application
            System.err.println("Failed to write JDBC log to file: " + e.getMessage());
        }
    }

    /**
     * Get or create FileLogger for the given logger name
     *
     * @param loggerName Logger name
     * @return FileLogger instance or null if creation failed
     */
    private FileLogger getOrCreateFileLogger(String loggerName) {
        return fileLoggerCache.computeIfAbsent(loggerName, name -> {
            try {
                // Create FileLogger with custom configuration
                String filePrefix = name.replace('.', '_');

                FileLogger logger = FileLogger.builder()
                        .logDirectory(LOG_DIR)
                        .logFilePrefix(filePrefix)
                        .queueCapacity(10000)      // Queue capacity
                        .batchSize(100)            // Batch size
                        .flushIntervalMs(1000)     // Flush every 1 second
                        .maxFileSizeMB(100)        // Max file size 100MB
                        .autoTimestamp(true)       // Auto add timestamp
                        .enableCompression(true)
                        .compressIntervalMs(1000 * 60 * 30)
                        .maxFileSizeMB(100)
                        .build();

                System.out.println("[CustomLogDelegator] Created FileLogger for: " + name + " -> " + LOG_DIR + "/" + filePrefix);

                return logger;
            } catch (IOException e) {
                System.err.println("Failed to create FileLogger for logger: " + name + ", error: " + e.getMessage());
                return null;
            }
        });
    }

    /**
     * Flush all FileLoggers to ensure data is written to disk
     */
    public static void flushAllWriters() {
        fileLoggerCache.forEach((name, logger) -> {
            try {
                if (logger != null) {
                    logger.forceFlush();
                }
            } catch (Exception e) {
                System.err.println("Failed to flush FileLogger for: " + name + ", error: " + e.getMessage());
            }
        });
    }

    /**
     * Flush FileLogger for specific logger name
     *
     * @param loggerName Logger name
     */
    public static void flushWriter(String loggerName) {
        FileLogger logger = fileLoggerCache.get(loggerName);
        if (logger != null) {
            try {
                logger.forceFlush();
            } catch (Exception e) {
                System.err.println("Failed to flush FileLogger for: " + loggerName + ", error: " + e.getMessage());
            }
        }
    }

    /**
     * Close all FileLoggers and clear cache
     * This should be called when shutting down or when no longer needed
     */
    public static void closeAllWriters() {
        System.out.println("[CustomLogDelegator] Closing all FileLoggers, total: " + fileLoggerCache.size());
        fileLoggerCache.forEach((name, logger) -> {
            try {
                if (logger != null) {
                    FileLogger.LoggerStats stats = logger.getStats();
                    System.out.println("[CustomLogDelegator] Closing FileLogger for: " + name + ", stats: " + stats);
                    logger.close();
                }
            } catch (Exception e) {
                System.err.println("Failed to close FileLogger for: " + name + ", error: " + e.getMessage());
            }
        });
        fileLoggerCache.clear();
        System.out.println("[CustomLogDelegator] All FileLoggers closed");
    }

    /**
     * Close FileLogger for specific logger name
     *
     * @param loggerName Logger name
     */
    public static void closeWriter(String loggerName) {
        FileLogger logger = fileLoggerCache.remove(loggerName);
        if (logger != null) {
            try {
                FileLogger.LoggerStats stats = logger.getStats();
                System.out.println("[CustomLogDelegator] Closing FileLogger for: " + loggerName + ", stats: " + stats);
                logger.close();
            } catch (Exception e) {
                System.err.println("Failed to close FileLogger for: " + loggerName + ", error: " + e.getMessage());
            }
        }
    }

    /**
     * Get queue size for monitoring
     *
     * @param loggerName Logger name
     * @return Queue size or -1 if logger not found
     */
    public static int getQueueSize(String loggerName) {
        FileLogger logger = fileLoggerCache.get(loggerName);
        if (logger != null) {
            return logger.getStats().getQueueSize();
        }
        return -1;
    }

    /**
     * Get statistics for specific logger
     *
     * @param loggerName Logger name
     * @return LoggerStats or null if logger not found
     */
    public static FileLogger.LoggerStats getStats(String loggerName) {
        FileLogger logger = fileLoggerCache.get(loggerName);
        return logger != null ? logger.getStats() : null;
    }
}
