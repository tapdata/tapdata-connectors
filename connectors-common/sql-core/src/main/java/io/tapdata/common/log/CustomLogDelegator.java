package io.tapdata.common.log;

import net.sf.log4jdbc.log.SpyLogDelegator;
import net.sf.log4jdbc.sql.Spy;
import net.sf.log4jdbc.sql.resultsetcollector.ResultSetCollector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


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
            return "postgres.jdbc.custom";
        }
    };

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
    }

    @Override
    public void sqlTimingOccurred(Spy spy, long execTime, String methodCall, String sql) {
        sqlTimingLogger.info(sql + " {" + execTime + " msec}");
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
}

