package io.tapdata.common;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.exception.AbstractExceptionCollector;
import io.tapdata.common.exception.ExceptionCollector;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * abstract jdbc context
 *
 * @author Jarad
 * @date 2022/5/30
 */
public abstract class JdbcContext implements AutoCloseable {

    private final static String TAG = JdbcContext.class.getSimpleName();
    private final HikariDataSource hikariDataSource;
    private final CommonDbConfig config;
    protected ExceptionCollector exceptionCollector = new AbstractExceptionCollector() {
    };

    public JdbcContext(CommonDbConfig config) {
        this.config = config;
        this.hikariDataSource = HikariConnection.getHikariDataSource(config);
    }

    public CommonDbConfig getConfig() {
        return config;
    }

    /**
     * get sql connection
     *
     * @return Connection
     * @throws SQLException SQLException
     */
    public Connection getConnection() throws SQLException {
        try {
            return hikariDataSource.getConnection();
        } catch (SQLException e) {
            exceptionCollector.collectUserPwdInvalid(getConfig().getUser(), e);
            exceptionCollector.revealException(e);
            throw e;
        }
    }

    /**
     * query version of database
     *
     * @return version description
     */
    public String queryVersion() throws SQLException {
        try (
                Connection connection = getConnection()
        ) {
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            return databaseMetaData.getDatabaseMajorVersion() + "." + databaseMetaData.getDatabaseMinorVersion();
        }
    }

    public void queryWithNext(String sql, ResultSetConsumer resultSetConsumer) throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.setFetchSize(2000); //protected from OM
            try (
                    ResultSet resultSet = statement.executeQuery(sql)
            ) {
                if (EmptyKit.isNotNull(resultSet)) {
                    resultSet.next(); //move to first row
                    resultSetConsumer.accept(resultSet);
                }
            }
        }
    }

    public void nextQueryWithTimeout(String sql, ResultSetConsumer resultSetConsumer, ArrayList<String> timeoutSqls) throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            if (!timeoutSqls.isEmpty()) {
                for (String timeoutSql : timeoutSqls) {
                    statement.execute(timeoutSql);
                }
            }
            statement.setFetchSize(2000); //protected from OM
            try (
                    ResultSet resultSet = statement.executeQuery(sql)
            ) {
                if (EmptyKit.isNotNull(resultSet)) {
                    resultSet.next(); //move to first row
                    resultSetConsumer.accept(resultSet);
                }
            }
        }
    }

    public void queryWithTimeout(String sql, ResultSetConsumer resultSetConsumer, ArrayList<String> timeoutSqls) throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        ) {
            if (!timeoutSqls.isEmpty()) {
                for (String timeoutSql : timeoutSqls) {
                    statement.execute(timeoutSql);
                }
            }
            statement.setFetchSize(2000); //protected from OM
            try (
                    ResultSet resultSet = statement.executeQuery(sql)
            ) {
                if (EmptyKit.isNotNull(resultSet)) {
                    resultSetConsumer.accept(resultSet);
                }
            }
        }
    }

    public void query(String sql, ResultSetConsumer resultSetConsumer) throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        ) {
            statement.setFetchSize(2000); //protected from OM
            try (
                    ResultSet resultSet = statement.executeQuery(sql)
            ) {
                if (EmptyKit.isNotNull(resultSet)) {
                    resultSetConsumer.accept(resultSet);
                }
            }
        }
    }

    public void streamQueryWithTimeout(String querySql, ResultSetConsumer resultSetConsumer, ArrayList<String> timeoutSqls, Integer fetchSize) throws Exception {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        ) {
            if (!timeoutSqls.isEmpty()) {
                for (String sql : timeoutSqls) {
                    statement.execute(sql);
                }
            }
            statement.setFetchSize(fetchSize);
            try (
                    ResultSet resultSet = statement.executeQuery(querySql)
            ) {
                if (EmptyKit.isNotNull(resultSet)) {
                    resultSetConsumer.accept(resultSet);
                }
            }
        }
    }

    public void normalQuery(String sql, ResultSetConsumer resultSetConsumer) throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            if (EmptyKit.isNotNull(resultSet)) {
                resultSetConsumer.accept(resultSet);
            }
        }
    }

    @Deprecated
    public void query(PreparedStatement preparedStatement, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + preparedStatement);
        try (
                ResultSet resultSet = preparedStatement.executeQuery()
        ) {
            if (EmptyKit.isNotNull(resultSet)) {
                resultSet.next();
                resultSetConsumer.accept(resultSet);
            }
        } catch (SQLException e) {
            throw new SQLException("Execute query failed, sql: " + preparedStatement + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }

    public void prepareQuery(String prepareSql, List<Object> params, ResultSetConsumer resultSetConsumer) throws SQLException {
        try (
                Connection connection = getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement(prepareSql)
        ) {
            preparedStatement.setFetchSize(2000);
            int pos = 1;
            for (Object obj : params) {
                preparedStatement.setObject(pos++, obj);
            }
            try (
                    ResultSet resultSet = preparedStatement.executeQuery()
            ) {
                if (EmptyKit.isNotNull(resultSet)) {
                    resultSetConsumer.accept(resultSet);
                }
            }
        }
    }

    public void execute(String sql) throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.execute(sql);
            connection.commit();
        }
    }

    public void batchExecute(List<String> sqlList) throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            for (String sql : sqlList) {
                statement.execute(sql);
            }
            connection.commit();
        }
    }

    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) throws SQLException {
        List<String> temp = list();
        query(queryAllTablesSql(getConfig().getSchema(), tableNames),
                resultSet -> {
                    while (resultSet.next()) {
                        String tableName = resultSet.getString("tableName");
                        if (EmptyKit.isNotBlank(tableName)) {
                            temp.add(tableName);
                        }
                        if (temp.size() >= batchSize) {
                            consumer.accept(temp);
                            temp.clear();
                        }
                    }
                });
        if (EmptyKit.isNotEmpty(temp)) {
            consumer.accept(temp);
            temp.clear();
        }
    }

    /**
     * query tableNames and Comments from one database and one schema
     *
     * @param tableNames some tables(all tables if tableName is empty or null)
     * @return List<TableName and Comments>
     */
    public List<DataMap> queryAllTables(List<String> tableNames) throws SQLException {
        List<DataMap> tableList = list();
        query(queryAllTablesSql(getConfig().getSchema(), tableNames),
                resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        return tableList;
    }

    protected String queryAllTablesSql(String schema, List<String> tableNames) {
        throw new UnsupportedOperationException();
    }

    /**
     * query all column info from some tables
     *
     * @param tableNames some tables(all tables if tableName is empty or null)
     * @return List<column info>
     */
    public List<DataMap> queryAllColumns(List<String> tableNames) throws SQLException {
        List<DataMap> columnList = list();
        query(queryAllColumnsSql(getConfig().getSchema(), tableNames),
                resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        return columnList;
    }

    protected String queryAllColumnsSql(String schema, List<String> tableNames) {
        throw new UnsupportedOperationException();
    }

    /**
     * query all index info from some tables
     *
     * @param tableNames some tables(all tables if tableName is empty or null)
     * @return List<index info>
     */
    public List<DataMap> queryAllIndexes(List<String> tableNames) throws SQLException {
        List<DataMap> columnList = list();
        query(queryAllIndexesSql(getConfig().getSchema(), tableNames),
                resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        return columnList;
    }

    protected String queryAllIndexesSql(String schema, List<String> tableNames) {
        throw new UnsupportedOperationException();
    }

    public List<DataMap> queryAllForeignKeys(List<String> tableNames) throws SQLException {
        List<DataMap> foreignKeyList = list();
        try {
            query(queryAllForeignKeysSql(getConfig().getSchema(), tableNames),
                    resultSet -> foreignKeyList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (UnsupportedOperationException ignore) {
        }
        return foreignKeyList;
    }

    protected String queryAllForeignKeysSql(String schema, List<String> tableNames) {
        throw new UnsupportedOperationException();
    }

    public Long queryTimestamp() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        if (EmptyKit.isNotNull(hikariDataSource)) {
            hikariDataSource.close();
        }
        config.deleteSSlFile();
    }

    //static Hikari connection
    static class HikariConnection {

        public static HikariDataSource create() {
            return new HikariDataSource();
        }

        public static HikariDataSource getHikariDataSource(CommonDbConfig config) throws IllegalArgumentException {
            if (EmptyKit.isNull(config)) {
                throw new IllegalArgumentException("Config cannot be null");
            }
            HikariDataSource hikariDataSource = create();
            //need 4 attributes
            hikariDataSource.setDriverClassName(config.getJdbcDriver());
            String jdbcUrl = config.getDatabaseUrl();
            hikariDataSource.setJdbcUrl(jdbcUrl);
            hikariDataSource.setUsername(config.getUser());
            hikariDataSource.setPassword(config.getPassword());
            hikariDataSource.setMinimumIdle(getInteger(jdbcUrl, "Min Idle", 1));
            hikariDataSource.setMaximumPoolSize(Math.max(config.getWriteThreadSize() + 5, getInteger(jdbcUrl, "Max Pool Size", 20)));
            if (EmptyKit.isNotNull(config.getProperties())) {
                hikariDataSource.setDataSourceProperties(config.getProperties());
            }
            hikariDataSource.setAutoCommit(false);
            hikariDataSource.setIdleTimeout(60 * 1000L);
            hikariDataSource.setKeepaliveTime(60 * 1000L);
            hikariDataSource.setMaxLifetime(600 * 1000L);
            return hikariDataSource;
        }

        protected static String getParamFromUrl(String databaseUrl, String tag) {
            if (null == databaseUrl || null == tag) return null;
            if (databaseUrl.contains(tag)) {
                int index = databaseUrl.indexOf(tag) + tag.length();
                int end = databaseUrl.indexOf(";", index);
                if (end < 0) {
                    end = databaseUrl.length();
                }
                String substring = databaseUrl.substring(index, end);
                substring = replaceAll(replaceAll(substring, "=", ""), " ", "");
                return substring;
            }
            return null;
        }

        protected static int getInteger(String databaseUrl, String tag, int defaultValue) {
            try {
                String valueStr = getParamFromUrl(databaseUrl, tag);
                if (null != valueStr && !"".equals(valueStr)) {
                    return Integer.parseInt(valueStr);
                }
                return defaultValue;
            } catch (NumberFormatException ignore) {
                return defaultValue;
            }
        }

        public static String replaceAll(String str, String old, String to) {
            while (str.contains(old)) {
                str = str.replace(old, to);
            }
            return str;
        }
    }
}
