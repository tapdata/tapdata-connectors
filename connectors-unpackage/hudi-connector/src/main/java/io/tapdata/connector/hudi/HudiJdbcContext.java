package io.tapdata.connector.hudi;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hive.config.HiveConfig;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry;
import org.apache.spark.sql.execution.datasources.jdbc.DriverWrapper;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.list;

public class HudiJdbcContext extends HiveJdbcContext {
    private final static String HUDI_ALL_TABLE = "show tables";
    public static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS %s ( %s ) %s";
    private static final String GET_TABLE_INFO_SQL = "SELECT TABLE_ROWS,DATA_LENGTH  FROM information_schema.tables WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'";

    public HudiJdbcContext(HiveConfig config) {
        super(config);
    }
    public Connection getConnection() throws SQLException {
        return createConnectionFactory();
    }

    private Connection createConnectionFactory() throws SQLException {
        Map<String, String> options = new HashMap<>();
        options.put("url", getConfig().getDatabaseUrl());
        options.put("driver", "org.apache.hive.jdbc.HiveDriver");
        options.put("nullable", "true");
        String driverClass = options.get(JDBCOptions.JDBC_DRIVER_CLASS());
        DriverRegistry.register(driverClass);
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        Driver driver = null;
        while (drivers.hasMoreElements()) {
            Driver d = drivers.nextElement();
            if (d instanceof DriverWrapper) {
                if (((DriverWrapper) d).wrapped().getClass().getCanonicalName().equals(driverClass)) {
                    driver = d;
                }
            } else if (d.getClass().getCanonicalName().equals(driverClass)) {
                driver = d;
            }
            if (driver != null) {
                break;
            }
        }

        Objects.requireNonNull(driver, String.format("Did not find registered driver with class %s", driverClass));

        Properties properties = new Properties();
        properties.putAll(options);
        Connection connect;
        String url = options.get(JDBCOptions.JDBC_URL());
        connect = driver.connect(url, properties);
        Objects.requireNonNull(connect, String.format("The driver could not open a JDBC connection. Check the URL: %s", url));
        return connect;
    }

    @Override
    public void batchExecute(List<String> sqlList) throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            for (String sql : sqlList) {
                statement.execute(sql);
            }
        }
    }
    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) throws SQLException {
        List<DataMap> tableList = list();
        query(HUDI_ALL_TABLE, resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        if (EmptyKit.isNotEmpty(tableNames)) {
            return tableList.stream().filter(t -> tableNames.contains(t.getString("tableName"))).collect(Collectors.toList());
        }
        return tableList;
    }

    public boolean tableIfExists(String tableName) throws SQLException {
        if (null == tableName || tableName.isEmpty()) return false;
        AtomicBoolean ifExists = new AtomicBoolean(false);
        query(String.format("SHOW TABLES LIKE '%s'", tableName), resultSet -> {
            while (resultSet.next()) {
                if (tableName.equals(resultSet.getString("tableName"))) {
                    ifExists.set(true);
                    break;
                }
            }
        });
        return ifExists.get();
    }

    private Map<String, String> getTableModule() {
        Map<String, String> map = new HashMap<>();
        map.put("Detailed Table Information", "table_info");
        map.put("# Partition Information", "partition_info");
        map.put("# col_name", "partition_info");
        return map;
    }

    public void execute(String sql) throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            statement.execute(sql);
        }
    }

    public DataMap getTableInfo(String tableName) throws SQLException {
        DataMap dataMap = DataMap.create();
        List<String> list = new ArrayList<>();
        list.add("TABLE_ROWS");
        list.add("DATA_LENGTH");
        query(String.format(GET_TABLE_INFO_SQL, getConfig().getDatabase(), tableName), resultSet -> {
            while (resultSet.next()) {
                dataMap.putAll(DbKit.getRowFromResultSet(resultSet, list));
            }
        });
        return dataMap;
    }
}
