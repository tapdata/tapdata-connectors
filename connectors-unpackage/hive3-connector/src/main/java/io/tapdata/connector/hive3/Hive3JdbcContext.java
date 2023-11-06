package io.tapdata.connector.hive3;

import io.tapdata.connector.hive.HiveJdbcContext;

import java.sql.Connection;
import java.sql.SQLException;

public class Hive3JdbcContext extends HiveJdbcContext {

    public Hive3JdbcContext(Hive3Config mrsHive3Config) {
        super(mrsHive3Config);
    }

    public Connection getConnection() throws SQLException {
        Connection connection = super.getConnection();
        connection.setAutoCommit(true);
        return connection;
    }


}
