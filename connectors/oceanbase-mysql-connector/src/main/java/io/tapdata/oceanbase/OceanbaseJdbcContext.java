package io.tapdata.oceanbase;

import com.oceanbase.jdbc.OceanBaseStatement;
import com.zaxxer.hikari.pool.HikariProxyStatement;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OceanbaseJdbcContext extends MysqlJdbcContextV2 {

    public OceanbaseJdbcContext(CommonDbConfig config) {
        super(config);
    }

    public void queryWithStream(String sql, ResultSetConsumer resultSetConsumer) throws Throwable {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement()
        ) {
            if (statement instanceof HikariProxyStatement) {
                OceanBaseStatement statementImpl = statement.unwrap(OceanBaseStatement.class);
                if (null != statementImpl) {
                    statementImpl.setFetchSize(Integer.MIN_VALUE);
                }
            }
            try (
                    ResultSet resultSet = statement.executeQuery(sql)
            ) {
                if (null != resultSet) {
                    resultSetConsumer.accept(resultSet);
                }
            }
        } catch (SQLException e) {
            throw new Exception("Execute steaming query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }
}
