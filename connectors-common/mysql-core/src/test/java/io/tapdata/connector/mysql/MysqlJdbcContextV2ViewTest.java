package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.config.MysqlConfig;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

class MysqlJdbcContextV2ViewTest {

    @Test
    void queryAllTablesIncludesViewsAndTheirType() {
        MysqlConfig config = new MysqlConfig();
        config.setHost("localhost");
        config.setPort(3306);
        config.setDatabase("tapdata");
        config.setUser("root");
        config.setPassword("password");
        MysqlJdbcContextV2 context = new MysqlJdbcContextV2(config);

        String sql = context.queryAllTablesSql("tapdata", Collections.emptyList());

        assertTrue(sql.contains("TABLE_TYPE `tableType`"));
        assertTrue(sql.contains("TABLE_TYPE IN ('BASE TABLE', 'VIEW')"));
    }
}
