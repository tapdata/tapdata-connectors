package io.tapdata.connector.mariadb;

import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

public class TestMariadbTest {
    @Test
    void test_main(){
        MysqlConfig mysqlConfig = new MysqlConfig();
        mysqlConfig.setDatabase("test");
        mysqlConfig.setUser("user");
        mysqlConfig.setPassword("123456");
        mysqlConfig.setHost("127.0.0.1");
        mysqlConfig.setPort(3306);
        Consumer<TestItem> consumer = testItem -> {};
        MariadbTest mariadbTest = new MariadbTest(mysqlConfig,consumer,new ConnectionOptions());
        Assertions.assertNotNull(mariadbTest);
    }
}
