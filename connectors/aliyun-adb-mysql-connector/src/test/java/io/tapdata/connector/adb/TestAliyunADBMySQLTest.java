package io.tapdata.connector.adb;

import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.mockito.Mockito.mock;

public class TestAliyunADBMySQLTest {
    @Test
    void test_main(){
        Consumer<TestItem> consumer = testItem -> {
        };
        MysqlConfig mysqlConfig = new MysqlConfig();
        mysqlConfig.setJdbcDriver("com.mysql.cj.jdbc.Driver");
        mysqlConfig.setDatabase("test");
        mysqlConfig.setUser("user");
        mysqlConfig.setPassword("123456");
        AliyunADBMySQLTest aliyunADBMySQLTest = new AliyunADBMySQLTest(mysqlConfig,consumer,new ConnectionOptions());
        Assertions.assertNotNull(aliyunADBMySQLTest);
    }
}
