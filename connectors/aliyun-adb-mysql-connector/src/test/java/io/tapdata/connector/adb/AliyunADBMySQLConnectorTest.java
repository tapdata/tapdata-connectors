package io.tapdata.connector.adb;

import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AliyunADBMySQLConnectorTest {
    @Nested
    class ConnectionTest{
        AliyunMysqlConfig mysqlConfig;
        AliyunADBMySQLConnector connector;
        @BeforeEach
        void init(){
            mysqlConfig = mock(AliyunMysqlConfig.class);
            connector = mock(AliyunADBMySQLConnector.class);
        }

        @Test
        void test_main(){
            TapConnectionContext connectionContext = mock(TapConnectionContext.class);
            when(connectionContext.getConnectionConfig()).thenReturn(new DataMap());
            Consumer<TestItem> consumer = testItem -> {
            };
            doCallRealMethod().when(connector).connectionTest(any(),any());
            Assertions.assertThrows(IllegalArgumentException.class,()->{
                connector.connectionTest(connectionContext,consumer);
            });
        }
    }
}
