package io.tapdata.connector.clickhouse;

import com.zaxxer.hikari.pool.HikariPool;
import io.tapdata.connector.clickhouse.config.ClickhouseConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connection.TableInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import ru.yandex.clickhouse.except.ClickHouseUnknownException;

import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ClickhouseConnectorTest {

    @Test
    void testRegisterCapabilitiesCountByPartitionFilter(){
        ClickhouseConnector clickhouseConnector =new ClickhouseConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(clickhouseConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertNotNull(connectorFunctions.getCountByPartitionFilterFunction());
    }
    @Test
    void test_connectionTest(){
        ClickhouseConnector connector = mock(ClickhouseConnector.class);
        ClickhouseConfig config = mock(ClickhouseConfig.class);
        TapConnectionContext connectionContext = mock(TapConnectionContext.class);
        when(connectionContext.getConnectionConfig()).thenReturn(new DataMap());
        Consumer<TestItem> consumer = testItem -> {
        };
        doCallRealMethod().when(connector).connectionTest(any(),any());
        Assertions.assertThrows(HikariPool.PoolInitializationException.class,()->{
            connector.connectionTest(connectionContext,consumer);
        });
    }

    @Test
    void testGetTableInfo() throws SQLException {
        ClickhouseConnector clickhouseConnector =new ClickhouseConnector();
        TapConnectionContext tapConnectorContext = mock(TapConnectionContext.class);
        ClickhouseJdbcContext clickhouseJdbcContext = mock(ClickhouseJdbcContext.class);
        ReflectionTestUtils.setField(clickhouseConnector,"clickhouseJdbcContext",clickhouseJdbcContext);
        doNothing().when(clickhouseJdbcContext).query(any(String.class),any(ResultSetConsumer.class));
        DataMap dataMap = new DataMap();
        long total_rows = 4L;
        dataMap.put("total_rows",total_rows);
        dataMap.put("total_bytes",8);
        when(clickhouseJdbcContext.getTableInfo("test")).thenReturn(dataMap);
        TableInfo actualData = ReflectionTestUtils.invokeMethod(clickhouseConnector,"getTableInfo",tapConnectorContext,"test");
        Assertions.assertTrue(total_rows == actualData.getNumOfRows());

    }
}
