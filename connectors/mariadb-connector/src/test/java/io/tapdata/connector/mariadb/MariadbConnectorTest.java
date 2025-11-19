package io.tapdata.connector.mariadb;

import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.TestItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MariadbConnectorTest {
    @Test
    void test_connectionTest(){
        MariadbConnector mariadbConnector = new MariadbConnector();
        TapConnectionContext connectionContext = mock(TapConnectionContext.class);
        when(connectionContext.getConnectionConfig()).thenReturn(new DataMap());
        Consumer<TestItem> consumer = testItem -> {
        };
        Assertions.assertThrows(IllegalArgumentException.class,()->mariadbConnector.connectionTest(connectionContext,consumer));
    }
}
