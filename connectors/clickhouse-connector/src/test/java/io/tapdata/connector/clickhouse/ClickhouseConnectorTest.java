package io.tapdata.connector.clickhouse;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.mock;

class ClickhouseConnectorTest {

    @Test
    void testRegisterCapabilitiesCountByPartitionFilter(){
        ClickhouseConnector clickhouseConnector =new ClickhouseConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(clickhouseConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertNotNull(connectorFunctions.getCountByPartitionFilterFunction());
    }
}
