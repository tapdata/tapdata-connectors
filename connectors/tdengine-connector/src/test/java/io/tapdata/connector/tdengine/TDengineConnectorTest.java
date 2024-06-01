package io.tapdata.connector.tdengine;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class TDengineConnectorTest {
    @Test
    void testRegisterCapabilitiesCountByPartitionFilter(){
        TDengineConnector tDengineConnector = new TDengineConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(tDengineConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertNotNull(connectorFunctions.getCountByPartitionFilterFunction());
    }
}
