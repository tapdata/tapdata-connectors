package io.tapdata.connector.greenplum;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class GreenplumConnectorTest {
    @Test
    void testRegisterCapabilitiesCountByPartitionFilter(){
        GreenplumConnector greenplumConnector = new GreenplumConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(greenplumConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertNotNull(connectorFunctions.getCountByPartitionFilterFunction());
    }
}
