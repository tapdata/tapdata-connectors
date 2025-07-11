import io.tapdata.connector.starrocks.StarrocksConnector;
import io.tapdata.connector.starrocks.bean.StarrocksConfig;
import io.tapdata.connector.starrocks.streamload.StarrocksStreamLoader;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.mock;

public class StarrocksConnectorTest {

    @Test
    void testRegisterCapabilitiesCountByPartitionFilter(){
        StarrocksConnector clickhouseConnector = new StarrocksConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(clickhouseConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertNotNull(connectorFunctions.getCountByPartitionFilterFunction());
    }
    @Nested
    class GetStarrocksStreamLoaderTest{
        @DisplayName("test getStarrocksStreamLoader useHTTPS is true")
        @Test
        void test1() {
            StarrocksConfig StarrocksConfig = new StarrocksConfig();
            StarrocksConfig.setUseHTTPS(true);
            StarrocksConnector StarrocksConnector = new StarrocksConnector();
            ReflectionTestUtils.setField(StarrocksConnector, "StarrocksConfig", StarrocksConfig);
            Assertions.assertDoesNotThrow(()->StarrocksConnector.getStarrocksStreamLoader());
        }
        @DisplayName("test getStarrocksStreamLoader useHTTPS is false")
        @Test
        void test2(){
            StarrocksConfig StarrocksConfig = new StarrocksConfig();
            StarrocksConfig.setUseHTTPS(false);
            StarrocksConnector StarrocksConnector = new StarrocksConnector();
            ReflectionTestUtils.setField(StarrocksConnector, "StarrocksConfig", StarrocksConfig);
            Assertions.assertDoesNotThrow(()->StarrocksConnector.getStarrocksStreamLoader());
        }

    }
}
