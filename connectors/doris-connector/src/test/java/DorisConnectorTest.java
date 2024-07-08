import io.tapdata.connector.doris.DorisConnector;
import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.connector.doris.streamload.DorisStreamLoader;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.mock;

public class DorisConnectorTest {

    @Test
    void testRegisterCapabilitiesCountByPartitionFilter(){
        DorisConnector clickhouseConnector = new DorisConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(clickhouseConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertNotNull(connectorFunctions.getCountByPartitionFilterFunction());
    }
    @Nested
    class GetDorisStreamLoaderTest{
        @DisplayName("test getDorisStreamLoader useHTTPS is true")
        @Test
        void test1() {
            DorisConfig dorisConfig = new DorisConfig();
            dorisConfig.setUseHTTPS(true);
            DorisConnector dorisConnector = new DorisConnector();
            ReflectionTestUtils.setField(dorisConnector, "dorisConfig", dorisConfig);
            Assertions.assertDoesNotThrow(()->dorisConnector.getDorisStreamLoader());
        }
        @DisplayName("test getDorisStreamLoader useHTTPS is false")
        @Test
        void test2(){
            DorisConfig dorisConfig = new DorisConfig();
            dorisConfig.setUseHTTPS(false);
            DorisConnector dorisConnector = new DorisConnector();
            ReflectionTestUtils.setField(dorisConnector, "dorisConfig", dorisConfig);
            Assertions.assertDoesNotThrow(()->dorisConnector.getDorisStreamLoader());
        }

    }
}
