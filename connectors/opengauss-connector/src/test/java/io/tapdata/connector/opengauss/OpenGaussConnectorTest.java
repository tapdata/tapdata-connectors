package io.tapdata.connector.opengauss;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.utils.TypeConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class OpenGaussConnectorTest {
    private static MockedStatic<InstanceFactory> instanceFactory;

    @BeforeAll
    static void beforeAll() {
        instanceFactory = mockStatic(InstanceFactory.class);
        instanceFactory.when(() -> InstanceFactory.instance(TypeConverter.class)).thenReturn(mock(TypeConverter.class));
        instanceFactory.when(() -> InstanceFactory.instance(TapUtils.class)).thenReturn(mock(TapUtils.class));
    }

    @AfterAll
    static void afterAll() {
        instanceFactory.close();
    }

    @Test
    void testRegisterCapabilitiesCountByPartitionFilter(){
        OpenGaussConnector openGaussConnector = new OpenGaussConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(openGaussConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertNotNull(connectorFunctions.getCountByPartitionFilterFunction());
    }

    @Nested
    class GetHashSplitStringSqlTest {
        TapTable tapTable;
        OpenGaussConnector connector;

        @BeforeEach
        void setUp() {
            connector = mock(OpenGaussConnector.class);
            tapTable = new TapTable();
            tapTable.setNameFieldMap(new LinkedHashMap<>());
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
        }

        @Test
        void testEmptyField() {
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testNotPrimaryKeys() {
            tapTable.add(new TapField("ID", "INT"));
            tapTable.add(new TapField("TITLE", "VARCHAR(64)"));

            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testTrue() {
            tapTable.add(new TapField("ID", "INT").primaryKeyPos(1));
            tapTable.add(new TapField("TITLE", "VARCHAR(64)"));

            assertNotNull(connector.getHashSplitStringSql(tapTable));
        }
    }
}
