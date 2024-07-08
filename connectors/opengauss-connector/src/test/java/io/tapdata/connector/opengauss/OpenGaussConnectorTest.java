package io.tapdata.connector.opengauss;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

public class OpenGaussConnectorTest {
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
            tapTable = mock(TapTable.class);
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
        }

        private LinkedHashMap<String, TapField> generateFieldMap(TapField... fields) {
            if (null == fields) {
                return null;
            }
            LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();
            for (TapField field : fields) {
                fieldMap.put(field.getName(), field);
            }
            return fieldMap;
        }

        @Test
        void testEmptyField() {
            // null getNameFieldMap
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));

            // empty getNameFieldMap
            when(tapTable.getNameFieldMap()).thenReturn(new LinkedHashMap<>());
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testNotPrimaryKeys() {
            LinkedHashMap<String, TapField> fieldMap = generateFieldMap(
                new TapField("ID", "INT"),
                new TapField("TITLE", "VARCHAR(64)")
            );
            when(tapTable.getNameFieldMap()).thenReturn(fieldMap);

            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testTrue() {
            LinkedHashMap<String, TapField> fieldMap = generateFieldMap(
                new TapField("ID", "INT").primaryKeyPos(1),
                new TapField("TITLE", "VARCHAR(64)")
            );
            when(tapTable.getNameFieldMap()).thenReturn(fieldMap);

            assertNotNull(connector.getHashSplitStringSql(tapTable));
        }
    }
}
