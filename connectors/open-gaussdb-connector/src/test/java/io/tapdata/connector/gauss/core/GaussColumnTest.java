package io.tapdata.connector.gauss.core;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GaussColumnTest {
    DataMap dataMap;
    GaussColumn gaussColumn;
    @BeforeEach
    void init() {
        gaussColumn = mock(GaussColumn.class);
        dataMap = mock(DataMap.class);
        when(gaussColumn.init(dataMap)).thenCallRealMethod();
    }
    @Nested
    class GetTapFieldTest {
        @BeforeEach
        void init() {
            when(dataMap.getString("nullable")).thenReturn("null");
            when(dataMap.getInteger("fieldTypeOid")).thenReturn(10);

            when(gaussColumn.getTapField()).thenCallRealMethod();
        }
        @Test
        void testNullColumnName() {
            TapField tapField = gaussColumn.getTapField();
            Assertions.assertNull(tapField);
        }
        @Test
        void testNullDataType() {
            when(dataMap.getString("columnName")).thenReturn("columnName");
            gaussColumn.init(dataMap);
            TapField tapField = gaussColumn.getTapField();
            Assertions.assertNull(tapField);
        }
        @Test
        void testNullRemarks() {
            when(dataMap.getString("columnName")).thenReturn("columnName");
            when(dataMap.getString("dataType")).thenReturn("dataType");
            gaussColumn.init(dataMap);
            TapField tapField = gaussColumn.getTapField();
            Assertions.assertNotNull(tapField);
        }
        @Test
        void testNormal() {
            when(dataMap.getString("columnName")).thenReturn("columnName");
            when(dataMap.getString("dataType")).thenReturn("dataType");
            when(dataMap.getString("columnComment")).thenReturn("columnComment");
            gaussColumn.init(dataMap);
            TapField tapField = gaussColumn.getTapField();
            Assertions.assertNotNull(tapField);
        }
    }

    @Nested
    class InitTest {
        @BeforeEach
        void init() {
            when(dataMap.getString(anyString())).thenReturn("result");
        }

        @Test
        void testNullFieldTypeOid() {
            when(dataMap.getInteger(anyString())).thenReturn(null);
            GaussColumn init = gaussColumn.init(dataMap);
            Assertions.assertEquals(0, init.columnTypeOid);
        }

        @Test
        void testNullDataType() {
            when(dataMap.getString("dataType")).thenReturn(null);
            when(dataMap.getInteger(anyString())).thenReturn(0);
            GaussColumn init = gaussColumn.init(dataMap);
            Assertions.assertEquals(0, init.columnTypeOid);
        }

        @Test
        void testNotNullFieldTypeOid() {
            when(dataMap.getInteger(anyString())).thenReturn(10);
            GaussColumn init = gaussColumn.init(dataMap);
            Assertions.assertEquals(10, init.columnTypeOid);
        }
    }
}
