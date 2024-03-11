package io.tapdata.connector.gauss.core;

import io.tapdata.common.JdbcContext;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.sql.Connection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;

public class GaussDBRecordWriterTest {
    JdbcContext jdbcContext;
    TapTable tapTable;
    String version;

    @BeforeEach
    void init() {
        jdbcContext = mock(JdbcContext.class);
        tapTable = mock(TapTable.class);
        version = "9.2";
    }
    @Test
    void testInstanceWithConnection() {
        try(MockedStatic<GaussDBRecordWriter> gw = mockStatic(GaussDBRecordWriter.class);
            MockedConstruction<GaussDBRecordWriter> construction = mockConstruction(GaussDBRecordWriter.class)) {
            gw.when(() -> GaussDBRecordWriter.instance(jdbcContext, tapTable, version)).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> GaussDBRecordWriter.instance(jdbcContext, tapTable, version));
        }
    }

    @Test
    void testInstanceWithoutConnection() {
        Connection connection = mock(Connection.class);
        try(MockedStatic<GaussDBRecordWriter> gw = mockStatic(GaussDBRecordWriter.class);
            MockedConstruction<GaussDBRecordWriter> construction = mockConstruction(GaussDBRecordWriter.class)) {
            gw.when(() -> GaussDBRecordWriter.instance(jdbcContext, connection, tapTable, version)).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> GaussDBRecordWriter.instance(jdbcContext, connection, tapTable, version));
        }
    }
}
