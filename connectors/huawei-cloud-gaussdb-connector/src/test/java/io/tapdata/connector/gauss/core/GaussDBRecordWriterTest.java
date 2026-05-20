package io.tapdata.connector.gauss.core;

import io.tapdata.common.JdbcContext;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.time.ZoneId;
import java.util.TimeZone;

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
    @Disabled
    void testInstanceWithConnection() {
        try (MockedStatic<GaussDBRecordWriter> gw = mockStatic(GaussDBRecordWriter.class);
             MockedConstruction<GaussDBRecordWriter> construction = mockConstruction(GaussDBRecordWriter.class)) {
            gw.when(() -> new GaussDBRecordWriter(jdbcContext, tapTable)).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> new GaussDBRecordWriter(jdbcContext, tapTable));
        }
    }

    @Test
    @Disabled
    void testInstanceWithoutConnection() {
        Connection connection = mock(Connection.class);
        try (MockedStatic<GaussDBRecordWriter> gw = mockStatic(GaussDBRecordWriter.class);
             MockedConstruction<GaussDBRecordWriter> construction = mockConstruction(GaussDBRecordWriter.class)) {
            gw.when(() -> new GaussDBRecordWriter(jdbcContext, connection, tapTable)).thenCallRealMethod();
            Assertions.assertDoesNotThrow(() -> new GaussDBRecordWriter(jdbcContext, connection, tapTable));
        }
    }

    @Test
    void test()

    {

    String input = "\\x31323335373735356767";  // 输入的十六进制转义字符串
      byte[] bytes =  input.getBytes();
      byte[] newBytes = new byte[bytes.length / 2 - 1];
    for(int i = 2; i < bytes.length; i+=2) {
        newBytes[i / 2 - 1] = (byte)((bytes[i]-48)*16+bytes[i+1]-48);
    }
    System.out.println(new String(newBytes));
}
}
