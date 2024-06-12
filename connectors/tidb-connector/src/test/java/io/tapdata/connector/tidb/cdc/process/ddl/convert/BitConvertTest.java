package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class BitConvertTest {
    BitConvert convert;
    @BeforeEach
    void setUp() {
        convert = new BitConvert();
    }
    @Test
    void testNull() {
        Assertions.assertNull(convert.convert(null));
    }
    @Test
    void testString() {
        Object x = convert.convert("x");
        Assertions.assertNotNull(x);
        Assertions.assertEquals(byte[].class.getName(), x.getClass().getName());
    }
    @Test
    void testByte() {
        Object x = convert.convert(new byte[]{0});
        Assertions.assertNotNull(x);
        Assertions.assertEquals(byte[].class.getName(), x.getClass().getName());
    }
    @Test
    void testMap() {
        Object x = convert.convert(new HashMap<>());
        Assertions.assertNotNull(x);
        Assertions.assertEquals(0, x);
    }
}