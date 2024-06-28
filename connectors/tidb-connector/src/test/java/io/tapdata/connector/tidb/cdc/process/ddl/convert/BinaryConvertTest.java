package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BinaryConvertTest {
    BinaryConvert convert;
    @BeforeEach
    void init() {
        convert = new BinaryConvert();
    }
    @Test
    void testNull() {
        Assertions.assertNull(convert.convert(null));
    }
    @Test
    void testByte() {
        Object convert = this.convert.convert(new byte[]{0});
        Assertions.assertNotNull(convert);
    }
    @Test
    void testString() {
        Object convert = this.convert.convert("x");
        Assertions.assertNotNull(convert);
        Assertions.assertEquals(byte[].class.getName(), convert.getClass().getName());
    }
}