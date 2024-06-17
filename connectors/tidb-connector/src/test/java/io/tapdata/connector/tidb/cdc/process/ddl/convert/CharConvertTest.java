package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CharConvertTest {
    CharConvert convert;

    @BeforeEach
    void setUp() {
        convert = new CharConvert(1);
        convert = new CharConvert("1");
        convert = new CharConvert("1");
        convert = new CharConvert(-1);
    }

    @Test
    void testNull() {
        Assertions.assertNull(convert.convert(null));
    }
    @Test
    void testLess() {
        Object convert = this.convert.convert("");
        Assertions.assertNotNull(convert);
        Assertions.assertEquals("", convert);
    }

    @Test
    void testMore() {
        Object convert = this.convert.convert("111");
        Assertions.assertNotNull(convert);
        Assertions.assertEquals("1", convert);
    }
}