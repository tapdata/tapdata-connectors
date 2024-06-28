package io.tapdata.connector.tidb.cdc.process.analyse;


import io.tapdata.connector.tidb.cdc.process.ddl.convert.Convert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DefaultConvertTest {
    DefaultConvert defaultConvert;
    @BeforeEach
    void init() {
        defaultConvert = new DefaultConvert();
    }
    @Nested
    class ConvertTest {
        @Test
        void testNormal() {
            Convert defaultConvert = new io.tapdata.connector.tidb.cdc.process.ddl.convert.DefaultConvert();
            Object convert = DefaultConvertTest.this.defaultConvert.convert(1, defaultConvert);
            Assertions.assertEquals(1, convert);
        }
        @Test
        void testNullValue() {
            Convert defaultConvert = new io.tapdata.connector.tidb.cdc.process.ddl.convert.DefaultConvert();
            Object convert = DefaultConvertTest.this.defaultConvert.convert(null, defaultConvert);
            Assertions.assertNull(convert);
        }

        @Test
        void testNullConvert() {
            Object convert = DefaultConvertTest.this.defaultConvert.convert(1, null);
            Assertions.assertEquals(1, convert);
        }
    }
}