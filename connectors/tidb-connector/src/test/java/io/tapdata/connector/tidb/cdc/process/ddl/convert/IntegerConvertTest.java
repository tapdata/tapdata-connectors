package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IntegerConvertTest {

    @Test
    void convert() {
        Assertions.assertNull(new IntegerConvert(false).convert(null));
        Assertions.assertNotNull(new IntegerConvert(false).convert("1"));
        Assertions.assertNotNull(new IntegerConvert(false).convert(1));
        Assertions.assertNotNull(new IntegerConvert(true).convert("1"));
        Assertions.assertNotNull(new IntegerConvert(true).convert("x"));
    }
}