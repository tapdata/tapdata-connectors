package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class DoubleConvertTest {

    @Test
    void convert() {
        Assertions.assertNull(new DoubleConvert(false,1, 1).convert(null));
        Assertions.assertNotNull(new DoubleConvert(false,1, 1).convert("1.1"));
        Assertions.assertNotNull(new DoubleConvert(false,1, 1).convert("x"));
        Assertions.assertNotNull(new DoubleConvert(false,"1", "1").convert(1.1));
        Assertions.assertNotNull(new DoubleConvert(false).convert(new HashMap<>()));
    }
}