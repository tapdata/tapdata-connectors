package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class SmallIntConvertTest {

    @Test
    void convert() {
        Assertions.assertNull(new SmallIntConvert(false).convert(null));
        Assertions.assertNotNull(new SmallIntConvert(false).convert("null"));
        Assertions.assertNotNull(new SmallIntConvert(false).convert("1"));
        Assertions.assertNotNull(new SmallIntConvert(true).convert("1"));
        Assertions.assertNotNull(new SmallIntConvert(true).convert(1));
        Assertions.assertNotNull(new SmallIntConvert(true).convert(new HashMap<>()));
    }
}