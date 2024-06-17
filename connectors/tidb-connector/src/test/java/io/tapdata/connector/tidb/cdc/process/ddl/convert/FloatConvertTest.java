package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class FloatConvertTest {

    @Test
    void convert() {
        Assertions.assertNull(new FloatConvert(false).convert(null));
        Assertions.assertNotNull(new FloatConvert(false, 1, 1).convert("1.1"));
        Assertions.assertNotNull(new FloatConvert(false, "1", "1").convert(1.1));
        Assertions.assertNotNull(new FloatConvert(false).convert("x"));
        Assertions.assertNotNull(new FloatConvert(false).convert(new HashMap<>()));
    }
}