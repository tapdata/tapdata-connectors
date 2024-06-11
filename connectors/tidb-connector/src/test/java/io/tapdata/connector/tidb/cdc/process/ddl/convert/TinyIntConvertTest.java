package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class TinyIntConvertTest {

    @Test
    void convert() {
        Assertions.assertNull(new TinyIntConvert(true).convert(null));
        Assertions.assertNotNull(new TinyIntConvert(false).convert("1"));
        Assertions.assertNotNull(new TinyIntConvert(true).convert("1"));
        Assertions.assertNotNull(new TinyIntConvert(false).convert(1));
        Assertions.assertNotNull(new TinyIntConvert(true).convert(1));
        Assertions.assertNotNull(new TinyIntConvert(false).convert("x"));
        Assertions.assertNotNull(new TinyIntConvert(false).convert(new HashMap<>()));
    }
}