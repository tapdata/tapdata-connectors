package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class YearConvertTest {

    @Test
    void convert() {
        Assertions.assertNull(new YearConvert(false).convert(null));
        Assertions.assertNotNull(new YearConvert(false).convert(2024));
        Assertions.assertNotNull(new YearConvert(true).convert("2024"));
        Assertions.assertNotNull(new YearConvert(false).convert(2024));
        Assertions.assertNotNull(new YearConvert(true).convert("2024"));
        Assertions.assertNotNull(new YearConvert(true).convert("xxx"));
        Assertions.assertNotNull(new YearConvert(true).convert(new HashMap<>()));
    }
}