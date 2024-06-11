package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class LongConvertTest {

    @Test
    void convert() {
        Assertions.assertNull(new LongConvert(false).convert(null));
        Assertions.assertNotNull(new LongConvert(false).convert(1));
        Assertions.assertNotNull(new LongConvert(false).convert("1"));
        Assertions.assertNotNull(new LongConvert(true).convert("1"));
        Assertions.assertNotNull(new LongConvert(false).convert("x"));
    }
}