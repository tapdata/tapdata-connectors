package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class EnumConvertTest {

    @Test
    void convert() {
        Assertions.assertNull(new EnumConvert().convert(null));
        Assertions.assertNotNull(new EnumConvert().convert("null"));
        Assertions.assertNotNull(new EnumConvert().convert(1));
    }
}