package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class VarCharConvertTest {

    @Test
    void convert() {
        Assertions.assertNull(new VarCharConvert().convert(null));
        Assertions.assertNotNull(new VarCharConvert().convert("null"));
        Assertions.assertNotNull(new VarCharConvert().convert(1));
    }
}