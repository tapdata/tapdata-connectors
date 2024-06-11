package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultConvertTest {
    @Test
    void testNormal() {
        Assertions.assertNull(new DefaultConvert().convert(1));
    }
}