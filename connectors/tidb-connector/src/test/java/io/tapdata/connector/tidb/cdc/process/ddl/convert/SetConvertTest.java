package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SetConvertTest {

    @Test
    void convert() {
        Assertions.assertNull(new SetConvert().convert(null));
        Assertions.assertNotNull(new SetConvert().convert("null"));
        Assertions.assertNotNull(new SetConvert().convert(1));
    }
}