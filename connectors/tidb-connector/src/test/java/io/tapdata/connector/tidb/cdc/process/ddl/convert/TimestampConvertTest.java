package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.TimeZone;

class TimestampConvertTest {

    @Test
    void convert() {
        Assertions.assertNotNull(new TimestampConvert("1", TimeZone.getDefault()).convert("2024-06-11 17:14:00.1"));
        Assertions.assertNotNull(new TimestampConvert("0", TimeZone.getDefault()).convert(System.currentTimeMillis()));
    }
}