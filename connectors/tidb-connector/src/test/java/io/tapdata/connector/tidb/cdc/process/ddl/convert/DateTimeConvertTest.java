package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

class DateTimeConvertTest {

    @Test
    void convert() {
        Assertions.assertNotNull(new DateTimeConvert("0", TimeZone.getDefault()).convert("2024-06-11 16:51:00"));
        Assertions.assertNotNull(new DateTimeConvert("3", TimeZone.getDefault()).convert("2024-06-11 16:51:00.123"));
    }
}