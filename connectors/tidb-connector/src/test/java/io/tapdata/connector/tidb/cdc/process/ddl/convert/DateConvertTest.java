package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

class DateConvertTest {

    @Test
    void convert() {
        Assertions.assertNotNull(new DateConvert(TimeZone.getDefault()).convert("2024-06-11"));
    }
}