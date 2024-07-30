package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import io.tapdata.entity.error.CoreException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.TimeZone;

class TimeConvertTest {

    @Test
    void convert() {
        Assertions.assertNull(new TimeConvert("1", TimeZone.getDefault()).convert(null));
        Assertions.assertThrows(CoreException.class, () -> new TimeConvert("1", TimeZone.getDefault()).convert("2024-06-11 17:13:00.1"));
    }
}