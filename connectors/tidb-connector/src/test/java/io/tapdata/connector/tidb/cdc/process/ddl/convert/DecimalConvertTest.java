package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class DecimalConvertTest {

    @Test
    void testConvert() {
        Assertions.assertNull(new DecimalConvert(1,1).convert(null));
        Assertions.assertNotNull(new DecimalConvert(2,1).convert("1.1"));
        Assertions.assertNotNull(new DecimalConvert(1,1).convert(new BigDecimal("1.1")));
        Assertions.assertNotNull(new DecimalConvert(1,1).convert(1.1));
        Assertions.assertNotNull(new DecimalConvert("1","1").convert(new HashMap<>()));
    }
}