package io.tapdata.connector.gauss.enums;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectionEnumTest {
    @Test
    public void testParams() {
        Assertions.assertEquals("database", ConnectionEnum.DATABASE_TAG);
    }
}
