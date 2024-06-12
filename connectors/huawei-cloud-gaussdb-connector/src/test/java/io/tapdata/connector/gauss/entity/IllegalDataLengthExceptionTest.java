package io.tapdata.connector.gauss.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class IllegalDataLengthExceptionTest {
    @Test
    void testNormal() {
        Assertions.assertDoesNotThrow(() -> new IllegalDataLengthException(""));
    }
}
