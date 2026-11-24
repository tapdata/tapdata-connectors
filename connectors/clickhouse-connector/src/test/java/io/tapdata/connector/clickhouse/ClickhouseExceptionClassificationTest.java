package io.tapdata.connector.clickhouse;

import io.tapdata.exception.*;
import org.junit.jupiter.api.Test;
import java.sql.SQLException;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;

class ClickhouseExceptionClassificationTest {
    private final ClickhouseExceptionCollector collector = new ClickhouseExceptionCollector();
    @Test
    void serverFailureIsClassifiedAndRetainsOriginalCause() {
        SQLException cause = new SQLException("database rejected request", "HY000", 394);
        TapPdkTerminateByServerEx result = assertThrows(TapPdkTerminateByServerEx.class, () -> collector.collectTerminateByServer(cause));
        assertSame(cause, result.getCause());
    }

    @Test
    void authFailureIsClassifiedAndRetainsOriginalCause() {
        SQLException cause = new SQLException("database rejected request", "HY000", 516);
        TapPdkUserPwdInvalidEx result = assertThrows(TapPdkUserPwdInvalidEx.class, () -> collector.collectUserPwdInvalid("reader", cause));
        assertSame(cause, result.getCause());
    }

    @Test
    void readFailureIsClassifiedAndRetainsOriginalCause() {
        SQLException cause = new SQLException("database rejected request", "HY000", 497);
        TapPdkReadMissingPrivilegesEx result = assertThrows(TapPdkReadMissingPrivilegesEx.class, () -> collector.collectReadPrivileges("select", Collections.singletonList("SELECT"), cause));
        assertSame(cause, result.getCause());
    }

    @Test
    void writeFailureIsClassifiedAndRetainsOriginalCause() {
        SQLException cause = new SQLException("database rejected request", "HY000", 497);
        TapPdkWriteMissingPrivilegesEx result = assertThrows(TapPdkWriteMissingPrivilegesEx.class, () -> collector.collectWritePrivileges("insert", Collections.singletonList("INSERT"), cause));
        assertSame(cause, result.getCause());
    }

    @Test
    void unrelatedFailureIsNotMisclassified() {
        SQLException cause = new SQLException("unrelated");
        assertDoesNotThrow(() -> collector.collectTerminateByServer(cause));
        assertDoesNotThrow(() -> collector.collectUserPwdInvalid("reader", cause));
        assertDoesNotThrow(() -> collector.collectReadPrivileges("select", Collections.emptyList(), cause));
        assertDoesNotThrow(() -> collector.collectWritePrivileges("insert", Collections.emptyList(), cause));
    }
}
