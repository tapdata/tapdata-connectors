package io.tapdata.connector.mysql;

import com.mysql.cj.exceptions.StatementIsClosedException;
import io.tapdata.exception.TapPdkRetryableEx;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MysqlStatementRetryPolicyTest {
    @ParameterizedTest
    @ValueSource(strings = {"plain", "unrelated", "S1009", "S1000", "nested"})
    void onlyRetryableClosedStatementWithMatchingSqlStateTriggersRecovery(String mode) {
        StatementIsClosedException closed = mock(StatementIsClosedException.class);
        when(closed.getSQLState()).thenReturn("S1000".equals(mode) ? "S1000" : "S1009");
        Throwable cause = "unrelated".equals(mode) ? new IllegalStateException("other failure") : closed;
        if ("nested".equals(mode)) cause = new IllegalStateException("wrapper", cause);
        Throwable failure = "plain".equals(mode) ? cause : new TapPdkRetryableEx("mysql", cause);
        Boolean recover = ReflectionTestUtils.invokeMethod(new MysqlConnector(), "checkStatementClosed", failure);
        assertEquals(Boolean.valueOf("S1009".equals(mode) || "nested".equals(mode)), recover);
    }
}
