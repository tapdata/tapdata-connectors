package io.tapdata.connector.mysql;

import io.tapdata.common.JdbcContext;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.sql.*;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MysqlCallableResourceTest {
    @Test
    void statementIsClosedWhenConfigurationFails() throws Exception {
        verifyFailedConfiguration(false);
    }

    @Test
    void closeFailureDoesNotReplaceOriginalFailure() throws Exception {
        verifyFailedConfiguration(true);
    }

    private void verifyFailedConfiguration(boolean closeFails) throws Exception {
        MysqlConnector connector = new MysqlConnector();
        JdbcContext context = mock(JdbcContext.class);
        Connection connection = mock(Connection.class);
        CallableStatement statement = mock(CallableStatement.class);
        SQLException original = new SQLException("streaming configuration rejected");
        SQLException closeFailure = new SQLException("statement close failed");
        ReflectionTestUtils.setField(connector, "jdbcContext", context);
        when(context.getConnection()).thenReturn(connection);
        when(connection.prepareCall("{call demo_proc()}")).thenReturn(statement);
        doThrow(original).when(statement).setFetchSize(Integer.MIN_VALUE);
        if (closeFails) doThrow(closeFailure).when(statement).close();

        ExecuteResult<?> result = connector.call("demo_proc", Collections.emptyList());

        assertNotNull(result.getError());
        assertSame(original, result.getError().getCause());
        verify(statement).close();
        verify(connection).close();
        verify(statement, never()).execute();
        if (closeFails) assertArrayEquals(new Throwable[]{closeFailure}, original.getSuppressed());
    }
}
