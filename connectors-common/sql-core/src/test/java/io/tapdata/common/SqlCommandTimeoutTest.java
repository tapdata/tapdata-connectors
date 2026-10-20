package io.tapdata.common;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLTimeoutException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqlCommandTimeoutTest {
    @Test
    void validatesTimeoutAndBatchSizeWithoutTruncationOrOverflow() {
        assertEquals(0, SqlCommandParams.getQueryTimeoutSeconds(Collections.emptyMap()));
        assertEquals(1000, SqlCommandParams.getInteger(Collections.emptyMap(), "batchSize", 1000));
        for (Object valid : Arrays.asList(0, 0L, "0", new java.math.BigDecimal("0.0"))) {
            assertEquals(0, SqlCommandParams.getQueryTimeoutSeconds(Collections.singletonMap("queryTimeoutSeconds", valid)));
        }
        for (Object valid : Arrays.asList(8, 8L, "8", new java.math.BigDecimal("8.0"))) {
            assertEquals(8, SqlCommandParams.getQueryTimeoutSeconds(Collections.singletonMap("queryTimeoutSeconds", valid)));
            assertEquals(8, SqlCommandParams.getInteger(Collections.singletonMap("batchSize", valid), "batchSize", 1000));
        }
        for (Object invalid : Arrays.asList(-1, "1.5", "oops", true, "2147483648", "9223372036854775808")) {
            assertThrows(IllegalArgumentException.class,
                    () -> SqlCommandParams.getQueryTimeoutSeconds(Collections.singletonMap("queryTimeoutSeconds", invalid)));
            assertThrows(IllegalArgumentException.class,
                    () -> SqlCommandParams.getInteger(Collections.singletonMap("batchSize", invalid), "batchSize", 1000));
        }
        assertThrows(IllegalArgumentException.class,
                () -> SqlCommandParams.getInteger(Collections.singletonMap("batchSize", 0), "batchSize", 1000));
        assertEquals(Integer.MAX_VALUE, SqlCommandParams.getQueryTimeoutSeconds(
                Collections.singletonMap("queryTimeoutSeconds", Integer.MAX_VALUE)));
    }

    @Test
    void legacyExecutorNeverSilentlyIgnoresPositiveTimeout() throws Throwable {
        SqlExecutor executor = mock(SqlExecutor.class, CALLS_REAL_METHODS);
        executor.execute("select 1", null, null, null, 10, 0);
        verify(executor).execute("select 1", null, null, null, 10);
        assertThrows(UnsupportedOperationException.class,
                () -> executor.execute("select 1", null, null, null, 10, 8));
        verify(executor, times(1)).execute("select 1", null, null, null, 10);
    }

    @Test
    void commandPassesTimeoutToJdbcAndReportsTimeoutErrors() throws Exception {
        for (Integer timeout : Arrays.asList(null, 0, 8)) {
            Connection connection = mock(Connection.class);
            Statement statement = mock(Statement.class);
            when(connection.createStatement()).thenReturn(statement);
            when(statement.execute("select 1")).thenThrow(new SQLTimeoutException("expired"));
            Map<String, Object> params = new HashMap<>();
            params.put("sql", "select 1");
            params.put("batchSize", 10L);
            if (timeout != null) { params.put("queryTimeoutSeconds", timeout); }
            List<ExecuteResult> results = new ArrayList<>();
            new SqlExecuteCommandFunction(new DefaultSqlExecutor()).executeSqlCommand(null,
                    TapExecuteCommand.create().command("executeQuery").params(params), () -> connection, () -> true, results::add);
            assertEquals(1, results.size());
            assertTrue(results.get(0).getError() instanceof SQLTimeoutException);
            if (timeout == null || timeout == 0) { verify(statement, never()).setQueryTimeout(anyInt()); }
            else { verify(statement).setQueryTimeout(timeout); }
            verify(statement).close();
            verify(connection).close();
        }
    }

    @Test
    void unsupportedDriverTimeoutFailsClearlyBeforeExecuting() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        doThrow(new SQLFeatureNotSupportedException()).when(statement).setQueryTimeout(8);
        SQLFeatureNotSupportedException failure = assertThrows(SQLFeatureNotSupportedException.class,
                () -> new DefaultSqlExecutor().execute("select 1", () -> connection, ignored -> {}, () -> true, 10, 8));
        assertTrue(failure.getMessage().contains("set 0"));
        verify(statement, never()).execute(anyString());
    }

    static class BaseConnector extends CommonDbConnector {
        @Override public ConnectionOptions connectionTest(TapConnectionContext context, Consumer<TestItem> consumer) { return null; }
        @Override public void registerCapabilities(ConnectorFunctions functions, TapCodecsRegistry registry) { }
        @Override public void onStart(TapConnectionContext context) { }
        @Override public void onStop(TapConnectionContext context) { }
    }

    static class LegacyConnector extends BaseConnector {
        int calls;
        @Override protected void execute(String sql, Consumer<Object> consumer, int batchSize) { calls++; consumer.accept("legacy"); }
    }

    @Test
    void commonConnectorPreservesLegacyOverrideAndRejectsUnsupportedTimeout() throws Throwable {
        LegacyConnector connector = new LegacyConnector();
        List<Object> values = new ArrayList<>();
        connector.execute("select 1", values::add, 10, 0);
        assertEquals(Collections.singletonList("legacy"), values);
        assertThrows(UnsupportedOperationException.class, () -> connector.execute("select 1", values::add, 10, 8));
        assertEquals(1, connector.calls);
    }

    @Test
    void commonCommandAppliesPositiveTimeoutAndLeavesOmittedOrZeroUnset() throws Throwable {
        for (Integer timeout : Arrays.asList(null, 0, 8)) {
            BaseConnector connector = new BaseConnector();
            JdbcContext jdbc = mock(JdbcContext.class);
            Connection connection = mock(Connection.class);
            Statement statement = mock(Statement.class);
            ReflectionTestUtils.setField(connector, "jdbcContext", jdbc);
            when(jdbc.getConnection()).thenReturn(connection);
            when(connection.createStatement()).thenReturn(statement);
            when(statement.execute("select 1")).thenThrow(new SQLTimeoutException("expired"));
            Map<String, Object> params = new HashMap<>();
            params.put("sql", "select 1");
            if (timeout != null) { params.put("queryTimeoutSeconds", timeout); }
            List<ExecuteResult> results = new ArrayList<>();
            connector.executeCommand(null, TapExecuteCommand.create().command("executeQuery").params(params), results::add);
            assertTrue(results.get(0).getError() instanceof SQLTimeoutException);
            if (timeout == null || timeout == 0) { verify(statement, never()).setQueryTimeout(anyInt()); }
            else { verify(statement).setQueryTimeout(timeout); }
        }
    }
}
