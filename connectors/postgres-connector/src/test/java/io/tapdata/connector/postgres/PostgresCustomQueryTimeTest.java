package io.tapdata.connector.postgres;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PostgresCustomQueryTimeTest {
    @Test
    void commandPreservesInstantAndWallClockAcrossTimezoneModesAndDst() throws Throwable {
        TimeZone previous = TimeZone.getDefault();
        try {
            for (String zone : Arrays.asList("UTC", "Asia/Shanghai", "America/Los_Angeles")) {
                TimeZone.setDefault(TimeZone.getTimeZone(zone));
                for (boolean legacy : Arrays.asList(true, false)) {
                    // Winter, summer DST, and the repeated local hour during DST rollback.
                    for (String time : Arrays.asList("2026-01-20T04:30:00.123456Z",
                            "2026-07-20T04:30:00.123456Z", "2026-11-01T08:30:00Z", "2026-11-01T09:30:00Z")) {
                        assertCommandResult(legacy, Instant.parse(time));
                    }
                }
            }
        } finally {
            TimeZone.setDefault(previous);
        }
    }

    private static class QueryConnector extends PostgresConnector {
        void query(Map<String, Object> params, List<ExecuteResult> results) throws Throwable {
            executeCommand(null, TapExecuteCommand.create().command("executeQuery").params(params), results::add);
        }
    }

    private void assertCommandResult(boolean legacy, Instant instant) throws Throwable {
        QueryConnector connector = spy(new QueryConnector());
        doReturn(true).when(connector).isAlive();
        PostgresConfig config = new PostgresConfig();
        config.setOldVersionTimezone(legacy);
        config.setZoneOffsetHour(0);
        ReflectionTestUtils.setField(connector, "postgresConfig", config);
        JdbcContext jdbc = mock(JdbcContext.class);
        ReflectionTestUtils.setField(connector, "jdbcContext", jdbc);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(jdbc.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute("select times")).thenReturn(true);
        when(statement.getResultSet()).thenReturn(rs);
        when(rs.next()).thenReturn(true, false);
        when(rs.getMetaData()).thenReturn(metadata);
        String[] names = {"zoned_alias", "local_alias", "null_alias", "amount"};
        String[] types = {"timestamptz", "timestamp", "timestamptz", "money"};
        when(metadata.getColumnCount()).thenReturn(names.length);
        for (int i = 0; i < names.length; i++) {
            when(metadata.getColumnLabel(i + 1)).thenReturn(names[i]);
            when(metadata.getColumnTypeName(i + 1)).thenReturn(types[i]);
        }
        Timestamp timestamp = Timestamp.from(instant);
        when(rs.getTimestamp("zoned_alias")).thenReturn(timestamp);
        when(rs.getObject("zoned_alias")).thenReturn(timestamp);
        when(rs.getString("local_alias")).thenReturn("2026-07-20 12:30:00.123456");
        when(rs.getString("amount")).thenReturn("$12.34");
        Map<String, Object> params = new HashMap<>();
        params.put("sql", "select times");
        params.put("batchSize", 10);
        params.put("queryTimeoutSeconds", 8);
        List<ExecuteResult> results = new ArrayList<>();
        connector.query(params, results);
        assertEquals(1, results.size());
        assertNull(results.get(0).getError());
        DataMap row = (DataMap) ((List<?>) results.get(0).getResult()).get(0);
        assertEquals(instant, row.get("zoned_alias"));
        assertEquals(LocalDateTime.parse("2026-07-20T12:30:00.123456"), row.get("local_alias"));
        assertNull(row.get("null_alias"));
        assertEquals(new java.math.BigDecimal("12.34"), row.get("amount"));
        // Snapshot/CDC conversion remains on its existing compatibility path.
        if (legacy) assertSame(timestamp, connector.filterData(timestamp, "timestamptz"));
        verify(statement).setQueryTimeout(8);
        verify(rs).close();
        verify(statement).close();
        verify(connection).close();
    }
}
