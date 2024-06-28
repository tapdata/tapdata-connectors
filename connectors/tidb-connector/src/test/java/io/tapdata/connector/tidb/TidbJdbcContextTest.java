package io.tapdata.connector.tidb;

import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.mysql.config.MysqlConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TidbJdbcContextTest {
    TidbJdbcContext jdbcContext;
    @BeforeEach
    void setUp() {
        jdbcContext = mock(TidbJdbcContext.class);
    }

    @Nested
    class QuerySafeGcPoint {
        @Test
        void testQuerySafeGcPoint() throws SQLException {
            ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.next()).thenReturn(true, true, false);
            when(resultSet.getString("VARIABLE_NAME")).thenReturn("xxx", "tikv_gc_safe_point");
            when(resultSet.getString("VARIABLE_VALUE")).thenReturn("20240607-09:56:42.704 +0800");
            when(jdbcContext.querySafeGcPoint()).thenCallRealMethod();
            doAnswer(a -> {
                ResultSetConsumer argument = a.getArgument(1, ResultSetConsumer.class);
                argument.accept(resultSet);
                return null;
            }).when(jdbcContext).query(anyString(), any(ResultSetConsumer.class));
            Assertions.assertNotNull(jdbcContext.querySafeGcPoint());
        }
        @Test
        void testNormal() throws SQLException {
            ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.next()).thenReturn(false);
            when(jdbcContext.querySafeGcPoint()).thenCallRealMethod();
            doAnswer(a -> {
                ResultSetConsumer argument = a.getArgument(1, ResultSetConsumer.class);
                argument.accept(resultSet);
                return null;
            }).when(jdbcContext).query(anyString(), any(ResultSetConsumer.class));
            Assertions.assertNotNull(jdbcContext.querySafeGcPoint());
        }
    }

    @Nested
    class QueryGcLifeTime {
        @Test
        void testNormal() throws SQLException {
            ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.next()).thenReturn(true, true, false);
            when(resultSet.getString("Variable_name")).thenReturn("xxx", "tidb_gc_life_time");
            when(resultSet.getString("Value")).thenReturn("","20240607-09:56:42.704 +0800");
            when(jdbcContext.queryGcLifeTime()).thenCallRealMethod();
            doAnswer(a -> {
                ResultSetConsumer argument = a.getArgument(1, ResultSetConsumer.class);
                argument.accept(resultSet);
                return null;
            }).when(jdbcContext).query(anyString(), any(ResultSetConsumer.class));
            Assertions.assertNotNull(jdbcContext.queryGcLifeTime());
        }
        @Test
        void testNull() throws SQLException {
            ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.next()).thenReturn(true, true, false);
            when(resultSet.getString("Variable_name")).thenReturn("xxx");
            when(resultSet.getString("Value")).thenReturn("");
            when(jdbcContext.queryGcLifeTime()).thenCallRealMethod();
            doAnswer(a -> {
                ResultSetConsumer argument = a.getArgument(1, ResultSetConsumer.class);
                argument.accept(resultSet);
                return null;
            }).when(jdbcContext).query(anyString(), any(ResultSetConsumer.class));
            Assertions.assertNull(jdbcContext.queryGcLifeTime());
        }
    }

    @Nested
    class QueryTimeZone {
        io.tapdata.connector.mysql.config.MysqlConfig mysqlConfig;
        ResultSet resultSet;
        @BeforeEach
        void init() throws SQLException {
            resultSet = mock(ResultSet.class);
            mysqlConfig = mock(MysqlConfig.class);
            when(jdbcContext.getConfig()).thenReturn(mysqlConfig);
            doAnswer(a -> {
                ResultSetConsumer argument = a.getArgument(1, ResultSetConsumer.class);
                argument.accept(resultSet);
                return null;
            }).when(jdbcContext).queryWithNext(anyString(), any(ResultSetConsumer.class));
            when(jdbcContext.queryTimeZone()).thenCallRealMethod();
        }

        @Test
        void testNormal() throws SQLException {
            when(mysqlConfig.getTimezone()).thenReturn("+08:00");
            Assertions.assertNotNull(jdbcContext.queryTimeZone());
        }
        @Test
        void test1() throws SQLException {
            when(mysqlConfig.getTimezone()).thenReturn(null);
            when(resultSet.getLong(1)).thenReturn(-8L);
            Assertions.assertNotNull(jdbcContext.queryTimeZone());
        }
        @Test
        void test2() throws SQLException {
            when(mysqlConfig.getTimezone()).thenReturn(null);
            when(resultSet.getLong(1)).thenReturn(8L);
            Assertions.assertNotNull(jdbcContext.queryTimeZone());
        }
    }
}