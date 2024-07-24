package io.tapdata.connector.mysql;

import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.SQLException;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestMysqlConnectionTest {
    @Nested
    class testTimeDifference{
        MysqlConnectionTest mysqlConnectionTest = mock(MysqlConnectionTest.class, Answers.CALLS_REAL_METHODS);
        MysqlJdbcContextV2 mysqlJdbcContextV2 = mock(MysqlJdbcContextV2.class);
        @BeforeEach
        void before(){
            ReflectionTestUtils.setField(mysqlConnectionTest,"jdbcContext",mysqlJdbcContextV2);
            Consumer<TestItem> consumer =(item)->{};
            ReflectionTestUtils.setField(mysqlConnectionTest,"consumer",consumer);
            ConnectionOptions connectionOptions = new ConnectionOptions();
            ReflectionTestUtils.setField(mysqlConnectionTest,"connectionOptions",connectionOptions);
        }

        @Test
        void testFail() throws SQLException {
            when(mysqlJdbcContextV2.queryTimestamp()).thenReturn(System.currentTimeMillis()+2000);
            mysqlConnectionTest.testTimeDifference();
            ConnectionOptions connectionOptions = (ConnectionOptions) ReflectionTestUtils.getField(mysqlConnectionTest,"connectionOptions");
            Assertions.assertTrue(connectionOptions.getTimeDifference() > 1000);
        }

        @Test
        void testPass() throws SQLException {
            when(mysqlJdbcContextV2.queryTimestamp()).thenReturn(System.currentTimeMillis());
            mysqlConnectionTest.testTimeDifference();
            ConnectionOptions connectionOptions = (ConnectionOptions) ReflectionTestUtils.getField(mysqlConnectionTest,"connectionOptions");
            Assertions.assertTrue(connectionOptions.getTimeDifference() < 1000);
        }

        @Test
        void testQuerySourceTimeFail() throws SQLException {
            when(mysqlJdbcContextV2.queryTimestamp()).thenThrow(new SQLException());
            mysqlConnectionTest.testTimeDifference();
            ConnectionOptions connectionOptions = (ConnectionOptions) ReflectionTestUtils.getField(mysqlConnectionTest,"connectionOptions");
            Assertions.assertNull(connectionOptions.getTimeDifference());
        }
    }
}
