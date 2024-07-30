package io.tapdata.connector.postgres;

import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.SQLException;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPostgresTest {
    @Nested
    class testTimeDifference{
        PostgresTest postgresTest;
        PostgresJdbcContext postgresJdbcContext;
        @BeforeEach
        void init(){
            PostgresConfig postgresConfig = new PostgresConfig();
            postgresConfig.setUser("user");
            postgresConfig.setPassword("123456");
            postgresConfig.setHost("localhost");
            postgresConfig.setDatabase("test");
            postgresConfig.setPort(5432);
            Consumer<TestItem> consumer = testItem -> {
            };
            postgresTest = new PostgresTest(postgresConfig,consumer,new ConnectionOptions());
            postgresJdbcContext = mock(PostgresJdbcContext.class);
            ReflectionTestUtils.setField(postgresTest,"jdbcContext",postgresJdbcContext);
        }
        @Test
        void test_Pass() throws SQLException {
            when(postgresJdbcContext.queryTimestamp()).thenReturn(System.currentTimeMillis());
            postgresTest.testTimeDifference();
            ConnectionOptions connectionOptions = (ConnectionOptions) ReflectionTestUtils.getField(postgresTest,"connectionOptions");
            Assertions.assertTrue(connectionOptions.getTimeDifference() < 1000);
        }

        @Test
        void test_Fail() throws SQLException {
            when(postgresJdbcContext.queryTimestamp()).thenReturn(System.currentTimeMillis() + 2000);
            postgresTest.testTimeDifference();
            ConnectionOptions connectionOptions = (ConnectionOptions) ReflectionTestUtils.getField(postgresTest,"connectionOptions");
            Assertions.assertTrue(connectionOptions.getTimeDifference() > 1000);
        }

        @Test
        void testQuerySourceTimeFail() throws SQLException {
            when(postgresJdbcContext.queryTimestamp()).thenThrow(new SQLException());
            postgresTest.testTimeDifference();
            ConnectionOptions connectionOptions = (ConnectionOptions) ReflectionTestUtils.getField(postgresTest,"connectionOptions");
            Assertions.assertNull(connectionOptions.getTimeDifference());
        }
    }

}
