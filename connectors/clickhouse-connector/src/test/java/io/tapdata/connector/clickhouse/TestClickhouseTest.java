package io.tapdata.connector.clickhouse;

import io.tapdata.connector.clickhouse.config.ClickhouseConfig;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.SQLException;
import java.util.function.Consumer;

import static org.mockito.Mockito.*;

public class TestClickhouseTest {
    ClickhouseTest clickhouseTest;
    ClickhouseJdbcContext clickhouseJdbcContext;
    @BeforeEach
    void init(){
        clickhouseTest = mock(ClickhouseTest.class);
        clickhouseJdbcContext = mock(ClickhouseJdbcContext.class);
        Consumer<TestItem> consumer = testItem -> {
        };
        ReflectionTestUtils.setField(clickhouseTest,"jdbcContext",clickhouseJdbcContext);
        ReflectionTestUtils.setField(clickhouseTest,"connectionOptions",new ConnectionOptions());
        ReflectionTestUtils.setField(clickhouseTest,"consumer",consumer);
    }

    @Nested
    class TestTimeDifference{
        @Test
        void testPass() throws SQLException {
            when(clickhouseJdbcContext.queryTimestamp()).thenReturn(System.currentTimeMillis());
            doCallRealMethod().when(clickhouseTest).testTimeDifference();
            doCallRealMethod().when(clickhouseTest).getTimeDifference(any());
            clickhouseTest.testTimeDifference();
            ConnectionOptions connectionOptions = (ConnectionOptions) ReflectionTestUtils.getField(clickhouseTest,"connectionOptions");
            Assertions.assertTrue(connectionOptions.getTimeDifference() < 1000);
        }
        @Test
        void testFail() throws SQLException {
            when(clickhouseJdbcContext.queryTimestamp()).thenReturn(System.currentTimeMillis() + 2000);
            doCallRealMethod().when(clickhouseTest).testTimeDifference();
            doCallRealMethod().when(clickhouseTest).getTimeDifference(any());
            clickhouseTest.testTimeDifference();
            ConnectionOptions connectionOptions = (ConnectionOptions) ReflectionTestUtils.getField(clickhouseTest,"connectionOptions");
            Assertions.assertTrue(connectionOptions.getTimeDifference() > 1000);
        }

        @Test
        void testQuerySourceTimeFail() throws SQLException {
            when(clickhouseJdbcContext.queryTimestamp()).thenThrow(new SQLException());
            doCallRealMethod().when(clickhouseTest).testTimeDifference();
            clickhouseTest.testTimeDifference();
            ConnectionOptions connectionOptions = (ConnectionOptions) ReflectionTestUtils.getField(clickhouseTest,"connectionOptions");
            Assertions.assertNull(connectionOptions.getTimeDifference());
        }
    }
}
