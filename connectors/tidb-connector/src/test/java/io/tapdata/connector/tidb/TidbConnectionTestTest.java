package io.tapdata.connector.tidb;


import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.SQLException;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TiDbConnectionTestTest {
    TidbConnectionTest tidbConnectionTest;
    TidbConfig tidbConfig;
    Consumer<TestItem> consumer;
    TidbJdbcContext jdbcContext;
    @BeforeEach
    void init() {
        tidbConnectionTest = mock(TidbConnectionTest.class);
        tidbConfig = new TidbConfig();
        consumer = mock(Consumer.class);
        jdbcContext = mock(TidbJdbcContext.class);

        ReflectionTestUtils.setField(tidbConnectionTest, "tidbConfig", tidbConfig);
        ReflectionTestUtils.setField(tidbConnectionTest, "consumer", consumer);
        ReflectionTestUtils.setField(tidbConnectionTest, "jdbcContext", jdbcContext);
        doNothing().when(consumer).accept(any(TestItem.class));
    }

    @Nested
    class TestPDServerTest {
        @BeforeEach
        void init() {
            when(tidbConnectionTest.testPbserver()).thenCallRealMethod();
        }

        @Test
        void testHttp() {
            tidbConfig.setPdServer("http://127.0.0.1:8000");
            try(MockedStatic<NetUtil> nu = mockStatic(NetUtil.class)) {
                nu.when(() -> NetUtil.validateHostPortWithSocket("127.0.0.1", 8000)).then(a -> null);
                Assertions.assertTrue(tidbConnectionTest::testPbserver);
            }
        }
        @Test
        void testHttps() {
            tidbConfig.setPdServer("https://127.0.0.1:8000");
            try(MockedStatic<NetUtil> nu = mockStatic(NetUtil.class)) {
                nu.when(() -> NetUtil.validateHostPortWithSocket("127.0.0.1", 8000)).then(a -> null);
                Assertions.assertTrue(tidbConnectionTest::testPbserver);
            }
        }
        @Test
        void testNoProtocol() {
            tidbConfig.setPdServer("127.0.0.1:8000");
            try(MockedStatic<NetUtil> nu = mockStatic(NetUtil.class)) {
                nu.when(() -> NetUtil.validateHostPortWithSocket("127.0.0.1", 8000)).then(a -> null);
                Assertions.assertTrue(tidbConnectionTest::testPbserver);
            }
        }
        @Test
        void testNotUrl() {
            tidbConfig.setPdServer("dhsjhf:12i.0.0.1:111");
            Assertions.assertFalse(tidbConnectionTest::testPbserver);
        }
    }

    @Nested
    class TestGcLifeTimeTest {
        @BeforeEach
        void init() {
            when(tidbConnectionTest.testGcLifeTime()).thenCallRealMethod();
        }

        @Test
        void testSucceed() throws SQLException {
            when(jdbcContext.queryGcLifeTime()).thenReturn("24h0m0s");
            when(tidbConnectionTest.spilt(anyString())).thenReturn(true);
            Assertions.assertTrue(tidbConnectionTest::testGcLifeTime);
        }

        @Test
        void testWarn() throws SQLException {
            when(jdbcContext.queryGcLifeTime()).thenReturn("24h0m0s");
            when(tidbConnectionTest.spilt(anyString())).thenReturn(false);
            Assertions.assertTrue(tidbConnectionTest::testGcLifeTime);
        }

        @Test
        void testFailed() throws SQLException {
            when(jdbcContext.queryGcLifeTime()).thenAnswer(a -> {
                throw new SQLException("Connection timeout");
            });
            Assertions.assertFalse(tidbConnectionTest::testGcLifeTime);
        }
    }

    @Nested
    class SpiltTest {
        @BeforeEach
        void init() {
            when(tidbConnectionTest.spilt(anyString())).thenCallRealMethod();
        }
        @Test
        void testLessThanTwo() {
            Assertions.assertFalse(tidbConnectionTest.spilt("1m0s"));
        }
        @Test
        void testMoreThanThree() {
            Assertions.assertTrue(tidbConnectionTest.spilt("1d0h3m4s"));
        }
        @Test
        void testHNotNumber() {
            Assertions.assertFalse(tidbConnectionTest.spilt("2-0h0m0s"));
        }
        @Test
        void testHLess24() {
            Assertions.assertFalse(tidbConnectionTest.spilt("19h0m0s"));
        }
        @Test
        void testHMore24() {
            Assertions.assertTrue(tidbConnectionTest.spilt("25h0m0s"));
        }
    }
}