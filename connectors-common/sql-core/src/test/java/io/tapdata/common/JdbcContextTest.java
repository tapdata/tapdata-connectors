package io.tapdata.common;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.sql.SQLException;
import java.util.Properties;

import static org.mockito.Mockito.*;

public class JdbcContextTest {

    @Nested
    class HikariConnectionTest {
        @Nested
        class GetParamFromUrlTest {
            @Test
            void testDatabaseUrlIsNull() {
                Assertions.assertDoesNotThrow(() -> {
                    JdbcContext.HikariConnection.getParamFromUrl(null, "tag");
                });
            }

            @Test
            void testTagIsNull() {
                Assertions.assertDoesNotThrow(() -> {
                    JdbcContext.HikariConnection.getParamFromUrl("url", null);
                });
            }

            @Test
            void testDatabaseUrlNotContainTag() {
                Assertions.assertDoesNotThrow(() -> {
                    JdbcContext.HikariConnection.getParamFromUrl("url", "tag");
                });
            }

            @Test
            void testDatabaseUrlContainTag() {
                String key = JdbcContext.HikariConnection.getParamFromUrl("jdbc:mysql:127.0.0.1:3306?key=1", "key");
                Assertions.assertEquals("1", key);
            }

            @Test
            void testDatabaseUrlContainTag1() {
                String key = JdbcContext.HikariConnection.getParamFromUrl("jdbc:mysql:127.0.0.1:3306?key=2;number=1", "key");
                Assertions.assertEquals("2", key);
            }
        }

        @Nested
        class GetIntegerTest {
            @Test
            void testValueIsNull() {
                int key = JdbcContext.HikariConnection.getInteger(
                        "jdbc:mysql:127.0.0.1:3306?number=1",
                        "key",
                        100);
                Assertions.assertEquals(100, key);
            }

            @Test
            void testValueIsEmpty() {
                int key = JdbcContext.HikariConnection.getInteger(
                        "jdbc:mysql:127.0.0.1:3306?number=1;key=",
                        "key",
                        100);
                Assertions.assertEquals(100, key);
            }

            @Test
            void testException() {
                int key = JdbcContext.HikariConnection.getInteger(
                        "jdbc:mysql:127.0.0.1:3306?number=1;key=iii",
                        "key",
                        100);
                Assertions.assertEquals(100, key);
            }

            @Test
            void testNormal() {
                int key = JdbcContext.HikariConnection.getInteger(
                        "jdbc:mysql:127.0.0.1:3306?number=1;key=99",
                        "key",
                        100);
                Assertions.assertEquals(99, key);
            }
        }

        @Nested
        class ReplaceAllTest {
            @Test
            void testNormal() {
                Assertions.assertDoesNotThrow(() -> {
                    JdbcContext.HikariConnection.replaceAll("xx-xx-xx", "-", "_");
                });
            }
        }

        @Nested
        class GetHikariDataSourceTest {
            @Test
            void testNormal() {
                try (
                        MockedStatic<JdbcContext.HikariConnection> hikariConnectionMockedStatic = mockStatic(JdbcContext.HikariConnection.class);
                        HikariDataSource hikariDataSource = mock(HikariDataSource.class)
                ) {
                    hikariConnectionMockedStatic.when(JdbcContext.HikariConnection::create).thenReturn(hikariDataSource);
                    CommonDbConfig config = spy(CommonDbConfig.class);
                    config.setDatabase("test");
                    doAnswer(invocationOnMock -> null).when(hikariDataSource).setDriverClassName(anyString());
                    hikariConnectionMockedStatic.when(() -> JdbcContext.HikariConnection.getHikariDataSource(config)).thenCallRealMethod();
                    JdbcContext.HikariConnection.getHikariDataSource(config);
                    verify(config, times(1)).getDatabaseUrl();
                }
            }

            @Test
            void testNullConfig() {
                try (
                        MockedStatic<JdbcContext.HikariConnection> hikariConnectionMockedStatic = mockStatic(JdbcContext.HikariConnection.class);
                        HikariDataSource hikariDataSource = mock(HikariDataSource.class)
                ) {
                    hikariConnectionMockedStatic.when(JdbcContext.HikariConnection::create).thenReturn(hikariDataSource);
                    doAnswer(invocationOnMock -> null).when(hikariDataSource).setDriverClassName(anyString());
                    hikariConnectionMockedStatic.when(() -> JdbcContext.HikariConnection.getHikariDataSource(any())).thenCallRealMethod();
                    Assertions.assertThrows(IllegalArgumentException.class, () -> JdbcContext.HikariConnection.getHikariDataSource(null));
                }
            }

            @Test
            void testHasProperties() {
                try (
                        MockedStatic<JdbcContext.HikariConnection> hikariConnectionMockedStatic = mockStatic(JdbcContext.HikariConnection.class);
                        HikariDataSource hikariDataSource = mock(HikariDataSource.class)
                ) {
                    hikariConnectionMockedStatic.when(JdbcContext.HikariConnection::create).thenReturn(hikariDataSource);
                    CommonDbConfig config = spy(CommonDbConfig.class);
                    config.setDatabase("test");
                    Properties properties = new Properties();
                    properties.put("key", "value");
                    config.setProperties(properties);
                    doAnswer(invocationOnMock -> null).when(hikariDataSource).setDriverClassName(anyString());
                    hikariConnectionMockedStatic.when(() -> JdbcContext.HikariConnection.getHikariDataSource(config)).thenCallRealMethod();
                    JdbcContext.HikariConnection.getHikariDataSource(config);
                    verify(config, times(1)).getDatabaseUrl();
                }
            }

            @Test
            void testCreateHikariConnection() {
                try (
                        HikariDataSource hikariDataSource = JdbcContext.HikariConnection.create()
                ) {

                }
            }
        }
    }
    @Nested
    class QueryTimestampTest{
        @Test
        void test() throws SQLException {
            JdbcContext jdbcContext = mock(JdbcContext.class);
            doCallRealMethod().when(jdbcContext).queryTimestamp();
            Assertions.assertThrows(UnsupportedOperationException.class, jdbcContext::queryTimestamp);
        }

    }
}
