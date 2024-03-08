package io.tapdata.common;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class JdbcContextTest {

    @Nested
    class HikariConnectionTest {
//
//        @Nested
//        class GetHikariDataSourceTest {
//            CommonDbConfig config;
//            @BeforeEach
//            void init() {
//                config = mock(CommonDbConfig.class);
//
//                when(config.getJdbcDriver()).thenReturn("");
//                when(config.getDatabaseUrl()).thenReturn("");
//                when(config.getUser()).thenReturn("");
//                when(config.getPassword()).thenReturn("");
//                when(config.getProperties()).thenReturn(mock(Properties.class));
//            }
//            void assertVerify(CommonDbConfig c, int jd, int db, int u, int pwd, int pro) {
//                try (MockedStatic<JdbcContext.HikariConnection> jk = mockStatic(JdbcContext.HikariConnection.class)){
//                    jk.when(() -> JdbcContext.HikariConnection.getHikariDataSource(c)).thenCallRealMethod();
//
//                    HikariDataSource source = JdbcContext.HikariConnection.getHikariDataSource(c);
//                    Assertions.assertNotNull(source);
//
//                    verify(config, times(jd)).getJdbcDriver();
//                    verify(config, times(db)).getDatabaseUrl();
//                    verify(config, times(u)).getUser();
//                    verify(config, times(pwd)).getPassword();
//                    verify(config, times(pro)).getProperties();
//                }
//            }
//
//            @Test
//            void testCommonDbConfigIsNull() {
//                Assertions.assertThrows(IllegalArgumentException.class, () -> {
//                    assertVerify(null , 0, 0, 0, 0, 0);
//                });
//            }
//            @Test
//            void testPropertiesIsNull() {
//                when(config.getProperties()).thenReturn(null);
//                Assertions.assertDoesNotThrow(() -> {
//                    assertVerify(null , 1, 1, 1, 1, 1);
//                });
//            }
//            @Test
//            void testNormal() {
//                Assertions.assertDoesNotThrow(() -> {
//                    assertVerify(config , 1, 1, 1, 1, 2);
//                });
//            }
//        }

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
    }
}
