package io.tapdata.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
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
    }
}
