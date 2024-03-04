package io.tapdata.connector.gauss.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Date;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimeUtilTest {
    @Nested
    class ParseDataTest {
        Date assertVerify(String dataStr, String format) {
            try(MockedStatic<TimeUtil> util = mockStatic(TimeUtil.class)) {
                util.when(() -> TimeUtil.parseDate(dataStr,format)).thenCallRealMethod();
                return TimeUtil.parseDate(dataStr,format);
            }
        }
        @Test
        void testNullDateStr() {
            Assertions.assertDoesNotThrow(() -> {
                Date date = assertVerify(null, "yyyy-dd-MM");
                Assertions.assertNull(date);
            });
        }
        @Test
        void testNullFormatStr() {
            Assertions.assertDoesNotThrow(() -> {
                Date date = assertVerify("2024-03-01", null);
                Assertions.assertNull(date);
            });
        }
        @Test
        void testErrorFormatStr() {
            Assertions.assertThrows(RuntimeException.class, () -> {
                Date date = assertVerify("2024-03-01", "yyyy-dd-MMxjhio");
                Assertions.assertNotNull(date);
            });
        }
        @Test
        void testErrorDateStr() {
            Assertions.assertThrows(RuntimeException.class, () -> {
                Date date = assertVerify("2089sbb24-083-01", "yyyy-dd-MM");
                Assertions.assertNotNull(date);
            });
        }
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> {
                Date date = assertVerify("2024-083-01", "yyyy-dd-MM");
                Assertions.assertNotNull(date);
            });
        }
    }

    @Nested
    class ParseDataTimeTest {

    }

    @Nested
    class ParseTimestamp1Test {
        Date excepted;
        @BeforeEach
        void init() {
            excepted = mock(Date.class);
            when(excepted.getTime()).thenReturn(1L);
        }
        @Test
        public void testNullDate() {
            assertVerify(null, 0);
        }
        @Test
        public void testNotNullDate() {
            assertVerify(excepted, 1);
        }
        void assertVerify(Date date, int exceptedTimes) {
            try (MockedStatic<TimeUtil> util = mockStatic(TimeUtil.class)) {
                util.when(() -> TimeUtil.parseDate(anyString(), anyString())).thenReturn(date);
                util.when(() -> TimeUtil.parseTimestamp(anyString(), anyString(), anyLong())).thenCallRealMethod();
                long parseTimestamp = TimeUtil.parseTimestamp("2024-03-04", "yyyy-MM-dd", 1L);
                Assertions.assertEquals(1L, parseTimestamp);
                util.verify(() -> TimeUtil.parseDate(anyString(), anyString()), times(1));
            } finally {
                long time = verify(excepted, times(exceptedTimes)).getTime();
            }
        }
    }

    @Nested
    class ParseTimestamp2Test {

    }

    @Nested
    class ParseTimestamp3Test {

    }
}
