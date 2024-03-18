package io.tapdata.connector.gauss.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Date;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
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
        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> {
                TimeUtil.parseDateTime("2024-03-08 16:47:55.666", 3, false);
            });
        }
        @Test
        void testFactionLessThanZero() {
            Assertions.assertDoesNotThrow(() -> {
                TimeUtil.parseDateTime("2024-03-08 16:47:55.666", 0, false);
            });
        }
        @Test
        void testFactionMoreThanZeroNotContainMillSec() {
            Assertions.assertDoesNotThrow(() -> {
                TimeUtil.parseDateTime("2024-03-08 16:47:55", 3, false);
            });
        }
        @Test
        void testFactionMoreThanZeroContainMillSecMore3() {
            Assertions.assertDoesNotThrow(() -> {
                TimeUtil.parseDateTime("2024-03-08 16:47:00.", 3, false);
            });
        }
        @Test
        void testFactionLessThanZeroAndNotContainMillSec() {
            Assertions.assertDoesNotThrow(() -> {
                TimeUtil.parseDateTime("2024-03-08 16:47:55", 0, false);
            });
        }
        @Test
        void testWithZone1() {
            Assertions.assertDoesNotThrow(() -> {
                TimeUtil.parseDateTime("2024-03-08 16:47:00.555+8:00", 3, true);
            });
        }
        @Test
        void testWithZone2() {
            Assertions.assertDoesNotThrow(() -> {
                TimeUtil.parseDateTime("2024-03-08 16:47:00.555-8:00", 3, true);
            });
        }
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
        void assertVerify(String str, long timestamp, int t1, int t2) {
            try(MockedStatic<TimeUtil> t = mockStatic(TimeUtil.class)) {
                t.when(() -> TimeUtil.parseTimestamp(anyString(), anyString(), anyInt())).thenReturn(timestamp);
                t.when(() -> TimeUtil.parseTimestamp(any(String[].class), anyLong(), anyInt(), anyBoolean())).thenReturn(timestamp);
                t.when(() -> TimeUtil.parseTimestamp(str, 1)).thenCallRealMethod();
                long l = TimeUtil.parseTimestamp(str, 1);
            }
        }
        @Test
        void testSplitLengthLessThanZero() {
            assertVerify("", 100L, 1, 1);
        }
        @Test
        void testDataStrIsNull() {
            assertVerify(null, 100L, 1, 1);
        }
        @Test
        void testSplitLengthLessThanOne() {
            assertVerify("2024-03-08 16:26:00", 0L, 1, 1);
        }
        @Test
        void testContainsAdd() {
            assertVerify("2024-03-08 16:26:00.1+8", 100l, 1, 1);
        }
        @Test
        void testContainsInc() {
            assertVerify("2024-03-08 16:26:00.1-8", 100L, 1, 1);
        }
        @Test
        void testContainsOther() {
            assertVerify("2024-03-08 16:26:00.18", 100L, 1, 1);
        }
    }

    @Nested
    class ParseTimestamp3Test {
        @Test
        void testSplit1LengthLessThanZero() {
            long l = TimeUtil.parseTimestamp(new String[0], 0L, 0, false);
            Assertions.assertEquals(0L, l);
        }
        @Test
        void testSplit1LengthLessThanOne() {
            long l = TimeUtil.parseTimestamp(new String[]{"100"}, 0L, 1, false);
            Assertions.assertEquals(100L, l);
        }
        @Test
        void testSplit1LengthMoreThanOne() {
            long l = TimeUtil.parseTimestamp(new String[]{"3600000", "1"}, 0L, 1, false);
            Assertions.assertEquals(7200000L, l);
        }
        @Test
        void testSplit1LengthMoreThanOneAdd() {
            long l = TimeUtil.parseTimestamp(new String[]{"3600000", "1"}, 0L, 1, true);
            Assertions.assertEquals(0L, l);
        }
    }
}
