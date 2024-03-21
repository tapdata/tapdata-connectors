package io.tapdata.mongodb.decoder.impl;

import io.tapdata.entity.error.CoreException;
import io.tapdata.util.DateUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Date;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutoUpdateDateFilterTimeTest {
    AutoUpdateDateFilterTime dateFilterTime;
    @BeforeEach
    void init() {
        dateFilterTime = mock(AutoUpdateDateFilterTime.class);
    }

    @Test
    void testGetFunctionName() {
        when(dateFilterTime.getFunctionName()).thenCallRealMethod();
        Assertions.assertEquals("$dynamicDate", dateFilterTime.getFunctionName());
    }

    @Test
    void testParam() {
        Assertions.assertEquals("$dynamicDate", AutoUpdateDateFilterTime.DYNAMIC_DATE);
        Assertions.assertEquals("format", AutoUpdateDateFilterTime.FORMAT);
        Assertions.assertEquals("toString", AutoUpdateDateFilterTime.TO_STRING);
        Assertions.assertEquals("subtract", AutoUpdateDateFilterTime.SUBTRACT);
    }

    @Nested
    class CovertTimestampTest {
        @BeforeEach
        void init() {
            when(dateFilterTime.covert(any(Date.class), anyLong())).thenReturn(mock(Date.class));
            when(dateFilterTime.covertTimestamp(anyLong(), anyLong())).thenCallRealMethod();
            when(dateFilterTime.getFunctionName()).thenReturn("name");
        }

        @Test
        void testNormal(){
            Assertions.assertDoesNotThrow(() -> dateFilterTime.covertTimestamp(System.currentTimeMillis(), 100L));
            verify(dateFilterTime, times(0)).getFunctionName();
        }

        @Test
        void testDateTimeLessThanZero(){
            Assertions.assertThrows(CoreException.class, () -> dateFilterTime.covertTimestamp(-1L, 100L));
            verify(dateFilterTime, times(2)).getFunctionName();
        }
    }

    @Nested
    class CovertTimeTest {
        Date covert;
        @BeforeEach
        void init() {
            covert = mock(Date.class);
            when(covert.getTime()).thenReturn(1L);
            when(dateFilterTime.replaceDate(anyString())).thenReturn("2024-03-20");
            when(dateFilterTime.getFunctionName()).thenReturn("name");

            when(dateFilterTime.covertTime(anyString(), anyLong(), anyBoolean())).thenCallRealMethod();
            when(dateFilterTime.covert(any(Object.class), anyLong())).thenReturn(covert);
        }
        void assertVerify(String dateFormat, boolean toString, Class<?> excepted) {
            try (MockedStatic<DateUtil> du = mockStatic(DateUtil.class)){
                du.when(() -> DateUtil.determineDateFormat(anyString())).thenReturn(dateFormat);
                du.when(() -> DateUtil.parse(anyString())).thenReturn(covert);
                du.when(() -> DateUtil.timeStamp2Date(anyString(), anyString())).thenReturn("dateStr");
                Object time = dateFilterTime.covertTime("", 0, toString);
                Assertions.assertNotNull(time);
                Assertions.assertEquals(excepted.getName(), time.getClass().getName());
                du.verify(() -> DateUtil.determineDateFormat(anyString()), times(1));
                du.verify(() -> DateUtil.parse(anyString()), times(null == dateFormat? 0 :1));
                du.verify(() -> DateUtil.timeStamp2Date(anyString(), anyString()), times(null == dateFormat? 0 :1));
            }
            verify(dateFilterTime, times(1)).replaceDate(anyString());
            verify(dateFilterTime, times(null == dateFormat ? 0 : 1)).covert(any(Object.class), anyLong());
        }

        @Test
        void testFormatStringIsNull() {
            Assertions.assertThrows(CoreException.class, () -> assertVerify(null, false, null));
        }

        @Test
        void testToStringIsTrue(){
            Assertions.assertDoesNotThrow(() -> assertVerify("", true, String.class));
        }

        @Test
        void testNormal() {
            Assertions.assertDoesNotThrow(() -> assertVerify("", false, Date.class));
        }
    }

    @Nested
    class FormatTest {
        @BeforeEach
        void init() {
            when(dateFilterTime.format(anyString(), anyString(), anyString())).thenCallRealMethod();
        }

        @Test
        void testNormal() {
            String format = dateFilterTime.format("aaaYYY", "YYY", "");
            Assertions.assertEquals("aaa", format);
        }

        @Test
        void testNotContains() {
            String format = dateFilterTime.format("good", "yh", "");
            Assertions.assertEquals("good", format);
        }
    }

    @Nested
    class CovertTest {
        Date res;
        @BeforeEach
        void init() {
            res = mock(Date.class);

            when(dateFilterTime.calculateDate(any(Date.class), anyLong())).thenReturn(res);
            when(dateFilterTime.covert(any(Date.class), anyLong())).thenCallRealMethod();
            when(dateFilterTime.covert(any(String.class), anyLong())).thenCallRealMethod();
        }

        @Test
        void testCovertDate() {
            Assertions.assertDoesNotThrow(() -> dateFilterTime.covert(mock(Date.class), 0L));
        }

        @Test
        void testCovertNotDate() {
            Assertions.assertThrows(CoreException.class, () -> dateFilterTime.covert("", 0L));
        }
    }

    @Nested
    class ExecuteTest {
        AutoUpdateDateFilterTime filterTime;
        @BeforeEach
        void init() {
            filterTime = mock(AutoUpdateDateFilterTime.class);

            when(filterTime.execute(any(Object.class), anyMap())).thenCallRealMethod();
            when(filterTime.covertTimestamp(anyLong(), anyLong())).thenReturn(0);
            when(filterTime.covertTime(anyString(), anyLong(), anyBoolean())).thenReturn("");
        }
        @Nested
        class FunctionObjIsMapTest {
            Map<String, Object> map;
            @BeforeEach
            void init() {
                map = mock(Map.class);
                when(map.get(AutoUpdateDateFilterTime.FORMAT)).thenReturn("2024-03-20");
                when(map.get(AutoUpdateDateFilterTime.TO_STRING)).thenReturn(false);
                when(map.get(AutoUpdateDateFilterTime.SUBTRACT)).thenReturn(1000L);
            }
            @Test
            void testSubtractIsNumber() {
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(0)).covertTimestamp(anyLong(), anyLong());
                verify(filterTime, times(1)).covertTime(anyString(), anyLong(), anyBoolean());
            }
            @Test
            void testSubtractIsNumberString() {
                when(map.get(AutoUpdateDateFilterTime.SUBTRACT)).thenReturn("1000");
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(0)).covertTimestamp(anyLong(), anyLong());
                verify(filterTime, times(1)).covertTime(anyString(), anyLong(), anyBoolean());
            }
            @Test
            void testSubtractNotNumber() {
                when(map.get(AutoUpdateDateFilterTime.SUBTRACT)).thenReturn("uuu");
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(0)).covertTimestamp(anyLong(), anyLong());
                verify(filterTime, times(1)).covertTime(anyString(), anyLong(), anyBoolean());
            }

            @Test
            void testFilterIsNumber() {
                when(map.get(AutoUpdateDateFilterTime.FORMAT)).thenReturn(System.currentTimeMillis());
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTimestamp(anyLong(), anyLong());
                verify(filterTime, times(0)).covertTime(anyString(), anyLong(), anyBoolean());
            }

            @Test
            void testFilterIsNumberString() {
                when(map.get(AutoUpdateDateFilterTime.FORMAT)).thenReturn(String.valueOf(System.currentTimeMillis()));
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTimestamp(anyLong(), anyLong());
                verify(filterTime, times(0)).covertTime(anyString(), anyLong(), anyBoolean());
            }

            @Test
            void testFilterIsDateString() {
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(0)).covertTimestamp(anyLong(), anyLong());
                verify(filterTime, times(1)).covertTime(anyString(), anyLong(), anyBoolean());
            }

            @Test
            void testFilterIsNull() {
                when(map.get(AutoUpdateDateFilterTime.FORMAT)).thenReturn(null);
                Assertions.assertThrows(IllegalArgumentException.class, () -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(0)).covertTimestamp(anyLong(), anyLong());
                verify(filterTime, times(0)).covertTime(anyString(), anyLong(), anyBoolean());
            }
        }

        @Nested
        class FunctionObjNotMapTest {
            @Test
            void testFilterIsNull() {
                Assertions.assertThrows(IllegalArgumentException.class, () -> filterTime.execute("map", mock(Map.class)));
                verify(filterTime, times(0)).covertTimestamp(anyLong(), anyLong());
                verify(filterTime, times(0)).covertTime(anyString(), anyLong(), anyBoolean());
            }
        }
    }

}
