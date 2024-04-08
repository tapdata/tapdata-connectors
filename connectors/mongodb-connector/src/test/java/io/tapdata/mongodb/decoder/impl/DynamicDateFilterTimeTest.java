package io.tapdata.mongodb.decoder.impl;

import io.tapdata.entity.error.CoreException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DynamicDateFilterTimeTest {
    DynamicDateFilterTime dateFilterTime;
    @BeforeEach
    void init() {
        dateFilterTime = mock(DynamicDateFilterTime.class);
    }

    @Test
    void testGetFunctionName() {
        when(dateFilterTime.getFunctionName()).thenCallRealMethod();
        Assertions.assertEquals("$dynamicDate", dateFilterTime.getFunctionName());
    }

    @Test
    void testParam() {
        Assertions.assertEquals("$dynamicDate", DynamicDateFilterTime.DYNAMIC_DATE);
        Assertions.assertEquals("customFormat", DynamicDateFilterTime.CUSTOM_FORMAT);
        Assertions.assertEquals("toStringFormat", DynamicDateFilterTime.TO_STRING_FORMAT);
        Assertions.assertEquals("subtract", DynamicDateFilterTime.SUBTRACT);
        Assertions.assertEquals("yyyy-MM-dd HH:mm:ss.SSS", DynamicDateFilterTime.DEFAULT_DATE_FORMAT);
        Assertions.assertEquals("covertType", DynamicDateFilterTime.COVERT_TYPE);
        Assertions.assertEquals("date", DynamicDateFilterTime.TO_DATE);
        Assertions.assertEquals("timestamp", DynamicDateFilterTime.TO_TIMESTAMP);
        Assertions.assertEquals("string", DynamicDateFilterTime.TO_DATE_STRING);
    }

    @Nested
    class CovertTimeTest {
        Date covert;
        @BeforeEach
        void init() {
            covert = mock(Date.class);
            when(covert.getTime()).thenReturn(1L);
            when(dateFilterTime.getFunctionName()).thenReturn("name");

            when(dateFilterTime.covertTime(anyString(), anyInt(), anyString())).thenCallRealMethod();
        }

        void assertVerify(String dt, int time, String tsf, int getTimes, Class<?> excepted) {
            when(dateFilterTime.covertTime(dt, time, tsf)).thenCallRealMethod();
            Object o = dateFilterTime.covertTime(dt, time, tsf);
            Assertions.assertNotNull(o);
            Assertions.assertEquals(excepted.getName(), o.getClass().getName());
            verify(dateFilterTime, times(getTimes)).getFunctionName();
        }

        @Test
        void testDateTimeIsNullAndToStringFormatIsNul() {
            Assertions.assertDoesNotThrow(() -> {
               assertVerify(null, 0, null, 0, Date.class);
            });
        }

        @Test
        void testDateTimeIsNull() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(null, 0, "yyyy-MM-dd", 0, String.class);
            });
        }

        @Test
        void testParseDateTimeThrowException() {
            Assertions.assertThrows(CoreException.class, () -> {
                assertVerify("error-format", 0, "yyyy-MM-dd", 1, Date.class);
            });
        }

        @Test
        void testToStringFormatIsNull() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify("yyyy-MM-dd 00:00:00.000", 0, null, 0, Date.class);
            });
        }

        @Test
        void testToStringFormatParseThrowException() {
            Assertions.assertDoesNotThrow(() -> {
                assertVerify("yyyy-MM-dd 00:00:00.000", 0, "$", 0, String.class);
            });
        }
    }

    @Nested
    class ExecuteTest {
        DynamicDateFilterTime filterTime;
        @BeforeEach
        void init() {
            filterTime = mock(DynamicDateFilterTime.class);

            when(filterTime.execute(any(Object.class), anyMap())).thenCallRealMethod();
            when(filterTime.covertTime(anyString(), anyInt(), anyString())).thenReturn("");
            Date d = mock(Date.class);
            when(d.getTime()).thenReturn(1L);
            when(filterTime.covertTime(anyString(), anyInt(), any())).thenReturn(d);
        }
        @Nested
        class FunctionObjIsMapTest {
            Map<String, Object> map;
            @BeforeEach
            void init() {
                map = mock(Map.class);
                when(map.get(DynamicDateFilterTime.CUSTOM_FORMAT)).thenReturn("2024-03-20 00:00:00.000");
                when(map.get(DynamicDateFilterTime.TO_STRING_FORMAT)).thenReturn("yyyy-MM-dd");
                when(map.get(DynamicDateFilterTime.SUBTRACT)).thenReturn(1000);
                when(map.get(DynamicDateFilterTime.COVERT_TYPE)).thenReturn(null);
            }
            @Test
            void testCustomFormatIsNull(){
                when(filterTime.covertTime(null, 1000, "yyyy-MM-dd")).thenReturn("");
                when(map.get(DynamicDateFilterTime.CUSTOM_FORMAT)).thenReturn(null);
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTime(null, 1000, "yyyy-MM-dd");
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }
            @Test
            void testCustomFormatIsDateString(){
                when(map.get(DynamicDateFilterTime.CUSTOM_FORMAT)).thenReturn("yyyy-MM-dd 00:00:00.000");
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTime(anyString(), anyInt(), anyString());
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }


            @Test
            void testCustomFormatIsFailedDateString(){
                when(map.get(DynamicDateFilterTime.CUSTOM_FORMAT)).thenReturn("yyyy-MM-dd");
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTime(anyString(), anyInt(), anyString());
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }

            @Test
            void testCustomFormatNotString(){
                when(map.get(DynamicDateFilterTime.CUSTOM_FORMAT)).thenReturn(0);
                Assertions.assertThrows(IllegalArgumentException.class, () -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(0)).covertTime(anyString(), anyInt(), anyString());
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }

            @Test
            void testToStringFormatIsNull(){
                when(filterTime.covertTime("2024-03-20 00:00:00.000", 1000, null)).thenReturn("");
                when(map.get(DynamicDateFilterTime.TO_STRING_FORMAT)).thenReturn(null);
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTime("2024-03-20 00:00:00.000", 1000, null);
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }
            @Test
            void testToStringFormatIsString(){
                when(map.get(DynamicDateFilterTime.TO_STRING_FORMAT)).thenReturn("yyyy-MM-dd");
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTime(anyString(), anyInt(), anyString());
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }
            @Test
            void testToStringFormatNotString(){
                when(map.get(DynamicDateFilterTime.TO_STRING_FORMAT)).thenReturn(0);
                Assertions.assertThrows(IllegalArgumentException.class, () -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(0)).covertTime(anyString(), anyInt(), anyString());
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }

            @Test
            void testSubtractIsNull(){
                when(map.get(DynamicDateFilterTime.SUBTRACT)).thenReturn(null);
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTime(anyString(), anyInt(), anyString());
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }
            @Test
            void testSubtractIsNumberString(){
                when(map.get(DynamicDateFilterTime.SUBTRACT)).thenReturn("1000");
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTime(anyString(), anyInt(), anyString());
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }
            @Test
            void testSubtractIsNumber(){
                when(map.get(DynamicDateFilterTime.SUBTRACT)).thenReturn(1000);
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTime(anyString(), anyInt(), anyString());
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }
            @Test
            void testSubtractNotNumberString(){
                when(map.get(DynamicDateFilterTime.SUBTRACT)).thenReturn("code");
                Assertions.assertThrows(IllegalArgumentException.class, () -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(0)).covertTime(anyString(), anyInt(), anyString());
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }

            @Test
            void testToDate() {
                when(map.get(DynamicDateFilterTime.COVERT_TYPE)).thenReturn(DynamicDateFilterTime.TO_DATE);
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTime(anyString(), anyInt(), any());
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }
            @Test
            void testToString() {
                when(map.get(DynamicDateFilterTime.COVERT_TYPE)).thenReturn(DynamicDateFilterTime.TO_DATE_STRING);
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTime(anyString(), anyInt(), anyString());
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }
            @Test
            void testToTimestamp() {
                when(map.get(DynamicDateFilterTime.COVERT_TYPE)).thenReturn(DynamicDateFilterTime.TO_TIMESTAMP);
                Assertions.assertDoesNotThrow(() -> filterTime.execute(map, mock(Map.class)));
                verify(filterTime, times(1)).covertTime(anyString(), anyInt(), any());
                verify(map, times(1)).get(DynamicDateFilterTime.CUSTOM_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.TO_STRING_FORMAT);
                verify(map, times(1)).get(DynamicDateFilterTime.SUBTRACT);
            }
        }

        @Nested
        class FunctionObjNotMapTest {
            @Test
            void testFilterIsNull() {
                when(filterTime.covertTime(null, 0, null)).thenReturn("");
                Assertions.assertDoesNotThrow(() -> filterTime.execute("map", mock(Map.class)));
                verify(filterTime, times(1)).covertTime(null, 0, null);
            }
        }
    }

}
