package io.tapdata.connector.csv;

import io.tapdata.util.DateUtil;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvValueConverterTest {

    @Test
    void inferDataTypeCoversContainerAndScalarValues() {
        assertEquals("OBJECT", CsvValueConverter.inferDataType(new HashMap<>()));
        assertEquals("ARRAY", CsvValueConverter.inferDataType(Collections.singletonList("value")));
        assertEquals("ARRAY", CsvValueConverter.inferDataType(new String[]{"value"}));
        assertEquals("STRING", CsvValueConverter.inferDataType(null));
        assertEquals("STRING", CsvValueConverter.inferDataType(""));
        assertEquals("BOOLEAN", CsvValueConverter.inferDataType("true"));
        assertEquals("INTEGER", CsvValueConverter.inferDataType("42"));
        assertEquals("NUMBER", CsvValueConverter.inferDataType("42.5"));
        assertEquals("DATETIME", CsvValueConverter.inferDataType("17-05-2024 12:30:45"));
        assertEquals("STRING", CsvValueConverter.inferDataType("17 May 2024"));
        assertEquals("STRING", CsvValueConverter.inferDataType("25:00:00"));
        assertEquals("STRING", CsvValueConverter.inferDataType("2024-02-30 12:00:00"));
        assertEquals("TEXT", CsvValueConverter.inferDataType(repeat('x', 201)));
    }

    @Test
    void parseCoversEmptyAndScalarTypes() {
        assertNull(CsvValueConverter.parse(null, CsvValueConverter.DATE));
        assertNull(CsvValueConverter.parse("", CsvValueConverter.DATE));
        assertNull(CsvValueConverter.parse("25:00:00", CsvValueConverter.TIME));
        assertNull(CsvValueConverter.parse("12:00", CsvValueConverter.TIME));
        assertNull(CsvValueConverter.parse("12:00:00.1.2", CsvValueConverter.TIME));
        assertTrue((Boolean) CsvValueConverter.parse("true", "BOOLEAN"));
        assertFalse((Boolean) CsvValueConverter.parse("false", "BOOLEAN"));
        assertEquals(42, CsvValueConverter.parse("42", "INTEGER"));
        assertEquals(new BigDecimal("42.5"), CsvValueConverter.parse("42.5", "NUMBER"));
        assertEquals("value", CsvValueConverter.parse("value", "STRING"));
        assertNull(CsvValueConverter.parse("unsupported", CsvValueConverter.DATETIME));
    }

    @Test
    void parseCoversDateTimeAndDateFallbackBranches() {
        assertEquals(Instant.parse("2024-06-17T12:30:45Z"),
                CsvValueConverter.parse("2024-06-17T12:30:45Z", CsvValueConverter.DATETIME));
        assertNull(CsvValueConverter.parse("0999-12-31 12:00:00", CsvValueConverter.DATETIME));
        assertNull(CsvValueConverter.parse("0999-12-31 12:00", CsvValueConverter.DATE));
        assertNull(CsvValueConverter.parse("20240101", CsvValueConverter.DATE));
        assertNull(CsvValueConverter.parse("123456", CsvValueConverter.DATE));
        assertEquals(LocalDate.of(2024, 8, 8),
                CsvValueConverter.parse("8/8/2024", CsvValueConverter.DATE));
        assertEquals(LocalDate.of(2024, 8, 8),
                CsvValueConverter.parse("8-8-2024", CsvValueConverter.DATE));
    }

    @Test
    void parseDateTimeKeepsWallClockValueForFormatsWithoutOffset() {
        // "dd-MM-yyyy HH:mm:ss" 不匹配 FLEXIBLE_DATETIME，走 determineDateFormat 分支
        assertEquals(LocalDateTime.of(2024, 5, 17, 12, 30, 45),
                CsvValueConverter.parse("17-05-2024 12:30:45", CsvValueConverter.DATETIME));
        // FLEXIBLE_DATETIME 分支：不带时区的墙钟值按本地 LocalDateTime 返回
        assertEquals(LocalDateTime.of(2024, 6, 17, 12, 30, 45),
                CsvValueConverter.parse("2024-06-17 12:30:45", CsvValueConverter.DATETIME));
        assertEquals(LocalDateTime.of(2024, 6, 17, 12, 30, 45),
                CsvValueConverter.parse("2024-06-17T12:30:45", CsvValueConverter.DATETIME));
    }

    @Test
    void privateDefensiveBranchesAreCovered() throws Exception {
        assertThrows(DateTimeException.class,
                () -> invoke("parseFlexibleDateTime", new Class[]{String.class}, "not-a-datetime"));
        assertFalse((Boolean) invoke("isDateTime", new Class[]{String.class}, "123456"));
        assertFalse((Boolean) invoke("isSupportedYear", new Class[]{int.class}, 10000));
        assertThrows(Exception.class,
                () -> invoke("parseDateOnly", new Class[]{String.class}, "2024-01"));
    }

    @Test
    void dateFormatGuardHandlesPartialCalendarFormats() throws Exception {
        Field dateFormatsField = DateUtil.class.getDeclaredField("DATE_FORMAT_REGEXPS");
        dateFormatsField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, String> dateFormats = (Map<String, String>) dateFormatsField.get(null);
        String monthOnlyValue = "May 12";
        String dayOnlyValue = "12 12";
        String monthOnlyPattern = "^May 12$";
        String dayOnlyPattern = "^12 12$";
        dateFormats.put(monthOnlyPattern, "MMM HH");
        dateFormats.put(dayOnlyPattern, "dd HH");
        try {
            assertFalse((Boolean) invoke("isDateTime", new Class[]{String.class}, monthOnlyValue));
            assertFalse((Boolean) invoke("isDateTime", new Class[]{String.class}, dayOnlyValue));
            assertThrows(DateTimeException.class,
                    () -> invoke("parseDate", new Class[]{String.class}, monthOnlyValue));
            assertThrows(DateTimeException.class,
                    () -> invoke("parseDate", new Class[]{String.class}, dayOnlyValue));
        } finally {
            dateFormats.remove(monthOnlyPattern);
            dateFormats.remove(dayOnlyPattern);
        }
    }

    private static Object invoke(String methodName, Class<?>[] parameterTypes, Object... arguments) throws Exception {
        Method method = CsvValueConverter.class.getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        try {
            return method.invoke(null, arguments);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw e;
        }
    }

    private static String repeat(char value, int count) {
        char[] values = new char[count];
        Arrays.fill(values, value);
        return new String(values);
    }
}
