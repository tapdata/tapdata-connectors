package io.tapdata.connector.csv;

import io.tapdata.common.util.MatchUtil;
import io.tapdata.util.DateUtil;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class CsvValueConverter {
    static final String DATE = "DATE";
    static final String TIME = "TIME";
    static final String DATETIME = "DATETIME";
    private static final int MIN_SUPPORTED_YEAR = 1000;
    private static final int MAX_SUPPORTED_YEAR = 9999;

    private static final Pattern DATE_ONLY = Pattern.compile("^(?:\\d{4}[-/]\\d{1,2}[-/]\\d{1,2}|\\d{1,2}-\\d{1,2}-\\d{4}|\\d{1,2}/\\d{1,2}/\\d{4})$");
    private static final Pattern TIME_ONLY = Pattern.compile("^\\d{1,2}:\\d{1,2}:\\d{1,2}(?:\\.\\d{1,9})?$");
    private static final Pattern FLEXIBLE_DATETIME = Pattern.compile("^(\\d{4})([-/])(\\d{1,2})\\2(\\d{1,2})[ T](\\d{1,2}):(\\d{1,2}):(\\d{1,2})(?:\\.(\\d{1,9}))?$");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_TIME;

    private CsvValueConverter() {
    }

    static String inferDataType(Object rawValue) {
        if (rawValue instanceof Map) {
            return "OBJECT";
        }
        if (rawValue instanceof Collection || (rawValue != null && rawValue.getClass().isArray())) {
            return "ARRAY";
        }

        String value = rawValue == null ? "" : String.valueOf(rawValue);
        if (value.isEmpty()) {
            return "STRING";
        }
        if (isDate(value)) {
            return DATE;
        }
        if (isTime(value)) {
            return TIME;
        }
        if (MatchUtil.matchBoolean(value)) {
            return "BOOLEAN";
        }
        if (MatchUtil.matchInteger(value)) {
            return "INTEGER";
        }
        if (MatchUtil.matchNumber(value)) {
            return "NUMBER";
        }
        if (isDateTime(value)) {
            return DATETIME;
        }
        return value.length() > 200 ? "TEXT" : "STRING";
    }

    static Object parse(String value, String dataType) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        switch (dataType) {
            case DATE:
                try {
                    return parseDate(value);
                } catch (RuntimeException e) {
                    //非法日期触发“日期/时间解析失败”会被静默转换为 null是预期结果，非法日期不能变成字符串输出，按字符串原样会导致字段类型和结果不一致。
                    return null;
                }
            case TIME:
                try {
                    return parseTime(value);
                } catch (RuntimeException e) {
                    return null;
                }
            case DATETIME:
                try {
                    return parseDateTime(value);
                } catch (RuntimeException e) {
                    return null;
                }
            case "BOOLEAN":
                return "true".equalsIgnoreCase(value);
            case "INTEGER":
                return Integer.parseInt(value);
            case "NUMBER":
                return new BigDecimal(value);
            default:
                return value;
        }
    }

    private static boolean isDate(String value) {
        if (!DATE_ONLY.matcher(value).matches()) {
            return false;
        }
        try {
            parseDate(value);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static boolean isTime(String value) {
        if (!TIME_ONLY.matcher(value).matches()) {
            return false;
        }
        try {
            parseTime(value);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static boolean isDateTime(String value) {
        if (FLEXIBLE_DATETIME.matcher(value).matches()) {
            try {
                parseFlexibleDateTime(value);
                return true;
            } catch (RuntimeException e) {
                return false;
            }
        }
        String dateFormat = DateUtil.determineDateFormat(value);
        if (dateFormat == null || !dateFormat.contains("H")
                || (!dateFormat.contains("y") && !dateFormat.contains("M") && !dateFormat.contains("d"))) {
            return false;
        }
        try {
            DateUtil.parseInstant(value);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static Instant parseDateTime(String value) {
        if (FLEXIBLE_DATETIME.matcher(value).matches()) {
            return parseFlexibleDateTime(value);
        }
        String dateFormat = DateUtil.determineDateFormat(value);
        if (dateFormat == null) {
            throw new DateTimeParseException("Unsupported CSV datetime format", value, 0);
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateFormat);
        TemporalAccessor parsed = formatter.parse(value);
        // Preserve an explicit offset instead of reducing the value to a LocalDateTime.
        if (parsed.isSupported(ChronoField.INSTANT_SECONDS)) {
            return Instant.from(parsed);
        }
        LocalDateTime localDateTime;
        if (dateFormat.contains("H")) {
            localDateTime = LocalDateTime.from(parsed);
        } else {
            localDateTime = LocalDate.from(parsed).atStartOfDay();
        }
        if (dateFormat.contains("'Z'")) {
            return localDateTime.toInstant(ZoneOffset.UTC);
        }
        return toSystemDefaultInstant(localDateTime);
    }

    private static Instant parseFlexibleDateTime(String value) {
        Matcher matcher = FLEXIBLE_DATETIME.matcher(value);
        if (!matcher.matches()) {
            throw new DateTimeParseException("Unsupported CSV datetime format", value, 0);
        }

        int year = Integer.parseInt(matcher.group(1));
        if (!isSupportedYear(year)) {
            throw new DateTimeException("CSV datetime year is outside the supported range");
        }
        LocalDate date = LocalDate.of(year, Integer.parseInt(matcher.group(3)), Integer.parseInt(matcher.group(4)));
        StringBuilder timeValue = new StringBuilder()
                .append(matcher.group(5)).append(':')
                .append(matcher.group(6)).append(':')
                .append(matcher.group(7));
        if (matcher.group(8) != null) {
            timeValue.append('.').append(matcher.group(8));
        }
        LocalTime time = parseTime(timeValue.toString());
        return toSystemDefaultInstant(LocalDateTime.of(date, time));
    }

    private static Instant toSystemDefaultInstant(LocalDateTime value) {
        // CSV values without an offset are wall-clock values; resolve them before DateTime(LocalDateTime) can assume UTC.
        return value.atZone(ZoneId.systemDefault()).toInstant();
    }

    private static LocalDate parseDate(String value) {
        String separator = value.indexOf('/') >= 0 ? "/" : "-";
        String[] parts = value.split(separator);
        if (parts.length != 3) {
            throw new DateTimeParseException("Unsupported CSV date format", value, 0);
        }

        int first = Integer.parseInt(parts[0]);
        int second = Integer.parseInt(parts[1]);
        int third = Integer.parseInt(parts[2]);
        LocalDate date;
        if (parts[0].length() == 4) {
            date = LocalDate.of(first, second, third);
        } else if ("/".equals(separator)) {
            date = LocalDate.of(third, first, second);
        } else {
            date = LocalDate.of(third, second, first);
        }
        if (!isSupportedYear(date.getYear())) {
            // Keep CSV parsing and the DATE codec aligned with the supported range in spec_csv.json.
            throw new DateTimeParseException("CSV date year is outside the supported range", value, 0);
        }
        return date;
    }

    private static boolean isSupportedYear(int year) {
        return year >= MIN_SUPPORTED_YEAR && year <= MAX_SUPPORTED_YEAR;
    }

    private static LocalTime parseTime(String value) {
        String[] timeAndFraction = value.split("\\.", -1);
        if (timeAndFraction.length > 2) {
            throw new DateTimeParseException("Unsupported CSV time format", value, 0);
        }
        String[] parts = timeAndFraction[0].split(":", -1);
        if (parts.length != 3) {
            throw new DateTimeParseException("Unsupported CSV time format", value, 0);
        }
        String normalized = String.format(Locale.ROOT, "%02d:%02d:%02d",
                Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
        if (timeAndFraction.length == 2) {
            normalized += "." + timeAndFraction[1];
        }
        return LocalTime.parse(normalized, TIME_FORMATTER);
    }
}
