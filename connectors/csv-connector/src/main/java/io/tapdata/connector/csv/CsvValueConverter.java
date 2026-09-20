package io.tapdata.connector.csv;

import io.tapdata.common.util.MatchUtil;
import io.tapdata.util.DateUtil;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class CsvValueConverter {
    static final String DATE = "DATE";
    static final String TIME = "TIME";
    static final String DATETIME = "DATETIME";

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

    private static LocalDateTime parseDateTime(String value) {
        if (FLEXIBLE_DATETIME.matcher(value).matches()) {
            return parseFlexibleDateTime(value);
        }
        String dateFormat = DateUtil.determineDateFormat(value);
        if (dateFormat == null) {
            throw new DateTimeParseException("Unsupported CSV datetime format", value, 0);
        }
        return LocalDateTime.parse(value, DateTimeFormatter.ofPattern(dateFormat));
    }

    private static LocalDateTime parseFlexibleDateTime(String value) {
        Matcher matcher = FLEXIBLE_DATETIME.matcher(value);
        if (!matcher.matches()) {
            throw new DateTimeParseException("Unsupported CSV datetime format", value, 0);
        }

        int year = Integer.parseInt(matcher.group(1));
        if (year < 1000) {
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
        return LocalDateTime.of(date, time);
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
        if (parts[0].length() == 4) {
            return LocalDate.of(first, second, third);
        }
        if ("/".equals(separator)) {
            return LocalDate.of(third, first, second);
        }
        return LocalDate.of(third, second, first);
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
