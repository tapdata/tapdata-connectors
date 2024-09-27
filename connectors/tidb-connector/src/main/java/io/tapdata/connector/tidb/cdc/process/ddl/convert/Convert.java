package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import io.tapdata.entity.error.CoreException;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

public interface Convert {
    String COLUMN_NAME = "ColumnName";
    String COLUMN_TYPE = "ColumnType";
    String COLUMN_PRECISION = "ColumnPrecision";
    String COLUMN_SCALE = "ColumnScale";

    Object convert(Object fromValue);

    default int parseInt(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    default Object covertToDate(Object fromValue, int precision, String format, TimeZone timezone) {
        if (precision < 0) precision = 0;
        if (fromValue instanceof String) {
            try {
                return LocalDate.parse((String) fromValue, DateTimeFormatter.ofPattern(String.format(format, Convert.timePrecision(precision))));
            } catch (Exception e) {
                throw new CoreException(101, e, e.getMessage());
            }
        }
        return fromValue;
    }
    default Object covertToTime(Object fromValue, int precision, String format, TimeZone timezone) {
        if (precision < 0) precision = 0;
        if (fromValue instanceof String) {
            try {
                return LocalTime.parse((String) fromValue, DateTimeFormatter.ofPattern(String.format(format, Convert.timePrecision(precision))));
            } catch (Exception e) {
                throw new CoreException(101, e, e.getMessage());
            }
        }
        return fromValue;
    }

    default Object covertDateTime(Object fromValue, int precision, String format, TimeZone timezone) {
        if (precision < 0) precision = 0;
        if (fromValue instanceof String) {
            try {
                return LocalDateTime.parse((String) fromValue, DateTimeFormatter.ofPattern(String.format(format, Convert.timePrecision(precision)))).atZone(timezone.toZoneId()).toInstant();
            } catch (Exception e) {
                throw new CoreException(101, e, e.getMessage());
            }
        }
        return fromValue;
    }

    default Object covertToDateTime(Object fromValue, int precision, String format, TimeZone timezone) {
        if  (precision < 1) return covertDateTime(fromValue, precision, format, timezone);
        if (fromValue instanceof String) {
            try {
                DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                        .appendPattern(format)
                        .optionalStart()
                        .appendFraction(ChronoField.NANO_OF_SECOND, 0, precision, true)
                        .optionalEnd()
                        .toFormatter();
                return LocalDateTime.parse((String) fromValue, formatter);
            } catch (Exception e) {
                throw new CoreException(101, e, e.getMessage());
            }
        }
        return fromValue;
    }

    static String timePrecision(int precision) {
        if (precision <= 0) return "";
        StringBuilder builder = new StringBuilder(".");
        for (int index = 0; index < precision; index++) {
            builder.append("s");
        }
        return builder.toString();
    }

    static Convert instance(Map<String, Object> convertInfo, TimeZone timezone, TimeZone dbTimezone) {
        String columnType = String.valueOf(convertInfo.get(COLUMN_TYPE)).toUpperCase();
        Object columnPrecision = convertInfo.get(COLUMN_PRECISION);
        Object columnScale = convertInfo.get(COLUMN_SCALE);
        switch (columnType) {
            case "CHAR":
                return new CharConvert(String.valueOf(Optional.ofNullable(columnPrecision).orElse(columnScale)));
            case "VARCHAR":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "JSON":
                return new VarCharConvert();
            case "BINARY":
            case "VARBINARY":
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
                return new BinaryConvert();
            case "BIT UNSIGNED":
            case "BIT":
                return new BitConvert();
            case "TINYINT UNSIGNED":
                return new TinyIntConvert(true);
            case "TINYINT":
                return new TinyIntConvert(false);
            case "SMALLINT":
                return new SmallIntConvert(false);
            case "SMALLINT UNSIGNED":
                return new SmallIntConvert(true);
            case "INT UNSIGNED":
            case "MEDIUMINT UNSIGNED":
                return new IntegerConvert(true);
            case "INT":
            case "MEDIUMINT":
                return new IntegerConvert(false);
            case "BIGINT UNSIGNED":
                return new LongConvert(true);
            case "BIGINT":
                return new LongConvert(false);
            case "DECIMAL":
                return new DecimalConvert(String.valueOf(columnPrecision), String.valueOf(columnScale));
            case "FLOAT":
                return new FloatConvert(false, String.valueOf(columnPrecision), String.valueOf(columnScale));
            case "FLOAT UNSIGNED":
                return new FloatConvert(true, String.valueOf(columnPrecision), String.valueOf(columnScale));
            case "DOUBLE":
                return new DoubleConvert(false, String.valueOf(columnPrecision), String.valueOf(columnScale));
            case "DOUBLE UNSIGNED":
                return new DoubleConvert(true, String.valueOf(columnPrecision), String.valueOf(columnScale));
            case "TIMESTAMP":
                return new TimestampConvert(String.valueOf(Optional.ofNullable(columnPrecision).orElse(columnScale)), timezone, dbTimezone);
            case "DATETIME":
                return new DateTimeConvert(String.valueOf(Optional.ofNullable(columnPrecision).orElse(columnScale)), timezone);
            case "TIME":
                return new TimeConvert(String.valueOf(Optional.ofNullable(columnPrecision).orElse(columnScale)), timezone);
            case "DATE":
                return new DateConvert(timezone);
            case "YEAR UNSIGNED":
                return new YearConvert(true);
            case "YEAR":
                return new YearConvert(false);
            case "ENUM":
                return new EnumConvert();
            case "SET":
                return new SetConvert();
            default:
                return new DefaultConvert();
        }
    }
}
