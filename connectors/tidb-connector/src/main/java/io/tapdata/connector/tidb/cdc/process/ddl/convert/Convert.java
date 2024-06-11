package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import io.tapdata.entity.error.CoreException;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Map;
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

    default Object covertToDateTime(Object fromValue, int precision, String format, TimeZone timezone) {
        if (fromValue instanceof String) {
            try {
                SimpleDateFormat f = new SimpleDateFormat(String.format(format, Convert.timePrecision(precision)));
                f.setTimeZone(timezone);
                return f.parse((String) fromValue, new ParsePosition(precision));
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
            builder.append("S");
        }
        return builder.toString();
    }

    static Convert instance(Map<String, Object> convertInfo, TimeZone timezone) {
        String columnType = String.valueOf(convertInfo.get(COLUMN_TYPE)).toUpperCase();
        String columnPrecision = String.valueOf(convertInfo.get(COLUMN_PRECISION));
        String columnScale = String.valueOf(convertInfo.get(COLUMN_SCALE));
        switch (columnType) {
            case "CHAR":
                return new CharConvert(columnPrecision);
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
                return new DecimalConvert(columnPrecision, columnScale);
            case "FLOAT":
                return new FloatConvert(false, columnPrecision, columnScale);
            case "FLOAT UNSIGNED":
                return new FloatConvert(true, columnPrecision, columnScale);
            case "DOUBLE":
                return new DoubleConvert(false, columnPrecision, columnScale);
            case "DOUBLE UNSIGNED":
                return new DoubleConvert(true, columnPrecision, columnScale);
            case "TIMESTAMP":
                return new TimestampConvert(columnPrecision, timezone);
            case "DATETIME":
                return new DateTimeConvert(columnPrecision, timezone);
            case "TIME":
                return new TimeConvert(columnPrecision, timezone);
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
