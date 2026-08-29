package io.tapdata.connector.excel.util;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/8/21 15:13 Create
 * @description
 */
public final class CellValueConvert {
    public static final String STRING_DATA_TYPE = "STRING";
    public static final String TEXT_DATA_TYPE = "TEXT";
    public static final DateTimeFormatter LOCAL_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final DateTimeFormatter LOCAL_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");
    public static final DateTimeFormatter LOCAL_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private CellValueConvert() {

    }

    public static String toExcelDataType(Object val) {
        if (val instanceof LocalDate) {
            return "DATE";
        }
        if (val instanceof LocalTime) {
            return "TIME";
        }
        if (val instanceof LocalDateTime) {
            return "DATETIME";
        }
        return val.getClass().getSimpleName().toUpperCase();
    }

    static Object stringValue(Object val) {
        String temporalValue = formatTemporalValue(val);
        if (temporalValue != null) {
            return temporalValue;
        }
        return parseValue(val);
    }

    public static Object parseOriginValue(Object val, String fieldType) {
        if (isStringField(fieldType)) {
            return stringValue(val);
        }
        if (val instanceof String && StringUtils.isBlank((String) val)) {
            return null;
        }
        return val;
    }

    public static Object parseValue(Object val) {
        if (val instanceof Double || val instanceof Float || val instanceof Long) {
            val = BigDecimal.valueOf(((Number) val).doubleValue()).stripTrailingZeros().toPlainString();
        } else {
            val = EmptyKit.isNull(val) ? "null" : String.valueOf(val);
        }
        return val;
    }

    public static Object parseValue(Object val, String fieldDataType) {
        return parseValue(val, val, fieldDataType);
    }

    public static Object parseValue(Object val, Object displayValue, String fieldDataType) {
        if (isStringField(fieldDataType)) {
            String temporalValue = formatTemporalValue(val);
            if (temporalValue != null) {
                return temporalValue;
            }
            return parseValue(displayValue);
        }
        if (displayValue instanceof String && StringUtils.isBlank((String) displayValue)) {
            return null;
        }
        return parseValue(val);
    }

    public static String getFieldDataType(TapTable tapTable, String fieldName) {
        if (tapTable == null || tapTable.getNameFieldMap() == null) {
            return null;
        }
        TapField tapField = tapTable.getNameFieldMap().get(fieldName);
        return tapField == null ? null : tapField.getDataType();
    }

    public static boolean isStringField(String fieldDataType) {
        if (EmptyKit.isBlank(fieldDataType)) {
            return false;
        }
        return STRING_DATA_TYPE.equalsIgnoreCase(fieldDataType)
                || TEXT_DATA_TYPE.equalsIgnoreCase(fieldDataType);
    }

    public static String formatTemporalValue(Object val) {
        if (val instanceof LocalDate) {
            return ((LocalDate) val).format(LOCAL_DATE_FORMATTER);
        }
        if (val instanceof LocalTime) {
            return ((LocalTime) val).format(LOCAL_TIME_FORMATTER);
        }
        if (val instanceof LocalDateTime) {
            return ((LocalDateTime) val).format(LOCAL_DATE_TIME_FORMATTER);
        }
        return null;
    }
}
