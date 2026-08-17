package io.tapdata.connector.paimon.schema;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import io.tapdata.entity.schema.TapField;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.utils.Pair;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Locale;

/** Converts Tap CDC schema types to the connector's Paimon storage contract. */
public final class PaimonDataTypeConverter {

    private static final int DEFAULT_TIME_PRECISION = 3;
    private static final int MAX_TIME_PRECISION = 3;
    private static final int DEFAULT_TIMESTAMP_PRECISION = 6;
    private static final int DEFAULT_DECIMAL_PRECISION = 38;
    private static final int DEFAULT_DECIMAL_SCALE = 10;

    private PaimonDataTypeConverter() {}

    public static DataType toPaimonDataType(TapField tapField) {
        if (tapField == null || tapField.getDataType() == null) {
            return DataTypes.STRING();
        }

        ParsedType parsedType = parse(tapField.getDataType());
        try {
            switch (parsedType.name) {
                case "BOOLEAN":
                    return DataTypes.BOOLEAN();
                case "TINYINT":
                    return DataTypes.TINYINT();
                case "SMALLINT":
                    return DataTypes.SMALLINT();
                case "INT":
                case "INTEGER":
                    return DataTypes.INT();
                case "BIGINT":
                    return DataTypes.BIGINT();
                case "FLOAT":
                    return DataTypes.FLOAT();
                case "DOUBLE":
                    return DataTypes.DOUBLE();
                case "DECIMAL":
                    Pair<Integer, Integer> decimal = decimalPrecisionAndScale(parsedType);
                    return DataTypes.DECIMAL(decimal.getLeft(), decimal.getRight());
                case "DATE":
                    return DataTypes.DATE();
                case "TIME":
                    return DataTypes.TIME(timePrecision(parsedType));
                case "TIMESTAMP":
                    return DataTypes.TIMESTAMP(fraction(parsedType, DEFAULT_TIMESTAMP_PRECISION));
                case "TIMESTAMP_LTZ":
                case "TIMESTAMP WITH LOCAL TIME ZONE":
                    return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(
                            fraction(parsedType, DEFAULT_TIMESTAMP_PRECISION));
                case "BINARY":
                    return DataTypes.BINARY(length(parsedType));
                case "VARBINARY":
                    return DataTypes.VARBINARY(length(parsedType));
                case "BYTES":
                    return DataTypes.BYTES();
                case "CHAR":
                    return DataTypes.CHAR(length(parsedType));
                case "VARCHAR":
                    return DataTypes.VARCHAR(length(parsedType));
                case "STRING":
                case "ARRAY":
                case "MAP":
                case "ROW":
                case "MULTISET":
                case "VARIANT":
                    return DataTypes.STRING();
                default:
                    return DataTypes.STRING();
            }
        } catch (PaimonFatalWriteException e) {
            throw e;
        } catch (RuntimeException e) {
            throw invalidType(parsedType.original, e.getMessage(), e);
        }
    }

    /** Converts an external CDC value to the Java class required by Paimon InternalRow. */
    public static Object toInternalValue(String fieldName, Object value, DataType targetType) {
        if (targetType == null) {
            throw conversionError(fieldName, value, null, "target type is missing");
        }
        if (isNativeComplex(targetType)) {
            throw conversionError(
                    fieldName,
                    value,
                    targetType,
                    "native complex Paimon targets are unsupported; store CDC complex values as JSON STRING");
        }
        if (value == null) {
            return null;
        }

        try {
            switch (targetType.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return value instanceof BinaryString
                            ? value
                            : BinaryString.fromString(String.valueOf(value));
                case BOOLEAN:
                    if (!(value instanceof Boolean)) {
                        throw failure("expected java.lang.Boolean");
                    }
                    return value;
                case BINARY:
                case VARBINARY:
                    if (!(value instanceof byte[])) {
                        throw failure("expected byte[]");
                    }
                    return value;
                case TINYINT:
                    return toExactByte(value);
                case SMALLINT:
                    return toExactShort(value);
                case INTEGER:
                    return toExactInt(value);
                case BIGINT:
                    return toExactLong(value);
                case FLOAT:
                    return finiteFloat(value);
                case DOUBLE:
                    return finiteDouble(value);
                case DECIMAL:
                    return decimal(value, (DecimalType) targetType);
                case DATE:
                    return toExactInt(value);
                case TIME_WITHOUT_TIME_ZONE:
                    int millisOfDay = exactInteger(value).intValueExact();
                    if (millisOfDay < 0 || millisOfDay >= 86_400_000) {
                        throw failure("TIME milliseconds must be between 0 and 86399999");
                    }
                    int precision = ((TimeType) targetType).getPrecision();
                    int precisionUnitMillis = timePrecisionUnitMillis(precision);
                    if (millisOfDay % precisionUnitMillis != 0) {
                        throw failure("TIME value exceeds the declared target precision");
                    }
                    return millisOfDay;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    if (value instanceof Timestamp) {
                        return value;
                    }
                    if (!(value instanceof java.sql.Timestamp)) {
                        throw failure("expected java.sql.Timestamp or Paimon Timestamp");
                    }
                    java.sql.Timestamp sqlTimestamp = (java.sql.Timestamp) value;
                    return Timestamp.fromEpochMillis(
                            sqlTimestamp.getTime(), sqlTimestamp.getNanos() % 1_000_000);
                default:
                    throw failure("unsupported Paimon target type");
            }
        } catch (ValueConversionFailure e) {
            throw conversionError(fieldName, value, targetType, e.reason);
        } catch (ArithmeticException | ClassCastException e) {
            throw conversionError(
                    fieldName, value, targetType, "value is outside the exact target range");
        }
    }

    private static boolean isNativeComplex(DataType targetType) {
        switch (targetType.getTypeRoot()) {
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
            case VARIANT:
                return true;
            default:
                return false;
        }
    }

    private static int timePrecisionUnitMillis(int precision) {
        switch (precision) {
            case 0:
                return 1_000;
            case 1:
                return 100;
            case 2:
                return 10;
            default:
                return 1;
        }
    }

    public static int getFieldLength(String dataType) {
        return length(parse(dataType));
    }

    public static int getFieldFraction(String dataType) {
        ParsedType parsedType = parse(dataType);
        return "TIME".equals(parsedType.name)
                ? timePrecision(parsedType)
                : fraction(parsedType, DEFAULT_TIMESTAMP_PRECISION);
    }

    public static Pair<Integer, Integer> getFieldPrecisionAndScale(String dataType) {
        return decimalPrecisionAndScale(parse(dataType));
    }

    private static int timePrecision(ParsedType parsedType) {
        int precision = fraction(parsedType, DEFAULT_TIME_PRECISION);
        if (precision < 0 || precision > MAX_TIME_PRECISION) {
            throw new PaimonFatalWriteException(
                    "PAIMON_TIME_PRECISION_UNSUPPORTED: TIME precision must be between 0 and 3");
        }
        return precision;
    }

    private static int fraction(ParsedType parsedType, int defaultValue) {
        if (parsedType.arguments == null) {
            return defaultValue;
        }
        if (parsedType.arguments.indexOf(',') >= 0) {
            throw invalidType(parsedType.original, "expected one precision argument", null);
        }
        return parseInt(parsedType.original, parsedType.arguments, "precision");
    }

    private static int length(ParsedType parsedType) {
        if (parsedType.arguments == null) {
            return Integer.MAX_VALUE;
        }
        if (parsedType.arguments.indexOf(',') >= 0) {
            throw invalidType(parsedType.original, "expected one length argument", null);
        }
        long length;
        try {
            length = Long.parseLong(parsedType.arguments.trim());
        } catch (NumberFormatException e) {
            throw invalidType(parsedType.original, "length is not an integer", e);
        }
        if (length <= 0) {
            throw invalidType(parsedType.original, "length must be greater than zero", null);
        }
        return length > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) length;
    }

    private static Pair<Integer, Integer> decimalPrecisionAndScale(ParsedType parsedType) {
        if (parsedType.arguments == null) {
            return Pair.of(DEFAULT_DECIMAL_PRECISION, DEFAULT_DECIMAL_SCALE);
        }
        String[] values = parsedType.arguments.split(",", -1);
        if (values.length != 2) {
            throw invalidType(parsedType.original, "expected precision and scale", null);
        }
        return Pair.of(
                parseInt(parsedType.original, values[0], "precision"),
                parseInt(parsedType.original, values[1], "scale"));
    }

    /**
     * Fast-path integer conversions for boxed-primitive sources ({@link Byte}, {@link Short},
     * {@link Integer}, {@link Long}), which are the common CDC shapes. These avoid the
     * {@link BigInteger} allocation that {@link #exactInteger(Object)} would impose, while keeping
     * the exact-range contract: out-of-range values throw {@link ArithmeticException}, which the
     * caller ({@link #toInternalValue}) translates into {@code PAIMON_VALUE_CONVERSION_FAILED}.
     * {@code BigInteger}/{@code BigDecimal}/{@code Float}/{@code Double} sources still fall back to
     * {@code exactInteger}.{@code xxxValueExact()} so their semantics are unchanged.
     */
    private static byte toExactByte(Object value) {
        if (value instanceof Byte) {
            return (byte) value;
        }
        if (value instanceof Integer || value instanceof Long
                || value instanceof Short) {
            long lv = ((Number) value).longValue();
            if (lv < Byte.MIN_VALUE || lv > Byte.MAX_VALUE) {
                throw new ArithmeticException("out of byte range");
            }
            return (byte) lv;
        }
        return exactInteger(value).byteValueExact();
    }

    private static short toExactShort(Object value) {
        if (value instanceof Short) {
            return (short) value;
        }
        if (value instanceof Integer || value instanceof Long
                || value instanceof Byte) {
            long lv = ((Number) value).longValue();
            if (lv < Short.MIN_VALUE || lv > Short.MAX_VALUE) {
                throw new ArithmeticException("out of short range");
            }
            return (short) lv;
        }
        return exactInteger(value).shortValueExact();
    }

    private static int toExactInt(Object value) {
        if (value instanceof Integer) {
            return (int) value;
        }
        if (value instanceof Long || value instanceof Short || value instanceof Byte) {
            // Math.toIntExact throws ArithmeticException on overflow, matching intValueExact().
            return Math.toIntExact(((Number) value).longValue());
        }
        return exactInteger(value).intValueExact();
    }

    private static long toExactLong(Object value) {
        if (value instanceof Long) {
            return (long) value;
        }
        if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return ((Number) value).longValue();
        }
        return exactInteger(value).longValueExact();
    }

    private static BigInteger exactInteger(Object value) {
        if (!(value instanceof Number)) {
            throw failure("expected a numeric source value");
        }
        try {
            if (value instanceof BigInteger) {
                return (BigInteger) value;
            }
            if (value instanceof Byte
                    || value instanceof Short
                    || value instanceof Integer
                    || value instanceof Long) {
                return BigInteger.valueOf(((Number) value).longValue());
            }
            return numberAsBigDecimal((Number) value).toBigIntegerExact();
        } catch (ArithmeticException | NumberFormatException e) {
            throw failure("integer target requires a finite whole number", e);
        }
    }

    private static Float finiteFloat(Object value) {
        if (!(value instanceof Number)) {
            throw failure("expected a numeric source value");
        }
        float converted = ((Number) value).floatValue();
        if (!Float.isFinite(converted)) {
            throw failure("FLOAT target requires a finite in-range value");
        }
        return converted;
    }

    private static Double finiteDouble(Object value) {
        if (!(value instanceof Number)) {
            throw failure("expected a numeric source value");
        }
        double converted = ((Number) value).doubleValue();
        if (!Double.isFinite(converted)) {
            throw failure("DOUBLE target requires a finite in-range value");
        }
        return converted;
    }

    private static Decimal decimal(Object value, DecimalType targetType) {
        BigDecimal source;
        if (value instanceof Decimal) {
            source = ((Decimal) value).toBigDecimal();
        } else if (value instanceof BigDecimal) {
            source = (BigDecimal) value;
        } else if (value instanceof BigInteger) {
            source = new BigDecimal((BigInteger) value);
        } else if (value instanceof Number) {
            try {
                source = numberAsBigDecimal((Number) value);
            } catch (ArithmeticException | NumberFormatException e) {
                throw failure("DECIMAL target requires a finite numeric value", e);
            }
        } else if (value instanceof CharSequence) {
            try {
                String text = value.toString().trim();
                if (text.isEmpty()) {
                    throw failure("DECIMAL target requires a non-empty numeric string");
                }
                source = new BigDecimal(text);
            } catch (NumberFormatException e) {
                throw failure("DECIMAL target requires a valid numeric string", e);
            }
        } else {
            throw failure("DECIMAL target requires a Number or numeric string");
        }

        Decimal converted =
                Decimal.fromBigDecimal(source, targetType.getPrecision(), targetType.getScale());
        if (converted == null) {
            throw failure("DECIMAL precision overflow after applying target scale");
        }
        return converted;
    }

    private static BigDecimal numberAsBigDecimal(Number value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        }
        if (value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long) {
            return BigDecimal.valueOf(value.longValue());
        }
        if (value instanceof Float || value instanceof Double) {
            double converted = value.doubleValue();
            if (!Double.isFinite(converted)) {
                throw new ArithmeticException("non-finite number");
            }
            return BigDecimal.valueOf(converted);
        }
        return new BigDecimal(value.toString());
    }

    private static int parseInt(String original, String value, String label) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw invalidType(original, label + " is not an integer", e);
        }
    }

    private static ParsedType parse(String dataType) {
        if (dataType == null) {
            return new ParsedType("STRING", null, "null");
        }
        String normalized =
                dataType.trim().toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");
        int open = normalized.indexOf('(');
        int close = normalized.lastIndexOf(')');
        if ((open < 0) != (close < 0) || (open >= 0 && close < open)) {
            throw invalidType(dataType, "unbalanced parentheses", null);
        }

        String name;
        String arguments = null;
        if (open < 0) {
            name = normalized;
        } else {
            if (normalized.indexOf('(', open + 1) >= 0
                    || normalized.substring(open + 1, close).indexOf(')') >= 0) {
                throw invalidType(dataType, "nested parentheses are not supported", null);
            }
            String prefix = normalized.substring(0, open).trim();
            String suffix = normalized.substring(close + 1).trim();
            name = suffix.isEmpty() ? prefix : prefix + ' ' + suffix;
            arguments = normalized.substring(open + 1, close).trim();
            if (arguments.isEmpty()) {
                throw invalidType(dataType, "empty type arguments", null);
            }
        }
        return new ParsedType(name, arguments, dataType);
    }

    private static PaimonFatalWriteException invalidType(
            String original, String reason, Throwable cause) {
        String message =
                "PAIMON_INVALID_DATA_TYPE: invalid Tap data type "
                        + original
                        + (reason == null || reason.isEmpty() ? "" : " (" + reason + ')');
        PaimonFatalWriteException exception = new PaimonFatalWriteException(message);
        if (cause != null) {
            exception.initCause(cause);
        }
        return exception;
    }

    private static PaimonFatalWriteException conversionError(
            String fieldName,
            Object value,
            DataType targetType,
            String reason) {
        String target = targetType == null ? "<missing>" : targetType.asSQLString();
        String source = value == null ? "null" : value.getClass().getName();
        return new PaimonFatalWriteException(
                        "PAIMON_VALUE_CONVERSION_FAILED: field="
                                + (fieldName == null ? "<unknown>" : fieldName)
                                + ", target="
                                + target
                                + ", source="
                                + source
                                + ", reason="
                                + reason);
    }

    private static ValueConversionFailure failure(String reason) {
        return new ValueConversionFailure(reason, null);
    }

    private static ValueConversionFailure failure(String reason, Throwable cause) {
        return new ValueConversionFailure(reason, cause);
    }

    private static final class ParsedType {
        private final String name;
        private final String arguments;
        private final String original;

        private ParsedType(String name, String arguments, String original) {
            this.name = name;
            this.arguments = arguments;
            this.original = original;
        }
    }

    private static final class ValueConversionFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private final String reason;

        private ValueConversionFailure(String reason, Throwable cause) {
            super(reason, cause);
            this.reason = reason;
        }
    }
}
