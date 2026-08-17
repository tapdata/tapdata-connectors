package io.tapdata.connector.paimon.schema;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;

import io.tapdata.entity.schema.TapField;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Collections;
import java.util.Locale;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonDataTypeConverterTest {

    @Test
    void schemaTypeNormalizationMustBeLocaleIndependentAndPreserveDefaults() {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));

            assertEquals(DataTypes.INT(), type(" int "));
            assertEquals(DataTypes.INT(), type("integer"));
            assertEquals(DataTypes.DECIMAL(38, 10), type("decimal"));
            assertEquals(DataTypes.TIME(3), type("time"));
            assertEquals(DataTypes.TIME(0), type(" time ( 0 ) "));
            assertEquals(DataTypes.TIME(3), type("TIME(3)"));
            assertEquals(DataTypes.TIMESTAMP(6), type("timestamp"));
            assertEquals(
                    DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6), type("timestamp_ltz"));
            assertEquals(
                    DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                    type("timestamp(3) with local time zone"));
            assertEquals(DataTypes.CHAR(Integer.MAX_VALUE), type("char"));
            assertEquals(DataTypes.VARCHAR(Integer.MAX_VALUE), type("varchar"));
            assertEquals(DataTypes.BINARY(Integer.MAX_VALUE), type("binary"));
            assertEquals(DataTypes.VARBINARY(Integer.MAX_VALUE), type("varbinary"));
            assertEquals(DataTypes.STRING(), type("source_specific_type"));
            assertEquals(3, PaimonDataTypeConverter.getFieldFraction("TIME"));
            assertEquals(6, PaimonDataTypeConverter.getFieldFraction("TIMESTAMP"));
            assertThrows(PaimonFatalWriteException.class, () -> type("TIME(4)"));
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test
    void explicitDecimalParametersMustOverrideDefaultsAndRejectInvalidScale() {
        assertEquals(DataTypes.DECIMAL(18, 2), type("DECIMAL(18,2)"));
        assertEquals(DataTypes.DECIMAL(10, 0), type("DECIMAL(10,0)"));
        assertThrows(PaimonFatalWriteException.class, () -> type("DECIMAL(2,3)"));
    }

    @ParameterizedTest
    @MethodSource("complexTapTypes")
    void complexTapSchemaTypesMustUseJsonStringStorage(String tapType) {
        assertEquals(DataTypes.STRING(), type(tapType));
    }

    private static Stream<String> complexTapTypes() {
        return Stream.of("ARRAY", "MAP", "ROW", "MULTISET", "VARIANT");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("exactIntegerValues")
    void integerTargetsMustUseExactPaimonInternalClasses(
            String scenario, Object source, DataType target, Object expected) {
        Object converted = PaimonDataTypeConverter.toInternalValue("number", source, target);

        assertEquals(expected, converted, scenario);
        assertEquals(expected.getClass(), converted.getClass(), scenario);
    }

    private static Stream<Arguments> exactIntegerValues() {
        return Stream.of(
                Arguments.of("tiny lower bound", Byte.MIN_VALUE, DataTypes.TINYINT(), Byte.MIN_VALUE),
                Arguments.of("tiny upper bound", 127L, DataTypes.TINYINT(), Byte.MAX_VALUE),
                Arguments.of(
                        "small lower bound",
                        (long) Short.MIN_VALUE,
                        DataTypes.SMALLINT(),
                        Short.MIN_VALUE),
                Arguments.of(
                        "small upper bound",
                        BigInteger.valueOf(Short.MAX_VALUE),
                        DataTypes.SMALLINT(),
                        Short.MAX_VALUE),
                Arguments.of(
                        "int lower bound",
                        (long) Integer.MIN_VALUE,
                        DataTypes.INT(),
                        Integer.MIN_VALUE),
                Arguments.of(
                        "int upper bound",
                        BigDecimal.valueOf(Integer.MAX_VALUE),
                        DataTypes.INT(),
                        Integer.MAX_VALUE),
                Arguments.of(
                        "bigint lower bound",
                        BigInteger.valueOf(Long.MIN_VALUE),
                        DataTypes.BIGINT(),
                        Long.MIN_VALUE),
                Arguments.of(
                        "bigint upper bound",
                        new BigDecimal(Long.toString(Long.MAX_VALUE)),
                        DataTypes.BIGINT(),
                        Long.MAX_VALUE),
                // Fast-path sources (boxed primitives) bypass BigInteger and must stay exact.
                Arguments.of("int from Integer", Integer.MAX_VALUE, DataTypes.INT(), Integer.MAX_VALUE),
                Arguments.of("int from Byte", (byte) 7, DataTypes.INT(), 7),
                Arguments.of("int from Short", (short) 1000, DataTypes.INT(), 1000),
                Arguments.of("bigint from Integer", 42, DataTypes.BIGINT(), 42L),
                Arguments.of("bigint from Short", (short) -5, DataTypes.BIGINT(), -5L),
                Arguments.of("bigint from Byte", (byte) 3, DataTypes.BIGINT(), 3L),
                Arguments.of("tinyint from Byte", (byte) -128, DataTypes.TINYINT(), Byte.MIN_VALUE),
                Arguments.of("smallint from Short", (short) 32000, DataTypes.SMALLINT(), (short) 32000),
                Arguments.of("date from Integer", 19083, DataTypes.DATE(), 19083));
    }

    @ParameterizedTest
    @MethodSource("invalidIntegerValues")
    void integerTargetsMustRejectFractionsOverflowsAndWrongSourceTypes(
            Object source, DataType target) {
        assertConversionFailure("number", source, target);
    }

    private static Stream<Arguments> invalidIntegerValues() {
        return Stream.of(
                Arguments.of(1.5D, DataTypes.INT()),
                Arguments.of(128, DataTypes.TINYINT()),
                Arguments.of(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), DataTypes.BIGINT()),
                Arguments.of("1", DataTypes.INT()),
                Arguments.of(Double.NaN, DataTypes.INT()),
                // Fast-path overflow checks: boxed-primitive sources must reject out-of-range targets.
                Arguments.of(Long.MAX_VALUE, DataTypes.INT()),
                Arguments.of(Long.MAX_VALUE, DataTypes.TINYINT()),
                Arguments.of(Long.MAX_VALUE, DataTypes.SMALLINT()),
                Arguments.of(Integer.MAX_VALUE + 1L, DataTypes.DATE()),
                Arguments.of(Short.MAX_VALUE + 1, DataTypes.SMALLINT()),
                Arguments.of((long) Byte.MAX_VALUE + 1L, DataTypes.TINYINT()));
    }

    @Test
    void decimalConversionMustAcceptCdcNumberShapesAndUsePaimonRounding() {
        Decimal fromString =
                assertInstanceOf(
                        Decimal.class,
                        PaimonDataTypeConverter.toInternalValue(
                                "amount", "1.235", DataTypes.DECIMAL(5, 2)));
        Decimal fromLong =
                assertInstanceOf(
                        Decimal.class,
                        PaimonDataTypeConverter.toInternalValue(
                                "amount", 12L, DataTypes.DECIMAL(5, 2)));
        Decimal fromBigInteger =
                assertInstanceOf(
                        Decimal.class,
                        PaimonDataTypeConverter.toInternalValue(
                                "amount", BigInteger.valueOf(7), DataTypes.DECIMAL(5, 2)));

        assertEquals("1.24", fromString.toString());
        assertEquals("12.00", fromLong.toString());
        assertEquals("7.00", fromBigInteger.toString());
        assertConversionFailure("amount", "999.99", DataTypes.DECIMAL(4, 2));
        assertConversionFailure("amount", Double.POSITIVE_INFINITY, DataTypes.DECIMAL(5, 2));
        assertConversionFailure("amount", "not-a-number", DataTypes.DECIMAL(5, 2));
    }

    @Test
    void scalarTargetsMustMatchPaimonInternalRowContract() {
        byte[] bytes = new byte[] {1, 2, 3};

        assertEquals(Boolean.TRUE, internal("flag", true, DataTypes.BOOLEAN()));
        assertSame(bytes, internal("payload", bytes, DataTypes.BYTES()));
        assertArrayEquals(bytes, (byte[]) internal("payload", bytes, DataTypes.VARBINARY(3)));
        assertEquals(12, internal("event_date", 12L, DataTypes.DATE()));
        assertEquals(86_399_999, internal("event_time", 86_399_999L, DataTypes.TIME(3)));
        assertEquals(1_000, internal("event_time", 1_000L, DataTypes.TIME(0)));
        assertEquals(10, internal("event_time", 10L, DataTypes.TIME(2)));
        assertEquals(
                "42",
                assertInstanceOf(
                                BinaryString.class,
                                internal("legacy_number", 42, DataTypes.STRING()))
                        .toString());
        assertEquals(1.25F, internal("float_value", 1.25D, DataTypes.FLOAT()));
        assertEquals(1.25D, internal("double_value", 1.25F, DataTypes.DOUBLE()));
        assertNull(internal("nullable", null, DataTypes.STRING()));

        assertConversionFailure("event_time", -1, DataTypes.TIME(3));
        assertConversionFailure("event_time", 86_400_000, DataTypes.TIME(3));
        assertConversionFailure("event_time", 1_001, DataTypes.TIME(0));
        assertConversionFailure("event_time", 11, DataTypes.TIME(2));
        assertConversionFailure("event_date", "12", DataTypes.DATE());
        assertConversionFailure(
                "event_date", (long) Integer.MAX_VALUE + 1L, DataTypes.DATE());
        assertConversionFailure("flag", 1, DataTypes.BOOLEAN());
        assertConversionFailure("payload", "bytes", DataTypes.BYTES());
        assertConversionFailure("float_value", Double.POSITIVE_INFINITY, DataTypes.FLOAT());
    }

    @Test
    void timestampConversionMustPreserveNegativeEpochAndNanoseconds() {
        java.sql.Timestamp sqlTimestamp =
                java.sql.Timestamp.from(Instant.ofEpochSecond(-1, 123_456_789));

        Timestamp converted =
                assertInstanceOf(
                        Timestamp.class,
                        internal("created_at", sqlTimestamp, DataTypes.TIMESTAMP(6)));

        assertEquals(-877L, converted.getMillisecond());
        assertEquals(456_789, converted.getNanoOfMillisecond());
        assertSame(
                converted,
                internal("created_at", converted, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6)));
        assertConversionFailure("created_at", Instant.EPOCH, DataTypes.TIMESTAMP(6));
    }

    @Test
    void convertedScalarValuesMustSurvivePaimonInternalRowSerialization() {
        byte[] bytes = new byte[] {1, 2, 3};
        java.sql.Timestamp sqlTimestamp =
                java.sql.Timestamp.from(Instant.ofEpochSecond(-1, 123_456_789));

        assertTrue(serialized("flag", true, DataTypes.BOOLEAN()).getBoolean(0));
        assertArrayEquals(
                bytes, serialized("payload", bytes, DataTypes.VARBINARY(3)).getBinary(0));
        assertEquals(12, serialized("event_date", 12L, DataTypes.DATE()).getInt(0));
        assertEquals(
                86_399_999,
                serialized("event_time", 86_399_999, DataTypes.TIME(3)).getInt(0));
        assertEquals(
                "legacy",
                serialized("text", "legacy", DataTypes.STRING()).getString(0).toString());
        assertEquals(
                "1.24",
                serialized("amount", "1.235", DataTypes.DECIMAL(5, 2))
                        .getDecimal(0, 5, 2)
                        .toString());

        Timestamp timestamp =
                serialized("created_at", sqlTimestamp, DataTypes.TIMESTAMP(6))
                        .getTimestamp(0, 6);
        assertEquals(-877L, timestamp.getMillisecond());
        assertEquals(456_789, timestamp.getNanoOfMillisecond());
    }

    @ParameterizedTest
    @MethodSource("nativeComplexTargets")
    void nativeComplexPaimonTargetsMustFailBeforeSerialization(DataType target) {
        PaimonFatalWriteException thrown =
                assertConversionFailure("payload", Collections.singletonMap("key", "value"), target);

        assertTrue(thrown.getMessage().contains("JSON STRING"));
        assertFalse(thrown.getMessage().contains("{key=value}"));
    }

    @Test
    void nativeComplexPaimonTargetMustBeRejectedEvenWhenValueIsNull() {
        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                internal(
                                        "payload",
                                        null,
                                        DataTypes.ARRAY(DataTypes.STRING())));

        assertTrue(thrown.getMessage().contains("source=null"));
        assertTrue(thrown.getMessage().contains("JSON STRING"));
    }

    private static Stream<DataType> nativeComplexTargets() {
        return Stream.of(
                DataTypes.ARRAY(DataTypes.STRING()),
                DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                DataTypes.ROW(DataTypes.STRING()),
                DataTypes.MULTISET(DataTypes.STRING()),
                DataTypes.VARIANT());
    }

    private static DataType type(String dataType) {
        return PaimonDataTypeConverter.toPaimonDataType(new TapField("field", dataType));
    }

    private static Object internal(String fieldName, Object value, DataType target) {
        return PaimonDataTypeConverter.toInternalValue(fieldName, value, target);
    }

    private static BinaryRow serialized(String fieldName, Object value, DataType target) {
        GenericRow row = GenericRow.of(internal(fieldName, value, target));
        return new InternalRowSerializer(target).toBinaryRow(row);
    }

    private static PaimonFatalWriteException assertConversionFailure(
            String fieldName, Object source, DataType target) {
        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () -> PaimonDataTypeConverter.toInternalValue(fieldName, source, target));
        String message = thrown.getMessage();
        assertTrue(message.contains("PAIMON_VALUE_CONVERSION_FAILED"));
        assertTrue(message.contains("field=" + fieldName));
        assertTrue(message.contains("target=" + target.asSQLString()));
        assertTrue(message.contains("source=" + source.getClass().getName()));
        return thrown;
    }
}
