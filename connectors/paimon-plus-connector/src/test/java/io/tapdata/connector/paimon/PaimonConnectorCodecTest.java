package io.tapdata.connector.paimon;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import io.tapdata.connector.paimon.schema.PaimonDataTypeConverter;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.type.TapArray;
import io.tapdata.entity.schema.type.TapMap;
import io.tapdata.entity.schema.type.TapRaw;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapArrayValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapMapValue;
import io.tapdata.entity.schema.value.TapRawValue;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonConnectorCodecTest {

    private TapCodecsRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new TapCodecsRegistry();
        new PaimonConnector().registerCapabilities(new ConnectorFunctions(), registry);
    }

    @Test
    void dateCodecMustUseFloorDivisionBeforeUnixEpoch() {
        Object converted =
                registry.getCustomFromTapValueCodec(TapDateValue.class)
                        .fromTapValue(
                                new TapDateValue(new DateTime(Instant.ofEpochSecond(-1))));

        assertEquals(-1, converted);
    }

    @ParameterizedTest
    @CsvSource({
        "12:34:56.123456, 45296123",
        "12:34:56.123000, 45296123",
        "15:24:00.326786, 55440326",
        "23:59:59.999999, 86399999",
        "23:59:59.999000, 86399999",
        "00:00:00.000001, 0",
        "00:00:00.000000001, 0",
        "00:00:00.000000, 0"
    })
    void timeCodecMustTruncateSubMillisecondPrecisionLikeLegacyConnector(
            String time, int expectedMillis) {
        assertEquals(expectedMillis, convertTime(DateTime.withTimeStr(time)));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "-00:00:00.000001",
        "-12:34:56.654321",
        "24:00:00.000000",
        "838:59:59.000000",
        "-838:59:59.000000"
    })
    void timeCodecMustRejectNegativeAndExtendedTimesBeforeIntegerConversion(String time) {
        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () -> convertTime(DateTime.withTimeStr(time)));

        assertTrue(thrown.getMessage().contains("value must be within one day"));
    }

    @Test
    void timeCodecMustRejectNormalizedNegativeTimeAndNextDay() {
        assertThrows(
                PaimonFatalWriteException.class,
                () -> convertTime(Instant.ofEpochSecond(-1, 999_999_000)));
        assertThrows(
                PaimonFatalWriteException.class,
                () -> convertTime(Instant.ofEpochSecond(86_400)));
    }

    @ParameterizedTest
    @ValueSource(longs = {Long.MIN_VALUE, Long.MAX_VALUE})
    void timeCodecMustRejectExtremeSecondsBeforeMultiplication(long seconds) {
        DateTime time = new DateTime(Instant.EPOCH);
        time.setSeconds(seconds);

        PaimonFatalWriteException thrown =
                assertThrows(PaimonFatalWriteException.class, () -> convertTime(time));

        assertTrue(thrown.getMessage().contains("value must be within one day"));
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 6})
    void truncatedTimeMustSerializeAgainstCurrentAndLegacyPhysicalTimeColumns(int precision) {
        Object converted = convertTime(DateTime.withTimeStr("12:34:56.123456"));
        DataType targetType = DataTypes.TIME(precision);
        Object internal = PaimonDataTypeConverter.toInternalValue("time_val", converted, targetType);

        BinaryRow serialized =
                new InternalRowSerializer(targetType).toBinaryRow(GenericRow.of(internal));

        assertEquals(45_296_123, serialized.getInt(0));
    }

    @Test
    void complexCodecsMustOverrideTargetTypeAndSerializeJson() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", 1);

        assertEquals("STRING", registry.getDataTypeByTapType(TapArray.class));
        assertEquals("STRING", registry.getDataTypeByTapType(TapMap.class));
        assertEquals("STRING", registry.getDataTypeByTapType(TapRaw.class));
        assertEquals(
                "[1,\"value\"]",
                registry.getCustomFromTapValueCodec(TapArrayValue.class)
                        .fromTapValue(new TapArrayValue(Arrays.asList(1, "value"))));
        assertEquals(
                "{\"id\":1}",
                registry.getCustomFromTapValueCodec(TapMapValue.class)
                        .fromTapValue(new TapMapValue(map)));
        assertEquals(
                "{\"id\":1}",
                registry.getCustomFromTapValueCodec(TapRawValue.class)
                        .fromTapValue(new TapRawValue(map)));
    }

    @Test
    void capabilitiesMustExposeReadOnlyExecuteQuery() {
        ConnectorFunctions functions = new ConnectorFunctions();
        new PaimonConnector().registerCapabilities(functions, new TapCodecsRegistry());

        assertNotNull(functions.getExecuteCommandFunction());
    }

    private Object convertTime(Instant instant) {
        return convertTime(new DateTime(instant));
    }

    private Object convertTime(DateTime time) {
        return registry.getCustomFromTapValueCodec(TapTimeValue.class)
                .fromTapValue(new TapTimeValue(time));
    }
}
