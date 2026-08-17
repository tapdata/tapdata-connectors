package io.tapdata.connector.paimon;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void timeCodecMustAcceptOnlyMillisecondPrecisionWithinOneDay() {
        assertEquals(
                86_399_999,
                convertTime(Instant.ofEpochSecond(86_399, 999_000_000)));
        assertThrows(
                PaimonFatalWriteException.class,
                () -> convertTime(Instant.ofEpochSecond(1, 1)));
        assertThrows(
                PaimonFatalWriteException.class,
                () -> convertTime(Instant.ofEpochSecond(-1)));
        assertThrows(
                PaimonFatalWriteException.class,
                () -> convertTime(Instant.ofEpochSecond(86_400)));
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

    private Object convertTime(Instant instant) {
        return registry.getCustomFromTapValueCodec(TapTimeValue.class)
                .fromTapValue(new TapTimeValue(new DateTime(instant)));
    }
}
