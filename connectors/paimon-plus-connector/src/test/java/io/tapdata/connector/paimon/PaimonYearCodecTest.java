package io.tapdata.connector.paimon;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import io.tapdata.connector.paimon.schema.PaimonDataTypeConverter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapYear;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.TimeZone;
import java.util.Map;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaimonYearCodecTest {

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles"})
    void jdbcYearDatesMustSerializeAsFourDigitsInEveryTimezone(String zone) {
        TimeZone before = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zone));
            for (int year : new int[] {1901, 2000, 2026, 2155}) {
                java.sql.Date jdbcYear = java.sql.Date.valueOf(year + "-01-01");
                TapYearValue value = new TapYearValue(new DateTime(jdbcYear));
                value.setOriginValue(jdbcYear);

                assertStoredYear(Integer.toString(year), value);
            }
        } finally {
            TimeZone.setDefault(before);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1901, 2026, 2155})
    void cdcNumericYearsMustRetainTheirValueIncludingZeroYear(int year) {
        TapYearValue value = new TapYearValue(year);
        value.setOriginValue(year);
        assertStoredYear(year == 0 ? "0000" : Integer.toString(year), value);
    }

    @Test
    void normalizedYearWithoutOriginMustUseItsDateTimeValue() {
        assertStoredYear("2026", new TapYearValue(
                new DateTime(LocalDateTime.of(2026, 1, 1, 0, 0))));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1901, 2000, 2026, 2155})
    void numericJdbcYearsMustSurviveTheRealSourceAndTargetFilters(int year) {
        TapCodecsRegistry registry = new TapCodecsRegistry();
        new PaimonConnector().registerCapabilities(new ConnectorFunctions(), registry);
        TapCodecsFilterManager filters = TapCodecsFilterManager.create(registry);
        Map<String, Object> row = new LinkedHashMap<>();
        // Connector/J yearIsDateType=false returns Short, while CDC returns Integer.
        row.put("year_val", (short) year);
        filters.transformToTapValueMap(row, Map.of("year_val", new TapField("year_val", "year(4)").tapType(new TapYear())));
        filters.transformFromTapValueMap(row);
        assertEquals(year == 0 ? "0000" : Integer.toString(year), row.get("year_val"));
    }

    @Test
    void absentYearMustRemainNull() {
        assertNull(convert(null));
        assertNull(convert(new TapYearValue()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "10000", "2026.5"})
    void valuesThatCannotBeRepresentedAsFourDigitYearsMustFail(String invalid) {
        TapYearValue value = new TapYearValue(2026);
        value.setOriginValue(new BigDecimal(invalid));
        assertThrows(PaimonFatalWriteException.class, () -> convert(value));
    }

    private void assertStoredYear(String expected, TapYearValue value) {
        Object converted = convert(value);
        assertEquals(expected, converted);
        Object internal = PaimonDataTypeConverter.toInternalValue(
                "year_val", converted, DataTypes.CHAR(4));
        BinaryRow row = new InternalRowSerializer(DataTypes.CHAR(4))
                .toBinaryRow(GenericRow.of(internal));
        assertEquals(expected, row.getString(0).toString());
    }

    private Object convert(TapYearValue value) {
        TapCodecsRegistry registry = new TapCodecsRegistry();
        new PaimonConnector().registerCapabilities(new ConnectorFunctions(), registry);
        return registry.getFromTapValueCodec(TapYearValue.class).fromTapValue(value);
    }
}
