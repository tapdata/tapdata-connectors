package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.PaimonConnector;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.schema.PaimonDataTypeConverter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.impl.ToTapDateCodec;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

class PaimonDateCodecTest {
    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles"})
    void snapshotAndStringDatesMustRetainCalendarDay(String zone) {
        TimeZone before = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zone));
            TapCodecsRegistry registry = new TapCodecsRegistry();
            new PaimonConnector().registerCapabilities(new ConnectorFunctions(), registry);
            for (String date : new String[] {"1000-01-01", "1969-12-31", "1980-01-01",
                    "2000-02-29", "2026-10-15", "2038-01-19", "9999-12-31"}) {
                for (Object origin : new Object[] {java.sql.Date.valueOf(date), date,
                        LocalDate.parse(date).atStartOfDay()}) {
                    TapDateValue value = new ToTapDateCodec().toTapValue(origin, new TapDate());
                    value.setOriginValue(origin);
                    assertEquals(Math.toIntExact(LocalDate.parse(date).toEpochDay()),
                            registry.getFromTapValueCodec(TapDateValue.class).fromTapValue(value),
                            zone + " " + origin.getClass() + " " + date);
                }
            }
            assertEquals(-1, registry.getFromTapValueCodec(TapDateValue.class)
                    .fromTapValue(new TapDateValue(new DateTime(Instant.ofEpochSecond(-1)))));
        } finally {
            TimeZone.setDefault(before);
        }
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("formattedDateCases")
    void formattedDateTextMustRetainCalendarDay(String zone, String text, LocalDate expected) {
        TimeZone before = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zone));
            // Custom source codecs may normalize text using the source's local timezone.
            TapDateValue value = new TapDateValue(new DateTime(
                    expected.atStartOfDay(ZoneId.systemDefault()).toInstant()));
            value.setOriginValue(text);
            assertEquals(Math.toIntExact(expected.toEpochDay()), convertDate(value));
        } finally {
            TimeZone.setDefault(before);
        }
    }

    private static Stream<Arguments> formattedDateCases() {
        return Stream.of("UTC", "Asia/Shanghai", "America/Los_Angeles", "Europe/London")
                .flatMap(zone -> Stream.of("1980-01-01", "2000-02-29", "2026-10-15", "9999-12-31")
                        .map(LocalDate::parse)
                        .flatMap(date -> Stream.of(date.toString(), date.toString().replace('-', '/'),
                                date.format(DateTimeFormatter.BASIC_ISO_DATE),
                                date + " 00:00:00", date + "T23:59:59.123456",
                                date + "T00:00:00+08:00")
                                .map(text -> Arguments.of(zone, text, date))));
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles", "Europe/London"})
    void dateTimeTextAcceptedByPdkMustRetainCalendarDay(String zone) {
        TimeZone before = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zone));
            String origin = "2026-10-15 00:00:00";
            TapDateValue value = new ToTapDateCodec().toTapValue(origin, new TapDate());
            value.setOriginValue(origin);
            assertEquals(Math.toIntExact(LocalDate.of(2026, 10, 15).toEpochDay()), convertDate(value));
        } finally {
            TimeZone.setDefault(before);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"0000-00-00", "0000-00-00 00:00:00", "0000/00/00", "00000000",
            "0000-01-01", "2026-02-30", "2026-00-15", "2026-10-00", "2026-10-15 25:00:00"})
    void invalidDateTextMustBecomeNullInsteadOfUsingLenientNormalization(String origin) {
        TapDateValue value = new TapDateValue(new DateTime(LocalDate.of(2026, 1, 1).atStartOfDay()));
        value.setOriginValue(origin);
        assertNull(convertDate(value));
    }

    @Test
    void illegalZeroDateAndLenientlyParsedZeroDateMustBothBecomeNull() {
        TapDateValue illegal = new TapDateValue(new DateTime("0000-00-00", DateTime.DATE_TYPE));
        illegal.setOriginValue("0000-00-00");
        assertNull(convertDate(illegal));
        TapDateValue lenient = new ToTapDateCodec().toTapValue("0000-00-00", new TapDate());
        lenient.setOriginValue("0000-00-00");
        assertNull(convertDate(lenient));
    }

    @ParameterizedTest
    @ValueSource(strings = {"15 Oct 2026", "2026-10-15 00:00:00 UTC",
            "2026-10-15 00:00:00.123 +08:00", "2026-10-15  00:00:00"})
    void customDateTextMustKeepItsAlreadyNormalizedValue(String origin) {
        LocalDate expected = LocalDate.of(2026, 10, 15);
        TapDateValue value = new TapDateValue(new DateTime(expected.atStartOfDay()));
        value.setOriginValue(origin);
        assertEquals(Math.toIntExact(expected.toEpochDay()), convertDate(value));
    }

    private static Object convertDate(TapDateValue value) {
        TapCodecsRegistry registry = new TapCodecsRegistry();
        new PaimonConnector().registerCapabilities(new ConnectorFunctions(), registry);
        return registry.getFromTapValueCodec(TapDateValue.class).fromTapValue(value);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("storedDateCases")
    void readingDateMustRetainCalendarDayThroughTapCodec(String zone, LocalDate expected)
            throws Exception {
        TimeZone before = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zone));
            Method convert = PaimonService.class.getDeclaredMethod(
                    "convertPaimonValueToJava", InternalRow.class, int.class, DataType.class);
            convert.setAccessible(true);
            PaimonService service = new PaimonService(new PaimonConfig(), mock(Log.class));
            int epochDay = Math.toIntExact(expected.toEpochDay());
            Object actual = convert.invoke(service, GenericRow.of(epochDay), 0, DataTypes.DATE());

            TapDateValue value = new ToTapDateCodec().toTapValue(actual, new TapDate());
            assertNotNull(value, "Paimon DATE must be supported by the PDK DATE codec");
            assertEquals(expected.atStartOfDay(), value.getValue().toLocalDateTime());
            value.setOriginValue(actual);

            TapCodecsRegistry registry = new TapCodecsRegistry();
            new PaimonConnector().registerCapabilities(new ConnectorFunctions(), registry);
            Object converted = registry.getFromTapValueCodec(TapDateValue.class).fromTapValue(value);
            Object internal = PaimonDataTypeConverter.toInternalValue("date", converted, DataTypes.DATE());
            BinaryRow row = new InternalRowSerializer(DataTypes.DATE()).toBinaryRow(GenericRow.of(internal));
            assertEquals(epochDay, row.getInt(0));
        } finally {
            TimeZone.setDefault(before);
        }
    }

    private static Stream<Arguments> storedDateCases() {
        return Stream.of("UTC", "Asia/Shanghai", "America/Los_Angeles", "Pacific/Apia")
                .flatMap(zone -> Stream.of(
                        "0001-01-01", "0004-02-29", "1000-01-01", "1500-03-01",
                        "1582-10-04", "1582-10-05", "1582-10-06", "1582-10-07",
                        "1582-10-08", "1582-10-09", "1582-10-10", "1582-10-11",
                        "1582-10-12", "1582-10-13", "1582-10-14", "1582-10-15",
                        "1969-12-31", "1970-01-01", "1980-01-01", "1980-02-29",
                        "2000-02-29", "2011-12-30", "2026-10-15", "2038-01-19", "9999-12-31")
                        .map(date -> Arguments.of(zone, LocalDate.parse(date))));
    }
}
