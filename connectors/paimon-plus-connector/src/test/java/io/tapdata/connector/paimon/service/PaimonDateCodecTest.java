package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.PaimonConnector;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.impl.ToTapDateCodec;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.type.TapDate;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
            for (String date : new String[] {"1000-01-01", "1969-12-31", "2026-10-15", "9999-12-31"}) {
                for (Object origin : new Object[] {java.sql.Date.valueOf(date), date}) {
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

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles"})
    void readingDateMustReturnTheStoredCalendarDay(String zone) throws Exception {
        TimeZone before = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zone));
            Method convert = PaimonService.class.getDeclaredMethod(
                    "convertPaimonValueToJava", org.apache.paimon.data.InternalRow.class,
                    int.class, DataType.class);
            convert.setAccessible(true);
            PaimonService service = new PaimonService(new PaimonConfig(), mock(Log.class));
            for (String text : new String[] {"1000-01-01", "1969-12-31", "2026-10-15", "9999-12-31"}) {
                LocalDate expected = LocalDate.parse(text);
                java.sql.Date actual = (java.sql.Date) convert.invoke(service,
                        GenericRow.of(Math.toIntExact(expected.toEpochDay())), 0, DataTypes.DATE());
                assertEquals(expected, actual.toLocalDate(), zone + " " + text);
            }
        } finally {
            TimeZone.setDefault(before);
        }
    }
}
