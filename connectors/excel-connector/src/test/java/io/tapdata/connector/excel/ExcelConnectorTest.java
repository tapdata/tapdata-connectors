package io.tapdata.connector.excel;

import io.tapdata.connector.excel.util.CellValueConvert;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExcelConnectorTest {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    @Test
    void makeTapTableMapsJavaTimeValuesToExcelDataTypes() {
        ExcelConnector connector = new ExcelConnector();
        TapTable table = new TapTable("excel");
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("date_col", LocalDate.of(2024, 5, 17));
        sample.put("time_col", LocalTime.of(12, 30, 45));
        sample.put("datetime_col", LocalDateTime.of(2024, 5, 17, 12, 30, 45));

        connector.makeTapTable(table, sample, false);

        assertEquals("DATE", table.getNameFieldMap().get("date_col").getDataType());
        assertEquals("TIME", table.getNameFieldMap().get("time_col").getDataType());
        assertEquals("DATETIME", table.getNameFieldMap().get("datetime_col").getDataType());
    }

    @Test
    void parseValueFormatsTemporalValuesForStringFields() {

        assertEquals("2024-05-17", CellValueConvert.parseValue(LocalDate.of(2024, 5, 17), "STRING"));
        assertEquals("12:30:45", CellValueConvert.parseValue(LocalTime.of(12, 30, 45), "STRING"));
        assertEquals("2024-05-17 12:30:45.123456",
                CellValueConvert.parseValue(LocalDateTime.of(2024, 5, 17, 12, 30, 45, 123456000), "STRING"));
    }

    @Test
    void parseValueUsesDisplayValueForNonTemporalStringFields() {
        ExcelConnector connector = new ExcelConnector();

        assertEquals("10.00", CellValueConvert.parseValue(10L, "10.00", "STRING"));
    }

    @Test
    void registerCapabilitiesConvertsEverySecondLocalTimeToTapTime() {
        ExcelConnector connector = new ExcelConnector();
        TapCodecsRegistry codecRegistry = TapCodecsRegistry.create();
        connector.registerCapabilities(new ConnectorFunctions(), codecRegistry);
        ToTapValueCodec<?> toTapValueCodec = codecRegistry.getCustomToTapValueCodec(LocalTime.class);
        FromTapValueCodec<TapTimeValue> fromTapValueCodec = codecRegistry.getFromTapValueCodec(TapTimeValue.class);

        for (int secondOfDay = 0; secondOfDay < 24 * 60 * 60; secondOfDay++) {
            LocalTime localTime = LocalTime.ofSecondOfDay(secondOfDay);
            TapTimeValue tapTimeValue = (TapTimeValue) toTapValueCodec.toTapValue(localTime, null);
            Object value = fromTapValueCodec.fromTapValue(tapTimeValue);

            assertEquals(localTime.format(TIME_FORMATTER), value, "secondOfDay=" + secondOfDay);
        }
    }
}
