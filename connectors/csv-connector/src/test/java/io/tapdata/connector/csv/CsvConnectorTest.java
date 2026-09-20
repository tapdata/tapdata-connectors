package io.tapdata.connector.csv;

import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class CsvConnectorTest {

    @Test
    void makeTapTableAcceptsSingleDigitSlashDate() {
        TestCsvConnector connector = new TestCsvConnector();
        TapTable table = new TapTable("csv");
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("date_col", "2026/8/8");

        connector.makeTapTableForTest(table, sample, false);

        assertEquals("DATE", table.getNameFieldMap().get("date_col").getDataType());
    }

    @Test
    void makeTapTableKeepsCsvDateTimeKinds() {
        TestCsvConnector connector = new TestCsvConnector();
        TapTable table = new TapTable("csv");
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("date_col", "2024-05-17");
        sample.put("time_col", "12:30:45");
        sample.put("datetime_col", "2024-05-17 12:30:45.123456");

        connector.makeTapTableForTest(table, sample, false);

        assertEquals("DATE", table.getNameFieldMap().get("date_col").getDataType());
        assertEquals("TIME", table.getNameFieldMap().get("time_col").getDataType());
        assertEquals("DATETIME", table.getNameFieldMap().get("datetime_col").getDataType());
    }

    @Test
    void makeTapTableAcceptsLooseTimeValues() {
        TestCsvConnector connector = new TestCsvConnector();
        TapTable table = new TapTable("csv");
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("hour_col", "8:00:01");
        sample.put("second_col", "08:00:1");
        sample.put("all_parts_col", "8:0:1");

        connector.makeTapTableForTest(table, sample, false);

        assertEquals("TIME", table.getNameFieldMap().get("hour_col").getDataType());
        assertEquals("TIME", table.getNameFieldMap().get("second_col").getDataType());
        assertEquals("TIME", table.getNameFieldMap().get("all_parts_col").getDataType());
    }

    @Test
    void makeTapTableAcceptsLooseDateTimeValues() {
        TestCsvConnector connector = new TestCsvConnector();
        TapTable table = new TapTable("csv");
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("slash_col", "2026/8/8 8:00:1");
        sample.put("dash_col", "2026-8-8 08:00:01");

        connector.makeTapTableForTest(table, sample, false);

        assertEquals("DATETIME", table.getNameFieldMap().get("slash_col").getDataType());
        assertEquals("DATETIME", table.getNameFieldMap().get("dash_col").getDataType());
    }

    @Test
    void registerCapabilitiesProvidesCsvDateCodec() {
        CsvConnector connector = new CsvConnector();
        TapCodecsRegistry codecRegistry = TapCodecsRegistry.create();

        connector.registerCapabilities(new ConnectorFunctions(), codecRegistry);

        ToTapValueCodec<?> dateCodec = codecRegistry.getCustomToTapValueCodec(LocalDate.class);
        assertNotNull(dateCodec);
        assertNotNull(dateCodec.toTapValue(LocalDate.of(2026, 8, 8), null));
        assertNull(dateCodec.toTapValue(LocalDate.of(999, 12, 31), null));
        assertNull(dateCodec.toTapValue(null, null));
    }

    @Test
    void parseCsvDateSupportsSingleDigitMonthAndDay() {
        assertEquals(LocalDate.of(2026, 8, 8), CsvValueConverter.parse("2026/8/8", CsvValueConverter.DATE));
        assertNull(CsvValueConverter.parse("2026/2/30", CsvValueConverter.DATE));
        assertNull(CsvValueConverter.parse("not-a-date", CsvValueConverter.DATE));
    }

    @Test
    void parseCsvTimeSupportsSingleDigitParts() {
        assertEquals(LocalTime.of(8, 0, 1), CsvValueConverter.parse("8:00:01", CsvValueConverter.TIME));
        assertEquals(LocalTime.of(8, 0, 1), CsvValueConverter.parse("08:00:1", CsvValueConverter.TIME));
        assertEquals(LocalTime.of(8, 0, 1), CsvValueConverter.parse("8:0:1", CsvValueConverter.TIME));
    }

    @Test
    void parseCsvDateTimeSupportsSingleDigitParts() {
        Object parsed = CsvValueConverter.parse("2026/8/8 8:00:1", CsvValueConverter.DATETIME);

        assertEquals(LocalDateTime.of(2026, 8, 8, 8, 0, 1), parsed);
        assertEquals(LocalDateTime.of(2026, 9, 18, 12, 0, 1),
                CsvValueConverter.parse("2026-09-18 12:00:01", CsvValueConverter.DATETIME));
        assertNull(CsvValueConverter.parse("2026/2/30 8:00:1", CsvValueConverter.DATETIME));
    }

    @Test
    void justStringKeepsChineseValueWhenExistingSchemaIsInteger() throws Exception {
        TestCsvConnector connector = new TestCsvConnector();
        connector.setJustStringForTest(true);
        Map<String, Object> after = new HashMap<>();
        Map<String, String> dataTypeMap = new HashMap<>();
        dataTypeMap.put("address", "INTEGER");

        connector.putIntoMapForTest(after, new String[]{"address"}, new String[]{"柳州"}, dataTypeMap);

        assertEquals("柳州", after.get("address"));
    }

    @Test
    void justStringUsesNullForMissingValue() throws Exception {
        TestCsvConnector connector = new TestCsvConnector();
        connector.setJustStringForTest(true);
        Map<String, Object> after = new HashMap<>();
        Map<String, String> dataTypeMap = new HashMap<>();

        connector.putIntoMapForTest(after, new String[]{"address", "city"}, new String[]{"柳州"}, dataTypeMap);

        assertNull(after.get("city"));
    }

    private static class TestCsvConnector extends CsvConnector {
        private void makeTapTableForTest(TapTable table, Map<String, Object> sample, boolean justString) {
            makeTapTable(table, sample, justString);
        }

        private void setJustStringForTest(boolean justString) {
            fileConfig = new CsvConfig();
            fileConfig.setJustString(justString);
        }

        private void putIntoMapForTest(Map<String, Object> after, String[] headers, String[] data,
                                       Map<String, String> dataTypeMap) throws Exception {
            Method method = CsvConnector.class.getDeclaredMethod("putIntoMap", Map.class, String[].class,
                    String[].class, Map.class);
            method.setAccessible(true);
            method.invoke(this, after, headers, data, dataTypeMap);
        }
    }
}
