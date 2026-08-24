package io.tapdata.connector.excel;

import io.tapdata.connector.excel.util.CellValueConvert;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExcelConnectorTest {

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
}
