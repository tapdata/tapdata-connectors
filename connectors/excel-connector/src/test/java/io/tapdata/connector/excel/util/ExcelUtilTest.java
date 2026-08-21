package io.tapdata.connector.excel.util;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExcelUtilTest {

    @Test
    void getCellValueReadsWholeNumberWithGeneralFormatAsLong() throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Cell cell = createNumericCell(workbook, null);

            Object value = ExcelUtil.getCellValue(cell, null);

            assertEquals(Long.class, value.getClass());
            assertEquals(10L, value);
        }
    }

    @Test
    void getCellValueReadsWholeNumberWithDecimalFormatAsDouble() throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Cell cell = createNumericCell(workbook, "0.0");

            Object value = ExcelUtil.getCellValue(cell, null);

            assertTrue(value instanceof Double);
            assertEquals(10.0D, (Double) value, 0D);
        }
    }

    @Test
    void getCellDisplayValuePreservesDecimalFormat() throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Cell cell = createNumericCell(workbook, "0.0");

            String value = ExcelUtil.getCellDisplayValue(cell, null, new DataFormatter());

            assertEquals("10.0", value);
        }
    }

    @Test
    void getCellValueReadsDateOnlyCellAsLocalDate() throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Cell cell = createTemporalCell(workbook, LocalDateTime.of(2024, 5, 17, 0, 0), "yyyy-mm-dd");

            Object value = ExcelUtil.getCellValue(cell, null);

            assertEquals(LocalDate.class, value.getClass());
            assertEquals(LocalDate.of(2024, 5, 17), value);
        }
    }

    @Test
    void getCellValueReadsTimeOnlyCellAsLocalTime() throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Cell cell = createNumericCell(workbook, "hh:mm:ss");
            cell.setCellValue(0.5D);

            Object value = ExcelUtil.getCellValue(cell, null);

            assertEquals(LocalTime.class, value.getClass());
            assertEquals(LocalTime.of(12, 0), value);
        }
    }

    @Test
    void getCellValueKeepsDateTimeWhenValueContainsDateAndTime() throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Cell cell = createTemporalCell(workbook, LocalDateTime.of(2024, 5, 17, 13, 45, 30), "yyyy-mm-dd");

            Object value = ExcelUtil.getCellValue(cell, null);

            assertEquals(LocalDateTime.class, value.getClass());
            assertEquals(LocalDateTime.of(2024, 5, 17, 13, 45, 30), value);
        }
    }

    private Cell createNumericCell(Workbook workbook, String format) {
        Sheet sheet = workbook.createSheet();
        Row row = sheet.createRow(0);
        Cell cell = row.createCell(0);
        cell.setCellValue(10D);
        if (format != null) {
            DataFormat dataFormat = workbook.createDataFormat();
            CellStyle cellStyle = workbook.createCellStyle();
            cellStyle.setDataFormat(dataFormat.getFormat(format));
            cell.setCellStyle(cellStyle);
        }
        return cell;
    }

    private Cell createTemporalCell(Workbook workbook, LocalDateTime value, String format) {
        Cell cell = createNumericCell(workbook, format);
        cell.setCellValue(value);
        return cell;
    }
}
