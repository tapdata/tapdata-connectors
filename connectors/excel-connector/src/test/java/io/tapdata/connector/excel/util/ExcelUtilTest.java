package io.tapdata.connector.excel.util;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.CellRangeAddress;
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
    void getCellValueReadsEverySecondTimeOnlyCellAsLocalTime() throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Cell cell = createNumericCell(workbook, "hh:mm:ss");

            for (int secondOfDay = 0; secondOfDay < 24 * 60 * 60; secondOfDay++) {
                LocalTime expected = LocalTime.ofSecondOfDay(secondOfDay);
                cell.setCellValue(secondOfDay / (24D * 60D * 60D));

                Object value = ExcelUtil.getCellValue(cell, null);

                assertEquals(LocalTime.class, value.getClass(), "secondOfDay=" + secondOfDay);
                assertEquals(expected, value, "secondOfDay=" + secondOfDay);
            }
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

    @Test
    void getCellValueReadsDateOnlyScenariosAsLocalDate() throws Exception {
        assertDateOnlyCell(LocalDate.of(1900, 1, 1), "yyyy-mm-dd", "first 1900-date-system day");
        assertDateOnlyCell(LocalDate.of(1900, 2, 28), "m/d/yy", "date before excel leap-year boundary");
        assertDateOnlyCell(LocalDate.of(2000, 2, 29), "yyyy/mm/dd", "century leap day");
        assertDateOnlyCell(LocalDate.of(2024, 2, 29), "[$-409]dd-mmm-yyyy", "locale-prefixed date format");
        assertDateOnlyCell(LocalDate.of(2024, 12, 31), "yyyy \"year\" mm \"month\" dd", "quoted-text date format");
        assertDateOnlyCell(LocalDate.of(2025, 1, 1), "yyyy-mm-dd;@", "semicolon date format");
        assertDateOnlyCellWithBuiltInFormat(LocalDate.of(2024, 5, 17), (short) 58, "built-in date format 58");
        assertDateOnlyCellIn1904DateSystem(LocalDate.of(1904, 1, 2), "yyyy-mm-dd", "1904 date system");
    }

    @Test
    void getCellValueKeepsDateTimeScenariosAsLocalDateTime() throws Exception {
        assertDateTimeCell(LocalDateTime.of(2024, 5, 17, 13, 45, 30), "yyyy-mm-dd", "date format with time value");
        assertDateTimeCell(LocalDateTime.of(2024, 5, 17, 0, 0, 0), "yyyy-mm-dd hh:mm:ss", "datetime format at midnight");
        assertDateTimeCell(LocalDateTime.of(2024, 5, 17, 13, 45, 30), "hh:mm:ss", "time format with date value");
        assertDateTimeCellWithBuiltInFormat(LocalDateTime.of(2024, 5, 17, 13, 45, 30), (short) 58, "built-in date format 58 with time value");
    }

    @Test
    void getCellValueReadsDateFormulaAsLocalDate() throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Cell cell = createNumericCell(workbook, "yyyy-mm-dd");
            cell.setCellFormula("DATE(2024,2,29)");

            Object value = ExcelUtil.getCellValue(cell, workbook.getCreationHelper().createFormulaEvaluator());

            assertEquals(LocalDate.class, value.getClass());
            assertEquals(LocalDate.of(2024, 2, 29), value);
        }
    }

    @Test
    void getMergedCellValueReadsDateOnlyCellAsLocalDate() throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet();
            Row firstRow = sheet.createRow(0);
            Cell source = firstRow.createCell(0);
            setTemporalCellValue(workbook, source, LocalDateTime.of(2024, 5, 17, 0, 0), "yyyy-mm-dd");
            sheet.addMergedRegion(new CellRangeAddress(0, 1, 0, 1));
            Cell mergedCell = sheet.createRow(1).createCell(1);

            Object value = ExcelUtil.getMergedCellValue(sheet.getMergedRegions(), ExcelUtil.getMergedDataMap(sheet), mergedCell, null);

            assertEquals(LocalDate.class, value.getClass());
            assertEquals(LocalDate.of(2024, 5, 17), value);
        }
    }

    @Test
    void getCellValueKeepsTextThatLooksLikeDateAsString() throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet();
            Row row = sheet.createRow(0);
            Cell cell = row.createCell(0);
            cell.setCellValue("2024-05-17");

            Object value = ExcelUtil.getCellValue(cell, null);

            assertEquals(String.class, value.getClass());
            assertEquals("2024-05-17", value);
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

    private void assertDateOnlyCell(LocalDate expected, String format, String scenario) throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Cell cell = createTemporalCell(workbook, expected.atStartOfDay(), format);

            Object value = ExcelUtil.getCellValue(cell, null);

            assertEquals(LocalDate.class, value.getClass(), scenario);
            assertEquals(expected, value, scenario);
        }
    }

    private void assertDateOnlyCellWithBuiltInFormat(LocalDate expected, short dataFormat, String scenario) throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Cell cell = createNumericCell(workbook, null);
            setTemporalCellValue(workbook, cell, expected.atStartOfDay(), dataFormat);

            Object value = ExcelUtil.getCellValue(cell, null);

            assertEquals(LocalDate.class, value.getClass(), scenario);
            assertEquals(expected, value, scenario);
        }
    }

    private void assertDateOnlyCellIn1904DateSystem(LocalDate expected, String format, String scenario) throws Exception {
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            if (workbook.getCTWorkbook().getWorkbookPr() == null) {
                workbook.getCTWorkbook().addNewWorkbookPr();
            }
            workbook.getCTWorkbook().getWorkbookPr().setDate1904(true);
            Cell cell = createTemporalCell(workbook, expected.atStartOfDay(), format);

            Object value = ExcelUtil.getCellValue(cell, null);

            assertEquals(LocalDate.class, value.getClass(), scenario);
            assertEquals(expected, value, scenario);
        }
    }

    private void assertDateTimeCell(LocalDateTime expected, String format, String scenario) throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Cell cell = createTemporalCell(workbook, expected, format);

            Object value = ExcelUtil.getCellValue(cell, null);

            assertEquals(LocalDateTime.class, value.getClass(), scenario);
            assertEquals(expected, value, scenario);
        }
    }

    private void assertDateTimeCellWithBuiltInFormat(LocalDateTime expected, short dataFormat, String scenario) throws Exception {
        try (Workbook workbook = new XSSFWorkbook()) {
            Cell cell = createNumericCell(workbook, null);
            setTemporalCellValue(workbook, cell, expected, dataFormat);

            Object value = ExcelUtil.getCellValue(cell, null);

            assertEquals(LocalDateTime.class, value.getClass(), scenario);
            assertEquals(expected, value, scenario);
        }
    }

    private void setTemporalCellValue(Workbook workbook, Cell cell, LocalDateTime value, String format) {
        DataFormat dataFormat = workbook.createDataFormat();
        CellStyle cellStyle = workbook.createCellStyle();
        cellStyle.setDataFormat(dataFormat.getFormat(format));
        cell.setCellStyle(cellStyle);
        cell.setCellValue(value);
    }

    private void setTemporalCellValue(Workbook workbook, Cell cell, LocalDateTime value, short dataFormat) {
        CellStyle cellStyle = workbook.createCellStyle();
        cellStyle.setDataFormat(dataFormat);
        cell.setCellStyle(cellStyle);
        cell.setCellValue(value);
    }
}
