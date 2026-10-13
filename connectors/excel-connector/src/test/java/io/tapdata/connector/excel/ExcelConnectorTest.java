package io.tapdata.connector.excel;

import io.tapdata.common.FileOffset;
import io.tapdata.connector.excel.config.ExcelConfig;
import io.tapdata.connector.excel.util.CellValueConvert;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.ToTapValueCodec;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapTimeValue;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @Test
    void readOneFileReadsRowsFromWorkbook() throws Exception {
        String path = "/data/source.xlsx";
        TestExcelConnector connector = configuredConnector(path);
        byte[] workbookBytes = workbookBytes("Alice");
        doAnswer(invocation -> {
            Consumer<InputStream> consumer = invocation.getArgument(1);
            consumer.accept(new ByteArrayInputStream(workbookBytes));
            return null;
        }).when(connector.storage()).readFile(eq(path), any());

        AtomicReference<List<TapEvent>> events = new AtomicReference<>(new ArrayList<>());
        connector.read(new FileOffset(path, 2), table(), 100, (batch, offset) -> {
        }, events);

        assertEquals(1, events.get().size());
        assertEquals("Alice", ((TapInsertRecordEvent) events.get().get(0)).getAfter().get("name"));
    }

    @Test
    void readOneFileLogsTableAndFileWhenStorageReadFails() throws Exception {
        String path = "/data/source.xlsx";
        TestExcelConnector connector = configuredConnector(path);
        IOException failure = new IOException("file is locked");
        doThrow(failure).when(connector.storage()).readFile(eq(path), any());

        assertThrows(IOException.class, () -> connector.read(new FileOffset(path, 2), table(), 100,
                (batch, offset) -> {
                }, new AtomicReference<>(new ArrayList<>())));

        verify(connector.logger()).warn(contains("table: excel_table, path: /data/source.xlsx"));
    }

    private static TestExcelConnector configuredConnector(String path) throws Exception {
        TestExcelConnector connector = new TestExcelConnector();
        connector.configure(mock(TapFileStorage.class), excelConfig(), mock(Log.class));
        when(connector.storage().getFile(path)).thenReturn(new TapFile().path(path).lastModified(1L));
        return connector;
    }

    private static ExcelConfig excelConfig() {
        ExcelConfig config = new ExcelConfig();
        config.setFirstColumn(1);
        config.setLastColumn(1);
        config.setDataStartLine(2);
        config.setSheetNum(Collections.singletonList(1));
        config.setJustString(true);
        return config;
    }

    private static TapTable table() {
        return new TapTable("excel_table").add(new TapField("name", "STRING"));
    }

    private static byte[] workbookBytes(String value) throws IOException {
        try (XSSFWorkbook workbook = new XSSFWorkbook(); ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            workbook.createSheet().createRow(1).createCell(0).setCellValue(value);
            workbook.write(output);
            return output.toByteArray();
        }
    }

    private static class TestExcelConnector extends ExcelConnector {
        private Log logger;

        private void configure(TapFileStorage storage, ExcelConfig fileConfig, Log logger) {
            this.storage = storage;
            this.fileConfig = fileConfig;
            this.logger = logger;
            this.tapLogger = logger;
        }

        private TapFileStorage storage() {
            return storage;
        }

        private Log logger() {
            return logger;
        }

        private void read(FileOffset offset,
                          TapTable table,
                          int eventBatchSize,
                          java.util.function.BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer,
                          AtomicReference<List<TapEvent>> events) throws Exception {
            readOneFile(offset, table, eventBatchSize, eventsOffsetConsumer, events);
        }

        @Override
        public boolean isAlive() {
            return true;
        }
    }
}
