package io.tapdata.connector.excel;

import io.tapdata.connector.it.FileITSupport;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Excel connector integration tests")
public class ExcelConnectorIT {
    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void closeResources() throws Exception {
        Exception failure = null;
        for (int index = resources.size() - 1; index >= 0; index--) {
            try {
                resources.get(index).close();
            } catch (Exception error) {
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        resources.clear();
        if (failure != null) {
            throw failure;
        }
    }

    @Test
    @DisplayName("registers Excel source capabilities")
    void shouldRegisterCapabilities() {
        ConnectorFunctions functions = new ConnectorFunctions();
        new ExcelConnector().registerCapabilities(functions, TapCodecsRegistry.create());

        assertNotNull(functions.getBatchCountFunction());
        assertNotNull(functions.getBatchReadFunction());
        assertNotNull(functions.getStreamReadFunction());
        assertNotNull(functions.getTimestampToStreamOffsetFunction());
    }

    @Test
    @DisplayName("discovers date, time and datetime columns")
    void shouldDiscoverSchema() throws Throwable {
        ExcelFixture fixture = workbookFixture();
        resources.add(fixture);
        ExcelConnector connector = new ExcelConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "excel", FileITSupport.config(fixture.directory(), "excel_it"));
        resources.add(session);
        List<TapTable> discovered = new ArrayList<>();

        connector.discoverSchema(session.nodeContext, Collections.singletonList("excel_it"), 10, discovered::addAll);

        assertEquals(1, discovered.size());
        TapTable table = discovered.get(0);
        assertEquals("DATE", table.getNameFieldMap().get("create_date").getDataType());
        assertEquals("TIME", table.getNameFieldMap().get("create_time").getDataType());
        assertEquals("DATETIME", table.getNameFieldMap().get("create_datetime").getDataType());
        assertEquals("id", table.getNameFieldMap().get("id").getName());
    }

    @Test
    @DisplayName("reads workbook records with temporal values")
    void shouldBatchReadWorkbook() throws Throwable {
        ExcelFixture fixture = workbookFixture();
        resources.add(fixture);
        ExcelConnector connector = new ExcelConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "excel", FileITSupport.config(fixture.directory(), "excel_it"));
        resources.add(session);
        FileITSupport.Events capture = new FileITSupport.Events();
        TapTable table = new TapTable("excel_it");
        table.add(new io.tapdata.entity.schema.TapField("id", "DOUBLE"));
        table.add(new io.tapdata.entity.schema.TapField("name", "STRING"));
        table.add(new io.tapdata.entity.schema.TapField("create_date", "DATE"));
        table.add(new io.tapdata.entity.schema.TapField("create_time", "TIME"));
        table.add(new io.tapdata.entity.schema.TapField("create_datetime", "DATETIME"));

        session.functions.getBatchReadFunction().batchRead(session.nodeContext, table, null, 100, capture::accept);

        assertEquals(2, capture.events().size());
        Map<String, Object> first = ((TapInsertRecordEvent) capture.events().get(0)).getAfter();
        assertEquals(1L, ((Number) first.get("id")).longValue());
        assertTrue(first.get("create_date") instanceof LocalDate);
        assertTrue(first.get("create_time") instanceof LocalTime);
        assertTrue(first.get("create_datetime") instanceof LocalDateTime);
        assertEquals(LocalDate.of(2020, 1, 2), first.get("create_date"));
        assertEquals(LocalTime.of(3, 4, 5), first.get("create_time"));
        assertEquals(LocalDateTime.of(2020, 1, 2, 3, 4, 5), first.get("create_datetime"));
        assertNotNull(capture.offsets().get(capture.offsets().size() - 1));
    }

    @Test
    @DisplayName("uses an explicit header and a selected column range")
    void shouldUseExplicitHeaderAndColumnRange() throws Throwable {
        ExcelFixture fixture = workbookFixture();
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "excel_it")
                .kv("header", "name,create_date")
                .kv("colLocation", "B~C");
        ExcelConnector connector = new ExcelConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "excel", config);
        resources.add(session);
        List<TapTable> discovered = new ArrayList<>();
        connector.discoverSchema(session.nodeContext, Collections.singletonList("excel_it"), 10, discovered::addAll);

        List<String> res = new ArrayList<>();
        res.add("name");
        res.add("create_date");
        assertEquals(res, new ArrayList<>(discovered.get(0).getNameFieldMap().keySet()));
        assertEquals("DATE", discovered.get(0).getNameFieldMap().get("create_date").getDataType());
    }

    @Test
    @DisplayName("converts workbook values to strings with justString")
    void shouldReadJustString() throws Throwable {
        ExcelFixture fixture = workbookFixture();
        resources.add(fixture);
        ExcelConnector connector = new ExcelConnector();
        DataMap config = FileITSupport.config(fixture.directory(), "excel_it").kv("justString", true);
        FileITSupport.Session session = FileITSupport.open(connector, "excel", config);
        resources.add(session);
        FileITSupport.Events capture = new FileITSupport.Events();
        TapTable table = new TapTable("excel_it");
        table.add(new io.tapdata.entity.schema.TapField("id", "STRING"));
        table.add(new io.tapdata.entity.schema.TapField("name", "STRING"));
        table.add(new io.tapdata.entity.schema.TapField("create_date", "STRING"));
        table.add(new io.tapdata.entity.schema.TapField("create_time", "STRING"));
        table.add(new io.tapdata.entity.schema.TapField("create_datetime", "STRING"));

        session.functions.getBatchReadFunction().batchRead(session.nodeContext, table, null, 100, capture::accept);

        Map<String, Object> first = ((TapInsertRecordEvent) capture.events().get(0)).getAfter();
        assertTrue(first.values().stream().allMatch(value -> value == null || value instanceof String));
    }

    @Test
    @DisplayName("reads a workbook without a header row")
    void shouldReadHeaderlessWorkbook() throws Throwable {
        ExcelFixture fixture = workbookFixture(false);
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "excel_it")
                .kv("headerLine", 0)
                .kv("dataStartLine", 1);
        ExcelConnector connector = new ExcelConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "excel", config);
        resources.add(session);
        TapTable table = new TapTable("excel_it")
                .add(new io.tapdata.entity.schema.TapField("column2", "DOUBLE"))
                .add(new io.tapdata.entity.schema.TapField("column3", "STRING"))
                .add(new io.tapdata.entity.schema.TapField("column4", "DATE"))
                .add(new io.tapdata.entity.schema.TapField("column5", "TIME"))
                .add(new io.tapdata.entity.schema.TapField("column6", "DATETIME"));
        FileITSupport.Events capture = new FileITSupport.Events();

        session.functions.getBatchReadFunction().batchRead(session.nodeContext, table, null, 100, capture::accept);

        assertEquals(2, capture.events().size());
        assertEquals(1L, ((Number) ((TapInsertRecordEvent) capture.events().get(0)).getAfter().get("column2")).longValue());
    }

    @Test
    @DisplayName("reads all selected sheets")
    void shouldReadMultipleSheets() throws Throwable {
        ExcelFixture fixture = workbookFixture();
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "excel_it").kv("sheetLocation", "1~2");
        ExcelConnector connector = new ExcelConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "excel", config);
        resources.add(session);
        FileITSupport.Events capture = new FileITSupport.Events();

        session.functions.getBatchReadFunction().batchRead(session.nodeContext, excelTable(), null, 100, capture::accept);

        assertEquals(4, capture.events().size());
    }

    @Test
    @DisplayName("creates a workbook file offset")
    void shouldCreateTimestampOffset() throws Throwable {
        ExcelFixture fixture = workbookFixture();
        resources.add(fixture);
        ExcelConnector connector = new ExcelConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "excel", FileITSupport.config(fixture.directory(), "excel_it"));
        resources.add(session);

        Object offset = session.functions.getTimestampToStreamOffsetFunction()
                .timestampToStreamOffset(session.nodeContext, System.currentTimeMillis());

        assertEquals("FileOffset", offset.getClass().getSimpleName());
        assertNotNull(session.functions.getStreamReadFunction());
    }

    @Test
    @DisplayName("captures a newly appearing workbook through streamRead")
    void shouldStreamNewWorkbook() throws Throwable {
        ExcelFixture fixture = workbookFixture();
        resources.add(fixture);
        ExcelConnector connector = new ExcelConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "excel", FileITSupport.config(fixture.directory(), "excel_it"));
        resources.add(session);
        TapTable table = excelTable();
        session.register(table);
        Object offset = session.functions.getTimestampToStreamOffsetFunction()
                .timestampToStreamOffset(session.nodeContext, System.currentTimeMillis());
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        StreamReadConsumer consumer = StreamReadConsumer.create((events, ignored) -> {
            if (events != null && events.stream().anyMatch(event -> event instanceof TapInsertRecordEvent)) {
                received.countDown();
            }
        });
        Thread thread = new Thread(() -> {
            try {
                session.functions.getStreamReadFunction().streamRead(session.nodeContext,
                        Collections.singletonList(table.getId()), offset, 10, consumer);
            } catch (Throwable error) {
                failure.set(error);
            }
        }, "excel-connector-it-stream");
        thread.start();
        Files.copy(fixture.directory().resolve("fallback.xlsx"), fixture.directory().resolve("stream.xlsx"));
        try {
            assertTrue(received.await(10, TimeUnit.SECONDS), "new workbook was not emitted");
            assertNull(failure.get());
        } finally {
            consumer.streamReadEnded();
            session.close();
            thread.interrupt();
            thread.join(5000);
        }
        assertFalse(thread.isAlive(), "Excel stream thread did not stop");
    }

    private TapTable excelTable() {
        return new TapTable("excel_it")
                .add(new io.tapdata.entity.schema.TapField("id", "DOUBLE"))
                .add(new io.tapdata.entity.schema.TapField("name", "STRING"))
                .add(new io.tapdata.entity.schema.TapField("create_date", "DATE"))
                .add(new io.tapdata.entity.schema.TapField("create_time", "TIME"))
                .add(new io.tapdata.entity.schema.TapField("create_datetime", "DATETIME"));
    }

    private ExcelFixture workbookFixture() throws Exception {
        return workbookFixture(true);
    }

    private ExcelFixture workbookFixture(boolean includeHeader) throws Exception {
        Path directory = Files.createTempDirectory("excel-connector-it-");
        Path workbookPath = directory.resolve("fallback.xlsx");
        try (XSSFWorkbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("data");
            Row header = includeHeader ? sheet.createRow(0) : null;
            String[] headers;
            CellStyle dateStyle = workbook.createCellStyle();
            dateStyle.setDataFormat(workbook.getCreationHelper().createDataFormat().getFormat("yyyy-mm-dd"));
            CellStyle timeStyle = workbook.createCellStyle();
            timeStyle.setDataFormat(workbook.getCreationHelper().createDataFormat().getFormat("hh:mm:ss"));
            CellStyle dateTimeStyle = workbook.createCellStyle();
            dateTimeStyle.setDataFormat(workbook.getCreationHelper().createDataFormat().getFormat("yyyy-mm-dd hh:mm:ss.000"));
            try (InputStream input = ExcelConnectorIT.class.getResourceAsStream("/fixtures/excel/data.csv")) {
                if (input == null) {
                    throw new IllegalStateException("Missing fixture: excel/data.csv");
                }
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
                    headers = reader.readLine().split(",", -1);
                    if (includeHeader) {
                        for (int index = 0; index < headers.length; index++) {
                            header.createCell(index).setCellValue(headers[index]);
                        }
                    }
                    String line;
                    int rowIndex = includeHeader ? 1 : 0;
                    while ((line = reader.readLine()) != null) {
                        String[] values = line.split(",", -1);
                        addRow(sheet.createRow(rowIndex++), Integer.parseInt(values[0]), values[1],
                                LocalDate.parse(values[2]), LocalTime.parse(values[3]), LocalDateTime.parse(values[4]),
                                dateStyle, timeStyle, dateTimeStyle);
                    }
                }
            }
            if (includeHeader) {
                Sheet second = workbook.createSheet("data2");
                Row secondHeader = second.createRow(0);
                for (int index = 0; index < headers.length; index++) {
                    secondHeader.createCell(index).setCellValue(headers[index]);
                }
                addRow(second.createRow(1), 3, "Carol", LocalDate.of(2020, 1, 4), LocalTime.of(9, 10, 11),
                        LocalDateTime.of(2020, 1, 4, 9, 10, 11), dateStyle, timeStyle, dateTimeStyle);
                addRow(second.createRow(2), 4, "David", LocalDate.of(2020, 1, 5), LocalTime.of(12, 13, 14),
                        LocalDateTime.of(2020, 1, 5, 12, 13, 14), dateStyle, timeStyle, dateTimeStyle);
            }
            try (OutputStream output = Files.newOutputStream(workbookPath, StandardOpenOption.CREATE_NEW)) {
                workbook.write(output);
            }
        }
        return new ExcelFixture(directory, true);
    }

    private static void addRow(Row row, int id, String name, LocalDate date, LocalTime time, LocalDateTime dateTime,
                               CellStyle dateStyle, CellStyle timeStyle, CellStyle dateTimeStyle) {
        row.createCell(0).setCellValue(id);
        row.createCell(1).setCellValue(name);
        row.createCell(2).setCellValue(java.sql.Timestamp.valueOf(date.atStartOfDay()));
        row.getCell(2).setCellStyle(dateStyle);
        row.createCell(3).setCellValue(time.toSecondOfDay() / 86400D);
        row.getCell(3).setCellStyle(timeStyle);
        row.createCell(4).setCellValue(java.sql.Timestamp.valueOf(dateTime));
        row.getCell(4).setCellStyle(dateTimeStyle);
    }

    private static final class ExcelFixture implements AutoCloseable {
        private final Path directory;
        private final boolean deleteOnClose;

        private ExcelFixture(Path directory, boolean deleteOnClose) {
            this.directory = directory;
            this.deleteOnClose = deleteOnClose;
        }

        private Path directory() {
            return directory;
        }

        @Override
        public void close() throws Exception {
            if (!deleteOnClose) {
                return;
            }
            Files.walk(directory)
                    .sorted((left, right) -> right.compareTo(left))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception error) {
                            throw new IllegalStateException("Unable to delete workbook fixture " + path, error);
                        }
                    });
        }
    }
}
