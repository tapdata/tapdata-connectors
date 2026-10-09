package io.tapdata.connector.csv;

import io.tapdata.connector.it.FileITSupport;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.text.SimpleDateFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("CSV connector integration tests")
public class CsvConnectorIT {
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
    @DisplayName("registers every CSV source capability")
    void shouldRegisterCapabilities() {
        ConnectorFunctions functions = new ConnectorFunctions();
        new CsvConnector().registerCapabilities(functions, TapCodecsRegistry.create());

        assertNotNull(functions.getBatchCountFunction());
        assertNotNull(functions.getBatchReadFunction());
        assertNotNull(functions.getStreamReadFunction());
        assertNotNull(functions.getTimestampToStreamOffsetFunction());
        assertNotNull(functions.getWriteRecordFunction());
    }

    @Test
    @DisplayName("discovers the CSV header and inferred fields")
    void shouldDiscoverSchema() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("csv", "1.csv");
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "csv_it");
        CsvConnector connector = new CsvConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "csv", config);
        resources.add(session);

        List<TapTable> discovered = new ArrayList<>();
        connector.discoverSchema(session.nodeContext, Collections.singletonList("csv_it"), 10, discovered::addAll);

        assertEquals(1, discovered.size());
        TapTable table = discovered.get(0);
        assertEquals("csv_it", table.getId());
        assertEquals("INTEGER", table.getNameFieldMap().get("ID").getDataType());
        assertEquals("STRING", table.getNameFieldMap().get("Name").getDataType());
        assertEquals("INTEGER", table.getNameFieldMap().get("Age").getDataType());
    }

    @Test
    @DisplayName("discovers string-only CSV fields when justString is enabled")
    void shouldDiscoverJustStringSchema() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("csv", "1.csv");
        resources.add(fixture);
        CsvConnector connector = new CsvConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "csv", FileITSupport.config(fixture.directory(), "csv_it")
                .kv("justString", true));
        resources.add(session);
        List<TapTable> discovered = new ArrayList<>();

        connector.discoverSchema(session.nodeContext, Collections.singletonList("csv_it"), 10, discovered::addAll);

        assertEquals(1, discovered.size());
        assertTrue(discovered.get(0).getNameFieldMap().values().stream()
                .allMatch(field -> "STRING".equals(field.getDataType())));
    }

    @Test
    @DisplayName("uses an explicit header and skips blank or short rows")
    void shouldUseExplicitHeaderAndHandleShortRows() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("csv", "no-header.csv");
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "csv_it")
                .kv("header", "ID,Name,Age")
                .kv("headerLine", 0)
                .kv("dataStartLine", 2);
        CsvConnector connector = new CsvConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "csv", config);
        resources.add(session);
        FileITSupport.Events capture = new FileITSupport.Events();

        session.functions.getBatchReadFunction().batchRead(session.nodeContext, csvTable(), null, 10, capture::accept);

        assertEquals(3, capture.events().size());
        Map<String, Object> shortRow = ((TapInsertRecordEvent) capture.events().get(1)).getAfter();
        assertEquals("Short", shortRow.get("Name"));
        assertTrue(shortRow.containsKey("Age"));
        assertNull(shortRow.get("Age"));
    }

    @Test
    @DisplayName("reads the off-standard quoted delimiter branch")
    void shouldReadOffStandardCsv() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("csv", "off-standard.csv");
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "csv_it")
                .kv("offStandard", true)
                .kv("lineExpression", "\\\"([^\\\"]*)\\\"\\|?");
        CsvConnector connector = new CsvConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "csv", config);
        resources.add(session);
        List<TapTable> discovered = new ArrayList<>();
        connector.discoverSchema(session.nodeContext, Collections.singletonList("csv_it"), 10, discovered::addAll);
        FileITSupport.Events capture = new FileITSupport.Events();
        session.functions.getBatchReadFunction().batchRead(session.nodeContext, csvTable(), null, 1, capture::accept);

        assertEquals(2, capture.events().size());
        assertEquals("Off Alice", ((TapInsertRecordEvent) capture.events().get(0)).getAfter().get("Name"));
        assertEquals("INTEGER", discovered.get(0).getNameFieldMap().get("Age").getDataType());
    }

    @Test
    @DisplayName("maps a configured tab separator")
    void shouldReadTabSeparatedCsv() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("csv", "tab.csv");
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "csv_it").kv("separatorType", "\\t");
        CsvConnector connector = new CsvConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "csv", config);
        resources.add(session);
        FileITSupport.Events capture = new FileITSupport.Events();
        session.functions.getBatchReadFunction().batchRead(session.nodeContext, csvTable(), null, 2, capture::accept);

        assertEquals(2, capture.events().size());
        assertEquals("Tab Alice", ((TapInsertRecordEvent) capture.events().get(0)).getAfter().get("Name"));
        assertEquals(35, ((Number) ((TapInsertRecordEvent) capture.events().get(0)).getAfter().get("Age")).intValue());
    }

    @Test
    @DisplayName("reads CSV rows and returns a resumable file offset")
    void shouldBatchReadRows() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("csv", "1.csv");
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "csv_it");
        CsvConnector connector = new CsvConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "csv", config);
        resources.add(session);
        TapTable table = csvTable();
        FileITSupport.Events capture = new FileITSupport.Events();

        session.functions.getBatchReadFunction().batchRead(session.nodeContext, table, null, 3, capture::accept);

        List<TapEvent> events = capture.events();
        assertEquals(9, events.size());
        assertTrue(events.stream().allMatch(event -> event instanceof TapInsertRecordEvent));
        Map<String, Object> first = ((TapInsertRecordEvent) events.get(0)).getAfter();
        assertEquals(1L, ((Number) first.get("ID")).longValue());
        assertEquals("Alice", first.get("Name"));
        assertNotNull(first.get("Age"));
        assertFalse(capture.offsets().isEmpty());
        assertNotNull(capture.offsets().get(capture.offsets().size() - 1));
    }

    @Test
    @DisplayName("writes CSV records with an operation marker")
    void shouldWriteRows() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("csv", "1.csv");
        resources.add(fixture);
        Path output = Files.createTempDirectory("csv-connector-it-write-");
        resources.add(cleanup(output));
        DataMap config = FileITSupport.config(fixture.directory(), "csv_it")
                .kv("writeFilePath", output.toAbsolutePath().toString())
                .kv("fileNameExpression", "written.csv");
        CsvConnector connector = new CsvConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "csv", config);
        resources.add(session);
        TapTable table = csvTable();
        Map<String, Object> after = new HashMap<>();
        after.put("ID", 10);
        after.put("Name", "Jill");
        after.put("Age", 27);
        TapInsertRecordEvent event = new TapInsertRecordEvent().init().table(table.getId()).after(after);
        AtomicReference<WriteListResult<TapRecordEvent>> result = new AtomicReference<>();

        session.functions.getWriteRecordFunction().writeRecord(session.nodeContext,
                Collections.singletonList(event), table, result::set);

        assertNotNull(result.get());
        assertEquals(1L, result.get().getInsertedCount());
        String written = new String(Files.readAllBytes(output.resolve("written.csv")), StandardCharsets.UTF_8).replace("\"", "");
        assertTrue(written.contains("10,Jill,27,i"));
    }

    @Test
    @DisplayName("writes insert, update and delete operation markers")
    void shouldWriteAllDmlOperations() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("csv", "1.csv");
        resources.add(fixture);
        Path output = Files.createTempDirectory("csv-connector-it-dml-");
        resources.add(cleanup(output));
        DataMap config = FileITSupport.config(fixture.directory(), "csv_it")
                .kv("writeFilePath", output.toAbsolutePath().toString())
                .kv("fileNameExpression", "dml.csv");
        CsvConnector connector = new CsvConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "csv", config);
        resources.add(session);
        Map<String, Object> first = new HashMap<>();
        first.put("ID", 10);
        first.put("Name", "Insert");
        first.put("Age", 20);
        Map<String, Object> updated = new HashMap<>(first);
        updated.put("Name", "Update");
        Map<String, Object> deleted = new HashMap<>(updated);
        List<TapRecordEvent> events = Arrays.asList(
                new TapInsertRecordEvent().init().table("csv_it").after(first),
                new TapUpdateRecordEvent().init().table("csv_it").before(first).after(updated),
                new TapDeleteRecordEvent().init().table("csv_it").before(deleted));
        AtomicReference<WriteListResult<TapRecordEvent>> result = new AtomicReference<>();

        session.functions.getWriteRecordFunction().writeRecord(session.nodeContext, events, csvTable(), result::set);

        assertEquals(1L, result.get().getInsertedCount());
        assertEquals(1L, result.get().getModifiedCount());
        assertEquals(1L, result.get().getRemovedCount());
        String written = new String(Files.readAllBytes(output.resolve("dml.csv")), StandardCharsets.UTF_8).replace("\"", "");
        assertTrue(written.contains(",i"));
        assertTrue(written.contains(",u"));
        assertTrue(written.contains(",d"));
    }

    @Test
    @DisplayName("writes record-keyed CSV files")
    void shouldWriteRecordKeyedFiles() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("csv", "1.csv");
        resources.add(fixture);
        Path output = Files.createTempDirectory("csv-connector-it-record-");
        resources.add(cleanup(output));
        DataMap config = FileITSupport.config(fixture.directory(), "csv_it")
                .kv("writeFilePath", output.toAbsolutePath().toString())
                .kv("fileNameExpression", "${record.Name}.csv");
        CsvConnector connector = new CsvConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "csv", config);
        resources.add(session);
        List<TapRecordEvent> events = new ArrayList<>();
        Map<String, Object> alice = new HashMap<>();
        alice.put("ID", 11);
        alice.put("Name", "Alice");
        alice.put("Age", 22);
        Map<String, Object> bob = new HashMap<>();
        bob.put("ID", 12);
        bob.put("Name", "Bob");
        bob.put("Age", 23);
        events.add(new TapInsertRecordEvent().init().table("csv_it").after(alice));
        events.add(new TapInsertRecordEvent().init().table("csv_it").after(bob));
        AtomicReference<WriteListResult<TapRecordEvent>> result = new AtomicReference<>();

        session.functions.getWriteRecordFunction().writeRecord(session.nodeContext, events, csvTable(), result::set);

        assertEquals(2L, result.get().getInsertedCount());
        assertTrue(Files.exists(output.resolve("Alice.csv")));
        assertTrue(Files.exists(output.resolve("Bob.csv")));
    }

    @Test
    @DisplayName("writes date-partitioned CSV files")
    void shouldWriteDatePartitionedFile() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("csv", "1.csv");
        resources.add(fixture);
        Path output = Files.createTempDirectory("csv-connector-it-date-");
        resources.add(cleanup(output));
        DataMap config = FileITSupport.config(fixture.directory(), "csv_it")
                .kv("writeFilePath", output.toAbsolutePath().toString())
                .kv("fileNameExpression", "${date:yyyyMMdd}.csv");
        CsvConnector connector = new CsvConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "csv", config);
        resources.add(session);
        Map<String, Object> dateRecord = new HashMap<>();
        dateRecord.put("ID", 13);
        dateRecord.put("Name", "Date");
        dateRecord.put("Age", 24);
        TapInsertRecordEvent event = new TapInsertRecordEvent().init().table("csv_it").after(dateRecord);
        AtomicReference<WriteListResult<TapRecordEvent>> result = new AtomicReference<>();

        session.functions.getWriteRecordFunction().writeRecord(session.nodeContext, Collections.singletonList(event), csvTable(), result::set);

        assertEquals(1L, result.get().getInsertedCount());
        String expected = new SimpleDateFormat("yyyyMMdd").format(new Date()) + ".csv";
        assertTrue(Files.exists(output.resolve(expected)));
    }

    @Test
    @DisplayName("creates an offset containing the current CSV file set")
    void shouldCreateTimestampOffset() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("csv", "1.csv");
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "csv_it");
        CsvConnector connector = new CsvConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "csv", config);
        resources.add(session);

        Object offset = session.functions.getTimestampToStreamOffsetFunction()
                .timestampToStreamOffset(session.nodeContext, System.currentTimeMillis());

        assertNotNull(offset);
        assertNotNull(session.functions.getStreamReadFunction());
        assertTrue(offset.toString().contains("FileOffset") || offset.getClass().getSimpleName().equals("FileOffset"));
    }

    @Test
    @DisplayName("captures a newly appearing CSV file through streamRead")
    void shouldStreamNewCsvFile() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("csv", "1.csv");
        resources.add(fixture);
        CsvConnector connector = new CsvConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "csv", FileITSupport.config(fixture.directory(), "csv_it"));
        resources.add(session);
        TapTable table = csvTable();
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
        }, "csv-connector-it-stream");
        thread.start();
        FileITSupport.copyResource("csv", "stream.csv", fixture.directory().resolve("stream.csv"));
        try {
            assertTrue(received.await(10, TimeUnit.SECONDS), "new CSV file was not emitted");
            assertNull(failure.get());
        } finally {
            consumer.streamReadEnded();
            session.close();
            thread.interrupt();
            thread.join(5000);
        }
        assertFalse(thread.isAlive(), "CSV stream thread did not stop");
    }

    private static TapTable csvTable() {
        return new TapTable("csv_it")
                .add(new TapField("ID", "INTEGER"))
                .add(new TapField("Name", "STRING"))
                .add(new TapField("Age", "INTEGER"));
    }

    private static AutoCloseable cleanup(Path directory) {
        return () -> {
            try (Stream<Path> paths = Files.walk(directory)) {
                paths.sorted((left, right) -> right.compareTo(left)).forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (Exception error) {
                        throw new IllegalStateException("Unable to delete CSV output fixture " + path, error);
                    }
                });
            }
        };
    }
}
