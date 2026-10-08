package io.tapdata.connector.json;

import io.tapdata.connector.it.FileITSupport;
import io.tapdata.common.FileOffset;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("File stream connector integration tests")
public class FileStreamConnectorIT {
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
    @DisplayName("registers file stream source and target capabilities")
    void shouldRegisterCapabilities() {
        ConnectorFunctions functions = new ConnectorFunctions();
        new FileStreamConnector().registerCapabilities(functions, TapCodecsRegistry.create());

        assertNotNull(functions.getBatchCountFunction());
        assertNotNull(functions.getBatchReadFunction());
        assertNotNull(functions.getStreamReadFunction());
        assertNotNull(functions.getTimestampToStreamOffsetFunction());
        assertNotNull(functions.getWriteRecordFunction());
    }

    @Test
    @DisplayName("returns the fixed file schema")
    void shouldDiscoverSchema() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("file-stream", "payload.txt");
        resources.add(fixture);
        FileStreamConnector connector = new FileStreamConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "filestream", FileITSupport.config(fixture.directory(), "file"));
        resources.add(session);
        List<TapTable> discovered = new ArrayList<>();

        connector.discoverSchema(session.nodeContext, Collections.singletonList("file"), 10, discovered::addAll);

        assertEquals(1, discovered.size());
        assertEquals(java.util.Arrays.asList("file_name", "file_path", "file_size", "last_modified", "file_data"),
                new ArrayList<>(discovered.get(0).getNameFieldMap().keySet()));
        assertEquals("FILE", discovered.get(0).getNameFieldMap().get("file_data").getDataType());
        assertEquals("STRING", discovered.get(0).getNameFieldMap().get("file_name").getDataType());
        assertEquals("STRING", discovered.get(0).getNameFieldMap().get("file_path").getDataType());
        assertEquals("NUMBER", discovered.get(0).getNameFieldMap().get("file_size").getDataType());
        assertEquals("NUMBER", discovered.get(0).getNameFieldMap().get("last_modified").getDataType());
    }

    @Test
    @DisplayName("counts and reads one record per file")
    void shouldCountAndReadFiles() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("file-stream", "payload.txt");
        resources.add(fixture);
        Path second = fixture.directory().resolve("second.txt");
        Files.write(second, "second-payload".getBytes(StandardCharsets.UTF_8));
        FileStreamConnector connector = new FileStreamConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "filestream", FileITSupport.config(fixture.directory(), "file"));
        resources.add(session);
        FileITSupport.Events capture = new FileITSupport.Events();

        assertEquals(2L, session.functions.getBatchCountFunction().count(session.nodeContext, new TapTable("file")));
        session.functions.getBatchReadFunction().batchRead(session.nodeContext, new TapTable("file"), null, 10, capture::accept);

        assertEquals(2, capture.events().size());
        for (TapEvent event : capture.events()) {
            Map<String, Object> after = ((TapInsertRecordEvent) event).getAfter();
            String fileName = String.valueOf(after.get("file_name"));
            assertTrue(fileName.equals("payload.txt") || fileName.equals("second.txt"));
            assertTrue(String.valueOf(after.get("file_path")).endsWith(fileName));
            long size = ((Number) after.get("file_size")).longValue();
            assertTrue(size > 0);
            assertTrue(((Number) after.get("last_modified")).longValue() > 0);
            try (InputStream input = (InputStream) after.get("file_data")) {
                assertEquals(size, input.readAllBytes().length);
            }
        }
        assertNotNull(capture.offsets().get(capture.offsets().size() - 1));
    }

    @Test
    @DisplayName("honors recursive directory scanning and include filters")
    void shouldFilterRecursiveFiles() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("file-stream", "payload.txt");
        resources.add(fixture);
        Path nested = fixture.directory().resolve("nested").resolve("payload.txt");
        FileITSupport.copyResource("file-stream", "stream-payload.txt", nested);
        FileStreamConnector connector = new FileStreamConnector();
        DataMap config = FileITSupport.config(fixture.directory(), "file")
                .kv("recursive", true)
                .kv("includeRegString", "payload.txt");
        FileITSupport.Session session = FileITSupport.open(connector, "filestream", config);
        resources.add(session);

        assertEquals(2L, session.functions.getBatchCountFunction().count(session.nodeContext, new TapTable("file")));
    }

    @Test
    @DisplayName("counts only valid insert file events on write")
    void shouldIgnoreUnsupportedWriteEvents() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("file-stream", "payload.txt");
        resources.add(fixture);
        FileStreamConnector connector = new FileStreamConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "filestream", FileITSupport.config(fixture.directory(), "file"));
        resources.add(session);
        byte[] payload = "valid".getBytes(StandardCharsets.UTF_8);
        Map<String, Object> valid = new java.util.HashMap<>();
        valid.put("file_name", "valid.txt");
        valid.put("file_data", new ByteArrayInputStream(payload));
        Map<String, Object> missingData = new java.util.HashMap<>();
        missingData.put("file_name", "missing.txt");
        List<TapRecordEvent> events = Arrays.asList(
                new TapInsertRecordEvent().init().table("file").after(valid),
                new TapInsertRecordEvent().init().table("file").after(missingData),
                new TapUpdateRecordEvent().init().table("file").before(valid).after(valid),
                new TapDeleteRecordEvent().init().table("file").before(valid));
        AtomicReference<WriteListResult<TapRecordEvent>> result = new AtomicReference<>();

        session.functions.getWriteRecordFunction().writeRecord(session.nodeContext, events, new TapTable("file"), result::set);

        assertEquals(1L, result.get().getInsertedCount());
        assertTrue(Files.exists(fixture.directory().resolve("valid.txt")));
        assertEquals(false, Files.exists(fixture.directory().resolve("missing.txt")));
    }

    @Test
    @DisplayName("writes file data through the connector capability")
    void shouldWriteFileData() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("file-stream", "payload.txt");
        resources.add(fixture);
        FileStreamConnector connector = new FileStreamConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "filestream", FileITSupport.config(fixture.directory(), "file"));
        resources.add(session);
        byte[] payload = "written-by-it".getBytes(StandardCharsets.UTF_8);
        Map<String, Object> after = new java.util.HashMap<>();
        after.put("file_name", "written.txt");
        after.put("file_data", new ByteArrayInputStream(payload));
        AtomicReference<WriteListResult<TapRecordEvent>> result = new AtomicReference<>();

        TapInsertRecordEvent event = new TapInsertRecordEvent().init().table("file").after(after);
        session.functions.getWriteRecordFunction().writeRecord(session.nodeContext, Collections.singletonList(event),
                new TapTable("file"), result::set);

        assertNotNull(result.get());
        assertEquals(1L, result.get().getInsertedCount());
        assertArrayEquals(payload, Files.readAllBytes(fixture.directory().resolve("written.txt")));
    }

    @Test
    @DisplayName("creates a file offset for incremental scanning")
    void shouldCreateTimestampOffset() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("file-stream", "payload.txt");
        resources.add(fixture);
        FileStreamConnector connector = new FileStreamConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "filestream", FileITSupport.config(fixture.directory(), "file"));
        resources.add(session);

        Object offset = session.functions.getTimestampToStreamOffsetFunction()
                .timestampToStreamOffset(session.nodeContext, System.currentTimeMillis());

        assertEquals("FileOffset", offset.getClass().getSimpleName());
        assertEquals(1, ((FileOffset) offset).getAllFiles().size());
        assertNotNull(session.functions.getStreamReadFunction());
    }

    @Test
    @DisplayName("captures a newly appearing file through streamRead")
    void shouldStreamNewFile() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("file-stream", "payload.txt");
        resources.add(fixture);
        FileStreamConnector connector = new FileStreamConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "filestream", FileITSupport.config(fixture.directory(), "file"));
        resources.add(session);
        TapTable table = new TapTable("file");
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
        }, "file-stream-connector-it-stream");
        thread.start();
        FileITSupport.copyResource("file-stream", "stream-payload.txt", fixture.directory().resolve("stream-payload.txt"));
        try {
            assertTrue(received.await(10, TimeUnit.SECONDS), "new file was not emitted");
            assertNull(failure.get());
        } finally {
            consumer.streamReadEnded();
            session.close();
            thread.interrupt();
            thread.join(5000);
        }
        assertFalse(thread.isAlive(), "file stream thread did not stop");
    }
}
