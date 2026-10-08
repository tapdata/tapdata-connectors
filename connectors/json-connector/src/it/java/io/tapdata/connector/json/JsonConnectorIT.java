package io.tapdata.connector.json;

import io.tapdata.connector.it.FileITSupport;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("JSON connector integration tests")
public class JsonConnectorIT {
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
    @DisplayName("registers JSON read capabilities without target write")
    void shouldRegisterCapabilities() {
        ConnectorFunctions functions = new ConnectorFunctions();
        new JsonConnector().registerCapabilities(functions, TapCodecsRegistry.create());

        assertNotNull(functions.getBatchCountFunction());
        assertNotNull(functions.getBatchReadFunction());
        assertNotNull(functions.getStreamReadFunction());
        assertNotNull(functions.getTimestampToStreamOffsetFunction());
        assertNull(functions.getWriteRecordFunction());
    }

    @Test
    @DisplayName("discovers fields from a JSON array root")
    void shouldDiscoverArraySchema() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("json", "array.json");
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "json_it").kv("jsonType", "JSONArray");
        JsonConnector connector = new JsonConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "json", config);
        resources.add(session);

        List<TapTable> discovered = new ArrayList<>();
        connector.discoverSchema(session.nodeContext, Collections.singletonList("json_it"), 10, discovered::addAll);

        assertEquals(1, discovered.size());
        assertTrue(discovered.get(0).getNameFieldMap().keySet().containsAll(
                java.util.Arrays.asList("id", "name", "active")));
    }

    @Test
    @DisplayName("discovers nested JSON object, array and primitive types")
    void shouldDiscoverComplexArraySchema() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("json", "complex-array.json");
        resources.add(fixture);
        JsonConnector connector = new JsonConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "json", FileITSupport.config(fixture.directory(), "json_it")
                .kv("jsonType", "JSONArray"));
        resources.add(session);
        List<TapTable> discovered = new ArrayList<>();

        connector.discoverSchema(session.nodeContext, Collections.singletonList("json_it"), 10, discovered::addAll);

        TapTable table = discovered.get(0);
        assertEquals("NUMBER", table.getNameFieldMap().get("id").getDataType());
        assertEquals("NUMBER", table.getNameFieldMap().get("amount").getDataType());
        assertEquals("BOOLEAN", table.getNameFieldMap().get("active").getDataType());
        assertEquals("OBJECT", table.getNameFieldMap().get("profile").getDataType());
        assertEquals("STRING", table.getNameFieldMap().get("profile.city").getDataType());
        assertEquals("ARRAY", table.getNameFieldMap().get("tags").getDataType());
    }

    @Test
    @DisplayName("reads one event per JSON array object")
    void shouldReadArrayRecords() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("json", "array.json");
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "json_it").kv("jsonType", "JSONArray");
        JsonConnector connector = new JsonConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "json", config);
        resources.add(session);
        FileITSupport.Events capture = new FileITSupport.Events();

        session.functions.getBatchReadFunction().batchRead(session.nodeContext, new TapTable("json_it"), null, 100,
                capture::accept);

        List<TapEvent> events = capture.events();
        assertEquals(2, events.size());
        assertTrue(events.stream().allMatch(event -> event instanceof TapInsertRecordEvent));
        Map<String, Object> first = ((TapInsertRecordEvent) events.get(0)).getAfter();
        assertEquals(1L, ((Number) first.get("id")).longValue());
        assertEquals("Alice", first.get("name"));
        assertEquals(Boolean.TRUE, first.get("active"));
        assertNotNull(capture.offsets().get(capture.offsets().size() - 1));
    }

    @Test
    @DisplayName("preserves JSON decimal, nested and null values")
    void shouldReadComplexArrayRecords() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("json", "complex-array.json");
        resources.add(fixture);
        JsonConnector connector = new JsonConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "json", FileITSupport.config(fixture.directory(), "json_it")
                .kv("jsonType", "JSONArray"));
        resources.add(session);
        FileITSupport.Events capture = new FileITSupport.Events();

        session.functions.getBatchReadFunction().batchRead(session.nodeContext, new TapTable("json_it"), null, 1,
                capture::accept);

        assertEquals(2, capture.events().size());
        Map<String, Object> first = ((TapInsertRecordEvent) capture.events().get(0)).getAfter();
        assertEquals(new BigDecimal("12.50"), first.get("amount"));
        assertEquals("Shanghai", ((Map<?, ?>) first.get("profile")).get("city"));
        assertEquals(Arrays.asList("a", "b"), first.get("tags"));
        assertNull(first.get("nullable"));
    }

    @Test
    @DisplayName("injects __key for a JSON object root")
    void shouldReadObjectRecordsWithKeys() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("json", "object.json");
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "json_it").kv("jsonType", "JSONObject");
        JsonConnector connector = new JsonConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "json", config);
        resources.add(session);
        FileITSupport.Events capture = new FileITSupport.Events();

        session.functions.getBatchReadFunction().batchRead(session.nodeContext, new TapTable("json_it"), null, 100,
                capture::accept);

        List<TapEvent> events = capture.events();
        assertEquals(2, events.size());
        assertEquals("first", ((TapInsertRecordEvent) events.get(0)).getAfter().get("__key"));
        assertEquals("second", ((TapInsertRecordEvent) events.get(1)).getAfter().get("__key"));
    }

    @Test
    @DisplayName("discovers object-root keys and nested values")
    void shouldDiscoverObjectSchema() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("json", "object-complex.json");
        resources.add(fixture);
        JsonConnector connector = new JsonConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "json", FileITSupport.config(fixture.directory(), "json_it")
                .kv("jsonType", "JSONObject"));
        resources.add(session);
        List<TapTable> discovered = new ArrayList<>();
        connector.discoverSchema(session.nodeContext, Collections.singletonList("json_it"), 10, discovered::addAll);

        assertTrue(discovered.get(0).getNameFieldMap().containsKey("__key"));
        assertEquals("OBJECT", discovered.get(0).getNameFieldMap().get("profile").getDataType());
        assertEquals("STRING", discovered.get(0).getNameFieldMap().get("profile.city").getDataType());
    }

    @Test
    @DisplayName("reads all files in a JSON directory")
    void shouldReadMultipleJsonFiles() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("json", "array.json", "stream-array.json");
        resources.add(fixture);
        JsonConnector connector = new JsonConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "json", FileITSupport.config(fixture.directory(), "json_it")
                .kv("jsonType", "JSONArray"));
        resources.add(session);
        FileITSupport.Events capture = new FileITSupport.Events();

        session.functions.getBatchReadFunction().batchRead(session.nodeContext, new TapTable("json_it"), null, 10,
                capture::accept);

        assertEquals(3, capture.events().size());
        assertTrue(capture.events().stream().allMatch(event -> event instanceof TapInsertRecordEvent));
    }

    @Test
    @DisplayName("creates a JSON file offset")
    void shouldCreateTimestampOffset() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("json", "array.json");
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "json_it").kv("jsonType", "JSONArray");
        JsonConnector connector = new JsonConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "json", config);
        resources.add(session);

        Object offset = session.functions.getTimestampToStreamOffsetFunction()
                .timestampToStreamOffset(session.nodeContext, System.currentTimeMillis());

        assertNotNull(offset);
        assertEquals("FileOffset", offset.getClass().getSimpleName());
        assertNotNull(session.functions.getStreamReadFunction());
    }

    @Test
    @DisplayName("captures a newly appearing JSON file through streamRead")
    void shouldStreamNewJsonFile() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("json", "array.json");
        resources.add(fixture);
        JsonConnector connector = new JsonConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "json", FileITSupport.config(fixture.directory(), "json_it")
                .kv("jsonType", "JSONArray"));
        resources.add(session);
        TapTable table = new TapTable("json_it");
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
        }, "json-connector-it-stream");
        thread.start();
        FileITSupport.copyResource("json", "stream-array.json", fixture.directory().resolve("stream-array.json"));
        try {
            assertTrue(received.await(10, TimeUnit.SECONDS), "new JSON file was not emitted");
            assertNull(failure.get());
        } finally {
            consumer.streamReadEnded();
            session.close();
            thread.interrupt();
            thread.join(5000);
        }
        assertFalse(thread.isAlive(), "JSON stream thread did not stop");
    }
}
