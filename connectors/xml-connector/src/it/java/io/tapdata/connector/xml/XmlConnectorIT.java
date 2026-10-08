package io.tapdata.connector.xml;

import io.tapdata.connector.it.FileITSupport;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("XML connector integration tests")
public class XmlConnectorIT {
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
    @DisplayName("registers XML source capabilities without target write")
    void shouldRegisterCapabilities() {
        ConnectorFunctions functions = new ConnectorFunctions();
        new XmlConnector().registerCapabilities(functions, TapCodecsRegistry.create());

        assertNotNull(functions.getBatchCountFunction());
        assertNotNull(functions.getBatchReadFunction());
        assertNotNull(functions.getStreamReadFunction());
        assertNotNull(functions.getTimestampToStreamOffsetFunction());
        assertNull(functions.getWriteRecordFunction());
    }

    @Test
    @DisplayName("discovers the XML node selected by XPath")
    void shouldDiscoverSchema() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("xml", "items.xml");
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "xml_it");
        XmlConnector connector = new XmlConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "xml", config);
        resources.add(session);
        List<TapTable> discovered = new ArrayList<>();

        connector.discoverSchema(session.nodeContext, Collections.singletonList("xml_it"), 10, discovered::addAll);

        assertEquals(1, discovered.size());
        assertTrue(discovered.get(0).getNameFieldMap().containsKey("info"));
    }

    @Test
    @DisplayName("discovers nested XML values and repeated child fields")
    void shouldDiscoverNestedSchema() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("xml", "nested-items.xml");
        resources.add(fixture);
        XmlConnector connector = new XmlConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "xml", FileITSupport.config(fixture.directory(), "xml_it"));
        resources.add(session);
        List<TapTable> discovered = new ArrayList<>();
        connector.discoverSchema(session.nodeContext, Collections.singletonList("xml_it"), 10, discovered::addAll);

        assertEquals("STRING", discovered.get(0).getNameFieldMap().get("title").getDataType());
        assertEquals("ARRAY", discovered.get(0).getNameFieldMap().get("tag").getDataType());
    }

    @Test
    @DisplayName("reads XML text and CDATA while excluding comments and processing instructions")
    void shouldReadXmlRecords() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("xml", "items.xml");
        resources.add(fixture);
        DataMap config = FileITSupport.config(fixture.directory(), "xml_it");
        XmlConnector connector = new XmlConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "xml", config);
        resources.add(session);
        FileITSupport.Events capture = new FileITSupport.Events();
        TapTable table = new TapTable("xml_it").add(new TapField("info", "STRING"));

        session.functions.getBatchReadFunction().batchRead(session.nodeContext, table, null, 1, capture::accept);

        assertEquals(2, capture.events().size());
        for (TapEvent event : capture.events()) {
            Map<String, Object> after = ((TapInsertRecordEvent) event).getAfter();
            String info = String.valueOf(after.get("info"));
            assertFalse(info.contains("comment"));
            assertFalse(info.contains("processing"));
            assertTrue(info.contains("item"));
        }
        assertNotNull(capture.offsets().get(capture.offsets().size() - 1));
        assertEquals(2, capture.offsets().size());
    }

    @Test
    @DisplayName("reads nested XML maps and repeated child lists")
    void shouldReadNestedXmlRecords() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("xml", "nested-items.xml");
        resources.add(fixture);
        XmlConnector connector = new XmlConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "xml", FileITSupport.config(fixture.directory(), "xml_it"));
        resources.add(session);
        FileITSupport.Events capture = new FileITSupport.Events();
        TapTable table = new TapTable("xml_it")
                .add(new TapField("title", "STRING"))
                .add(new TapField("tag", "ARRAY"));

        session.functions.getBatchReadFunction().batchRead(session.nodeContext, table, null, 100, capture::accept);

        assertEquals(2, capture.events().size());
        Map<String, Object> first = ((TapInsertRecordEvent) capture.events().get(0)).getAfter();
        assertEquals("first", first.get("title"));
        assertEquals(Arrays.asList("A", "B"), first.get("tag"));
    }

    @Test
    @DisplayName("creates string schema when justString is enabled")
    void shouldDiscoverJustStringSchema() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("xml", "items.xml");
        resources.add(fixture);
        XmlConnector connector = new XmlConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "xml", FileITSupport.config(fixture.directory(), "xml_it")
                .kv("justString", true));
        resources.add(session);
        List<TapTable> discovered = new ArrayList<>();
        connector.discoverSchema(session.nodeContext, Collections.singletonList("xml_it"), 10, discovered::addAll);

        assertTrue(discovered.get(0).getNameFieldMap().values().stream()
                .allMatch(field -> "STRING".equals(field.getDataType())));
    }

    @Test
    @DisplayName("creates an XML file offset")
    void shouldCreateTimestampOffset() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("xml", "items.xml");
        resources.add(fixture);
        XmlConnector connector = new XmlConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "xml", FileITSupport.config(fixture.directory(), "xml_it"));
        resources.add(session);

        Object offset = session.functions.getTimestampToStreamOffsetFunction()
                .timestampToStreamOffset(session.nodeContext, System.currentTimeMillis());

        assertEquals("FileOffset", offset.getClass().getSimpleName());
        assertNotNull(session.functions.getStreamReadFunction());
    }

    @Test
    @DisplayName("captures a newly appearing XML file through streamRead")
    void shouldStreamNewXmlFile() throws Throwable {
        FileITSupport.Fixture fixture = FileITSupport.fixture("xml", "items.xml");
        resources.add(fixture);
        XmlConnector connector = new XmlConnector();
        FileITSupport.Session session = FileITSupport.open(connector, "xml", FileITSupport.config(fixture.directory(), "xml_it"));
        resources.add(session);
        TapTable table = new TapTable("xml_it").add(new TapField("info", "STRING"));
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
        }, "xml-connector-it-stream");
        thread.start();
        FileITSupport.copyResource("xml", "stream-items.xml", fixture.directory().resolve("stream-items.xml"));
        try {
            assertTrue(received.await(10, TimeUnit.SECONDS), "new XML file was not emitted");
            assertNull(failure.get());
        } finally {
            consumer.streamReadEnded();
            session.close();
            thread.interrupt();
            thread.join(5000);
        }
        assertFalse(thread.isAlive(), "XML stream thread did not stop");
    }
}
