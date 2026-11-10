package io.tapdata.it.ddl;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.TapDDLUnknownEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.support.TestStateMap;
import io.tapdata.it.support.TestTableMap;
import io.tapdata.kafka.KafkaEnhancedCoreConnector;
import io.tapdata.kafka.data.KafkaOffset;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Real broker/registry tests: a single schema version can add several fields before its DML record. */
public abstract class KafkaSchemaDdlIT {
    protected abstract KafkaEnhancedCoreConnector connector();

    @Test
    void should_stream_composite_add_columns_ddl() throws Throwable {
        captureSchemaChanges(true);
    }

    @Test
    void should_stream_read_ddl_dml_drop_ddl_dml_sequence() throws Throwable {
        captureSchemaChanges(false);
    }

    private void captureSchemaChanges(boolean composite) throws Throwable {
        String brokers = setting("brokers", "CONNECTOR_IT_KAFKA_BROKERS");
        String registry = setting("registry", "CONNECTOR_IT_KAFKA_REGISTRY");
        String topic = "tap_ddl_it_" + UUID.randomUUID().toString().replace("-", "");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", brokers);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", KafkaAvroSerializer.class.getName());
        properties.put("schema.registry.url", registry);
        properties.put("max.block.ms", "30000");
        List<TapEvent> received = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Throwable> error = new AtomicReference<>();
        KafkaEnhancedCoreConnector connector = connector();
        ConnectorFunctions functions = new ConnectorFunctions();
        connector.registerCapabilities(functions, TapCodecsRegistry.create());
        DataMap config = DataMap.create().kv("clusterURI", brokers).kv("schemaRegister", true)
                .kv("registrySchemaType", "AVRO").kv("schemaRegisterUrl", registry)
                .kv("keySerialization", "STRING").kv("valueSerialization", "STRING");
        TapNodeSpecification specification = new TapNodeSpecification();
        specification.setId("kafka_enhanced");
        TapConnectorContext node = new TapConnectorContext(specification, config, DataMap.create(), new TapLog());
        node.setStateMap(new TestStateMap());
        TestTableMap tables = new TestTableMap();
        TapTable table = new TapTable(topic)
                .add(new TapField("id", "long").tapType(TapSimplify.tapNumber().bit(64)).nullable(false).isPrimaryKey(true).primaryKeyPos(1))
                .add(new TapField("value", "string").tapType(TapSimplify.tapString()).nullable(false));
        tables.put(topic, table);
        node.setTableMap(tables);
        Thread reader = null;
        boolean initialized = false;
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", brokers))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
            try {
                connector.init(node);
                initialized = true;
                KafkaOffset offset = new KafkaOffset();
                offset.setOffset(topic, 0, 0L);
                StreamReadConsumer consumer = StreamReadConsumer.create((events, off) -> received.addAll(events));
                reader = new Thread(() -> {
                    try {
                        functions.getStreamReadFunction().streamRead(node, List.of(topic), offset, 100, consumer);
                    } catch (Throwable failure) {
                        error.set(failure);
                    }
                }, "tap-it-kafka-schema-ddl");
                reader.setDaemon(true);
                reader.start();
                Schema baseline = schema(false, composite);
                Schema added = schema(true, composite);
                try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(properties)) {
                    send(producer, topic, baseline, 1L, composite);
                    send(producer, topic, added, 2L, composite);
                    if (!composite) send(producer, topic, baseline, 3L, false);
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (System.nanoTime() < deadline && error.get() == null && !hasRow(received, composite ? 2L : 3L)) {
                    Thread.sleep(50L);
                }
                if (error.get() != null) throw new AssertionError("Kafka stream failed", error.get());
                List<TapEvent> events;
                synchronized (received) { events = new ArrayList<>(received); }
                assertTrue(hasRow(events, composite ? 2L : 3L), "missing evolved schema DML: " + events);
                int addIndex = index(events, TapNewFieldEvent.class);
                int firstDml = rowIndex(events, 2L);
                assertTrue(addIndex >= 0 && addIndex < firstDml, "ADD must precede DML: " + events);
                Set<String> fields = new HashSet<>();
                for (TapEvent event : events) {
                    if (event instanceof TapDDLEvent) {
                        assertFalse(event instanceof TapDDLUnknownEvent, "schema changes require valid DDL events");
                        assertEquals(topic, ((TapDDLEvent) event).getTableId());
                    }
                    if (event instanceof TapNewFieldEvent) {
                        for (TapField field : ((TapNewFieldEvent) event).getNewFields()) {
                            assertTrue(fields.add(field.getName()), "duplicate added column");
                            assertNotNull(field.getDataType());
                        }
                    }
                }
                assertEquals(composite ? Set.of("c_composite_a", "c_composite_b") : Set.of("c_ddl_probe"), fields);
                Map<String, Object> after = ((TapInsertRecordEvent) events.get(firstDml)).getAfter();
                assertEquals("schema-marker", after.get(composite ? "c_composite_a" : "c_ddl_probe"));
                if (composite) {
                    assertEquals("42", String.valueOf(after.get("c_composite_b")));
                } else {
                    int drop = index(events, TapDropFieldEvent.class);
                    int secondDml = rowIndex(events, 3L);
                    assertTrue(firstDml < drop && drop < secondDml, "expected ADD -> DML -> DROP -> DML: " + events);
                    assertEquals("c_ddl_probe", ((TapDropFieldEvent) events.get(drop)).getFieldName());
                    assertFalse(((TapInsertRecordEvent) events.get(secondDml)).getAfter().containsKey("c_ddl_probe"));
                }
            } finally {
                if (initialized) connector.stop(node);
                if (reader != null) {
                    reader.interrupt();
                    reader.join(10000L);
                    assertFalse(reader.isAlive(), "Kafka source must stop before deleting its topic");
                }
                admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
                new io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient(registry, 10)
                        .deleteSubject(topic + "-value");
            }
        }
    }

    private static Schema schema(boolean added, boolean composite) {
        String fields = "{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"value\",\"type\":\"string\"}";
        if (added) {
            fields += ",{\"name\":\"" + (composite ? "c_composite_a" : "c_ddl_probe")
                    + "\",\"type\":[\"null\",\"string\"],\"default\":null}";
            if (composite) fields += ",{\"name\":\"c_composite_b\",\"type\":[\"null\",\"long\"],\"default\":null}";
        }
        return new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"DdlRow\",\"fields\":[" + fields + "]}");
    }

    private static void send(KafkaProducer<String, GenericRecord> producer, String topic, Schema schema,
                             long id, boolean composite) throws Exception {
        GenericRecord row = new GenericData.Record(schema);
        row.put("id", id);
        row.put("value", "row-" + id);
        String marker = composite ? "c_composite_a" : "c_ddl_probe";
        if (schema.getField(marker) != null) row.put(marker, "schema-marker");
        if (schema.getField("c_composite_b") != null) row.put("c_composite_b", 42L);
        producer.send(new ProducerRecord<>(topic, String.valueOf(id), row)).get(30, TimeUnit.SECONDS);
    }

    private static boolean hasRow(List<TapEvent> events, long id) {
        synchronized (events) { return rowIndex(events, id) >= 0; }
    }

    private static int rowIndex(List<TapEvent> events, long id) {
        for (int i = 0; i < events.size(); i++) {
            if (events.get(i) instanceof TapInsertRecordEvent
                    && String.valueOf(id).equals(String.valueOf(((TapInsertRecordEvent) events.get(i)).getAfter().get("id")))) return i;
        }
        return -1;
    }

    private static int index(List<TapEvent> events, Class<? extends TapEvent> type) {
        for (int i = 0; i < events.size(); i++) if (type.isInstance(events.get(i))) return i;
        return -1;
    }

    private static String setting(String key, String environment) {
        String value = System.getProperty("connector.it.kafka." + key, System.getenv(environment));
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Set " + environment);
        return value;
    }
}
