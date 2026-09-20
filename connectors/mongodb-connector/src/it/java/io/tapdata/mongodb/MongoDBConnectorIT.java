package io.tapdata.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.ConnectorTestContext;
import io.tapdata.it.UnderTest;
import io.tapdata.it.dbforge.DbForgeLeaseProvider;
import io.tapdata.it.performance.PerformanceAdapter;
import io.tapdata.it.support.TestStateMap;
import io.tapdata.it.tpcc.TpccAdapter;
import io.tapdata.it.tpcc.TpccConnectorIT;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.bson.BsonRegularExpression;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.addToSet;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.popLast;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.unset;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MongoDBConnectorIT extends TpccConnectorIT {

    private MongoClient directClient;
    private DbForgeLeaseProvider dbForgeLeaseProvider;
    private DataMap dbForgeConnectionConfig;

    @Override
    protected PerformanceAdapter createPerformanceAdapter() {
        return new MongoPerformanceAdapter(context.getConfig());
    }

    @Override
    protected TpccAdapter createTpccAdapter() {
        return new MongoTpccAdapter(context.getConfig());
    }

    @Override
    protected long streamReadTimeoutSeconds() {
        return 120L;
    }

    @Override
    protected Set<String> requiredCapabilities() {
        return Stream.of("connectionTest", "discoverSchema", "getTableNames",
                "createTableV2", "dropTable", "batchCount", "batchRead", "streamRead",
                "timestampToStreamOffset", "queryByAdvanceFilter", "countByPartitionFilter",
                "writeRecord", "createIndex", "queryIndexes", "errorHandle",
                "executeCommand", "getTableInfo", "getReadPartitions", "queryFieldMinMaxValue",
                "transactionBegin", "transactionCommit", "transactionRollback").collect(Collectors.toSet());
    }

    @Test
    @UnderTest(value = "batchRead", requiresVerifier = true)
    @UnderTest(value = "writeRecord", requiresVerifier = true)
    @UnderTest(value = "streamRead", requiresVerifier = true)
    void should_round_trip_bson_nested_and_one_megabyte_values() throws Throwable {
        String largeText = repeat("mongo", 220000);
        byte[] largeBinary = new byte[1024 * 1024];
        Arrays.fill(largeBinary, (byte) 0x5a);
        Document source = fullTypeDocument("full-1", largeText, largeBinary);
        collection().insertOne(source);

        TapTable table = discoverTable();
        registerTable(table);
        Map<String, Object> batchRow = rowById(batchReadAll(table), "full-1");
        assertEquals(largeText, batchRow.get("large_text"));
        assertArrayEquals(largeBinary, binaryValue(batchRow.get("large_binary")));
        assertTrue(batchRow.get("nested") instanceof Map, "nested BSON document should remain structured");
        assertTrue(batchRow.get("array") instanceof List, "BSON array should remain structured");

        Map<String, Object> target = new LinkedHashMap<>(batchRow);
        target.put("_id", "full-2");
        assertEquals(1L, writeInsertEventsViaEngineCodec(Collections.singletonList(target), table));
        Map<String, Object> written = rowById(batchReadAll(table), "full-2");
        assertEquals(largeText, written.get("large_text"));
        assertArrayEquals(largeBinary, binaryValue(written.get("large_binary")));

        Capture capture = startCapture(table, currentOffset());
        try {
            collection().insertOne(fullTypeDocument("full-3", largeText, largeBinary));
            waitForIds(capture, "full-3");
            Map<String, Object> streamed = capture.rows.stream().filter(row -> "full-3".equals(stringId(row))).findFirst()
                    .orElseThrow(() -> new AssertionError("missing MongoDB full-type CDC row"));
            assertEquals(largeText, streamed.get("large_text"));
            assertArrayEquals(largeBinary, binaryValue(streamed.get("large_binary")));
        } finally {
            capture.stop();
        }
    }

    @Test
    @UnderTest(value = "streamRead", requiresVerifier = true)
    @UnderTest("timestampToStreamOffset")
    void should_resume_from_saved_offset() throws Throwable {
        collection().insertOne(new Document("_id", "resume-seed").append("value", "seed"));
        TapTable table = discoverTable();
        registerTable(table);
        Object savedOffset = currentOffset();
        collection().insertOne(new Document("_id", "resume-1").append("value", "written-while-stopped"));
        Capture capture = startCapture(table, savedOffset);
        try {
            waitForIds(capture, "resume-1");
            assertNotNull(capture.lastOffset.get(), "stream callback should provide a resumable offset");
        } finally {
            capture.stop();
        }
    }

    @Test
    @UnderTest(value = "streamRead", requiresVerifier = true)
    void should_stream_nested_insert_update_delete_and_array_operators() throws Throwable {
        collection().insertOne(new Document("_id", "mutation-seed").append("value", "seed"));
        TapTable table = discoverTable();
        registerTable(table);
        Capture capture = startCapture(table, currentOffset());
        try {
            collection().insertOne(new Document("_id", "mutation-1")
                    .append("profile", new Document("name", "before").append("obsolete", true))
                    .append("tags", new ArrayList<>(Arrays.asList("one", "two"))));
            waitForIds(capture, "mutation-1");
            collection().updateOne(eq("_id", "mutation-1"), combine(
                    set("profile.name", "after"), unset("profile.obsolete"), addToSet("tags", "three")));
            collection().updateOne(eq("_id", "mutation-1"), popLast("tags"));
            collection().deleteOne(eq("_id", "mutation-1"));
            waitForMutationEvents(capture, "mutation-1");
        } finally {
            capture.stop();
        }
    }

    @Test
    @UnderTest(value = "discoverSchema", requiresVerifier = true)
    @UnderTest(value = "batchRead", requiresVerifier = true)
    void should_discover_sparse_schema_and_preserve_nested_documents() throws Throwable {
        collection().insertMany(Arrays.asList(
                new Document("_id", "schema-1").append("name", "first").append("nested", new Document("left", 1)),
                new Document("_id", "schema-2").append("name", "second").append("optional", true)
                        .append("nested", new Document("right", Arrays.asList(1, 2, 3)))));
        TapTable table = discoverTable();
        registerTable(table);
        assertTrue(table.getNameFieldMap().containsKey("optional"), "sparse field should be discovered");
        assertTrue(table.getNameFieldMap().containsKey("nested"), "nested document field should be discovered");
        assertEquals(2, batchReadAll(table).size());
    }

    @Test
    @UnderTest(value = "streamRead", requiresVerifier = true)
    void should_stream_only_committed_transaction_documents() throws Throwable {
        collection().insertOne(new Document("_id", "tx-seed").append("value", "seed"));
        TapTable table = discoverTable();
        registerTable(table);
        Capture capture = startCapture(table, currentOffset());
        try (MongoClient client = client()) {
            try (ClientSession committed = client.startSession()) {
                committed.startTransaction();
                collection(client).insertOne(committed, new Document("_id", "tx-commit-1").append("value", "one"));
                collection(client).insertOne(committed, new Document("_id", "tx-commit-2").append("value", "two"));
                committed.commitTransaction();
            }
            try (ClientSession aborted = client.startSession()) {
                aborted.startTransaction();
                collection(client).insertOne(aborted, new Document("_id", "tx-abort-1").append("value", "never-visible"));
                aborted.abortTransaction();
            }
            waitForIds(capture, "tx-commit-1", "tx-commit-2");
            Thread.sleep(2000L);
            assertFalse(capture.ids.contains("tx-abort-1"), "aborted transaction must not appear in CDC");
            assertEquals(0L, collection(client).countDocuments(eq("_id", "tx-abort-1")));
        } finally {
            capture.stop();
        }
    }

    @Test
    @Tag("long-transaction")
    @UnderTest(value = "streamRead", requiresVerifier = true)
    void should_stream_large_committed_transaction() throws Throwable {
        collection().insertOne(new Document("_id", "transaction-seed").append("value", "seed"));
        TapTable table = discoverTable();
        registerTable(table);
        Capture capture = startCapture(table, currentOffset());
        int rows = Integer.parseInt(System.getProperty("mongo.longTransactionRows", "2000"));
        try (MongoClient client = client(); ClientSession session = client.startSession()) {
            session.startTransaction();
            List<Document> batch = new ArrayList<>();
            for (int index = 0; index < rows; index++) {
                batch.add(new Document("_id", "long-tx-" + index).append("value", repeat("payload", 64)));
            }
            collection(client).insertMany(session, batch);
            session.commitTransaction();
            waitForCount(capture, rows, 180);
            assertEquals(rows, capture.ids.stream().filter(id -> id.startsWith("long-tx-")).count());
        } finally {
            capture.stop();
        }
    }

    @Override
    protected ConnectorTestContext createContext() throws Throwable {
        MongodbConnector connector = new MongodbConnector();
        DataMap config = loadConnectionConfig();
        TapLog log = new TapLog();
        TapConnectorContext nodeContext = new TapConnectorContext(
                loadSpecification("spec.json"), config, DataMap.create(), log);
        nodeContext.setStateMap(new TestStateMap());
        nodeContext.setGlobalStateMap(new TestStateMap());
        ConnectorFunctions functions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = TapCodecsRegistry.create();
        connector.registerCapabilities(functions, codecRegistry);
        ConnectorTestContext testContext = ConnectorTestContext.builder()
                .connector(connector)
                .nodeContext(nodeContext)
                .connectorFunctions(functions)
                .codecRegistry(codecRegistry)
                .config(config)
                .log(log)
                .createTableReportsTableExists(false)
                .schemaDiscoveryRequiresSampleData(true)
                .schemaAllowsExtraFields(true)
                .schemaPrimaryKeyStrict(false)
                .executeCommandSupportsPing(false)
                .fieldMinMaxRequiresPartitionIndex(true)
                .build();
        testContext.getLog().info("[IT] MongoDB connection: uri={}, database={}",
                config.getString("uri"), config.getString("database"));
        return testContext;
    }

    private synchronized DataMap loadConnectionConfig() throws Exception {
        if (!DbForgeLeaseProvider.isDbForgeSelected()) {
            return readConnectionConfig("config/mongodb-connection.json");
        }
        if (dbForgeConnectionConfig == null) {
            dbForgeLeaseProvider = DbForgeLeaseProvider.fromEnvironment("tapdata-mongodb-connector-it");
            DbForgeLeaseProvider.Connection connection = dbForgeLeaseProvider.acquire("mongodb", "dedicated", "replicaset");
            dbForgeConnectionConfig = DataMap.create();
            dbForgeConnectionConfig.put("uri", connection.required("uri"));
            dbForgeConnectionConfig.put("database", connection.required("database"));
            System.out.printf("[IT] DBForge MongoDB lease acquired: leaseId=%s, database=%s%n",
                    dbForgeLeaseProvider.getLeaseId(), dbForgeConnectionConfig.getString("database"));
        }
        DataMap config = DataMap.create();
        config.putAll(dbForgeConnectionConfig);
        return config;
    }

    @AfterAll
    void releaseDbForgeLease() {
        if (dbForgeLeaseProvider == null) {
            return;
        }
        try {
            dbForgeLeaseProvider.release();
        } catch (Exception error) {
            throw new RuntimeException("Failed to release DBForge MongoDB lease", error);
        }
    }

    @Override
    protected List<Map<String, Object>> beforeWrite(List<Map<String, Object>> rows) {
        Set<Long> seen = new HashSet<>();
        long sequence = 1_000_001L;
        for (Map<String, Object> row : rows) {
            long value = ((Number) row.get("c_int")).longValue();
            if (!seen.add(value)) {
                row.put("c_int", sequence++);
            }
        }
        return rows;
    }

    @Override
    protected Map<String, Object> specialValueSamples() {
        Map<String, Object> samples = new LinkedHashMap<>();
        samples.put("obj_objectid", new ObjectId());
        samples.put("obj_binary", new Binary((byte) 0x80, new byte[]{1, 2, 3}));
        samples.put("obj_code", new Code("function() { return 1; }"));
        samples.put("obj_decimal128", Decimal128.parse("12345.6789"));
        samples.put("obj_symbol", new Symbol("sym"));
        samples.put("obj_bson_timestamp", new BsonTimestamp(1_700_000_000, 1));
        samples.put("obj_regex", new BsonRegularExpression("^tap.*", "i"));
        return samples;
    }

    @Override
    protected void dropResidualTables() {
        try {
            super.dropResidualTables();
        } finally {
            if (directClient != null) {
                directClient.close();
                directClient = null;
            }
        }
    }

    private Capture startCapture(TapTable table, Object offset) throws Throwable {
        Capture capture = new Capture();
        capture.consumer = StreamReadConsumer.create((events, callbackOffset) -> {
            if (events != null) {
                capture.events.addAll(events);
                for (TapEvent event : events) {
                    Map<String, Object> row = eventRow(event);
                    if (row != null) {
                        capture.rows.add(row);
                        String id = stringId(row);
                        if (id != null) {
                            capture.ids.add(id);
                        }
                    }
                }
            }
            capture.lastOffset.set(callbackOffset);
        });
        capture.thread = new Thread(() -> {
            try {
                functions().getStreamReadFunction().streamRead(nodeContext(), Collections.singletonList(table.getId()),
                        offset, 100, capture.consumer);
            } catch (Throwable throwable) {
                capture.error.set(throwable);
            }
        }, "tap-it-mongo-stream");
        capture.thread.setDaemon(true);
        capture.thread.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (capture.consumer.getState() != StreamReadConsumer.STATE_STREAM_READ_STARTED
                && capture.error.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(50L);
        }
        assertNull(capture.error.get(), () -> "MongoDB stream failed to start: " + capture.error.get());
        assertTrue(capture.thread.isAlive(), "MongoDB stream thread should remain active after startup");
        return capture;
    }

    private void waitForIds(Capture capture, String... ids) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (capture.error.get() == null && System.nanoTime() < deadline) {
            boolean found = true;
            for (String id : ids) {
                found &= capture.ids.contains(id);
            }
            if (found) {
                return;
            }
            Thread.sleep(250L);
        }
        assertNull(capture.error.get(), () -> "MongoDB stream failed: " + capture.error.get());
        for (String id : ids) {
            assertTrue(capture.ids.contains(id), "missing committed MongoDB document " + id);
        }
    }

    private void waitForMutationEvents(Capture capture, String id) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (capture.error.get() == null && System.nanoTime() < deadline) {
            boolean inserted = capture.events.stream().anyMatch(event -> event instanceof TapInsertRecordEvent && id.equals(stringId(eventRow(event))));
            boolean updated = capture.events.stream().anyMatch(event -> event instanceof TapUpdateRecordEvent && id.equals(stringId(eventRow(event))));
            boolean deleted = capture.events.stream().anyMatch(event -> event instanceof TapDeleteRecordEvent && id.equals(stringId(eventRow(event))));
            if (inserted && updated && deleted) {
                return;
            }
            Thread.sleep(250L);
        }
        assertNull(capture.error.get(), () -> "MongoDB stream failed: " + capture.error.get());
        assertTrue(capture.events.stream().anyMatch(event -> event instanceof TapInsertRecordEvent && id.equals(stringId(eventRow(event)))), "missing insert CDC event");
        assertTrue(capture.events.stream().anyMatch(event -> event instanceof TapUpdateRecordEvent && id.equals(stringId(eventRow(event)))), "missing update CDC event");
        assertTrue(capture.events.stream().anyMatch(event -> event instanceof TapDeleteRecordEvent && id.equals(stringId(eventRow(event)))), "missing delete CDC event");
    }

    private void waitForCount(Capture capture, int rows, int seconds) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (capture.ids.stream().filter(id -> id.startsWith("long-tx-")).count() < rows
                && capture.error.get() == null && System.nanoTime() < deadline) {
            Thread.sleep(500L);
        }
        assertNull(capture.error.get(), () -> "MongoDB stream failed: " + capture.error.get());
    }

    private Map<String, Object> eventRow(TapEvent event) {
        if (event instanceof TapInsertRecordEvent) {
            return ((TapInsertRecordEvent) event).getAfter();
        }
        if (event instanceof TapUpdateRecordEvent) {
            return ((TapUpdateRecordEvent) event).getAfter();
        }
        if (event instanceof TapDeleteRecordEvent) {
            return ((TapDeleteRecordEvent) event).getBefore();
        }
        return null;
    }

    private Object currentOffset() throws Throwable {
        return functions().getTimestampToStreamOffsetFunction().timestampToStreamOffset(nodeContext(), System.currentTimeMillis());
    }

    private MongoClient client() {
        return MongoClients.create(context.getConfig().getString("uri"));
    }

    private MongoCollection<Document> collection() {
        if (directClient == null) {
            directClient = client();
        }
        return collection(directClient);
    }

    private MongoCollection<Document> collection(MongoClient client) {
        return client.getDatabase(context.getConfig().getString("database")).getCollection(spec.getTableName());
    }

    private Document fullTypeDocument(String id, String largeText, byte[] largeBinary) {
        return new Document("_id", id)
                .append("string", "TapData")
                .append("int32", 42)
                .append("int64", 9_223_372_036_854_775_000L)
                .append("double", 12.5D)
                .append("decimal128", Decimal128.parse("1234567890.123456789"))
                .append("boolean", true)
                .append("date", new Date(1_704_067_200_123L))
                .append("object_id", new ObjectId())
                .append("regex", new BsonRegularExpression("^tap.*", "i"))
                .append("code", new Code("function() { return 1; }"))
                .append("symbol", new Symbol("tap-symbol"))
                .append("timestamp", new BsonTimestamp(1_700_000_000, 7))
                .append("nested", new Document("level", 1).append("child", new Document("name", "nested")))
                .append("array", Arrays.asList(1, "two", new Document("three", true)))
                .append("large_text", largeText)
                .append("large_binary", new Binary(largeBinary));
    }

    private Map<String, Object> rowById(List<Map<String, Object>> rows, String id) {
        return rows.stream().filter(row -> id.equals(stringId(row))).findFirst()
                .orElseThrow(() -> new AssertionError("missing MongoDB row " + id));
    }

    private String stringId(Map<String, Object> row) {
        if (row == null) {
            return null;
        }
        Object id = row.get("_id");
        return id == null ? null : String.valueOf(id);
    }

    private byte[] binaryValue(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof Binary) {
            return ((Binary) value).getData();
        }
        throw new AssertionError("unexpected binary value: " + value);
    }

    private String repeat(String value, int count) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        StringBuilder builder = new StringBuilder(bytes.length * count);
        for (int index = 0; index < count; index++) {
            builder.append(value);
        }
        return builder.toString();
    }

    private static final class Capture {
        private final List<String> ids = Collections.synchronizedList(new ArrayList<>());
        private final List<Map<String, Object>> rows = Collections.synchronizedList(new ArrayList<>());
        private final List<TapEvent> events = Collections.synchronizedList(new ArrayList<>());
        private final AtomicReference<Object> lastOffset = new AtomicReference<>();
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private StreamReadConsumer consumer;
        private Thread thread;

        private void stop() {
            if (consumer != null) {
                consumer.streamReadEnded();
            }
            if (thread != null) {
                thread.interrupt();
            }
        }
    }
}
