package io.tapdata.connector.paimon.service;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Real local S3 compatibility checks; run with scripts/run-paimon-s3-test.sh. */
@EnabledIfEnvironmentVariable(named = "PAIMON_RUSTFS_TEST", matches = "true")
class PaimonRustFsIntegrationTest {
    private static final String DATABASE = "default";
    private static final String TABLE = "records";
    private static final Identifier IDENTIFIER = Identifier.create(DATABASE, TABLE);

    @TempDir
    java.nio.file.Path tempDir;

    private AmazonS3 s3;
    private String bucket;

    @BeforeEach
    void createBucket() {
        s3 = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new EndpointConfiguration(
                        requiredEnv("PAIMON_S3_ENDPOINT"), "us-east-1"))
                .withPathStyleAccessEnabled(true)
                .withChunkedEncodingDisabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        requiredEnv("PAIMON_S3_ACCESS_KEY"), requiredEnv("PAIMON_S3_SECRET_KEY"))))
                .withClientConfiguration(new ClientConfiguration().withMaxErrorRetry(0)
                        .withConnectionTimeout(5_000).withSocketTimeout(10_000))
                .build();
        bucket = "paimon-test-" + UUID.randomUUID();
        s3.createBucket(bucket);
    }

    @AfterEach
    void closeClient() {
        if (s3 != null) {
            s3.shutdown();
        }
        // The script removes the entire disposable server and its anonymous volumes.
    }

    @ParameterizedTest
    @ValueSource(strings = {"s3", "s3a"})
    void connectorMustInsertUpdateDeleteAndReadAfterReopen(String scheme) throws Exception {
        PaimonConfig config = config(scheme);
        TapTable tapTable = new TapTable(TABLE)
                .add(new TapField("id", "INT").primaryKeyPos(1))
                .add(new TapField("value", "STRING"));
        TapConnectorContext context = context();
        PaimonService service = new PaimonService(config, mock(Log.class));
        try {
            service.init();
            Catalog catalog = catalog(service);
            catalog.createDatabase(DATABASE, true);
            catalog.createTable(IDENTIFIER, schema(), false);
            // Current connector capability detection falls back to Hadoop S3A for s3://.
            assertEquals("s3a://" + bucket + "/warehouse", config.getFullWarehousePath());
            assertTrue(((FileStoreTable) catalog.getTable(IDENTIFIER)).location()
                    .toString().startsWith("s3a://"));

            write(service, tapTable, context, new TapInsertRecordEvent().init().table(TABLE).after(row(1, "first")));
            assertEquals(Collections.singletonMap(1, "first"), read(catalog.getTable(IDENTIFIER)));
            write(service, tapTable, context, new TapUpdateRecordEvent().init().table(TABLE)
                    .before(row(1, "first")).after(row(1, "updated")));
            assertEquals(Collections.singletonMap(1, "updated"), read(catalog.getTable(IDENTIFIER)));
            write(service, tapTable, context, new TapInsertRecordEvent().init().table(TABLE).after(row(2, "kept")));
            write(service, tapTable, context, new TapDeleteRecordEvent().init().table(TABLE).before(row(1, "updated")));
            assertEquals(Collections.singletonMap(2, "kept"), read(catalog.getTable(IDENTIFIER)));
        } finally {
            service.close();
        }

        PaimonService reopened = new PaimonService(config, mock(Log.class));
        try {
            reopened.init();
            assertEquals(Collections.singletonMap(2, "kept"), read(catalog(reopened).getTable(IDENTIFIER)));
        } finally {
            reopened.close();
        }
    }

    @Test
    void nativePaimonS3MustCommitAndReadWithRustFs() throws Exception {
        Options options = new Options();
        options.set("warehouse", "s3://" + bucket + "/native-warehouse");
        options.set("s3.endpoint", requiredEnv("PAIMON_S3_ENDPOINT"));
        options.set("s3.access-key", requiredEnv("PAIMON_S3_ACCESS_KEY"));
        options.set("s3.secret-key", requiredEnv("PAIMON_S3_SECRET_KEY"));
        options.set("s3.region", "us-east-1");
        options.set("s3.path.style.access", "true");
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase(DATABASE, true);
            catalog.createTable(IDENTIFIER, schema(), false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(IDENTIFIER);
            assertTrue(table.location().toString().startsWith("s3://"));
            BatchWriteBuilder builder = table.newBatchWriteBuilder();
            try (BatchTableWrite writer = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
                writer.write(GenericRow.of(1, BinaryString.fromString("native-s3")));
                commit.commit(writer.prepareCommit());
            }
        }
        try (Catalog reopened = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            assertEquals(Collections.singletonMap(1, "native-s3"), read(reopened.getTable(IDENTIFIER)));
        }
    }

    @Test
    void conditionalWritesMustHaveOneWinnerAndRejectStaleEtag() throws Exception {
        ExecutorService callers = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<Boolean> first = callers.submit(() -> { start.await(); return createOwner("first"); });
            Future<Boolean> second = callers.submit(() -> { start.await(); return createOwner("second"); });
            start.countDown();
            boolean firstWon = first.get(15, TimeUnit.SECONDS);
            boolean secondWon = second.get(15, TimeUnit.SECONDS);
            assertEquals(1, (firstWon ? 1 : 0) + (secondWon ? 1 : 0));
            String owner = firstWon ? "first" : "second";
            assertEquals(owner, s3.getObjectAsString(bucket, "owner.json"));
            String etag = "\"" + s3.getObjectMetadata(bucket, "owner.json").getETag() + "\"";
            putOwner("replacement", "If-Match", etag);
            AmazonS3Exception stale = assertThrows(AmazonS3Exception.class,
                    () -> putOwner("late-release", "If-Match", etag));
            assertEquals(412, stale.getStatusCode());
            assertEquals("PreconditionFailed", stale.getErrorCode());
            assertEquals("replacement", s3.getObjectAsString(bucket, "owner.json"));
        } finally {
            callers.shutdownNow();
            assertTrue(callers.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private boolean createOwner(String owner) {
        try {
            putOwner(owner, "If-None-Match", "*");
            return true;
        } catch (AmazonS3Exception conflict) {
            assertEquals(412, conflict.getStatusCode());
            assertEquals("PreconditionFailed", conflict.getErrorCode());
            return false;
        }
    }

    private void putOwner(String owner, String header, String expected) {
        byte[] bytes = owner.getBytes(StandardCharsets.UTF_8);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        PutObjectRequest request = new PutObjectRequest(bucket, "owner.json", new ByteArrayInputStream(bytes), metadata);
        request.putCustomRequestHeader(header, expected);
        s3.putObject(request);
    }

    private PaimonConfig config(String scheme) {
        PaimonConfig config = new PaimonConfig();
        config.setStorageType("s3");
        config.setWarehouse(scheme + "://" + bucket + "/warehouse");
        config.setDatabase(DATABASE);
        config.setS3Endpoint(requiredEnv("PAIMON_S3_ENDPOINT"));
        config.setS3AccessKey(requiredEnv("PAIMON_S3_ACCESS_KEY"));
        config.setS3SecretKey(requiredEnv("PAIMON_S3_SECRET_KEY"));
        config.setS3Region("us-east-1");
        config.setDiskTmpDir(tempDir.resolve("spill").toString());
        config.setBatchAccumulationSize(0);
        config.setCommitIntervalMs(0);
        config.setEnableAsyncCommit(false);
        config.setWriteBufferSize(8);
        return config;
    }

    private Schema schema() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT()).column("value", DataTypes.STRING())
                .primaryKey("id").option("bucket", "1")
                .option("write-buffer-size", "8mb")
                .option("snapshot.expire.execution-mode", "sync").build();
    }

    @SuppressWarnings("unchecked")
    private TapConnectorContext context() {
        Map<String, Object> values = new LinkedHashMap<>();
        KVMap<Object> state = mock(KVMap.class);
        when(state.get(anyString())).thenAnswer(call -> values.get(call.getArgument(0)));
        when(state.putIfAbsent(anyString(), any())).thenAnswer(call ->
                values.putIfAbsent(call.getArgument(0), call.getArgument(1)));
        doAnswer(call -> { values.put(call.getArgument(0), call.getArgument(1)); return null; })
                .when(state).put(anyString(), any());
        TapConnectorContext context = mock(TapConnectorContext.class);
        when(context.getStateMap()).thenReturn(state);
        when(context.getLog()).thenReturn(mock(Log.class));
        return context;
    }

    private void write(PaimonService service, TapTable table, TapConnectorContext context, TapRecordEvent event)
            throws Exception {
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        event.addInfo("nodeIds", Collections.singletonList("rustfs-test-source"));
        service.writeRecords(Collections.singletonList(event), table, context);
    }

    private Map<String, Object> row(int id, String value) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("value", value);
        return row;
    }

    private Catalog catalog(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("catalog");
        field.setAccessible(true);
        return (Catalog) field.get(service);
    }

    private Map<Integer, String> read(Table table) throws Exception {
        Map<Integer, String> rows = new LinkedHashMap<>();
        ReadBuilder builder = table.newReadBuilder();
        try (RecordReader<InternalRow> reader = builder.newRead().createReader(builder.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        assertNull(rows.put(row.getInt(0), row.getString(1).toString()), "Duplicate primary key");
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        return rows;
    }

    private static String requiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Missing " + name + "; run scripts/run-paimon-s3-test.sh");
        }
        return value;
    }
}
