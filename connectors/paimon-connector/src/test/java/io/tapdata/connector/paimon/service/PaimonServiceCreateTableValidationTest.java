package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonServiceCreateTableValidationTest {

    private static final String DATABASE = "default";

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void catalogFallbackDefaultMustBeRejectedBeforeTableCreation() throws Exception {
        Catalog catalog =
                catalog(
                        "catalog-default-conflict",
                        Collections.singletonMap(
                                "table-default.deduplicate.ignore-delete", "true"));
        PaimonService service = service("catalog_default_conflict", catalog);
        Identifier identifier = Identifier.create(DATABASE, "catalog_default_conflict");

        try {
            // Paimon 1.3.1 AbstractCatalog#createTable copies table-default options with
            // putIfAbsent, while CoreOptions#ignoreDelete resolves the deduplicate fallback key.
            // The connector must mirror both steps before the catalog mutates table state.
            // Sources:
            // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/catalog/AbstractCatalog.java#L380-L405
            // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/catalog/AbstractCatalog.java#L649-L650
            // https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L477-L499
            PaimonFatalWriteException thrown =
                    assertThrows(
                            PaimonFatalWriteException.class,
                            () -> service.createTable(crossPartitionTable(identifier.getObjectName())));

            assertTrue(
                    thrown.getMessage()
                            .contains("PAIMON_KEY_DYNAMIC_IGNORE_DELETE_CONFLICT"));
            assertThrows(Catalog.TableNotExistException.class, () -> catalog.getTable(identifier));
        } finally {
            service.close();
        }
    }

    @Test
    void tablePropertyConflictMustBeRejectedBeforeTableCreation() throws Exception {
        Catalog catalog = catalog("table-property-conflict", Collections.emptyMap());
        PaimonConfig config = config("table_property_conflict");
        config.setTableProperties(
                Collections.singletonList(property("ignore-delete", "true")));
        PaimonService service = service(config, catalog);
        Identifier identifier = Identifier.create(DATABASE, "table_property_conflict");

        try {
            PaimonFatalWriteException thrown =
                    assertThrows(
                            PaimonFatalWriteException.class,
                            () -> service.createTable(crossPartitionTable(identifier.getObjectName())));

            assertTrue(
                    thrown.getMessage()
                            .contains("PAIMON_KEY_DYNAMIC_IGNORE_DELETE_CONFLICT"));
            assertThrows(Catalog.TableNotExistException.class, () -> catalog.getTable(identifier));
        } finally {
            service.close();
        }
    }

    @Test
    void tablePropertyMustOverrideCatalogDefaultWithPutIfAbsentSemantics() throws Exception {
        Catalog catalog =
                catalog(
                        "table-property-override",
                        Collections.singletonMap("table-default.ignore-delete", "true"));
        PaimonConfig config = config("table_property_override");
        config.setTableProperties(
                Collections.singletonList(property("ignore-delete", "false")));
        PaimonService service = service(config, catalog);
        Identifier identifier = Identifier.create(DATABASE, "table_property_override");

        try {
            assertTrue(service.createTable(crossPartitionTable(identifier.getObjectName())));
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);

            assertEquals(BucketMode.KEY_DYNAMIC, table.bucketMode());
            assertEquals("false", table.options().get("ignore-delete"));
        } finally {
            service.close();
        }
    }

    @ParameterizedTest(name = "{0} => {2}")
    @MethodSource("physicalBucketModes")
    void derivedBucketModeMustMatchMaterializedFileStoreTable(
            String tableName, Schema schema, BucketMode expectedBucketMode) throws Exception {
        Catalog catalog = catalog("bucket-golden-" + tableName, Collections.emptyMap());
        Identifier identifier = Identifier.create(DATABASE, tableName);

        try {
            // This is the physical oracle for the mirrored Paimon 1.3.1 selection chain:
            // AppendOnlyFileStore#bucketMode, TableSchema#crossPartitionUpdate and
            // KeyValueFileStore#bucketMode. The pre-create helper must equal the materialized
            // FileStoreTable for every schema/bucket branch.
            // Sources:
            // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/AppendOnlyFileStore.java#L72-L75
            // https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/schema/TableSchema.java#L200-L205
            // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java#L99-L109
            catalog.createTable(identifier, schema, false);
            FileStoreTable materialized = (FileStoreTable) catalog.getTable(identifier);

            assertEquals(
                    expectedBucketMode,
                    PaimonWriteSemanticContractResolver.deriveBucketMode(schema));
            assertEquals(expectedBucketMode, materialized.bucketMode());
        } finally {
            catalog.close();
        }
    }

    private static Stream<Arguments> physicalBucketModes() {
        return Stream.of(
                Arguments.of(
                        "append_unaware",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .option("bucket", "-1")
                                .build(),
                        BucketMode.BUCKET_UNAWARE),
                Arguments.of(
                        "append_fixed",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .option("bucket", "4")
                                .option("bucket-key", "id")
                                .build(),
                        BucketMode.HASH_FIXED),
                Arguments.of(
                        "primary_postpone",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .primaryKey("id")
                                .option("bucket", "-2")
                                .build(),
                        BucketMode.POSTPONE_MODE),
                Arguments.of(
                        "primary_hash_dynamic",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .primaryKey("id")
                                .option("bucket", "-1")
                                .build(),
                        BucketMode.HASH_DYNAMIC),
                Arguments.of(
                        "primary_hash_dynamic_partition_in_pk",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("pt", DataTypes.INT())
                                .partitionKeys("pt")
                                .primaryKey("id", "pt")
                                .option("bucket", "-1")
                                .build(),
                        BucketMode.HASH_DYNAMIC),
                Arguments.of(
                        "primary_key_dynamic",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("pt", DataTypes.INT())
                                .partitionKeys("pt")
                                .primaryKey("id")
                                .option("bucket", "-1")
                                .build(),
                        BucketMode.KEY_DYNAMIC),
                Arguments.of(
                        "primary_fixed",
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("pt", DataTypes.INT())
                                .partitionKeys("pt")
                                .primaryKey("id")
                                .option("bucket", "4")
                                .build(),
                        BucketMode.HASH_FIXED));
    }

    @Test
    void existingUnsafeTableMustFailBeforeWriteContextAndSnapshotCreation() throws Exception {
        Catalog catalog = catalog("existing-conflict", Collections.emptyMap());
        Identifier identifier = Identifier.create(DATABASE, "existing_conflict");
        catalog.createTable(
                identifier,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("pt", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .partitionKeys("pt")
                        .primaryKey("id")
                        .option("bucket", "-1")
                        .option("merge-engine", "deduplicate")
                        .option("ignore-delete", "true")
                        .build(),
                false);
        FileStoreTable target = (FileStoreTable) catalog.getTable(identifier);
        PaimonService service = service("existing_conflict", catalog);

        try {
            // Paimon 1.3.1 ExistingProcessor#create routes KEY_DYNAMIC + DEDUPLICATE through
            // DeleteExistingProcessor; with ignore-delete=true, RowKindFilter would remove the
            // old-partition DELETE. Resolve the physical table contract before allocating the
            // connector write context or creating a snapshot.
            // Sources:
            // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/crosspartition/ExistingProcessor.java#L59-L75
            // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/crosspartition/DeleteExistingProcessor.java#L46-L55
            // https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/utils/RowKindFilter.java#L37-L63
            PaimonFatalWriteException thrown =
                    assertThrows(
                            PaimonFatalWriteException.class,
                            () ->
                                    service.writeRecords(
                                            Collections.singletonList(
                                                    cdcInsert(
                                                            identifier.getObjectName(),
                                                            map(
                                                                    "id",
                                                                    1,
                                                                    "pt",
                                                                    1,
                                                                    "value",
                                                                    "not-written"))),
                                            crossPartitionTable(identifier.getObjectName()),
                                            context()));

            assertTrue(
                    thrown.getMessage()
                            .contains("PAIMON_KEY_DYNAMIC_IGNORE_DELETE_CONFLICT"));
            assertTrue(mapField(service, "tableWriteContexts").isEmpty());
            assertTrue(mapField(service, "physicalTableByLogicalTable").isEmpty());
            assertNull(target.snapshotManager().latestSnapshotIdFromFileSystem());
        } finally {
            service.close();
        }
    }

    private Catalog catalog(String name, Map<String, String> additionalOptions) throws Exception {
        java.nio.file.Path warehouse =
                Files.createDirectories(tempDir.resolve(name).resolve("warehouse"));
        Options options = new Options();
        options.set("warehouse", warehouse.toString());
        additionalOptions.forEach(options::set);
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        catalog.createDatabase(DATABASE, true);
        return catalog;
    }

    private PaimonService service(String tableName, Catalog catalog) throws Exception {
        return service(config(tableName), catalog);
    }

    private PaimonService service(PaimonConfig config, Catalog catalog) throws Exception {
        PaimonService service = new PaimonService(config, mock(Log.class));
        Field field = PaimonService.class.getDeclaredField("catalog");
        field.setAccessible(true);
        field.set(service, catalog);
        return service;
    }

    private PaimonConfig config(String tableName) {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase(DATABASE);
        config.setBucketMode("dynamic");
        config.setHashKey(false);
        config.setPartitionKey(Collections.singletonList("pt"));
        config.setEnableAutoCompaction(null);
        config.setWriteBufferSize(8);
        config.setBatchAccumulationSize(0);
        config.setCommitIntervalMs(0);
        config.setEnableAsyncCommit(false);
        config.setDiskTmpDir(tempDir.resolve(tableName).resolve("spill").toString());
        return config;
    }

    private TapTable crossPartitionTable(String tableName) {
        return new TapTable(tableName)
                .add(new TapField("id", "INT").primaryKeyPos(1))
                .add(new TapField("pt", "INT"))
                .add(new TapField("value", "STRING"));
    }

    private static LinkedHashMap<String, String> property(String key, String value) {
        LinkedHashMap<String, String> property = new LinkedHashMap<>();
        property.put("propKey", key);
        property.put("propValue", value);
        return property;
    }

    private static TapInsertRecordEvent cdcInsert(
            String tableName, Map<String, Object> after) {
        TapInsertRecordEvent event =
                new TapInsertRecordEvent().init().table(tableName).after(after);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        return event;
    }

    private static Map<String, Object> map(Object... values) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            result.put((String) values[i], values[i + 1]);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static TapConnectorContext context() {
        KVMap<Object> stateMap = mock(KVMap.class);
        when(stateMap.get(anyString())).thenReturn(null);
        TapConnectorContext context = mock(TapConnectorContext.class);
        when(context.getStateMap()).thenReturn(stateMap);
        when(context.getLog()).thenReturn(mock(Log.class));
        return context;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> mapField(PaimonService service, String fieldName)
            throws Exception {
        Field field = PaimonService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Map<String, ?>) field.get(service);
    }
}
