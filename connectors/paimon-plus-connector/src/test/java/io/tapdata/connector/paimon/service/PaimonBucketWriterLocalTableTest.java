package io.tapdata.connector.paimon.service;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** State-based LocalFileIO verification for every Paimon 1.3.1 bucket writer strategy. */
class PaimonBucketWriterLocalTableTest {

    private static final String DATABASE = "default";

    @TempDir java.nio.file.Path tempDir;

    private Catalog catalog;

    @AfterEach
    void closeCatalog() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    void hashFixedShouldWriteEveryConfiguredBucketAndKeepKeyBucketAfterRestart()
            throws Exception {
        FileStoreTable table = createHashFixedTable("fixed_four_buckets", 4);
        Map<Integer, Integer> idByExpectedBucket = findIdForEachFixedBucket(table, 4);

        try (PaimonTableWriteContext context =
                createContext(table, "fixed_four_buckets", "fixed-local-user")) {
            assertEquals(BucketMode.HASH_FIXED, context.bucketMode());
            for (Integer id : idByExpectedBucket.values()) {
                context.write(hashRow(RowKind.INSERT, id, "initial-" + id));
            }
            context.commit();
        }

        table = reload("fixed_four_buckets");
        Map<Integer, LocatedRow> initial =
                uniqueRowsById(readRowsByBucket(table, 0, -1, 1));
        assertEquals(reverse(idByExpectedBucket), bucketsById(initial));
        assertEquals(
                new LinkedHashSet<>(Arrays.asList(0, 1, 2, 3)),
                initial.values().stream()
                        .map(row -> row.bucket)
                        .collect(Collectors.toCollection(LinkedHashSet::new)));

        try (PaimonTableWriteContext restarted =
                createContext(table, "fixed_four_buckets", "fixed-local-user")) {
            for (Integer id : idByExpectedBucket.values()) {
                restarted.write(hashRow(RowKind.UPDATE_AFTER, id, "updated-" + id));
            }
            restarted.commit();
        }

        Map<Integer, LocatedRow> updated =
                uniqueRowsById(
                        readRowsByBucket(reload("fixed_four_buckets"), 0, -1, 1));
        assertEquals(reverse(idByExpectedBucket), bucketsById(updated));
        assertTrue(
                updated.values().stream()
                        .allMatch(row -> row.value.equals("updated-" + row.id)));
    }

    @Test
    void hashDynamicShouldExpandToDifferentBucketsAndReuseMappingAfterRestart()
            throws Exception {
        FileStoreTable table = createHashDynamicTable("hash_dynamic_buckets");
        List<Integer> ids = findIdsWithDistinctPrimaryKeyHashes(table, 6);

        try (PaimonTableWriteContext context =
                createContext(table, "hash_dynamic_buckets", "hash-dynamic-local-user")) {
            assertEquals(BucketMode.HASH_DYNAMIC, context.bucketMode());
            for (Integer id : ids) {
                context.write(hashRow(RowKind.INSERT, id, "initial-" + id));
            }
            context.commit();
        }

        table = reload("hash_dynamic_buckets");
        Map<Integer, LocatedRow> initial =
                uniqueRowsById(readRowsByBucket(table, 0, -1, 1));
        assertEquals(6, initial.size());
        assertEquals(6, distinctBucketCount(initial));
        Map<Integer, Integer> initialBuckets = bucketsById(initial);

        try (PaimonTableWriteContext restarted =
                createContext(table, "hash_dynamic_buckets", "hash-dynamic-local-user")) {
            for (Integer id : ids) {
                restarted.write(hashRow(RowKind.UPDATE_AFTER, id, "updated-" + id));
            }
            restarted.commit();
        }

        Map<Integer, LocatedRow> updated =
                uniqueRowsById(
                        readRowsByBucket(reload("hash_dynamic_buckets"), 0, -1, 1));
        assertEquals(initialBuckets, bucketsById(updated));
        assertEquals(6, distinctBucketCount(updated));
        assertTrue(
                updated.values().stream()
                        .allMatch(row -> row.value.equals("updated-" + row.id)));
    }

    @Test
    void keyDynamicShouldUseDifferentBucketsAndKeepOneRowAfterPartitionMove()
            throws Exception {
        FileStoreTable table = createKeyDynamicTable("key_dynamic_buckets");

        try (PaimonTableWriteContext context =
                createContext(table, "key_dynamic_buckets", "key-dynamic-local-user")) {
            assertEquals(BucketMode.KEY_DYNAMIC, context.bucketMode());
            for (int id = 0; id < 6; id++) {
                context.write(keyRow(RowKind.INSERT, 1, id, "initial-" + id));
            }
            context.commit();
        }

        table = reload("key_dynamic_buckets");
        Map<Integer, LocatedRow> initial =
                uniqueRowsById(readRowsByBucket(table, 1, 0, 2));
        assertEquals(6, initial.size());
        assertEquals(6, distinctBucketCount(initial));
        assertTrue(initial.values().stream().allMatch(row -> row.partition == 1));

        try (PaimonTableWriteContext restarted =
                createContext(table, "key_dynamic_buckets", "key-dynamic-local-user")) {
            for (int id = 0; id < 6; id++) {
                restarted.write(keyRow(RowKind.UPDATE_AFTER, 2, id, "moved-" + id));
            }
            restarted.commit();
        }

        Map<Integer, LocatedRow> moved =
                uniqueRowsById(
                        readRowsByBucket(reload("key_dynamic_buckets"), 1, 0, 2));
        assertEquals(6, moved.size());
        assertEquals(6, distinctBucketCount(moved));
        assertTrue(moved.values().stream().allMatch(row -> row.partition == 2));
        assertTrue(
                moved.values().stream().allMatch(row -> row.value.equals("moved-" + row.id)));
    }

    @Test
    void bucketUnawareShouldPersistAllReadableRowsInBucketZero() throws Exception {
        FileStoreTable table = createBucketUnawareTable("bucket_unaware_zero");

        try (PaimonTableWriteContext context =
                createContext(table, "bucket_unaware_zero", "bucket-unaware-local-user")) {
            assertEquals(BucketMode.BUCKET_UNAWARE, context.bucketMode());
            for (int index = 0; index < 5; index++) {
                int id = index % 2;
                context.write(hashRow(RowKind.INSERT, id, "append-" + index));
            }
            context.commit();
        }

        List<LocatedRow> rows =
                readRowsByBucket(reload("bucket_unaware_zero"), 0, -1, 1);
        assertEquals(5, rows.size());
        assertEquals(
                Collections.singleton(BucketMode.UNAWARE_BUCKET),
                rows.stream().map(row -> row.bucket).collect(Collectors.toSet()));
    }

    @Test
    void postponeShouldPersistEveryNewFileInPostponeBucket() throws Exception {
        FileStoreTable table = createPostponeTable("postpone_bucket_files");

        try (PaimonTableWriteContext context =
                createContext(table, "postpone_bucket_files", "postpone-local-user")) {
            assertEquals(BucketMode.POSTPONE_MODE, context.bucketMode());
            for (int id = 0; id < 5; id++) {
                context.write(hashRow(RowKind.INSERT, id, "postponed-" + id));
            }
            context.commit();
        }

        List<ManifestEntry> entries = latestDeltaEntries(reload("postpone_bucket_files"));
        assertEquals(5L, ManifestEntry.recordCountAdd(entries));
        assertTrue(
                entries.stream()
                        .filter(entry -> entry.kind() == FileKind.ADD)
                        .allMatch(entry -> entry.bucket() == BucketMode.POSTPONE_BUCKET));
    }

    private FileStoreTable createHashFixedTable(String tableName, int buckets) throws Exception {
        return createTable(
                tableName,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", String.valueOf(buckets))
                        .option("write-buffer-size", "8mb")
                        .build());
    }

    private FileStoreTable createHashDynamicTable(String tableName) throws Exception {
        return createTable(
                tableName,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "-1")
                        .option("dynamic-bucket.target-row-num", "1")
                        .option("dynamic-bucket.max-buckets", "16")
                        .option("write-buffer-size", "8mb")
                        .build());
    }

    private FileStoreTable createKeyDynamicTable(String tableName) throws Exception {
        return createTable(
                tableName,
                Schema.newBuilder()
                        .column("pt", DataTypes.INT())
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .partitionKeys("pt")
                        .primaryKey("id")
                        .option("bucket", "-1")
                        .option("dynamic-bucket.target-row-num", "1")
                        .option("dynamic-bucket.max-buckets", "16")
                        .option("write-buffer-size", "8mb")
                        .build());
    }

    private FileStoreTable createBucketUnawareTable(String tableName) throws Exception {
        return createTable(
                tableName,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .option("bucket", "-1")
                        .option("write-buffer-size", "8mb")
                        .build());
    }

    private FileStoreTable createPostponeTable(String tableName) throws Exception {
        return createTable(
                tableName,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", String.valueOf(BucketMode.POSTPONE_BUCKET))
                        .option("write-buffer-size", "8mb")
                        .build());
    }

    private FileStoreTable createTable(String tableName, Schema schema) throws Exception {
        initCatalog();
        Identifier identifier = Identifier.create(DATABASE, tableName);
        catalog.createTable(identifier, schema, false);
        return (FileStoreTable) catalog.getTable(identifier);
    }

    private FileStoreTable reload(String tableName) throws Exception {
        return (FileStoreTable) catalog.getTable(Identifier.create(DATABASE, tableName));
    }

    private PaimonTableWriteContext createContext(
            FileStoreTable table, String tableName, String commitUser) throws Exception {
        String spill = Files.createDirectories(tempDir.resolve("spill")).toString();
        return PaimonTableWriteContext.create(
                DATABASE + "." + tableName, tableName, table, commitUser, spill);
    }

    private Map<Integer, Integer> findIdForEachFixedBucket(FileStoreTable table, int bucketCount)
            throws Exception {
        Map<Integer, Integer> idByBucket = new LinkedHashMap<>();
        try (StreamTableWrite probe =
                table.newStreamWriteBuilder().withCommitUser("fixed-bucket-probe").newWrite()) {
            for (int id = 0; id < 10_000 && idByBucket.size() < bucketCount; id++) {
                GenericRow row = hashRow(RowKind.INSERT, id, "probe");
                idByBucket.putIfAbsent(probe.getBucket(row), id);
            }
        }
        assertEquals(bucketCount, idByBucket.size(), "Failed to find one key for every bucket");
        return idByBucket;
    }

    private List<Integer> findIdsWithDistinctPrimaryKeyHashes(FileStoreTable table, int count) {
        RowPartitionKeyExtractor extractor = new RowPartitionKeyExtractor(table.schema());
        Map<Integer, Integer> idByHash = new LinkedHashMap<>();
        for (int id = 0; id < 10_000 && idByHash.size() < count; id++) {
            GenericRow row = hashRow(RowKind.INSERT, id, "probe");
            idByHash.putIfAbsent(extractor.trimmedPrimaryKey(row).hashCode(), id);
        }
        assertEquals(count, idByHash.size(), "Failed to find distinct primary-key hashes");
        return new ArrayList<>(idByHash.values());
    }

    private List<LocatedRow> readRowsByBucket(
            FileStoreTable table, int idIndex, int partitionIndex, int valueIndex)
            throws Exception {
        ReadBuilder readBuilder = table.newReadBuilder();
        InternalRowSerializer serializer = new InternalRowSerializer(table.rowType());
        List<LocatedRow> result = new ArrayList<>();
        for (Split split : readBuilder.newScan().plan().splits()) {
            int bucket = ((DataSplit) split).bucket();
            try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(split)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    try {
                        InternalRow row;
                        while ((row = batch.next()) != null) {
                            InternalRow copy = serializer.copy(row);
                            int id = copy.getInt(idIndex);
                            Integer partition =
                                    partitionIndex < 0 ? null : copy.getInt(partitionIndex);
                            result.add(
                                    new LocatedRow(
                                            id,
                                            partition,
                                            bucket,
                                            copy.getString(valueIndex).toString()));
                        }
                    } finally {
                        batch.releaseBatch();
                    }
                }
            }
        }
        return result;
    }

    private Map<Integer, LocatedRow> uniqueRowsById(List<LocatedRow> rows) {
        Map<Integer, LocatedRow> result = new LinkedHashMap<>();
        for (LocatedRow row : rows) {
            LocatedRow previous = result.put(row.id, row);
            assertTrue(previous == null, "Primary key " + row.id + " appeared more than once");
        }
        return result;
    }

    private List<ManifestEntry> latestDeltaEntries(FileStoreTable table) throws Exception {
        Snapshot snapshot =
                table.latestSnapshot()
                        .orElseThrow(() -> new AssertionError("Missing committed snapshot"));
        List<ManifestEntry> entries = new ArrayList<>();
        List<ManifestFileMeta> manifests =
                table.manifestListReader().read(snapshot.deltaManifestList());
        for (ManifestFileMeta manifest : manifests) {
            entries.addAll(table.manifestFileReader().read(manifest.fileName()));
        }
        return entries;
    }

    private Map<Integer, Integer> bucketsById(Map<Integer, LocatedRow> rows) {
        Map<Integer, Integer> result = new LinkedHashMap<>();
        rows.forEach((id, row) -> result.put(id, row.bucket));
        return result;
    }

    private Map<Integer, Integer> reverse(Map<Integer, Integer> idByBucket) {
        Map<Integer, Integer> bucketById = new LinkedHashMap<>();
        idByBucket.forEach((bucket, id) -> bucketById.put(id, bucket));
        return bucketById;
    }

    private long distinctBucketCount(Map<Integer, LocatedRow> rows) {
        return rows.values().stream().map(row -> row.bucket).distinct().count();
    }

    private GenericRow hashRow(RowKind kind, int id, String value) {
        return GenericRow.ofKind(kind, id, BinaryString.fromString(value));
    }

    private GenericRow keyRow(RowKind kind, int partition, int id, String value) {
        return GenericRow.ofKind(kind, partition, id, BinaryString.fromString(value));
    }

    private void initCatalog() throws Exception {
        if (catalog != null) {
            return;
        }
        java.nio.file.Path warehouse = Files.createDirectories(tempDir.resolve("warehouse"));
        catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(warehouse.toUri())));
        catalog.createDatabase(DATABASE, true);
    }

    private static final class LocatedRow {
        private final int id;
        private final Integer partition;
        private final int bucket;
        private final String value;

        private LocatedRow(int id, Integer partition, int bucket, String value) {
            this.id = id;
            this.partition = partition;
            this.bucket = bucket;
            this.value = value;
        }
    }
}
