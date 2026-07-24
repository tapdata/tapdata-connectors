package io.tapdata.connector.paimon.service;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonWriteSemanticContractResolverTest {

    @ParameterizedTest(name = "{0}, crossPartition={1}, producer={2} => full={3}")
    @MethodSource("fullChangelogMatrix")
    void shouldResolveFullChangelogMatrix(
            BucketMode bucketMode,
            boolean crossPartition,
            ChangelogProducer producer,
            boolean expectedFullChangelog) {
        FileStoreTable table =
                table(bucketMode, crossPartition, MergeEngine.DEDUPLICATE, producer, false, false, null);

        PaimonWriteSemanticContract contract =
                PaimonWriteSemanticContractResolver.resolve("default.t", table);

        assertEquals(bucketMode, contract.bucketMode());
        assertEquals(crossPartition, contract.crossPartitionUpdate());
        assertEquals(expectedFullChangelog, contract.requiresFullChangelog());
    }

    private static Stream<Arguments> fullChangelogMatrix() {
        return Stream.of(
                Arguments.of(BucketMode.HASH_FIXED, true, ChangelogProducer.NONE, true),
                Arguments.of(BucketMode.POSTPONE_MODE, true, ChangelogProducer.NONE, true),
                Arguments.of(BucketMode.HASH_FIXED, false, ChangelogProducer.NONE, false),
                Arguments.of(BucketMode.POSTPONE_MODE, false, ChangelogProducer.NONE, false),
                Arguments.of(BucketMode.KEY_DYNAMIC, true, ChangelogProducer.NONE, false),
                Arguments.of(BucketMode.HASH_DYNAMIC, false, ChangelogProducer.NONE, false),
                Arguments.of(BucketMode.HASH_FIXED, true, ChangelogProducer.INPUT, true),
                Arguments.of(BucketMode.POSTPONE_MODE, true, ChangelogProducer.INPUT, true),
                Arguments.of(BucketMode.KEY_DYNAMIC, true, ChangelogProducer.INPUT, true),
                Arguments.of(BucketMode.HASH_DYNAMIC, false, ChangelogProducer.INPUT, true));
    }

    @ParameterizedTest(name = "{0} + {1}")
    @MethodSource("unsupportedCrossPartitionMergeEngines")
    void fixedAndPostponeCrossPartitionMustRejectUnsupportedMergeEngines(
            BucketMode bucketMode, MergeEngine mergeEngine) {
        FileStoreTable table =
                table(
                        bucketMode,
                        true,
                        mergeEngine,
                        ChangelogProducer.NONE,
                        false,
                        false,
                        null);

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () -> PaimonWriteSemanticContractResolver.resolve("default.t", table));

        assertTrue(thrown.getMessage().contains("PAIMON_UNSUPPORTED_CROSS_PARTITION_MERGE_ENGINE"));
        assertTrue(thrown.getMessage().contains(bucketMode.name()));
        assertTrue(thrown.getMessage().contains(mergeEngine.toString()));
    }

    private static Stream<Arguments> unsupportedCrossPartitionMergeEngines() {
        return Stream.of(BucketMode.HASH_FIXED, BucketMode.POSTPONE_MODE)
                .flatMap(
                        mode ->
                                Stream.of(
                                                MergeEngine.FIRST_ROW,
                                                MergeEngine.PARTIAL_UPDATE,
                                                MergeEngine.AGGREGATE)
                                        .map(engine -> Arguments.of(mode, engine)));
    }

    @ParameterizedTest(name = "ignoreDelete={0}, ignoreUpdateBefore={1}")
    @MethodSource("retractFilterConflicts")
    void fullChangelogMustRejectRetractFilters(
            boolean ignoreDelete, boolean ignoreUpdateBefore) {
        FileStoreTable table =
                table(
                        BucketMode.HASH_FIXED,
                        true,
                        MergeEngine.DEDUPLICATE,
                        ChangelogProducer.NONE,
                        ignoreDelete,
                        ignoreUpdateBefore,
                        null);

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () -> PaimonWriteSemanticContractResolver.resolve("default.t", table));

        assertTrue(thrown.getMessage().contains("PAIMON_RETRACT_FILTER_CONFLICT"));
    }

    private static Stream<Arguments> retractFilterConflicts() {
        return Stream.of(
                Arguments.of(true, false),
                Arguments.of(false, true),
                Arguments.of(true, true));
    }

    @Test
    void keyDynamicDeduplicateMustRejectIgnoredSyntheticDelete() {
        FileStoreTable table =
                table(
                        BucketMode.KEY_DYNAMIC,
                        true,
                        MergeEngine.DEDUPLICATE,
                        ChangelogProducer.NONE,
                        true,
                        false,
                        null);

        // Paimon 1.3.1 ExistingProcessor#create selects DeleteExistingProcessor for
        // KEY_DYNAMIC + DEDUPLICATE, which emits an old-partition DELETE. RowKindFilter#test
        // drops that DELETE when ignore-delete=true and can leave one primary key in two
        // partitions, so the connector must reject the contract before writer allocation.
        // Sources:
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/crosspartition/ExistingProcessor.java#L59-L75
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/crosspartition/DeleteExistingProcessor.java#L46-L55
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/utils/RowKindFilter.java#L37-L63
        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () -> PaimonWriteSemanticContractResolver.resolve("default.t", table));

        assertTrue(
                thrown.getMessage().contains("PAIMON_KEY_DYNAMIC_IGNORE_DELETE_CONFLICT"));
        assertTrue(thrown.getMessage().contains("ignore-delete=true"));
    }

    @Test
    void keyDynamicDeduplicateMustResolveIgnoreDeleteFallbackKey() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("pt", DataTypes.STRING())
                        .column("value", DataTypes.STRING())
                        .partitionKeys("pt")
                        .primaryKey("id")
                        .option("bucket", "-1")
                        .option("merge-engine", "deduplicate")
                        .option("deduplicate.ignore-delete", "true")
                        .build();
        FileStoreTable table =
                mockTable(BucketMode.KEY_DYNAMIC, TableSchema.create(0L, schema));

        // Paimon 1.3.1 CoreOptions#IGNORE_DELETE declares historical fallback keys and
        // CoreOptions#ignoreDelete resolves their effective value. Reading only the canonical
        // map key would miss this unsafe contract.
        // Sources:
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L477-L499
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L2317-L2331
        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () -> PaimonWriteSemanticContractResolver.resolve("default.t", table));

        assertTrue(
                thrown.getMessage().contains("PAIMON_KEY_DYNAMIC_IGNORE_DELETE_CONFLICT"));
        assertTrue(thrown.getMessage().contains("ignore-delete=true"));
    }

    @Test
    void keyDynamicConflictMustTakePriorityOverInputRetractConflict() {
        FileStoreTable table =
                table(
                        BucketMode.KEY_DYNAMIC,
                        true,
                        MergeEngine.DEDUPLICATE,
                        ChangelogProducer.INPUT,
                        true,
                        true,
                        null);

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () -> PaimonWriteSemanticContractResolver.resolve("default.t", table));

        assertTrue(
                thrown.getMessage().contains("PAIMON_KEY_DYNAMIC_IGNORE_DELETE_CONFLICT"));
        assertFalse(thrown.getMessage().contains("PAIMON_RETRACT_FILTER_CONFLICT"));
    }

    @ParameterizedTest(name = "{0}, crossPartition={1}, engine={2}, ignoreDelete={4}")
    @MethodSource("nonConflictingF1Contracts")
    void nonConflictingF1ContractsMustRemainAllowed(
            BucketMode bucketMode,
            boolean crossPartition,
            MergeEngine mergeEngine,
            ChangelogProducer producer,
            boolean ignoreDelete,
            boolean ignoreUpdateBefore) {
        FileStoreTable table =
                table(
                        bucketMode,
                        crossPartition,
                        mergeEngine,
                        producer,
                        ignoreDelete,
                        ignoreUpdateBefore,
                        null);

        PaimonWriteSemanticContract contract =
                PaimonWriteSemanticContractResolver.resolve("default.t", table);

        assertEquals(bucketMode, contract.bucketMode());
        assertEquals(mergeEngine, contract.mergeEngine());
    }

    private static Stream<Arguments> nonConflictingF1Contracts() {
        return Stream.of(
                Arguments.of(
                        BucketMode.KEY_DYNAMIC,
                        true,
                        MergeEngine.DEDUPLICATE,
                        ChangelogProducer.NONE,
                        false,
                        false),
                Arguments.of(
                        BucketMode.KEY_DYNAMIC,
                        true,
                        MergeEngine.DEDUPLICATE,
                        ChangelogProducer.NONE,
                        false,
                        true),
                Arguments.of(
                        BucketMode.KEY_DYNAMIC,
                        true,
                        MergeEngine.PARTIAL_UPDATE,
                        ChangelogProducer.NONE,
                        true,
                        false),
                Arguments.of(
                        BucketMode.KEY_DYNAMIC,
                        true,
                        MergeEngine.AGGREGATE,
                        ChangelogProducer.NONE,
                        true,
                        false),
                Arguments.of(
                        BucketMode.KEY_DYNAMIC,
                        true,
                        MergeEngine.FIRST_ROW,
                        ChangelogProducer.NONE,
                        true,
                        false),
                Arguments.of(
                        BucketMode.HASH_DYNAMIC,
                        false,
                        MergeEngine.DEDUPLICATE,
                        ChangelogProducer.NONE,
                        true,
                        false),
                Arguments.of(
                        BucketMode.HASH_FIXED,
                        false,
                        MergeEngine.DEDUPLICATE,
                        ChangelogProducer.NONE,
                        true,
                        false),
                Arguments.of(
                        BucketMode.POSTPONE_MODE,
                        false,
                        MergeEngine.DEDUPLICATE,
                        ChangelogProducer.NONE,
                        true,
                        false));
    }

    @ParameterizedTest(name = "primaryKey={0}, crossPartition={1}, bucket={2} => {3}")
    @MethodSource("newTableBucketModes")
    void newTableBucketModeMustMatchPaimonFileStoreSelection(
            boolean primaryKey,
            boolean crossPartition,
            int bucket,
            BucketMode expectedBucketMode) {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("pt", DataTypes.STRING())
                        .column("value", DataTypes.STRING())
                        .option("bucket", Integer.toString(bucket));
        if (primaryKey) {
            builder.primaryKey("id");
        }
        if (crossPartition) {
            builder.partitionKeys("pt");
        }
        Schema schema = builder.build();

        // Paimon 1.3.1 selects append-only and primary-key bucket modes in different stores;
        // TableSchema#crossPartitionUpdate decides KEY_DYNAMIC only for bucket=-1.
        // Sources:
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/AppendOnlyFileStore.java#L72-L75
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/schema/TableSchema.java#L200-L205
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java#L99-L109
        assertEquals(
                expectedBucketMode,
                PaimonWriteSemanticContractResolver.deriveBucketMode(schema));
    }

    private static Stream<Arguments> newTableBucketModes() {
        return Stream.of(
                Arguments.of(false, false, -1, BucketMode.BUCKET_UNAWARE),
                Arguments.of(false, false, -2, BucketMode.HASH_FIXED),
                Arguments.of(false, false, 4, BucketMode.HASH_FIXED),
                Arguments.of(true, false, -2, BucketMode.POSTPONE_MODE),
                Arguments.of(true, false, -1, BucketMode.HASH_DYNAMIC),
                Arguments.of(true, true, -1, BucketMode.KEY_DYNAMIC),
                Arguments.of(true, true, 4, BucketMode.HASH_FIXED));
    }

    @Test
    void optionalChangelogMustPreserveExistingRetractFilterConfiguration() {
        FileStoreTable table =
                table(
                        BucketMode.HASH_FIXED,
                        false,
                        MergeEngine.DEDUPLICATE,
                        ChangelogProducer.NONE,
                        true,
                        true,
                        null);

        PaimonWriteSemanticContract contract =
                PaimonWriteSemanticContractResolver.resolve("default.t", table);

        assertFalse(contract.requiresFullChangelog());
    }

    @Test
    void contractMustCaptureOrderedFieldsNullabilityAndRowKindField() {
        FileStoreTable table =
                table(
                        BucketMode.KEY_DYNAMIC,
                        true,
                        MergeEngine.DEDUPLICATE,
                        ChangelogProducer.INPUT,
                        false,
                        false,
                        "rk");

        PaimonWriteSemanticContract contract =
                PaimonWriteSemanticContractResolver.resolve("default.t", table);

        assertEquals(java.util.Arrays.asList("id", "pt", "value", "rk"), contract.targetFields());
        assertEquals(java.util.Collections.singleton("id"), contract.primaryKeys());
        assertEquals(java.util.Collections.singleton("pt"), contract.partitionKeys());
        assertEquals("rk", contract.rowKindField());
        assertEquals(3, contract.rowKindFieldIndex());
    }

    @Test
    void rowKindFieldMustExistAndUseCharacterStringType() {
        FileStoreTable table =
                table(
                        BucketMode.KEY_DYNAMIC,
                        true,
                        MergeEngine.DEDUPLICATE,
                        ChangelogProducer.NONE,
                        false,
                        false,
                        "id");

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () -> PaimonWriteSemanticContractResolver.resolve("default.t", table));

        assertTrue(thrown.getMessage().contains("PAIMON_ROW_KIND_FIELD_INVALID"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"id", "pt"})
    void rowKindFieldMustNotParticipateInPrimaryOrPartitionRouting(String rowKindField) {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("pt", DataTypes.STRING())
                        .column("value", DataTypes.STRING())
                        .partitionKeys("pt")
                        .primaryKey("id")
                        .option("bucket", "-1")
                        .option("rowkind.field", rowKindField)
                        .build();
        TableSchema tableSchema = TableSchema.create(0L, schema);
        FileStoreTable table = mockTable(BucketMode.KEY_DYNAMIC, tableSchema);

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () -> PaimonWriteSemanticContractResolver.resolve("default.t", table));

        assertTrue(thrown.getMessage().contains("PAIMON_ROW_KIND_ROUTING_FIELD_CONFLICT"));
        assertTrue(thrown.getMessage().contains(rowKindField));
    }

    @Test
    void rowKindFieldMustNotAlsoControlSequenceOrdering() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .column("rk", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "2")
                        .option("rowkind.field", "rk")
                        .option("sequence.field", "rk")
                        .build();
        FileStoreTable table =
                mockTable(BucketMode.HASH_FIXED, TableSchema.create(0L, schema));

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () -> PaimonWriteSemanticContractResolver.resolve("default.t", table));

        assertTrue(thrown.getMessage().contains("PAIMON_ROW_KIND_SEQUENCE_FIELD_CONFLICT"));
    }

    private static FileStoreTable table(
            BucketMode bucketMode,
            boolean crossPartition,
            MergeEngine mergeEngine,
            ChangelogProducer producer,
            boolean ignoreDelete,
            boolean ignoreUpdateBefore,
            String rowKindField) {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("pt", DataTypes.STRING())
                        .column("value", DataTypes.STRING());
        if (rowKindField != null && !"id".equals(rowKindField)) {
            builder.column(rowKindField, DataTypes.STRING());
        }
        if (crossPartition) {
            builder.partitionKeys("pt");
        }
        builder.primaryKey("id")
                .option("merge-engine", mergeEngine.toString())
                .option("changelog-producer", producer.toString())
                .option("ignore-delete", Boolean.toString(ignoreDelete))
                .option("ignore-update-before", Boolean.toString(ignoreUpdateBefore));
        if (rowKindField != null) {
            builder.option("rowkind.field", rowKindField);
        }

        TableSchema schema = TableSchema.create(0L, builder.build());
        return mockTable(bucketMode, schema);
    }

    private static FileStoreTable mockTable(BucketMode bucketMode, TableSchema schema) {
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.bucketMode()).thenReturn(bucketMode);
        when(table.schema()).thenReturn(schema);
        when(table.rowType()).thenReturn(schema.logicalRowType());
        when(table.coreOptions()).thenReturn(CoreOptions.fromMap(schema.options()));
        when(table.primaryKeys()).thenReturn(schema.primaryKeys());
        when(table.partitionKeys()).thenReturn(schema.partitionKeys());
        return table;
    }
}
