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
