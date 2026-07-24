package io.tapdata.connector.paimon.service;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.types.DataTypeFamily.CHARACTER_STRING;

/** Resolves and validates the Paimon 1.3.1 write semantics before writer resources are allocated. */
final class PaimonWriteSemanticContractResolver {

    private PaimonWriteSemanticContractResolver() {}

    static PaimonWriteSemanticContract resolve(String tableKey, FileStoreTable table) {
        Objects.requireNonNull(tableKey, "tableKey");
        Objects.requireNonNull(table, "table");

        BucketMode bucketMode = Objects.requireNonNull(table.bucketMode(), "bucketMode");
        TableSchema schema = Objects.requireNonNull(table.schema(), "schema");
        CoreOptions options = Objects.requireNonNull(table.coreOptions(), "coreOptions");
        MergeEngine mergeEngine = options.mergeEngine();
        ChangelogProducer changelogProducer = options.changelogProducer();
        boolean crossPartitionUpdate = schema.crossPartitionUpdate();
        boolean primaryKeyTable = !schema.primaryKeys().isEmpty();
        boolean fixedOrPostpone =
                bucketMode == BucketMode.HASH_FIXED || bucketMode == BucketMode.POSTPONE_MODE;

        validateKeyDynamicIgnoreDeleteConflict(
                tableKey, bucketMode, mergeEngine, options.ignoreDelete());

        // Fixed and postpone writes are routed directly to (partition, bucket). Unlike
        // KEY_DYNAMIC, they do not have GlobalIndexAssigner state to retract an old partition:
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java
        if (fixedOrPostpone
                && crossPartitionUpdate
                && mergeEngine != MergeEngine.DEDUPLICATE) {
            throw fatal(
                    "PAIMON_UNSUPPORTED_CROSS_PARTITION_MERGE_ENGINE",
                    tableKey,
                    bucketMode,
                    mergeEngine,
                    "only deduplicate supports connector-managed complete retract/add input");
        }

        boolean fullChangelogRequired =
                primaryKeyTable
                        && (changelogProducer == ChangelogProducer.INPUT
                                || (fixedOrPostpone && crossPartitionUpdate));

        // Paimon RowKindFilter drops DELETE/UPDATE_BEFORE for these options. Such filtering is
        // incompatible with the complete changelog contract:
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/utils/RowKindFilter.java
        if (fullChangelogRequired && (options.ignoreDelete() || options.ignoreUpdateBefore())) {
            throw fatal(
                    "PAIMON_RETRACT_FILTER_CONFLICT",
                    tableKey,
                    bucketMode,
                    mergeEngine,
                    "ignore-delete and ignore-update-before must both be false");
        }

        RowType rowType = schema.logicalRowType();
        List<String> targetFields = rowType.getFieldNames();
        Set<String> nonNullTargetFields = new LinkedHashSet<>();
        for (DataField field : rowType.getFields()) {
            if (!field.type().isNullable()) {
                nonNullTargetFields.add(field.name());
            }
        }
        nonNullTargetFields.addAll(schema.primaryKeys());

        Optional<String> configuredRowKindField = options.rowkindField();
        String rowKindField = configuredRowKindField.orElse(null);
        int rowKindFieldIndex = -1;
        if (rowKindField != null) {
            rowKindFieldIndex = targetFields.indexOf(rowKindField);
            DataType rowKindType =
                    rowKindFieldIndex < 0 ? null : rowType.getTypeAt(rowKindFieldIndex);
            // This mirrors Paimon 1.3.1 RowKindGenerator's field/type contract. The connector
            // also stores the resolved index so it never scans RowType on the per-row hot path.
            // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/table/sink/RowKindGenerator.java
            if (rowKindType == null || !rowKindType.is(CHARACTER_STRING)) {
                throw fatal(
                        "PAIMON_ROW_KIND_FIELD_INVALID",
                        tableKey,
                        bucketMode,
                        mergeEngine,
                        "rowkind.field must reference a character-string target field: "
                                + rowKindField);
            }
            // TableWriteImpl derives RowKind from this field and then routes with the same row.
            // If the operation marker is also a primary/partition key, +I/-U/+U/-D become
            // different physical keys or partitions and retracts cannot target the prior row.
            if (schema.primaryKeys().contains(rowKindField)
                    || schema.partitionKeys().contains(rowKindField)) {
                throw fatal(
                        "PAIMON_ROW_KIND_ROUTING_FIELD_CONFLICT",
                        tableKey,
                        bucketMode,
                        mergeEngine,
                        "rowkind.field cannot be a primary-key or partition field: "
                                + rowKindField);
            }
            if (options.sequenceField().contains(rowKindField)) {
                throw fatal(
                        "PAIMON_ROW_KIND_SEQUENCE_FIELD_CONFLICT",
                        tableKey,
                        bucketMode,
                        mergeEngine,
                        "rowkind.field cannot also be a sequence.field: " + rowKindField);
            }
        }

        return new PaimonWriteSemanticContract(
                bucketMode,
                crossPartitionUpdate,
                mergeEngine,
                changelogProducer,
                fullChangelogRequired,
                targetFields,
                nonNullTargetFields,
                new LinkedHashSet<>(schema.primaryKeys()),
                new LinkedHashSet<>(schema.partitionKeys()),
                rowKindField,
                rowKindFieldIndex);
    }

    static void validateNewTable(String tableKey, Schema schema) {
        Objects.requireNonNull(tableKey, "tableKey");
        Objects.requireNonNull(schema, "schema");
        CoreOptions options = CoreOptions.fromMap(schema.options());
        validateKeyDynamicIgnoreDeleteConflict(
                tableKey,
                deriveBucketMode(schema),
                options.mergeEngine(),
                options.ignoreDelete());
    }

    static BucketMode deriveBucketMode(Schema schema) {
        Objects.requireNonNull(schema, "schema");
        int bucket = CoreOptions.fromMap(schema.options()).bucket();

        // Paimon 1.3.1 AppendOnlyFileStore#bucketMode treats only bucket=-1 as
        // BUCKET_UNAWARE when no primary key exists; all other values use HASH_FIXED.
        // Source:
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/AppendOnlyFileStore.java#L72-L75
        if (schema.primaryKeys().isEmpty()) {
            return bucket == -1 ? BucketMode.BUCKET_UNAWARE : BucketMode.HASH_FIXED;
        }

        // Paimon 1.3.1 TableSchema#crossPartitionUpdate and
        // KeyValueFileStore#bucketMode select KEY_DYNAMIC only for bucket=-1 when a
        // partition key is not covered by the primary key. Mirroring both methods here keeps
        // the pre-create guard identical to the materialized FileStoreTable; an approximation
        // could either miss the unsafe synthetic DELETE path or reject a valid table.
        // Sources:
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/schema/TableSchema.java#L200-L205
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java#L99-L109
        boolean crossPartitionUpdate =
                !schema.partitionKeys().isEmpty()
                        && !new HashSet<>(schema.primaryKeys())
                                .containsAll(new HashSet<>(schema.partitionKeys()));
        if (bucket == BucketMode.POSTPONE_BUCKET) {
            return BucketMode.POSTPONE_MODE;
        }
        if (bucket == -1) {
            return crossPartitionUpdate ? BucketMode.KEY_DYNAMIC : BucketMode.HASH_DYNAMIC;
        }
        return BucketMode.HASH_FIXED;
    }

    private static void validateKeyDynamicIgnoreDeleteConflict(
            String tableKey,
            BucketMode bucketMode,
            MergeEngine mergeEngine,
            boolean ignoreDelete) {
        if (bucketMode != BucketMode.KEY_DYNAMIC
                || mergeEngine != MergeEngine.DEDUPLICATE
                || !ignoreDelete) {
            return;
        }

        // Paimon 1.3.1 ExistingProcessor#create selects DeleteExistingProcessor for
        // KEY_DYNAMIC + DEDUPLICATE, and DeleteExistingProcessor#processExists emits a DELETE
        // for the old partition. RowKindFilter#test drops that DELETE when ignore-delete=true,
        // which can leave the same primary key in two partitions; reject before any writer or
        // global-index resource is allocated. CoreOptions#ignoreDelete is used by both callers
        // so canonical and historical fallback keys have the same effective semantics.
        // Sources:
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/crosspartition/ExistingProcessor.java#L59-L75
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/crosspartition/DeleteExistingProcessor.java#L46-L55
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/utils/RowKindFilter.java#L37-L63
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L2317-L2331
        throw new PaimonFatalWriteException(
                "PAIMON_KEY_DYNAMIC_IGNORE_DELETE_CONFLICT"
                        + " table="
                        + tableKey
                        + ", bucketMode="
                        + bucketMode
                        + ", mergeEngine="
                        + mergeEngine
                        + ", ignore-delete="
                        + ignoreDelete
                        + ", reason=ignore-delete filters the old-partition synthetic DELETE");
    }

    private static PaimonFatalWriteException fatal(
            String reasonCode,
            String tableKey,
            BucketMode bucketMode,
            MergeEngine mergeEngine,
            String reason) {
        return new PaimonFatalWriteException(
                reasonCode
                        + " table="
                        + tableKey
                        + ", bucketMode="
                        + bucketMode
                        + ", mergeEngine="
                        + mergeEngine
                        + ", reason="
                        + reason);
    }
}
