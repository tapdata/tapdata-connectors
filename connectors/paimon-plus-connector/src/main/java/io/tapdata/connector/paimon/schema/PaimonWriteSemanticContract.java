package io.tapdata.connector.paimon.schema;

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable table-level contract governing safe DML writes. */
public final class PaimonWriteSemanticContract {

    private final BucketMode bucketMode;
    private final boolean crossPartitionUpdate;
    private final MergeEngine mergeEngine;
    private final ChangelogProducer changelogProducer;
    private final boolean fullChangelogRequired;
    // Paimon fixes the writer's row layout and key extractor together at creation:
    // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/PrimaryKeyFileStoreTable.java#L160-L173
    private final RowType rowType;
    private final List<String> targetFields;
    private final Set<String> nonNullTargetFields;
    private final Set<String> defaultedTargetFields;
    private final Set<String> primaryKeys;
    private final Set<String> partitionKeys;
    private final @Nullable String rowKindField;
    private final int rowKindFieldIndex;

    public PaimonWriteSemanticContract(
            BucketMode bucketMode,
            boolean crossPartitionUpdate,
            MergeEngine mergeEngine,
            ChangelogProducer changelogProducer,
            boolean fullChangelogRequired,
            RowType rowType,
            Set<String> nonNullTargetFields,
            Set<String> defaultedTargetFields,
            Set<String> primaryKeys,
            Set<String> partitionKeys,
            @Nullable String rowKindField,
            int rowKindFieldIndex) {
        this.bucketMode = Objects.requireNonNull(bucketMode, "bucketMode");
        this.crossPartitionUpdate = crossPartitionUpdate;
        this.mergeEngine = Objects.requireNonNull(mergeEngine, "mergeEngine");
        this.changelogProducer = Objects.requireNonNull(changelogProducer, "changelogProducer");
        this.fullChangelogRequired = fullChangelogRequired;
        this.rowType = Objects.requireNonNull(rowType, "rowType");
        this.targetFields = Collections.unmodifiableList(rowType.getFieldNames());
        this.nonNullTargetFields = immutableSet(nonNullTargetFields, "nonNullTargetFields");
        this.defaultedTargetFields = immutableSet(defaultedTargetFields, "defaultedTargetFields");
        this.primaryKeys = immutableSet(primaryKeys, "primaryKeys");
        this.partitionKeys = immutableSet(partitionKeys, "partitionKeys");
        this.rowKindField = rowKindField;
        this.rowKindFieldIndex = rowKindFieldIndex;
    }

    private static Set<String> immutableSet(Set<String> values, String name) {
        return Collections.unmodifiableSet(
                new LinkedHashSet<>(Objects.requireNonNull(values, name)));
    }

    public BucketMode bucketMode() {
        return bucketMode;
    }

    public boolean crossPartitionUpdate() {
        return crossPartitionUpdate;
    }

    public MergeEngine mergeEngine() {
        return mergeEngine;
    }

    public ChangelogProducer changelogProducer() {
        return changelogProducer;
    }

    public boolean requiresFullChangelog() {
        return fullChangelogRequired;
    }

    public RowType rowType() {
        return rowType;
    }

    public List<String> targetFields() {
        return targetFields;
    }

    public Set<String> nonNullTargetFields() {
        return nonNullTargetFields;
    }

    public Set<String> defaultedTargetFields() {
        return defaultedTargetFields;
    }

    public Set<String> primaryKeys() {
        return primaryKeys;
    }

    public Set<String> partitionKeys() {
        return partitionKeys;
    }

    @Nullable
    public String rowKindField() {
        return rowKindField;
    }

    public int rowKindFieldIndex() {
        return rowKindFieldIndex;
    }
}
