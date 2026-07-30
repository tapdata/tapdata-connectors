package io.tapdata.connector.paimon.service;

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.table.BucketMode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable table-level contract governing safe DML writes. */
final class PaimonWriteSemanticContract {

    private final BucketMode bucketMode;
    private final boolean crossPartitionUpdate;
    private final MergeEngine mergeEngine;
    private final ChangelogProducer changelogProducer;
    private final boolean fullChangelogRequired;
    private final List<String> targetFields;
    private final Set<String> nonNullTargetFields;
    private final Set<String> primaryKeys;
    private final Set<String> partitionKeys;
    private final @Nullable String rowKindField;
    private final int rowKindFieldIndex;

    PaimonWriteSemanticContract(
            BucketMode bucketMode,
            boolean crossPartitionUpdate,
            MergeEngine mergeEngine,
            ChangelogProducer changelogProducer,
            boolean fullChangelogRequired,
            List<String> targetFields,
            Set<String> nonNullTargetFields,
            Set<String> primaryKeys,
            Set<String> partitionKeys,
            @Nullable String rowKindField,
            int rowKindFieldIndex) {
        this.bucketMode = Objects.requireNonNull(bucketMode, "bucketMode");
        this.crossPartitionUpdate = crossPartitionUpdate;
        this.mergeEngine = Objects.requireNonNull(mergeEngine, "mergeEngine");
        this.changelogProducer = Objects.requireNonNull(changelogProducer, "changelogProducer");
        this.fullChangelogRequired = fullChangelogRequired;
        this.targetFields =
                Collections.unmodifiableList(
                        new ArrayList<>(Objects.requireNonNull(targetFields, "targetFields")));
        this.nonNullTargetFields = immutableSet(nonNullTargetFields, "nonNullTargetFields");
        this.primaryKeys = immutableSet(primaryKeys, "primaryKeys");
        this.partitionKeys = immutableSet(partitionKeys, "partitionKeys");
        this.rowKindField = rowKindField;
        this.rowKindFieldIndex = rowKindFieldIndex;
    }

    private static Set<String> immutableSet(Set<String> values, String name) {
        return Collections.unmodifiableSet(
                new LinkedHashSet<>(Objects.requireNonNull(values, name)));
    }

    BucketMode bucketMode() {
        return bucketMode;
    }

    boolean crossPartitionUpdate() {
        return crossPartitionUpdate;
    }

    MergeEngine mergeEngine() {
        return mergeEngine;
    }

    ChangelogProducer changelogProducer() {
        return changelogProducer;
    }

    boolean requiresFullChangelog() {
        return fullChangelogRequired;
    }

    List<String> targetFields() {
        return targetFields;
    }

    Set<String> nonNullTargetFields() {
        return nonNullTargetFields;
    }

    Set<String> primaryKeys() {
        return primaryKeys;
    }

    Set<String> partitionKeys() {
        return partitionKeys;
    }

    @Nullable
    String rowKindField() {
        return rowKindField;
    }

    int rowKindFieldIndex() {
        return rowKindFieldIndex;
    }
}
