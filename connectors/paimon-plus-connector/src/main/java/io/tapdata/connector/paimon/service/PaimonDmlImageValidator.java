package io.tapdata.connector.paimon.service;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.paimon.CoreOptions.MergeEngine;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Validates source DML maps before conversion erases missing-versus-explicit-null information. */
final class PaimonDmlImageValidator {

    private PaimonDmlImageValidator() {}

    static void validateBatch(
            String tableKey,
            PaimonWriteSemanticContract contract,
            PaimonGeneratedFieldDependencies generatedFields,
            TapTable tapTable,
            List<TapRecordEvent> recordEvents) {
        ValidationRequirements requirements =
                resolveRequirements(tableKey, contract, generatedFields, tapTable);
        for (TapRecordEvent event : Objects.requireNonNull(recordEvents, "recordEvents")) {
            if (event instanceof TapInsertRecordEvent) {
                validateImage(
                        tableKey,
                        contract,
                        requirements,
                        ((TapInsertRecordEvent) event).getAfter(),
                        "INSERT",
                        "PAIMON_INCOMPLETE_AFTER_IMAGE",
                        contract.requiresFullChangelog(),
                        false);
            } else if (event instanceof TapUpdateRecordEvent) {
                TapUpdateRecordEvent update = (TapUpdateRecordEvent) event;
                validateUpdateAfter(tableKey, contract, requirements, update.getAfter());
            } else if (event instanceof TapDeleteRecordEvent) {
                validateImage(
                        tableKey,
                        contract,
                        requirements,
                        ((TapDeleteRecordEvent) event).getBefore(),
                        "DELETE",
                        "PAIMON_INCOMPLETE_BEFORE_IMAGE",
                        contract.requiresFullChangelog(),
                        false);
            }
        }
    }

    static void validateInsert(
            String tableKey,
            PaimonWriteSemanticContract contract,
            PaimonGeneratedFieldDependencies generatedFields,
            TapTable tapTable,
            @Nullable Map<String, Object> after) {
        validateImage(
                tableKey,
                contract,
                resolveRequirements(tableKey, contract, generatedFields, tapTable),
                after,
                "INSERT",
                "PAIMON_INCOMPLETE_AFTER_IMAGE",
                contract.requiresFullChangelog(),
                false);
    }

    static void validateUpdate(
            String tableKey,
            PaimonWriteSemanticContract contract,
            PaimonGeneratedFieldDependencies generatedFields,
            TapTable tapTable,
            @Nullable Map<String, Object> before,
            @Nullable Map<String, Object> after) {
        ValidationRequirements requirements =
                resolveRequirements(tableKey, contract, generatedFields, tapTable);
        validateImage(
                tableKey,
                contract,
                requirements,
                before,
                "UPDATE_BEFORE",
                "PAIMON_INCOMPLETE_BEFORE_IMAGE",
                contract.requiresFullChangelog(),
                false);
        validateUpdateAfter(tableKey, contract, requirements, after);
    }

    static void validateDelete(
            String tableKey,
            PaimonWriteSemanticContract contract,
            PaimonGeneratedFieldDependencies generatedFields,
            TapTable tapTable,
            @Nullable Map<String, Object> before) {
        validateImage(
                tableKey,
                contract,
                resolveRequirements(tableKey, contract, generatedFields, tapTable),
                before,
                "DELETE",
                "PAIMON_INCOMPLETE_BEFORE_IMAGE",
                contract.requiresFullChangelog(),
                false);
    }

    private static void validateUpdateAfter(
            String tableKey,
            PaimonWriteSemanticContract contract,
            ValidationRequirements requirements,
            @Nullable Map<String, Object> after) {
        boolean deduplicateUpdateAfter = requirements.deduplicateUpdateAfterRequired;
        validateImage(
                tableKey,
                contract,
                requirements,
                after,
                "UPDATE_AFTER",
                deduplicateUpdateAfter
                        ? "PAIMON_DEDUPLICATE_INCOMPLETE_UPDATE_AFTER"
                        : "PAIMON_INCOMPLETE_AFTER_IMAGE",
                deduplicateUpdateAfter || contract.requiresFullChangelog(),
                deduplicateUpdateAfter);
    }

    private static ValidationRequirements resolveRequirements(
            String tableKey,
            PaimonWriteSemanticContract contract,
            PaimonGeneratedFieldDependencies generatedFields,
            TapTable tapTable) {
        Objects.requireNonNull(tableKey, "tableKey");
        Objects.requireNonNull(contract, "contract");
        Objects.requireNonNull(generatedFields, "generatedFields");
        Objects.requireNonNull(tapTable, "tapTable");
        // Paimon 1.3.1 DeduplicateMergeFunction stores a primary-key value as one complete
        // latest record. This is an engine/property contract, so every primary-key BucketMode
        // requires a complete UPDATE_AFTER even when full changelog input is otherwise optional.
        // Source:
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/DeduplicateMergeFunction.java#L27-L48
        boolean deduplicateUpdateAfterRequired =
                !contract.primaryKeys().isEmpty()
                        && contract.mergeEngine() == MergeEngine.DEDUPLICATE;
        if (!contract.requiresFullChangelog() && !deduplicateUpdateAfterRequired) {
            return ValidationRequirements.DISABLED;
        }

        LinkedHashSet<String> requiredSourceFields = new LinkedHashSet<>();
        LinkedHashSet<String> nonNullSourceFields = new LinkedHashSet<>();
        LinkedHashSet<String> defaultedSourceFields = new LinkedHashSet<>();
        for (String targetField : contract.targetFields()) {
            if (targetField.equals(contract.rowKindField())) {
                continue;
            }
            if (generatedFields.generatedTargetFields().contains(targetField)) {
                for (String dependency : generatedFields.sourceDependencies(targetField)) {
                    requiredSourceFields.add(dependency);
                    nonNullSourceFields.add(dependency);
                }
            } else {
                requiredSourceFields.add(targetField);
                if (contract.nonNullTargetFields().contains(targetField)
                        || contract.primaryKeys().contains(targetField)) {
                    nonNullSourceFields.add(targetField);
                }
                if (contract.defaultedTargetFields().contains(targetField)) {
                    defaultedSourceFields.add(targetField);
                }
            }
        }

        Map<String, TapField> tapFields = tapTable.getNameFieldMap();
        if (tapFields == null) {
            tapFields = Collections.emptyMap();
        }
        Set<String> unmappedFields = new LinkedHashSet<>();
        for (String sourceField : requiredSourceFields) {
            if (!tapFields.containsKey(sourceField)) {
                unmappedFields.add(sourceField);
            }
        }
        return new ValidationRequirements(
                requiredSourceFields,
                nonNullSourceFields,
                defaultedSourceFields,
                unmappedFields,
                deduplicateUpdateAfterRequired);
    }

    private static void validateImage(
            String tableKey,
            PaimonWriteSemanticContract contract,
            ValidationRequirements requirements,
            @Nullable Map<String, Object> image,
            String operation,
            String reasonCode,
            boolean enabled,
            boolean rejectDefaultedNull) {
        if (!enabled) {
            return;
        }

        Set<String> missingFields = new LinkedHashSet<>();
        Set<String> nullFields = new LinkedHashSet<>();
        Set<String> defaultedNullFields = new LinkedHashSet<>();
        for (String sourceField : requirements.requiredSourceFields) {
            if (image == null || !image.containsKey(sourceField)) {
                missingFields.add(sourceField);
            } else if (image.get(sourceField) == null) {
                if (requirements.nonNullSourceFields.contains(sourceField)) {
                    nullFields.add(sourceField);
                }
                if (rejectDefaultedNull
                        && requirements.defaultedSourceFields.contains(sourceField)) {
                    defaultedNullFields.add(sourceField);
                }
            }
        }

        if (requirements.unmappedFields.isEmpty()
                && missingFields.isEmpty()
                && nullFields.isEmpty()
                && defaultedNullFields.isEmpty()) {
            return;
        }

        // Paimon 1.3.1 DeduplicateMergeFunction#add keeps the latest value as the complete
        // record, so a sparse UPDATE_AFTER would erase omitted columns in every BucketMode.
        // Validate the original Map before TableWriteImpl#writeAndReturn checks nullability and
        // DefaultValueRow replaces nullable explicit nulls with schema defaults.
        // Sources:
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/DeduplicateMergeFunction.java#L27-L48
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java#L187-L213
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-common/src/main/java/org/apache/paimon/casting/DefaultValueRow.java#L209-L228
        // Do not append the source map or any field value; CDC images may contain credentials or
        // personal data.
        throw new PaimonFatalWriteException(
                reasonCode
                        + " table="
                        + tableKey
                        + ", bucketMode="
                        + contract.bucketMode()
                        + ", mergeEngine="
                        + contract.mergeEngine()
                        + ", operation="
                        + operation
                        + ", unmappedFields="
                        + requirements.unmappedFields
                        + ", missingFields="
                        + missingFields
                        + ", nullFields="
                        + nullFields
                        + (rejectDefaultedNull
                                ? ", defaultedNullFields=" + defaultedNullFields
                                : ""));
    }

    private static final class ValidationRequirements {
        private static final ValidationRequirements DISABLED =
                new ValidationRequirements(
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.emptySet(),
                        false);

        private final Set<String> requiredSourceFields;
        private final Set<String> nonNullSourceFields;
        private final Set<String> defaultedSourceFields;
        private final Set<String> unmappedFields;
        private final boolean deduplicateUpdateAfterRequired;

        private ValidationRequirements(
                Set<String> requiredSourceFields,
                Set<String> nonNullSourceFields,
                Set<String> defaultedSourceFields,
                Set<String> unmappedFields,
                boolean deduplicateUpdateAfterRequired) {
            this.requiredSourceFields = requiredSourceFields;
            this.nonNullSourceFields = nonNullSourceFields;
            this.defaultedSourceFields = defaultedSourceFields;
            this.unmappedFields = unmappedFields;
            this.deduplicateUpdateAfterRequired = deduplicateUpdateAfterRequired;
        }
    }
}
