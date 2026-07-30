package io.tapdata.connector.paimon.service;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;

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
                        "PAIMON_INCOMPLETE_AFTER_IMAGE");
            } else if (event instanceof TapUpdateRecordEvent) {
                TapUpdateRecordEvent update = (TapUpdateRecordEvent) event;
                validateImage(
                        tableKey,
                        contract,
                        requirements,
                        update.getBefore(),
                        "UPDATE_BEFORE",
                        "PAIMON_INCOMPLETE_BEFORE_IMAGE");
                validateImage(
                        tableKey,
                        contract,
                        requirements,
                        update.getAfter(),
                        "UPDATE_AFTER",
                        "PAIMON_INCOMPLETE_AFTER_IMAGE");
            } else if (event instanceof TapDeleteRecordEvent) {
                validateImage(
                        tableKey,
                        contract,
                        requirements,
                        ((TapDeleteRecordEvent) event).getBefore(),
                        "DELETE",
                        "PAIMON_INCOMPLETE_BEFORE_IMAGE");
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
                "PAIMON_INCOMPLETE_AFTER_IMAGE");
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
                "PAIMON_INCOMPLETE_BEFORE_IMAGE");
        validateImage(
                tableKey,
                contract,
                requirements,
                after,
                "UPDATE_AFTER",
                "PAIMON_INCOMPLETE_AFTER_IMAGE");
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
                "PAIMON_INCOMPLETE_BEFORE_IMAGE");
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
        if (!contract.requiresFullChangelog()) {
            return ValidationRequirements.DISABLED;
        }

        LinkedHashSet<String> requiredSourceFields = new LinkedHashSet<>();
        LinkedHashSet<String> nonNullSourceFields = new LinkedHashSet<>();
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
                requiredSourceFields, nonNullSourceFields, unmappedFields);
    }

    private static void validateImage(
            String tableKey,
            PaimonWriteSemanticContract contract,
            ValidationRequirements requirements,
            @Nullable Map<String, Object> image,
            String operation,
            String reasonCode) {
        if (!requirements.enabled) {
            return;
        }

        Set<String> missingFields = new LinkedHashSet<>();
        Set<String> nullFields = new LinkedHashSet<>();
        for (String sourceField : requirements.requiredSourceFields) {
            if (image == null || !image.containsKey(sourceField)) {
                missingFields.add(sourceField);
            } else if (image.get(sourceField) == null
                    && requirements.nonNullSourceFields.contains(sourceField)) {
                nullFields.add(sourceField);
            }
        }

        if (requirements.unmappedFields.isEmpty()
                && missingFields.isEmpty()
                && nullFields.isEmpty()) {
            return;
        }

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
                        + nullFields);
    }

    private static final class ValidationRequirements {
        private static final ValidationRequirements DISABLED =
                new ValidationRequirements(
                        Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), false);

        private final Set<String> requiredSourceFields;
        private final Set<String> nonNullSourceFields;
        private final Set<String> unmappedFields;
        private final boolean enabled;

        private ValidationRequirements(
                Set<String> requiredSourceFields,
                Set<String> nonNullSourceFields,
                Set<String> unmappedFields) {
            this(requiredSourceFields, nonNullSourceFields, unmappedFields, true);
        }

        private ValidationRequirements(
                Set<String> requiredSourceFields,
                Set<String> nonNullSourceFields,
                Set<String> unmappedFields,
                boolean enabled) {
            this.requiredSourceFields = requiredSourceFields;
            this.nonNullSourceFields = nonNullSourceFields;
            this.unmappedFields = unmappedFields;
            this.enabled = enabled;
        }
    }
}
