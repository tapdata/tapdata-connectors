package io.tapdata.connector.paimon.service;

import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.table.BucketMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class PaimonDmlImageValidatorTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("primaryKeyBucketModes")
    void deduplicateUpdateAfterMustRequireCompleteImageInEveryPrimaryKeyBucketMode(
            BucketMode bucketMode) {
        PaimonWriteSemanticContract contract =
                contract(
                        bucketMode,
                        MergeEngine.DEDUPLICATE,
                        false,
                        Arrays.asList("id", "pt", "value"),
                        set("id"),
                        Collections.emptySet(),
                        set("id"),
                        set("pt"),
                        null,
                        -1);

        // Paimon 1.3.1 DeduplicateMergeFunction#add replaces the complete value with the latest
        // record. Sparse UPDATE_AFTER is therefore unsafe in every primary-key BucketMode,
        // independently of the full-changelog routing contract.
        // Source:
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/mergetree/compact/DeduplicateMergeFunction.java#L27-L48
        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateUpdate(
                                        "default.t",
                                        contract,
                                        PaimonGeneratedFieldDependencies.none(),
                                        tapTable("id", "pt", "value"),
                                        null,
                                        map("id", 1, "pt", "A")));

        assertTrue(
                thrown.getMessage()
                        .contains("PAIMON_DEDUPLICATE_INCOMPLETE_UPDATE_AFTER"));
        assertTrue(thrown.getMessage().contains("missingFields=[value]"));
        assertTrue(thrown.getMessage().contains("bucketMode=" + bucketMode));
    }

    private static Stream<BucketMode> primaryKeyBucketModes() {
        return Stream.of(
                BucketMode.HASH_FIXED,
                BucketMode.HASH_DYNAMIC,
                BucketMode.KEY_DYNAMIC,
                BucketMode.POSTPONE_MODE);
    }

    @Test
    void deduplicateUpdateAfterMustClassifyEveryInvalidFieldWithoutLeakingValues() {
        PaimonWriteSemanticContract contract =
                contract(
                        BucketMode.KEY_DYNAMIC,
                        MergeEngine.DEDUPLICATE,
                        false,
                        Arrays.asList(
                                "id",
                                "missing_value",
                                "required_null",
                                "defaulted_null",
                                "target_only"),
                        set("id", "required_null"),
                        set("defaulted_null"),
                        set("id"),
                        Collections.emptySet(),
                        null,
                        -1);
        String secret = "SECRET-TARGET-VALUE";

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateUpdate(
                                        "default.t",
                                        contract,
                                        PaimonGeneratedFieldDependencies.none(),
                                        tapTable(
                                                "id",
                                                "missing_value",
                                                "required_null",
                                                "defaulted_null"),
                                        null,
                                        map(
                                                "id",
                                                1,
                                                "required_null",
                                                null,
                                                "defaulted_null",
                                                null,
                                                "target_only",
                                                secret)));

        assertTrue(thrown.getMessage().contains("unmappedFields=[target_only]"));
        assertTrue(thrown.getMessage().contains("missingFields=[missing_value]"));
        assertTrue(thrown.getMessage().contains("nullFields=[required_null]"));
        assertTrue(thrown.getMessage().contains("defaultedNullFields=[defaulted_null]"));
        assertFalse(thrown.getMessage().contains(secret));
        assertFalse(thrown.getMessage().contains("{"));
    }

    @Test
    void deduplicateUpdateAfterMustAllowExplicitNullableNullWithoutSchemaDefault() {
        PaimonWriteSemanticContract contract =
                contract(
                        BucketMode.HASH_FIXED,
                        MergeEngine.DEDUPLICATE,
                        false,
                        Arrays.asList("id", "nullable_value"),
                        set("id"),
                        Collections.emptySet(),
                        set("id"),
                        Collections.emptySet(),
                        null,
                        -1);

        assertDoesNotThrow(
                () ->
                        PaimonDmlImageValidator.validateUpdate(
                                "default.t",
                                contract,
                                PaimonGeneratedFieldDependencies.none(),
                                tapTable("id", "nullable_value"),
                                null,
                                map("id", 1, "nullable_value", null)));
    }

    @Test
    void deduplicateUpdateAfterMustClassifyNonNullFieldWithDefaultInBothNullSets() {
        PaimonWriteSemanticContract contract =
                contract(
                        BucketMode.HASH_FIXED,
                        MergeEngine.DEDUPLICATE,
                        false,
                        Arrays.asList("id", "required_defaulted_value"),
                        set("id", "required_defaulted_value"),
                        set("required_defaulted_value"),
                        set("id"),
                        Collections.emptySet(),
                        null,
                        -1);

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateUpdate(
                                        "default.t",
                                        contract,
                                        PaimonGeneratedFieldDependencies.none(),
                                        tapTable("id", "required_defaulted_value"),
                                        null,
                                        map(
                                                "id",
                                                1,
                                                "required_defaulted_value",
                                                null)));

        assertTrue(
                thrown.getMessage()
                        .contains("nullFields=[required_defaulted_value]"));
        assertTrue(
                thrown.getMessage()
                        .contains("defaultedNullFields=[required_defaulted_value]"));
    }

    @Test
    void deduplicateCompletenessRuleMustNotApplyToInsertOrOptionalUpdateBefore() {
        PaimonWriteSemanticContract contract =
                contract(
                        BucketMode.KEY_DYNAMIC,
                        MergeEngine.DEDUPLICATE,
                        false,
                        Arrays.asList("id", "pt", "value"),
                        set("id"),
                        Collections.emptySet(),
                        set("id"),
                        set("pt"),
                        null,
                        -1);
        TapTable table = tapTable("id", "pt", "value");

        assertDoesNotThrow(
                () ->
                        PaimonDmlImageValidator.validateInsert(
                                "default.t",
                                contract,
                                PaimonGeneratedFieldDependencies.none(),
                                table,
                                map("id", 1)));
        assertDoesNotThrow(
                () ->
                        PaimonDmlImageValidator.validateUpdate(
                                "default.t",
                                contract,
                                PaimonGeneratedFieldDependencies.none(),
                                table,
                                map("id", 1),
                                map("id", 1, "pt", "A", "value", "latest")));
    }

    @Test
    void sparseUpdateMustRemainAllowedOutsidePrimaryKeyDeduplicateContract() {
        PaimonWriteSemanticContract partialUpdate =
                contract(
                        BucketMode.HASH_DYNAMIC,
                        MergeEngine.PARTIAL_UPDATE,
                        false,
                        Arrays.asList("id", "value"),
                        set("id"),
                        Collections.emptySet(),
                        set("id"),
                        Collections.emptySet(),
                        null,
                        -1);
        PaimonWriteSemanticContract noPrimaryKey =
                contract(
                        BucketMode.HASH_FIXED,
                        MergeEngine.DEDUPLICATE,
                        false,
                        Arrays.asList("id", "value"),
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Collections.emptySet(),
                        null,
                        -1);

        assertDoesNotThrow(
                () ->
                        PaimonDmlImageValidator.validateUpdate(
                                "default.partial",
                                partialUpdate,
                                PaimonGeneratedFieldDependencies.none(),
                                tapTable("id", "value"),
                                null,
                                map("id", 1)));
        assertDoesNotThrow(
                () ->
                        PaimonDmlImageValidator.validateUpdate(
                                "default.append",
                                noPrimaryKey,
                                PaimonGeneratedFieldDependencies.none(),
                                tapTable("id", "value"),
                                null,
                                map("id", 1)));
    }

    @Test
    void deduplicateUpdateAfterMustValidateGeneratedFieldSourceDependencies() {
        PaimonWriteSemanticContract contract =
                contract(
                        BucketMode.HASH_DYNAMIC,
                        MergeEngine.DEDUPLICATE,
                        false,
                        Arrays.asList("_hash_key", "value"),
                        set("_hash_key"),
                        Collections.emptySet(),
                        set("_hash_key"),
                        Collections.emptySet(),
                        null,
                        -1);
        Map<String, java.util.Collection<String>> dependencies = new LinkedHashMap<>();
        dependencies.put("_hash_key", Arrays.asList("pk1", "pk2"));

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateUpdate(
                                        "default.t",
                                        contract,
                                        PaimonGeneratedFieldDependencies.of(dependencies),
                                        tapTable("pk1", "pk2", "value"),
                                        null,
                                        map("pk1", 1, "value", "latest")));

        assertTrue(
                thrown.getMessage()
                        .contains("PAIMON_DEDUPLICATE_INCOMPLETE_UPDATE_AFTER"));
        assertTrue(thrown.getMessage().contains("missingFields=[pk2]"));
        assertFalse(thrown.getMessage().contains("_hash_key="));
    }

    @Test
    void batchValidationMustCompileRequirementsOnceAndValidateEveryEvent() {
        TapTable table = spy(tapTable("id", "pt", "value"));
        java.util.List<TapRecordEvent> events =
                Arrays.asList(
                        new TapInsertRecordEvent()
                                .init()
                                .table("t")
                                .after(map("id", 1, "pt", "A", "value", "insert")),
                        new TapUpdateRecordEvent()
                                .init()
                                .table("t")
                                .before(map("id", 1, "pt", "A", "value", "insert"))
                                .after(map("id", 1, "pt", "B", "value", "update")),
                        new TapDeleteRecordEvent()
                                .init()
                                .table("t")
                                .before(map("id", 1, "pt", "B")));

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateBatch(
                                        "default.t",
                                        fullContract(),
                                        PaimonGeneratedFieldDependencies.none(),
                                        table,
                                        events));

        assertTrue(thrown.getMessage().contains("operation=DELETE"));
        assertTrue(thrown.getMessage().contains("value"));
        verify(table, times(1)).getNameFieldMap();
    }

    @Test
    void nullAndEmptyRequiredImagesMustFailForEveryDmlShape() {
        TapTable table = tapTable("id", "pt", "value");
        PaimonGeneratedFieldDependencies generated = PaimonGeneratedFieldDependencies.none();

        PaimonFatalWriteException insert =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateInsert(
                                        "default.t", fullContract(), generated, table, null));
        assertTrue(insert.getMessage().contains("PAIMON_INCOMPLETE_AFTER_IMAGE"));

        PaimonFatalWriteException update =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateUpdate(
                                        "default.t",
                                        fullContract(),
                                        generated,
                                        table,
                                        map("id", 1, "pt", "A", "value", "old"),
                                        null));
        assertTrue(update.getMessage().contains("operation=UPDATE_AFTER"));

        PaimonFatalWriteException delete =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateDelete(
                                        "default.t",
                                        fullContract(),
                                        generated,
                                        table,
                                        Collections.emptyMap()));
        assertTrue(delete.getMessage().contains("PAIMON_INCOMPLETE_BEFORE_IMAGE"));
    }

    @Test
    void insertMustRequireEveryMappedBusinessFieldButAllowExplicitNullableNull() {
        PaimonWriteSemanticContract contract = fullContract();
        TapTable table = tapTable("id", "pt", "value");
        Map<String, Object> complete = map("id", 1, "pt", null, "value", null);

        assertDoesNotThrow(
                () ->
                        PaimonDmlImageValidator.validateInsert(
                                "default.t",
                                contract,
                                PaimonGeneratedFieldDependencies.none(),
                                table,
                                complete));

        Map<String, Object> missing = map("id", 1, "pt", "A");
        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateInsert(
                                        "default.t",
                                        contract,
                                        PaimonGeneratedFieldDependencies.none(),
                                        table,
                                        missing));
        assertTrue(thrown.getMessage().contains("PAIMON_INCOMPLETE_AFTER_IMAGE"));
        assertTrue(thrown.getMessage().contains("value"));
    }

    @Test
    void updateMustValidateBeforeAndAfterIndependently() {
        Map<String, Object> incompleteBefore = map("id", 1, "pt", "A");
        Map<String, Object> completeAfter = map("id", 1, "pt", "B", "value", "new");

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateUpdate(
                                        "default.t",
                                        fullContract(),
                                        PaimonGeneratedFieldDependencies.none(),
                                        tapTable("id", "pt", "value"),
                                        incompleteBefore,
                                        completeAfter));

        assertTrue(thrown.getMessage().contains("PAIMON_INCOMPLETE_BEFORE_IMAGE"));
        assertTrue(thrown.getMessage().contains("operation=UPDATE_BEFORE"));
    }

    @Test
    void deleteMustRejectKeyOnlyImage() {
        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateDelete(
                                        "default.t",
                                        fullContract(),
                                        PaimonGeneratedFieldDependencies.none(),
                                        tapTable("id", "pt", "value"),
                                        map("id", 1)));

        assertTrue(thrown.getMessage().contains("PAIMON_INCOMPLETE_BEFORE_IMAGE"));
        assertTrue(thrown.getMessage().contains("pt"));
        assertTrue(thrown.getMessage().contains("value"));
    }

    @Test
    void primaryKeyAndGeneratedDependenciesMustBePresentAndNonNull() {
        PaimonWriteSemanticContract contract =
                contract(
                        true,
                        Arrays.asList("pt", "value", "_hash_key", "pk1", "pk2"),
                        set("_hash_key"),
                        set("_hash_key"),
                        set("pt"),
                        null,
                        -1);
        Map<String, java.util.Collection<String>> dependencies = new LinkedHashMap<>();
        dependencies.put("_hash_key", Arrays.asList("pk1", "pk2"));
        PaimonGeneratedFieldDependencies generated =
                PaimonGeneratedFieldDependencies.of(dependencies);
        Map<String, Object> image =
                map("pt", "A", "value", "v", "pk1", 1, "pk2", null);

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateInsert(
                                        "default.t",
                                        contract,
                                        generated,
                                        tapTable("pt", "value", "pk1", "pk2"),
                                        image));

        assertTrue(thrown.getMessage().contains("pk2"));
        assertFalse(thrown.getMessage().contains("_hash_key="));
    }

    @Test
    void rowKindFieldMustNotBeTrustedOrRequiredFromSource() {
        PaimonWriteSemanticContract contract =
                contract(
                        true,
                        Arrays.asList("id", "pt", "value", "rk"),
                        set("id"),
                        set("id"),
                        set("pt"),
                        "rk",
                        3);

        assertDoesNotThrow(
                () ->
                        PaimonDmlImageValidator.validateInsert(
                                "default.t",
                                contract,
                                PaimonGeneratedFieldDependencies.none(),
                                tapTable("id", "pt", "value"),
                                map("id", 1, "pt", "A", "value", "v")));
        assertDoesNotThrow(
                () ->
                        PaimonDmlImageValidator.validateUpdate(
                                "default.t",
                                contract,
                                PaimonGeneratedFieldDependencies.none(),
                                tapTable("id", "pt", "value"),
                                map("id", 1, "pt", "A", "value", "old"),
                                map("id", 1, "pt", "A", "value", "new")));
    }

    @Test
    void targetFieldWithoutTapMappingMustFailEvenIfEventContainsIt() {
        PaimonWriteSemanticContract contract =
                contract(
                        true,
                        Arrays.asList("id", "pt", "value", "target_only"),
                        set("id"),
                        set("id"),
                        set("pt"),
                        null,
                        -1);

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateInsert(
                                        "default.t",
                                        contract,
                                        PaimonGeneratedFieldDependencies.none(),
                                        tapTable("id", "pt", "value"),
                                        map(
                                                "id",
                                                1,
                                                "pt",
                                                "A",
                                                "value",
                                                "v",
                                                "target_only",
                                                "present")));

        assertTrue(thrown.getMessage().contains("target_only"));
    }

    @Test
    void optionalContractMustPreserveAfterOnlyAndPartialImageBehavior() {
        PaimonWriteSemanticContract optional =
                contract(
                        BucketMode.HASH_FIXED,
                        MergeEngine.PARTIAL_UPDATE,
                        false,
                        Arrays.asList("id", "pt", "value"),
                        set("id"),
                        Collections.emptySet(),
                        set("id"),
                        set("pt"),
                        null,
                        -1);

        assertDoesNotThrow(
                () ->
                        PaimonDmlImageValidator.validateUpdate(
                                "default.t",
                                optional,
                                PaimonGeneratedFieldDependencies.none(),
                                tapTable("id", "pt", "value"),
                                null,
                                map("id", 1)));
    }

    @Test
    void fatalMessageMustNotLeakFieldValuesOrWholeEvent() {
        String secret = "SECRET-VALUE-MUST-NOT-LEAK";
        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                PaimonDmlImageValidator.validateInsert(
                                        "default.t",
                                        fullContract(),
                                        PaimonGeneratedFieldDependencies.none(),
                                        tapTable("id", "pt", "value"),
                                        map("id", null, "pt", "A", "value", secret)));

        assertFalse(thrown.getMessage().contains(secret));
        assertFalse(thrown.getMessage().contains("{"));
    }

    private static PaimonWriteSemanticContract fullContract() {
        return contract(
                true,
                Arrays.asList("id", "pt", "value"),
                set("id"),
                set("id"),
                set("pt"),
                null,
                -1);
    }

    private static PaimonWriteSemanticContract contract(
            boolean full,
            java.util.List<String> fields,
            java.util.Set<String> nonNull,
            java.util.Set<String> primaryKeys,
            java.util.Set<String> partitionKeys,
            String rowKindField,
            int rowKindIndex) {
        return contract(
                BucketMode.HASH_FIXED,
                MergeEngine.DEDUPLICATE,
                full,
                fields,
                nonNull,
                Collections.emptySet(),
                primaryKeys,
                partitionKeys,
                rowKindField,
                rowKindIndex);
    }

    private static PaimonWriteSemanticContract contract(
            BucketMode bucketMode,
            MergeEngine mergeEngine,
            boolean full,
            java.util.List<String> fields,
            java.util.Set<String> nonNull,
            java.util.Set<String> defaulted,
            java.util.Set<String> primaryKeys,
            java.util.Set<String> partitionKeys,
            String rowKindField,
            int rowKindIndex) {
        return new PaimonWriteSemanticContract(
                bucketMode,
                true,
                mergeEngine,
                ChangelogProducer.NONE,
                full,
                fields,
                nonNull,
                defaulted,
                primaryKeys,
                partitionKeys,
                rowKindField,
                rowKindIndex);
    }

    private static TapTable tapTable(String... fields) {
        TapTable table = new TapTable("t");
        for (String field : fields) {
            table.add(new TapField(field, "STRING"));
        }
        return table;
    }

    private static java.util.Set<String> set(String... fields) {
        return new LinkedHashSet<>(Arrays.asList(fields));
    }

    private static Map<String, Object> map(Object... values) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            map.put((String) values[i], values[i + 1]);
        }
        return map;
    }
}
