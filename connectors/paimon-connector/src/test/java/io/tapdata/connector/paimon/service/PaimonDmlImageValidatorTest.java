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

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class PaimonDmlImageValidatorTest {

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
                        false,
                        Arrays.asList("id", "pt", "value"),
                        set("id"),
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
        return new PaimonWriteSemanticContract(
                BucketMode.HASH_FIXED,
                true,
                MergeEngine.DEDUPLICATE,
                ChangelogProducer.NONE,
                full,
                fields,
                nonNull,
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
