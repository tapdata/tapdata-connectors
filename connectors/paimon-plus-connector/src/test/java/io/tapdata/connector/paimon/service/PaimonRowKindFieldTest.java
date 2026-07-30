package io.tapdata.connector.paimon.service;

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PaimonRowKindFieldTest {

    @ParameterizedTest
    @EnumSource(RowKind.class)
    void mustSetInternalAndConfiguredStringRowKind(RowKind rowKind) {
        PaimonWriteSemanticContract contract =
                new PaimonWriteSemanticContract(
                        BucketMode.HASH_FIXED,
                        true,
                        MergeEngine.DEDUPLICATE,
                        ChangelogProducer.INPUT,
                        true,
                        Arrays.asList("id", "rk"),
                        Collections.singleton("id"),
                        Collections.singleton("id"),
                        Collections.emptySet(),
                        "rk",
                        1);
        GenericRow row = GenericRow.of(1, BinaryString.fromString("untrusted"));

        PaimonRowKindField.apply(contract, row, rowKind);

        assertEquals(rowKind, row.getRowKind());
        assertEquals(rowKind.shortString(), row.getString(1).toString());
    }

    @ParameterizedTest
    @EnumSource(RowKind.class)
    void unconfiguredContractMustOnlySetExistingInternalRowKind(RowKind rowKind) {
        PaimonWriteSemanticContract contract =
                PaimonWriteSemanticContractTestFactory.forMode(BucketMode.KEY_DYNAMIC);
        GenericRow row = GenericRow.of(1);

        PaimonRowKindField.apply(contract, row, rowKind);

        assertEquals(rowKind, row.getRowKind());
        assertEquals(1, row.getFieldCount());
    }
}
