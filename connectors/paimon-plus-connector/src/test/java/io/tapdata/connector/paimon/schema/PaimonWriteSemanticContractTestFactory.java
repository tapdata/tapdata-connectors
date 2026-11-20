package io.tapdata.connector.paimon.schema;

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.Arrays;
import java.util.Collections;

/** Minimal semantic contracts for bucket strategy unit tests. */
public final class PaimonWriteSemanticContractTestFactory {

    private PaimonWriteSemanticContractTestFactory() {}

    public static PaimonWriteSemanticContract forMode(BucketMode bucketMode, DataField... fields) {
        return new PaimonWriteSemanticContract(
                bucketMode,
                false,
                MergeEngine.DEDUPLICATE,
                ChangelogProducer.NONE,
                false,
                new RowType(Arrays.asList(fields)),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                null,
                -1);
    }
}
