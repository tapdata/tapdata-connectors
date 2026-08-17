package io.tapdata.connector.paimon.schema;

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.table.BucketMode;

import java.util.Collections;

/** Minimal semantic contracts for bucket strategy unit tests. */
public final class PaimonWriteSemanticContractTestFactory {

    private PaimonWriteSemanticContractTestFactory() {}

    public static PaimonWriteSemanticContract forMode(BucketMode bucketMode) {
        return new PaimonWriteSemanticContract(
                bucketMode,
                false,
                MergeEngine.DEDUPLICATE,
                ChangelogProducer.NONE,
                false,
                Collections.emptyList(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                Collections.emptySet(),
                null,
                -1);
    }
}
