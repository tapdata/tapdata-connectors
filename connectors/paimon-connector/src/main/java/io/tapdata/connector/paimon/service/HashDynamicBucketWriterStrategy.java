package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Hash-index routing strategy for Paimon dynamic bucket tables.
 *
 * <p>The partition plus trimmed-primary-key hash formula and prepare hook mirror Paimon 1.3.1's
 * {@code HashBucketAssigner} contract:
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/index/HashBucketAssigner.java
 */
final class HashDynamicBucketWriterStrategy extends AbstractPaimonBucketWriterStrategy {

    private final RowPartitionKeyExtractor extractor;
    private final BucketAssigner assigner;

    HashDynamicBucketWriterStrategy(
            PaimonBucketWriterStrategyContext context,
            PaimonBucketWriterRuntimeFactory runtimeFactory) {
        super(context, BucketMode.HASH_DYNAMIC, requiredPrimaryKeyFields(context.table()));
        PaimonBucketWriterRuntimeFactory runtime =
                Objects.requireNonNull(runtimeFactory, "runtimeFactory");
        this.extractor = new RowPartitionKeyExtractor(table.schema());
        this.assigner =
                Objects.requireNonNull(
                        runtime.createHashBucketAssigner(table, context.commitUser()),
                        "hashBucketAssigner");
    }

    @Override
    protected void doWrite(InternalRow row) throws Exception {
        int bucket =
                assigner.assign(
                        extractor.partition(row), extractor.trimmedPrimaryKey(row).hashCode());
        delegate.write(row, bucket);
    }

    @Override
    protected void beforePrepareCommit(long commitIdentifier) {
        assigner.prepareCommit(commitIdentifier);
    }

    static Set<String> requiredPrimaryKeyFields(FileStoreTable table) {
        return new LinkedHashSet<>(table.primaryKeys());
    }
}
