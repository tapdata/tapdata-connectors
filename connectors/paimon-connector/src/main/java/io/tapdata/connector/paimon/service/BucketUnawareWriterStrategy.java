package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;

import java.util.Collections;
import java.util.Objects;

/**
 * Native append-only path for bucket-unaware tables.
 *
 * <p>No explicit bucket is supplied because Paimon writes all files with bucket 0 while retaining
 * unrestricted file-level parallelism. Rows and RowKind are passed through unchanged; after any
 * configured RowKindFilter, Paimon's append writer rejects DELETE/UPDATE_BEFORE and accepts
 * UPDATE_AFTER as an add. A mutable source therefore requires an explicit append-only contract;
 * otherwise a missing before-image can silently append another version.
 * Sources:
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-common/src/main/java/org/apache/paimon/table/BucketMode.java#L57-L61
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/table/AppendOnlyFileStoreTable.java#L124-L143
 */
final class BucketUnawareWriterStrategy extends AbstractPaimonBucketWriterStrategy {

    BucketUnawareWriterStrategy(PaimonBucketWriterStrategyContext context) {
        super(validateAppendOnly(context), BucketMode.BUCKET_UNAWARE, Collections.emptySet());
    }

    private static PaimonBucketWriterStrategyContext validateAppendOnly(
            PaimonBucketWriterStrategyContext context) {
        Objects.requireNonNull(context, "context");
        if (!context.table().primaryKeys().isEmpty()) {
            throw new IllegalArgumentException(
                    "BUCKET_UNAWARE table must not define primary keys: " + context.tableKey());
        }
        return context;
    }

    @Override
    protected void doWrite(InternalRow row) throws Exception {
        delegate.write(row);
    }
}
