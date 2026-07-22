package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;

import java.util.Collections;
import java.util.Objects;

/**
 * Native append-only path for bucket-unaware tables.
 *
 * <p>Rows and their RowKind are intentionally passed through unchanged. Paimon owns retract
 * filtering through the existing table options:
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/table/AppendOnlyFileStoreTable.java
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
