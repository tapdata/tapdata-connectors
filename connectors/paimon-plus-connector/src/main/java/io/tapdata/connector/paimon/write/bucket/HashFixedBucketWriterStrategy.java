package io.tapdata.connector.paimon.write.bucket;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;

import java.util.Collections;

/**
 * Native Paimon writer path for fixed-hash bucket tables.
 *
 * <p>Do not pass an explicit bucket here: TableWriteImpl extracts partition/bucket key and applies
 * the table's configured fixed hash routing. This is the same fixed-mode branch selected by the
 * Paimon Flink sink:
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/FlinkSinkBuilder.java#L260-L277
 */
public final class HashFixedBucketWriterStrategy extends AbstractPaimonBucketWriterStrategy {

    HashFixedBucketWriterStrategy(PaimonBucketWriterStrategyContext context) {
        super(context, BucketMode.HASH_FIXED, Collections.emptySet());
    }

    @Override
    protected void doWrite(InternalRow row) throws Exception {
        delegate.write(row);
    }
}
