package io.tapdata.connector.paimon.write.bucket;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;

import java.util.Collections;

/**
 * Native Paimon writer path for postpone-bucket tables.
 *
 * <p>bucket=-2 delegates adaptive partition/bucket organization to Paimon's native writer; an
 * external explicit bucket would bypass that contract. Paimon's Flink sink likewise partitions
 * postpone input but leaves final bucket selection to the table write:
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/FlinkSinkBuilder.java#L280-L290
 */
public final class PostponeBucketWriterStrategy extends AbstractPaimonBucketWriterStrategy {

    PostponeBucketWriterStrategy(PaimonBucketWriterStrategyContext context) {
        super(context, BucketMode.POSTPONE_MODE, Collections.emptySet());
    }

    @Override
    protected void doWrite(InternalRow row) throws Exception {
        delegate.write(row);
    }
}
