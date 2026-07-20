package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;

import java.util.Collections;

/** Native Paimon writer path for postpone-bucket tables. */
final class PostponeBucketWriterStrategy extends AbstractPaimonBucketWriterStrategy {

    PostponeBucketWriterStrategy(PaimonBucketWriterStrategyContext context) {
        super(context, BucketMode.POSTPONE_MODE, Collections.emptySet());
    }

    @Override
    protected void doWrite(InternalRow row) throws Exception {
        delegate.write(row);
    }
}
