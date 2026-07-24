package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;

import java.util.Collections;

/** Native Paimon writer path for fixed-hash bucket tables. */
final class HashFixedBucketWriterStrategy extends AbstractPaimonBucketWriterStrategy {

    HashFixedBucketWriterStrategy(PaimonBucketWriterStrategyContext context) {
        super(context, BucketMode.HASH_FIXED, Collections.emptySet());
    }

    @Override
    protected void doWrite(InternalRow row) throws Exception {
        delegate.write(row);
    }
}
