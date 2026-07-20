package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.CommitMessage;

import java.util.List;
import java.util.Set;

/**
 * Connector-internal write contract for one Paimon bucket mode.
 *
 * <p>The interface intentionally excludes Paimon's explicit-bucket and direct-compaction APIs so
 * callers cannot bypass the connector commit state machine. Paimon 1.3.1 likewise selects a
 * distinct write topology for every {@link BucketMode}:
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/FlinkSinkBuilder.java
 */
interface PaimonBucketWriterStrategy extends AutoCloseable {

    BucketMode bucketMode();

    /** Ordered source fields which must exist and be non-null before row conversion. */
    Set<String> requiredRoutingFields();

    void write(InternalRow row) throws Exception;

    /** Runs mode-specific prepare work and raw writer prepare with the same identifier. */
    List<CommitMessage> prepareCommit(long commitIdentifier) throws Exception;

    @Override
    void close() throws Exception;
}
