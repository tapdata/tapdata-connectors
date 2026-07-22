package io.tapdata.connector.paimon.service;

import org.apache.paimon.table.sink.CommitMessage;

import java.util.List;
import java.util.Map;

/** Narrow connector-owned adapter for idempotently committing prepared Paimon messages. */
interface PaimonTableCommitter extends AutoCloseable {

    int filterAndCommit(Map<Long, List<CommitMessage>> pendingCommits);

    @Override
    void close() throws Exception;
}
