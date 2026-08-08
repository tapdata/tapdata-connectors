package io.tapdata.connector.paimon.write;

import org.apache.paimon.table.sink.CommitMessage;

import java.util.List;
import java.util.Map;

/** Narrow connector-owned adapter for committing prepared Paimon messages. */
public interface PaimonTableCommitter extends AutoCloseable {

    /**
     * First-attempt commit path. The caller must prove this identifier has not already committed.
     *
     * <p>Paimon 1.3.2 contract: {@code commit} skips recovery filtering and is therefore reserved
     * for the first attempt with one commit user, identifier and message list. Source: {@code
     * paimon-core/src/main/java/org/apache/paimon/table/sink/StreamTableCommit.java#commit}, lines
     * 52-62. Baseline: {@code apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e}.
     */
    void commit(long identifier, List<CommitMessage> messages);

    /**
     * Ambiguous-outcome recovery path for the exact original user, identifier and messages.
     *
     * <p>Paimon 1.3.2 contract: {@code filterAndCommit} first filters identifiers already committed
     * by the same user, then commits the remainder. Source: {@code
     * paimon-core/src/main/java/org/apache/paimon/table/sink/StreamTableCommit.java#filterAndCommit},
     * lines 64-76. Baseline: {@code
     * apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e}.
     */
    int filterAndCommit(Map<Long, List<CommitMessage>> pendingCommits);

    @Override
    void close() throws Exception;
}
