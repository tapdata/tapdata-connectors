package io.tapdata.connector.paimon.write;

import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The single production commit adapter for every bucket mode.
 *
 * <p>Paimon 1.3.2 separates the faster first attempt ({@code commit}) from ambiguous recovery
 * ({@code filterAndCommit}). Source: {@code
 * paimon-core/src/main/java/org/apache/paimon/table/sink/StreamTableCommit.java#commit/
 * #filterAndCommit}, lines 52-76. Baseline: {@code
 * apache/paimon@5c59e6cb01ed0b29563371f56e14fcade4597a2e}.
 */
public final class PaimonStreamTableCommitter implements PaimonTableCommitter {

    private final StreamTableCommit delegate;
    private volatile boolean closed;

    PaimonStreamTableCommitter(StreamTableCommit delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public void commit(long identifier, List<CommitMessage> messages) {
        ensureOpen();
        delegate.commit(identifier, Objects.requireNonNull(messages, "messages"));
    }

    @Override
    public int filterAndCommit(Map<Long, List<CommitMessage>> pendingCommits) {
        ensureOpen();
        return delegate.filterAndCommit(Objects.requireNonNull(pendingCommits, "pendingCommits"));
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        delegate.close();
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Paimon table committer is closed");
        }
    }
}
