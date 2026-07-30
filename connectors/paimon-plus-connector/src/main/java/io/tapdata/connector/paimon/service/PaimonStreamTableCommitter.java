package io.tapdata.connector.paimon.service;

import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The single production commit adapter for every bucket mode.
 *
 * <p>Paimon defines {@code filterAndCommit} as the retry-safe path for possibly committed
 * identifiers:
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/table/sink/StreamTableCommit.java
 */
final class PaimonStreamTableCommitter implements PaimonTableCommitter {

    private final StreamTableCommit delegate;
    private boolean closed;

    PaimonStreamTableCommitter(StreamTableCommit delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
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
