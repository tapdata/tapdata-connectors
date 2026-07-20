package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Final lifecycle template shared by all concrete bucket-mode strategies. */
abstract class AbstractPaimonBucketWriterStrategy implements PaimonBucketWriterStrategy {

    protected final String tableKey;
    protected final FileStoreTable table;
    protected final StreamTableWrite delegate;

    private final BucketMode expectedMode;
    private final Set<String> requiredRoutingFields;
    private boolean closed;

    AbstractPaimonBucketWriterStrategy(
            PaimonBucketWriterStrategyContext context,
            BucketMode expectedMode,
            Collection<String> requiredRoutingFields) {
        Objects.requireNonNull(context, "context");
        this.tableKey = context.tableKey();
        this.table = context.table();
        this.delegate = context.writer();
        this.expectedMode = Objects.requireNonNull(expectedMode, "expectedMode");
        if (table.bucketMode() != expectedMode) {
            throw new IllegalArgumentException(
                    "Paimon bucket strategy mode mismatch for "
                            + tableKey
                            + ": expected "
                            + expectedMode
                            + " but table uses "
                            + table.bucketMode());
        }
        LinkedHashSet<String> fields = new LinkedHashSet<>();
        for (String field : Objects.requireNonNull(requiredRoutingFields, "requiredRoutingFields")) {
            fields.add(Objects.requireNonNull(field, "requiredRoutingField"));
        }
        this.requiredRoutingFields = Collections.unmodifiableSet(fields);
    }

    @Override
    public final BucketMode bucketMode() {
        return expectedMode;
    }

    @Override
    public final Set<String> requiredRoutingFields() {
        return requiredRoutingFields;
    }

    @Override
    public final void write(InternalRow row) throws Exception {
        ensureOpen();
        doWrite(Objects.requireNonNull(row, "row"));
    }

    protected abstract void doWrite(InternalRow row) throws Exception;

    @Override
    public final List<CommitMessage> prepareCommit(long commitIdentifier) throws Exception {
        ensureOpen();
        beforePrepareCommit(commitIdentifier);
        return delegate.prepareCommit(false, commitIdentifier);
    }

    protected void beforePrepareCommit(long commitIdentifier) throws Exception {
        // Most modes have no independent state to prepare.
    }

    @Override
    public final void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;

        Exception failure = null;
        try {
            closeModeResources();
        } catch (Exception e) {
            failure = e;
        }
        try {
            delegate.close();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    protected void closeModeResources() throws Exception {
        // Most modes do not own a separate closeable runtime.
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Paimon bucket writer strategy is closed: " + tableKey);
        }
    }
}
