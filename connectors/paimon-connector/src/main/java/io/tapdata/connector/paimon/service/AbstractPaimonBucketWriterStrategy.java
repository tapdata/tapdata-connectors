package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Final lifecycle template shared by all concrete bucket-mode strategies. */
abstract class AbstractPaimonBucketWriterStrategy implements PaimonBucketWriterStrategy {

    protected final String tableKey;
    protected final FileStoreTable table;
    protected final StreamTableWrite delegate;

    private final BucketMode expectedMode;
    private final Map<String, Integer> requiredRoutingFieldIndexes;
    private boolean closed;

    AbstractPaimonBucketWriterStrategy(
            PaimonBucketWriterStrategyContext context,
            BucketMode expectedMode,
            Collection<String> requiredTargetFields) {
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
        for (String field : Objects.requireNonNull(requiredTargetFields, "requiredTargetFields")) {
            fields.add(Objects.requireNonNull(field, "requiredRoutingField"));
        }
        if (fields.isEmpty()) {
            this.requiredRoutingFieldIndexes = Collections.emptyMap();
        } else {
            List<String> targetFieldNames = table.rowType().getFieldNames();
            LinkedHashMap<String, Integer> indexes = new LinkedHashMap<>();
            for (String field : fields) {
                int index = targetFieldNames.indexOf(field);
                if (index < 0) {
                    throw new PaimonFatalWriteException(
                            "Paimon routing field '"
                                    + field
                                    + "' is absent from target row type for "
                                    + tableKey);
                }
                indexes.put(field, index);
            }
            this.requiredRoutingFieldIndexes = Collections.unmodifiableMap(indexes);
        }
    }

    @Override
    public final BucketMode bucketMode() {
        return expectedMode;
    }

    @Override
    public final void validateRoutingRow(InternalRow row, String operation) {
        Objects.requireNonNull(row, "row");
        Objects.requireNonNull(operation, "operation");
        for (Map.Entry<String, Integer> field : requiredRoutingFieldIndexes.entrySet()) {
            if (row.isNullAt(field.getValue())) {
                throw new PaimonFatalWriteException(
                        "Missing non-null Paimon routing field '"
                                + field.getKey()
                                + "' for "
                                + operation
                                + " on dynamic-bucket table "
                                + tableKey);
            }
        }
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
