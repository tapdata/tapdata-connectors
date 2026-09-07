package io.tapdata.connector.paimon.write.bucket;
import io.tapdata.connector.paimon.util.PaimonFailures;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
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
public abstract class AbstractPaimonBucketWriterStrategy implements PaimonBucketWriterStrategy {

    protected final io.tapdata.connector.paimon.service.PaimonStopResources.Scope stopScope;
    protected final String tableKey;
    protected final FileStoreTable table;
    protected final StreamTableWrite delegate;
    private final PaimonWriteSemanticContract writeSemanticContract;

    private final BucketMode expectedMode;
    private final Map<String, Integer> requiredRoutingFieldIndexes;
    private boolean closed;

    AbstractPaimonBucketWriterStrategy(
            PaimonBucketWriterStrategyContext context,
            BucketMode expectedMode,
            Collection<String> requiredTargetFields) {
        Objects.requireNonNull(context, "context");
        this.stopScope = context.stopScope();
        this.tableKey = context.tableKey();
        this.table = context.table();
        this.delegate = context.writer();
        this.writeSemanticContract = context.writeSemanticContract();
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
    public final PaimonWriteSemanticContract writeSemanticContract() {
        return writeSemanticContract;
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
        return prepareCommit(false, commitIdentifier);
    }

    @Override
    public final List<CommitMessage> prepareFinalCommit(long commitIdentifier) throws Exception {
        return prepareCommit(true, commitIdentifier);
    }

    private List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier) throws Exception {
        ensureOpen();
        stopScope.run("prepare bucket assignment", () -> beforePrepareCommit(commitIdentifier));
        // Paimon 1.3.2 prepareCommit(true) 会 flush、等待并消费结果，flush 自身还可能调度
        // Compaction，调用前不能 shutdown executor。普通提交保留 false；不能绕过模式钩子。
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java#L252
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/append/AppendOnlyWriter.java#L221
        return stopScope.call("native prepareCommit", () -> delegate.prepareCommit(waitCompaction, commitIdentifier));
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

        Throwable failure = null;
        try {
            stopScope.run("close bucket runtime", this::closeModeResources);
        } catch (Exception | Error e) {
            failure = e;
        }
        try {
            stopScope.run("close native table writer", delegate::close);
        } catch (Exception | Error e) {
            if (failure == null) {
                failure = e;
            } else if (failure != e) {
                PaimonFailures.append(failure, e);
            }
        }
        if (failure != null) {
            if (failure instanceof Error) { throw (Error) failure; }
            throw (Exception) failure;
        }
    }

    protected void closeModeResources() throws Exception {
        // Most modes do not own a separate closeable runtime.
    }

    private void ensureOpen() {
        stopScope.check("bucket strategy access");
        if (closed) {
            throw new IllegalStateException("Paimon bucket writer strategy is closed: " + tableKey);
        }
    }
}
