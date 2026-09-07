package io.tapdata.connector.paimon.write.bucket;
import io.tapdata.connector.paimon.util.PaimonFailures;
import io.tapdata.connector.paimon.schema.PaimonRowKindField;
import io.tapdata.connector.paimon.write.PaimonTableWriteContextFactory.IncompleteCleanupException;

import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.BucketMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Global key-index routing strategy for cross-partition primary-key updates.
 *
 * <p>Open, bootstrap, emitted DELETE/INSERT ordering and snapshot fencing follow Paimon 1.3.2:
 * https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java#L114-L273
 * and
 * https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/IndexBootstrap.java#L72-L125
 */
public final class KeyDynamicBucketWriterStrategy extends AbstractPaimonBucketWriterStrategy {

    private final GlobalIndexAssigner assigner;
    private final List<BucketedRow> emittedRows = new ArrayList<>();

    KeyDynamicBucketWriterStrategy(
            PaimonBucketWriterStrategyContext context,
            PaimonBucketWriterRuntimeFactory runtimeFactory)
            throws Exception {
        super(
                requireIoManager(context),
                BucketMode.KEY_DYNAMIC,
                HashDynamicBucketWriterStrategy.requiredPrimaryKeyFields(context.table()));
        PaimonBucketWriterRuntimeFactory runtime =
                Objects.requireNonNull(runtimeFactory, "runtimeFactory");
        this.assigner =
                Objects.requireNonNull(
                        stopScope.create("allocate global index assigner", () -> runtime.createGlobalIndexAssigner(table)), "globalIndexAssigner");
        try {
            stopScope.run("open global index assigner", () -> assigner.open(
                    0L,
                    context.ioManager(),
                    1,
                    0,
                    (row, bucket) -> emittedRows.add(new BucketedRow(row, bucket))));
            bootstrap(runtime);
        } catch (Exception | Error e) {
            try {
                // GlobalIndexAssigner.open 会建立 RocksDB 和 bootstrap Spill 缓冲；构造失败
                // 时 Factory 尚未得到 strategy，必须在这里关闭已取得的 assigner。
                // 原生 close 失败不构成清理证明，向 Factory 传递同一不完整信号并保留 IO。
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java#L114
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java#L276
                stopScope.run("rollback global index assigner", assigner::close);
            } catch (Exception | Error closeError) {
                if (closeError != e) { PaimonFailures.append(e, closeError); }
                throw new IncompleteCleanupException(tableKey, e);
            }
            throw e;
        }
    }

    private static PaimonBucketWriterStrategyContext requireIoManager(
            PaimonBucketWriterStrategyContext context) {
        Objects.requireNonNull(context, "context");
        if (context.ioManager() == null) {
            throw new IllegalStateException("KEY_DYNAMIC bucket mode requires an IOManager");
        }
        return context;
    }

    private void bootstrap(PaimonBucketWriterRuntimeFactory runtimeFactory) throws Exception {
        Long snapshotBefore = stopScope.call("index bootstrap snapshot before",
                () -> table.snapshotManager().latestSnapshotIdFromFileSystem());
        try (RecordReader<InternalRow> reader = new io.tapdata.connector.paimon.service.PaimonGuardedReader<>(
                stopScope.create("allocate index bootstrap reader", () -> runtimeFactory.createIndexBootstrapReader(table)), stopScope, false)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        final InternalRow key = row;
                        stopScope.run("bootstrap global key", () -> assigner.bootstrapKey(key));
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        stopScope.run("end global index bootstrap", () -> assigner.endBoostrap(false));
        emittedRows.clear();
        Long snapshotAfter = stopScope.call("index bootstrap snapshot after",
                () -> table.snapshotManager().latestSnapshotIdFromFileSystem());
        if (!Objects.equals(snapshotBefore, snapshotAfter)) {
            throw new IllegalStateException(
                    "Paimon table changed while bootstrapping KEY_DYNAMIC index; "
                            + "only one write job per physical table is supported");
        }
    }

    @Override
    protected void doWrite(InternalRow row) throws Exception {
        emittedRows.clear();
        try {
            stopScope.run("process global index input", () -> assigner.processInput(row));
            for (BucketedRow emitted : emittedRows) {
                // GlobalIndexAssigner synthesizes cross-partition DELETE rows by copying the
                // incoming row and changing only InternalRow.RowKind. RowKindGenerator later
                // trusts the configured rowkind field, so keep both representations consistent.
                PaimonRowKindField.apply(
                        writeSemanticContract(), emitted.row, emitted.row.getRowKind());
                stopScope.run("write global index emission", () -> delegate.write(emitted.row, emitted.bucket));
            }
        } finally {
            emittedRows.clear();
        }
    }

    @Override
    protected void closeModeResources() throws Exception {
        stopScope.run("close global index assigner", assigner::close);
    }

    private static final class BucketedRow {
        private final InternalRow row;
        private final int bucket;

        private BucketedRow(InternalRow row, int bucket) {
            this.row = row;
            this.bucket = bucket;
        }
    }
}
