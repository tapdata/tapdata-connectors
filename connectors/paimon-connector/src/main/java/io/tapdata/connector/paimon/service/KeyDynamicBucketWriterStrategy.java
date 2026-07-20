package io.tapdata.connector.paimon.service;

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
 * <p>Open, bootstrap, emitted DELETE/INSERT ordering and snapshot fencing follow Paimon 1.3.1:
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java
 * and
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/crosspartition/IndexBootstrap.java
 */
final class KeyDynamicBucketWriterStrategy extends AbstractPaimonBucketWriterStrategy {

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
                        runtime.createGlobalIndexAssigner(table), "globalIndexAssigner");
        try {
            assigner.open(
                    0L,
                    context.ioManager(),
                    1,
                    0,
                    (row, bucket) -> emittedRows.add(new BucketedRow(row, bucket)));
            bootstrap(runtime);
        } catch (Exception e) {
            try {
                assigner.close();
            } catch (Exception closeError) {
                e.addSuppressed(closeError);
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
        Long snapshotBefore = table.snapshotManager().latestSnapshotIdFromFileSystem();
        try (RecordReader<InternalRow> reader = runtimeFactory.createIndexBootstrapReader(table)) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        assigner.bootstrapKey(row);
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        assigner.endBoostrap(false);
        emittedRows.clear();
        Long snapshotAfter = table.snapshotManager().latestSnapshotIdFromFileSystem();
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
            assigner.processInput(row);
            for (BucketedRow emitted : emittedRows) {
                delegate.write(emitted.row, emitted.bucket);
            }
        } finally {
            emittedRows.clear();
        }
    }

    @Override
    protected void closeModeResources() throws Exception {
        assigner.close();
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
