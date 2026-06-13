package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableWrite;
import org.apache.paimon.types.RowType;

import java.util.List;

/**
 * StreamTableWrite wrapper that owns IOManager lifecycle for non-Flink usage.
 *
 * <p>Close order is strict: close write first, then close IOManager in finally block.
 */
public class ManagedIOStreamTableWrite implements StreamTableWrite {

    private final StreamTableWrite delegate;
    private final IOManager ioManager;
    private final List<String> spillDirs;

    public ManagedIOStreamTableWrite(StreamTableWrite delegate, IOManager ioManager, List<String> spillDirs) {
        this.delegate = delegate;
        this.ioManager = ioManager;
        this.spillDirs = spillDirs;
    }

    @Override
    public TableWrite withIOManager(IOManager ioManager) {
        delegate.withIOManager(ioManager);
        return this;
    }

    @Override
    public TableWrite withWriteType(RowType writeType) {
        delegate.withWriteType(writeType);
        return this;
    }

    @Override
    public TableWrite withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
        delegate.withMemoryPoolFactory(memoryPoolFactory);
        return this;
    }

    @Override
    public BinaryRow getPartition(InternalRow row) {
        return delegate.getPartition(row);
    }

    @Override
    public int getBucket(InternalRow row) {
        return delegate.getBucket(row);
    }

    @Override
    public void write(InternalRow row) throws Exception {
        delegate.write(row);
    }

    @Override
    public void write(InternalRow row, int bucket) throws Exception {
        delegate.write(row, bucket);
    }

    @Override
    public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle) throws Exception {
        delegate.writeBundle(partition, bucket, bundle);
    }

    @Override
    public void compact(BinaryRow partition, int bucket, boolean fullCompaction) throws Exception {
        delegate.compact(partition, bucket, fullCompaction);
    }

    @Override
    public TableWrite withMetricRegistry(MetricRegistry registry) {
        delegate.withMetricRegistry(registry);
        return this;
    }

    @Override
    public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier) throws Exception {
        return delegate.prepareCommit(waitCompaction, commitIdentifier);
    }

    @Override
    public void close() throws Exception {
        Exception writeError = null;
        try {
            delegate.close();
        } catch (Exception e) {
            writeError = e;
        }

        Exception ioManagerError = null;
        if (ioManager != null) {
            try {
                ioManager.close();
            } catch (Exception e) {
                ioManagerError = e;
            } finally {
                // Drop from the JVM-wide live registry; the dir is already deleted by ioManager.close().
                PaimonSpillDirCleaner.unregisterLiveDirs(spillDirs);
            }
        }

        if (writeError != null) {
            if (ioManagerError != null) {
                writeError.addSuppressed(ioManagerError);
            }
            throw writeError;
        }
        if (ioManagerError != null) {
            throw ioManagerError;
        }
    }
}

