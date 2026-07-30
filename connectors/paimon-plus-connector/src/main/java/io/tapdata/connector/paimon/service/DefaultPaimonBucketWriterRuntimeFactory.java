package io.tapdata.connector.paimon.service;

import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.index.HashBucketAssigner;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;

import java.io.IOException;

/**
 * Production factory retaining the Paimon 1.3.1 single-writer runtime parameters.
 *
 * <p>The 1/1/0 channel, assigner and id tuple matches Paimon's dynamic bucket topology for the
 * connector's one-writer-per-table contract:
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/FlinkSinkBuilder.java
 */
enum DefaultPaimonBucketWriterRuntimeFactory implements PaimonBucketWriterRuntimeFactory {
    INSTANCE;

    @Override
    public BucketAssigner createHashBucketAssigner(FileStoreTable table, String commitUser) {
        return new HashBucketAssigner(
                table.snapshotManager(),
                commitUser,
                table.store().newIndexFileHandler(),
                1,
                1,
                0,
                table.store().options().dynamicBucketTargetRowNum(),
                table.store().options().dynamicBucketMaxBuckets());
    }

    @Override
    public GlobalIndexAssigner createGlobalIndexAssigner(FileStoreTable table) {
        return new GlobalIndexAssigner(table);
    }

    @Override
    public RecordReader<InternalRow> createIndexBootstrapReader(FileStoreTable table)
            throws IOException {
        return new IndexBootstrap(table).bootstrap(1, 0);
    }
}
