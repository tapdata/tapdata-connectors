package io.tapdata.connector.paimon.write.bucket;

import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;

import java.io.IOException;

/** Package-private construction seam for Paimon dynamic-bucket runtimes. */
public interface PaimonBucketWriterRuntimeFactory {

    BucketAssigner createHashBucketAssigner(FileStoreTable table, String commitUser);

    GlobalIndexAssigner createGlobalIndexAssigner(FileStoreTable table);

    RecordReader<InternalRow> createIndexBootstrapReader(FileStoreTable table) throws IOException;
}
