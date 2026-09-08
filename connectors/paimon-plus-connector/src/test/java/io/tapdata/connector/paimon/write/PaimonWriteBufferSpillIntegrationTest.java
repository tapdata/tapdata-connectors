package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.config.PaimonWriteOptions;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.append.AppendOnlyWriter;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.postpone.PostponeBucketWriter;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.utils.RecordWriter;
import org.apache.paimon.compression.CompressOptions;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.mergetree.SortBufferWriteBuffer;
import org.apache.paimon.mergetree.compact.DeduplicateMergeFunction;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** 使用真实排序缓冲的容量及读取行为验证开关，不以目录是否存在代替验证。 */
class PaimonWriteBufferSpillIntegrationTest {
    @TempDir Path temp;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void runtimePrimaryKeyWriterSpillsAccordingToSwitchAndCommitsEveryRecord(boolean enabled)
            throws Exception {
        Options options = new Options();
        options.set("warehouse", temp.resolve("warehouse").toString());
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("default", true);
            Identifier id = Identifier.create("default", "pk_pressure");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.INT())
                    .column("value", DataTypes.STRING()).primaryKey("id").option("bucket", "1")
                    .option("write-buffer-size", "96 kb").option("page-size", "32 kb")
                    .option("write-buffer-spillable", Boolean.toString(!enabled))
                    .option("snapshot.expire.execution-mode", "SYNC").build(), false);
            PaimonConfig config = new PaimonConfig();
            config.setDiskOverflowWrite(enabled);
            FileStoreTable runtime = PaimonWriteOptions.runtimeWriteTable(
                    (FileStoreTable) catalog.getTable(id), config);
            org.apache.paimon.table.sink.StreamWriteBuilder builder = runtime.newStreamWriteBuilder();
            IOManager io = spy(new IOManagerImpl(temp.resolve("spill").toString()));
            java.util.concurrent.ExecutorService compact = java.util.concurrent.Executors.newSingleThreadExecutor();
            TableWriteImpl<?> writer = (TableWriteImpl<?>) builder.newWrite();
            writer.withIOManager(io).withCompactExecutor(compact);
            try (org.apache.paimon.table.sink.StreamTableCommit commit = builder.newCommit()) {
                String value = "x".repeat(256);
                for (int i = 0; i < 1000; i++) {
                    writer.write(GenericRow.of(i, BinaryString.fromString(value)));
                }
                commit.commit(1L, writer.prepareCommit(true, 1L));
                if (enabled) {
                    verify(io, atLeastOnce()).createBufferFileWriter(org.mockito.ArgumentMatchers.any());
                } else {
                    verify(io, never()).createBufferFileWriter(org.mockito.ArgumentMatchers.any());
                }
                org.apache.paimon.table.source.ReadBuilder read = runtime.newReadBuilder();
                java.util.BitSet ids = new java.util.BitSet();
                try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader =
                        read.newRead().createReader(read.newScan().plan())) {
                    org.apache.paimon.reader.RecordReader.RecordIterator<org.apache.paimon.data.InternalRow> batch;
                    while ((batch = reader.readBatch()) != null) {
                        try {
                            org.apache.paimon.data.InternalRow row;
                            while ((row = batch.next()) != null) {
                                assertFalse(ids.get(row.getInt(0)), "主键不能重复");
                                ids.set(row.getInt(0));
                                assertEquals(value, row.getString(1).toString());
                            }
                        } finally {
                            batch.releaseBatch();
                        }
                    }
                }
                assertEquals(1000, ids.cardinality());
                assertEquals(1000, ids.nextClearBit(0));
            } finally {
                try {
                    writer.close();
                } finally {
                    compact.shutdownNow();
                    assertTrue(compact.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
                    io.close();
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"append", "postpone"})
    void nativeWriterLimitCanForceBufferingEvenWhenOptionIsFalse(String mode) throws Exception {
        Options options = new Options();
        options.set("warehouse", temp.resolve("warehouse").toString());
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("default", true);
            Identifier id = Identifier.create("default", mode);
            Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT())
                    .column("pt", DataTypes.INT()).partitionKeys("pt")
                    .option("bucket", mode.equals("append") ? "-1" : "-2")
                    .option("write-max-writers-to-spill", "1")
                    .option("write-buffer-size", "1 mb")
                    .option("snapshot.expire.execution-mode", "SYNC");
            if (mode.equals("postpone")) {
                schema.primaryKey("id", "pt");
            }
            catalog.createTable(id, schema.build(), false);
            FileStoreTable runtime = PaimonWriteOptions.runtimeWriteTable(
                    (FileStoreTable) catalog.getTable(id), new PaimonConfig());
            assertFalse(runtime.coreOptions().writeBufferSpillable());
            IOManager io = new IOManagerImpl(temp.resolve("spill").toString());
            java.util.concurrent.ExecutorService compact = java.util.concurrent.Executors.newSingleThreadExecutor();
            TableWriteImpl<?> writer = (TableWriteImpl<?>) runtime.newStreamWriteBuilder().newWrite();
            writer.withIOManager(io).withCompactExecutor(compact);
            try {
                writer.write(GenericRow.of(1, 1));
                AbstractFileStoreWrite<?> write = (AbstractFileStoreWrite<?>) writer.getWrite();
                RecordWriter<?> first = write.writers().values().iterator().next().values().iterator().next().writer;
                assertFalse(buffered(first));
                // 第二个分区触发原版 forceBufferSpill，旧 Writer 也切换到缓冲模式。
                // https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/operation/BaseAppendFileStoreWrite.java
                // https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/postpone/PostponeBucketFileStoreWrite.java
                writer.write(GenericRow.of(2, 2));
                assertTrue(buffered(first));
                assertFalse(runtime.coreOptions().writeBufferSpillable(), "原生强制路径没有改写选项");
                assertFalse(writer.prepareCommit(true, 1L).isEmpty());
            } finally {
                try {
                    writer.close();
                } finally {
                    compact.shutdownNow();
                    assertTrue(compact.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
                    io.close();
                }
            }
        }
    }

    private static boolean buffered(RecordWriter<?> writer) {
        if (writer instanceof AppendOnlyWriter) {
            return ((AppendOnlyWriter) writer).getWriteBuffer() != null;
        }
        return ((PostponeBucketWriter) writer).useBufferedSinkWriter();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nativeSortBufferHonorsSwitchUnderMemoryPressure(boolean enabled) throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDiskOverflowWrite(enabled);
        CoreOptions options = CoreOptions.fromMap(Collections.singletonMap(
                CoreOptions.WRITE_BUFFER_SPILLABLE.key(), PaimonWriteOptions.spillValue(config)));
        RowType keyType = RowType.builder().field("k", DataTypes.INT()).build();
        RowType valueType = RowType.builder().field("v", DataTypes.STRING()).build();
        IOManager io = spy(new IOManagerImpl(temp.toString()));
        // Paimon 1.3.2 只有 ioManager != null && spillable 才使用外部排序缓冲。
        // https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/mergetree/SortBufferWriteBuffer.java
        SortBufferWriteBuffer buffer = new SortBufferWriteBuffer(keyType, valueType, null,
                new HeapMemorySegmentPool(3 * 4096, 4096), options.writeBufferSpillable(),
                MemorySize.MAX_VALUE, 128, CompressOptions.defaultOptions(), io);
        String value = "x".repeat(256);
        int accepted = 0;
        try {
            for (int i = 0; i < 1000; i++) {
                if (!buffer.put(i, RowKind.INSERT, GenericRow.of(i),
                        GenericRow.of(BinaryString.fromString(value)))) {
                    break;
                }
                accepted++;
            }
            if (enabled) {
                assertEquals(1000, accepted, "外部缓冲应能溢写后继续接收");
                verify(io, atLeastOnce()).createBufferFileWriter(org.mockito.ArgumentMatchers.any());
            } else {
                assertTrue(accepted > 0 && accepted < 1000, "内存满后向 Writer 返回 flush 信号");
                assertFalse(buffer.flushMemory(), "内存缓冲不能转为磁盘溢写");
                verify(io, never()).createBufferFileWriter(org.mockito.ArgumentMatchers.any());
            }
            AtomicInteger read = new AtomicInteger();
            buffer.forEach((a, b) -> Integer.compare(a.getInt(0), b.getInt(0)),
                    DeduplicateMergeFunction.factory().create(null), null, kv -> {
                        assertEquals(read.getAndIncrement(), kv.key().getInt(0));
                        assertEquals(value, kv.value().getString(0).toString());
                    });
            assertEquals(accepted, read.get(), "全部已接收记录可按键完整读回");
        } finally {
            buffer.clear();
            io.close();
        }
    }
}
