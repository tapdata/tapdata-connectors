package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractResolver;
import io.tapdata.connector.paimon.util.PaimonSpillDirCleaner;
import io.tapdata.connector.paimon.write.bucket.DefaultPaimonBucketWriterRuntimeFactory;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategyContext;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategyFactory;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * S16：六种真实写入模式的空最终 prepare。验证业务数据和持久化提交标识；
 * PDK offset callback 属于 Service，不能用 Context fixture 中自造的 offset 变量代替验证。
 */
class PaimonFinalPrepareBucketModeIntegrationTest {
    @TempDir java.nio.file.Path tempDir;

    enum Mode {
        HASH_FIXED(BucketMode.HASH_FIXED, "1", true, false),
        HASH_DYNAMIC(BucketMode.HASH_DYNAMIC, "-1", true, false),
        KEY_DYNAMIC(BucketMode.KEY_DYNAMIC, "-1", true, true),
        BUCKET_UNAWARE(BucketMode.BUCKET_UNAWARE, "-1", false, false),
        POSTPONE(BucketMode.POSTPONE_MODE, "-2", true, false),
        BUCKETED_APPEND(BucketMode.HASH_FIXED, "1", false, false);

        final BucketMode expectedBucketMode;
        final String buckets;
        final boolean primaryKey;
        final boolean partitioned;

        Mode(BucketMode mode, String buckets, boolean primaryKey, boolean partitioned) {
            this.expectedBucketMode = mode;
            this.buckets = buckets;
            this.primaryKey = primaryKey;
            this.partitioned = partitioned;
        }
    }

    @ParameterizedTest(name = "{0}: 已确认业务后的 final prepare 没有业务增量")
    @EnumSource(Mode.class)
    void finalPrepareMustKeepBusinessRowsAndConfirmedCommitIdentityUnchanged(Mode mode)
            throws Exception {
        try (Fixture fixture = new Fixture(mode)) {
            assertEquals(mode.expectedBucketMode, fixture.context.bucketMode());
            fixture.context.write(fixture.row(1, "one"));
            fixture.context.write(fixture.row(2, "two"));
            fixture.context.commit();
            List<String> expectedRows = mode.partitioned
                    ? Arrays.asList("1:1:one", "2:2:two") : Arrays.asList("1:one", "2:two");
            assertEquals(expectedRows, readRows(fixture.table, mode));
            if (mode == Mode.POSTPONE) {
                assertTrue(fixture.table.newReadBuilder().newScan().plan().splits().isEmpty(),
                        "POSTPONE 仍保持原生批读只看已分配真实 bucket 的契约");
            }
            long businessSnapshot = fixture.table.latestSnapshot().orElseThrow(AssertionError::new).id();
            assertEquals(1L, fixture.savedIdentifier.get());
            assertEquals(1, fixture.commitCalls.get());

            assertEquals(PaimonTableWriteContext.StopOutcome.SUCCESS,
                    fixture.context.closeForStop(true, phase -> { }));
            assertEquals(1, fixture.strategy.finalPrepareCalls.get(),
                    "通过真实模式策略的最终准备入口执行一次，不能绕过模式钩子");
            assertNotNull(fixture.strategy.finalMessages);
            for (CommitMessage message : fixture.strategy.finalMessages) {
                CommitMessageImpl nativeMessage = assertInstanceOf(CommitMessageImpl.class, message);
                assertTrue(nativeMessage.newFilesIncrement().isEmpty(),
                        mode + " 最终 DataIncrement 的 data/changelog/index 全部必须为空");
                assertTrue(nativeMessage.isEmpty(), "单轮业务已提交，没有额外 Compaction 结果");
            }
            assertFalse(PaimonNativeWriteAccess.hasFinalCompaction(fixture.strategy.finalMessages));
            assertEquals(businessSnapshot,
                    fixture.table.latestSnapshot().orElseThrow(AssertionError::new).id(),
                    "完全空的最终准备不能触发原生空 APPEND");
            assertEquals(expectedRows, readRows(fixture.table, mode));
            assertEquals(1L, fixture.savedIdentifier.get(), "最终空结果不推进持久化提交标识");
            assertEquals(1, fixture.stateSaves.get());
            assertEquals(1, fixture.commitCalls.get());
            assertEquals(0, fixture.recoveryCalls.get());
            assertFalse(fixture.context.hasPendingCommit());
            assertTrue(fixture.lifecycle.compactionExecutor().isTerminated());
            assertTrue(fixture.context.cleanupComplete());
            for (String spill : fixture.spillDirectories) {
                File directory = new File(spill);
                assertFalse(directory.exists());
                assertFalse(new File(directory.getParentFile(),
                        "." + directory.getName() + ".tapdata-owner.lock").exists());
            }
        }
    }

    private static List<String> readRows(FileStoreTable table, Mode mode) throws Exception {
        List<String> result = new ArrayList<>();
        ReadBuilder builder = table.newReadBuilder();
        // POSTPONE 的默认 BatchScan 会过滤 bucket=-2；changelog=NONE 的 StreamScan 可读取
        // 已确认的 postpone 文件。保留这个可见性边界，不能把过滤后的空批读当成 STOP 丢数。
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableBatchScan.java#L68
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java#L98
        try (RecordReader<InternalRow> reader = builder.newRead().createReader(
                mode == Mode.POSTPONE ? builder.newStreamScan().plan() : builder.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        result.add(mode.partitioned
                                ? row.getInt(0) + ":" + row.getInt(1) + ":" + row.getString(2)
                                : row.getInt(0) + ":" + row.getString(1));
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        Collections.sort(result);
        return result;
    }

    private final class Fixture implements AutoCloseable {
        final Mode mode;
        final AtomicLong savedIdentifier = new AtomicLong();
        final AtomicInteger stateSaves = new AtomicInteger();
        final AtomicInteger commitCalls = new AtomicInteger();
        final AtomicInteger recoveryCalls = new AtomicInteger();
        Catalog catalog;
        FileStoreTable table;
        IOManager io;
        PaimonCompactionLifecycle lifecycle;
        TableWriteImpl<?> rawWriter;
        StreamTableCommit rawCommitter;
        CapturingStrategy strategy;
        PaimonTableWriteContext context;
        List<String> spillDirectories = Collections.emptyList();

        Fixture(Mode mode) throws Exception {
            this.mode = mode;
            try {
                catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tempDir.toUri())));
                catalog.createDatabase("default", true);
                String tableName = mode.name().toLowerCase(java.util.Locale.ROOT);
                String tableKey = "default." + tableName;
                Schema.Builder schema = Schema.newBuilder();
                if (mode.partitioned) { schema.column("pt", DataTypes.INT()).partitionKeys("pt"); }
                schema.column("id", DataTypes.INT()).column("value", DataTypes.STRING());
                if (mode.primaryKey) { schema.primaryKey("id"); }
                if (mode == Mode.BUCKETED_APPEND) { schema.option("bucket-key", "id"); }
                schema.option("bucket", mode.buckets)
                        .option("snapshot.expire.execution-mode", "SYNC")
                        .option("write-buffer-size", "8mb")
                        .option("num-sorted-run.compaction-trigger", "100");
                if (mode == Mode.HASH_DYNAMIC || mode == Mode.KEY_DYNAMIC) {
                    schema.option("dynamic-bucket.target-row-num", "1")
                            .option("dynamic-bucket.max-buckets", "16");
                }
                if (mode == Mode.POSTPONE) {
                    schema.option("changelog-producer", "none").option("scan.mode", "latest-full");
                }
                Identifier identifier = Identifier.create("default", tableName);
                catalog.createTable(identifier, schema.build(), false);
                table = (FileStoreTable) catalog.getTable(identifier);
                io = IOManager.create(Files.createDirectory(tempDir.resolve("io")).toString());
                spillDirectories = PaimonSpillDirCleaner.registerLiveDirs(io);
                lifecycle = PaimonCompactionLifecycle.forTable(tableKey);
                String commitUser = "final-mode-" + tableName;
                StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(commitUser);
                rawWriter = (TableWriteImpl<?>) builder.newWrite().withIOManager(io);
                rawWriter.withCompactExecutor(lifecycle.compactionExecutor());
                rawCommitter = builder.newCommit();
                PaimonWriteSemanticContract contract = PaimonWriteSemanticContractResolver.resolve(tableKey, table);
                strategy = new CapturingStrategy(PaimonBucketWriterStrategyFactory.create(
                        new PaimonBucketWriterStrategyContext(
                                tableKey, table, rawWriter, commitUser, io, contract),
                        DefaultPaimonBucketWriterRuntimeFactory.INSTANCE));
                PaimonTableCommitter delegate = new PaimonStreamTableCommitter(rawCommitter);
                PaimonTableCommitter counted = new PaimonTableCommitter() {
                    @Override
                    public void commit(long id, List<CommitMessage> messages) {
                        commitCalls.incrementAndGet();
                        delegate.commit(id, messages);
                    }
                    @Override
                    public int filterAndCommit(Map<Long, List<CommitMessage>> pending) {
                        recoveryCalls.incrementAndGet();
                        return delegate.filterAndCommit(pending);
                    }
                    @Override
                    public void close() throws Exception { delegate.close(); }
                };
                context = new PaimonTableWriteContext(tableKey, tableName, commitUser, strategy, counted,
                        io, spillDirectories, 0L, id -> {
                            savedIdentifier.set(id);
                            stateSaves.incrementAndGet();
                        }, lifecycle, PaimonNativeWriteAccess.of(rawWriter));
            } catch (Exception failure) {
                try { close(); }
                catch (Exception cleanupFailure) { failure.addSuppressed(cleanupFailure); }
                throw failure;
            }
        }

        GenericRow row(int key, String value) {
            return mode.partitioned ? GenericRow.of(key, key, BinaryString.fromString(value))
                    : GenericRow.of(key, BinaryString.fromString(value));
        }

        @Override
        public void close() throws Exception {
            try {
                if (context != null) { context.close(); }
            } finally {
                if (lifecycle != null) {
                    lifecycle.compactionExecutor().shutdown();
                    assertTrue(lifecycle.compactionExecutor().awaitTermination(30L, TimeUnit.SECONDS));
                }
                try {
                    if (context == null) {
                        try {
                            if (strategy != null) { strategy.close(); }
                            else if (rawWriter != null) { rawWriter.close(); }
                        } finally {
                            try {
                                if (rawCommitter != null) { rawCommitter.close(); }
                            } finally {
                                if (io != null) { io.close(); }
                                PaimonSpillDirCleaner.releaseAfterClose(spillDirectories, true);
                            }
                        }
                    }
                } finally {
                    if (catalog != null) { catalog.close(); }
                }
            }
        }
    }

    /** 观察真实策略返回值，路由、模式钩子、prepare 和 close 都委托原实现。 */
    private static final class CapturingStrategy implements PaimonBucketWriterStrategy {
        final PaimonBucketWriterStrategy delegate;
        final AtomicInteger finalPrepareCalls = new AtomicInteger();
        List<CommitMessage> finalMessages;

        CapturingStrategy(PaimonBucketWriterStrategy delegate) { this.delegate = delegate; }
        @Override public BucketMode bucketMode() { return delegate.bucketMode(); }
        @Override public PaimonWriteSemanticContract writeSemanticContract() { return delegate.writeSemanticContract(); }
        @Override public void validateRoutingRow(InternalRow row, String operation) { delegate.validateRoutingRow(row, operation); }
        @Override public void write(InternalRow row) throws Exception { delegate.write(row); }
        @Override public List<CommitMessage> prepareCommit(long id) throws Exception { return delegate.prepareCommit(id); }
        @Override
        public List<CommitMessage> prepareFinalCommit(long id) throws Exception {
            // 原生 MergeTree/Append 的 prepare(true) 会等待；Postpone.prepare 只 flush，
            // sync 不调度任务。模式钩子仍需运行，才能证明动态 index 的 DataIncrement 为空。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/postpone/PostponeBucketWriter.java#L215-L237
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/append/AppendOnlyWriter.java#L221-L245
            finalPrepareCalls.incrementAndGet();
            finalMessages = new ArrayList<>(delegate.prepareFinalCommit(id));
            return finalMessages;
        }
        @Override public void close() throws Exception { delegate.close(); }
    }
}
