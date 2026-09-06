package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractResolver;
import io.tapdata.connector.paimon.util.PaimonSpillDirCleaner;
import io.tapdata.connector.paimon.write.bucket.DefaultPaimonBucketWriterRuntimeFactory;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategyContext;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategyFactory;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.CommitIncrement;
import org.apache.paimon.utils.RecordWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * SS01：固定 Paimon 1.3.2 的停止接缝，并以真实 Snapshot 检查连接器是否遗漏最终 Compaction。
 * 原生特征断言用于约束连接器适配方案，不代表已经修复生产停止路径。
 */
class PaimonNativeStopContractTest {

    private static final String TABLE_NAME = "native_stop_contract";
    private static final Identifier IDENTIFIER = Identifier.create("default", TABLE_NAME);
    private static final String TABLE_KEY = "default." + TABLE_NAME;

    @TempDir
    java.nio.file.Path tempDir;

    private Catalog catalog;
    private String ioTmpDir;

    @BeforeEach
    void setUp() throws Exception {
        ioTmpDir = Files.createDirectory(tempDir.resolve("io")).toString();
        catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tempDir.toUri())));
        catalog.createDatabase("default", true);
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "1");
        options.put("num-levels", "2");
        options.put("num-sorted-run.compaction-trigger", "100");
        options.put("sort-spill-threshold", "2");
        options.put("write-buffer-size", "1mb");
        options.put("snapshot.expire.execution-mode", "SYNC");
        catalog.createTable(
                IDENTIFIER,
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .options(options)
                        .build(),
                false);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    void finalPrepareWithNoNewBusinessRowsCollectsRealSpillCompaction() throws Exception {
        FileStoreTable table = table();
        try (RawFixture fixture = new RawFixture(table, "final-prepare", true)) {
            seedThreeCommittedVersions(fixture);
            fixture.writer.compact(BinaryRow.EMPTY_ROW, 0, true);
            assertRealSpillEntered(fixture.io, fixture.spillEntered);

            ExecutorService prepareCaller = daemonExecutor("native-final-prepare");
            CountDownLatch prepareEntered = new CountDownLatch(1);
            try {
                Future<List<CommitMessage>> prepared = prepareCaller.submit(() -> {
                    prepareEntered.countDown();
                    // Paimon 1.3.2 MergeTreeWriter.prepareCommit(true) 等待后 drainIncrement；
                    // 即使没有新业务行，compactBefore/compactAfter 仍然需要提交。
                    // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java#L252-L299
                    return fixture.writer.prepareCommit(true, 3L);
                });
                assertTrue(prepareEntered.await(10L, TimeUnit.SECONDS));
                assertFalse(prepared.isDone(), "真实 Spill 尚未放行，最终准备不能已经完成");
                assertEquals(0, fixture.io.closeCount());
                fixture.releaseSpill.countDown();

                List<CommitMessage> messages = prepared.get(30L, TimeUnit.SECONDS);
                assertEquals(1, messages.size());
                CommitMessageImpl message = nativeMessage(messages.get(0));
                assertTrue(message.newFilesIncrement().isEmpty(), "业务数据已在前面三轮提交");
                assertEquals(3, message.compactIncrement().compactBefore().size());
                assertEquals(1, message.compactIncrement().compactAfter().size());
                assertFalse(message.isEmpty());
                fixture.committer.commit(3L, messages);
                assertEquals(1, activeFileCount(table));
                assertEquals(Snapshot.CommitKind.COMPACT, latestSnapshot(table).commitKind());
            } finally {
                fixture.releaseSpill.countDown();
                shutdownAndAssertTerminated(prepareCaller);
            }
        }
    }

    @Test
    void finalPrepareCanSubmitCompactionBeforeItWaitsForTheResult() throws Exception {
        FileStoreTable table = table();
        try (RawFixture seed = new RawFixture(table, "prepare-trigger-seed", false)) {
            seedThreeCommittedVersions(seed);
        }

        FileStoreTable compactionEnabled = table.copy(
                Collections.singletonMap("num-sorted-run.compaction-trigger", "3"));
        try (RawFixture fixture = new RawFixture(compactionEnabled, "prepare-trigger", false)) {
            fixture.writer.write(row(3));
            assertEquals(0, fixture.executor.submitted.get(), "写入一行尚未 flush，不应预先调度任务");
            // Paimon 1.3.2 flushWriteBuffer 先 addNewFile，再 triggerCompaction；
            // prepareCommit(true) 的执行期间仍必须允许 executor 接收原生 CompactTask。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java#L209-L266
            List<CommitMessage> messages = fixture.writer.prepareCommit(true, 0L);
            assertTrue(fixture.executor.submitted.get() > 0, "prepare 必须实际调度 Compaction");
            assertRealSpillEntered(fixture.io, fixture.spillEntered);
            assertTrue(messages.stream().map(PaimonNativeStopContractTest::nativeMessage)
                    .anyMatch(message -> !message.compactIncrement().isEmpty()));
            fixture.committer.commit(0L, messages);
            assertEquals(1, activeFileCount(table));
        }
    }

    @Test
    void nativeCancelCompletesFutureWhileRealSpillWorkerRemainsAlive() throws Exception {
        try (RawFixture fixture = new RawFixture(table(), "native-cancel", true)) {
            seedThreeCommittedVersions(fixture);
            fixture.writer.compact(BinaryRow.EMPTY_ROW, 0, true);
            assertRealSpillEntered(fixture.io, fixture.spillEntered);
            Future<?> compaction = fixture.executor.lastSubmitted.get();
            assertNotNull(compaction);

            // Paimon 1.3.2 MergeTreeWriter.close cancel 后 sync，CompactFutureManager 将
            // CancellationException 转为空结果并清空引用；这不是任务执行结束证明。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java#L34-L68
            assertDoesNotThrow(fixture.writer::close);
            fixture.writerClosed = true;
            assertTrue(compaction.isCancelled());
            assertTrue(compaction.isDone());
            fixture.executor.shutdown();
            assertFalse(fixture.executor.awaitTermination(10L, TimeUnit.MILLISECONDS),
                    "任务被 latch 确定阻塞，Future done 不能等同于 executor termination");
            assertEquals(1L, fixture.executor.workerFinished.getCount());
            assertEquals(0, fixture.io.closeCount());
            for (File directory : fixture.io.spillingDirectories()) {
                assertTrue(directory.exists(), "真实任务仍能访问的目录必须保留");
            }

            fixture.releaseSpill.countDown();
            assertTrue(fixture.executor.awaitTermination(30L, TimeUnit.SECONDS));
            assertEquals(0L, fixture.executor.workerFinished.getCount());
            assertEquals(null, fixture.executor.workerFailure.get(),
                    "保留 IOManager 直到 termination，真实 Spill 不应遇到目录删除异常");
        }
    }

    @Test
    void nativeBucketSyncConsumesAFailedFutureExactlyOnce() throws Exception {
        try (RawFixture fixture = new RawFixture(table(), "failed-future", false)) {
            seedThreeCommittedVersions(fixture);
            IOException taskFailure = new IOException("SS01 确定性的原生任务故障");
            fixture.executor.failNextTask.set(taskFailure);
            fixture.writer.compact(BinaryRow.EMPTY_ROW, 0, true);

            RecordWriter<?> bucketWriter = nativeWrite(fixture.writer)
                    .writers().get(BinaryRow.EMPTY_ROW).get(0).writer;
            // Paimon 1.3.2 RecordWriter.sync -> innerGetCompactionResult：异常也会进入
            // finally 清空 taskFuture，所以失败桶需要先统一 sync 再做原生 close。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java#L47-L68
            ExecutionException observed = assertThrows(ExecutionException.class, bucketWriter::sync);
            assertSame(taskFailure, observed.getCause());
            assertDoesNotThrow(bucketWriter::sync, "第二次 sync 不得重放已经消费的失败 Future");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void failedMultiBucketPrepareHasAlreadyConsumedEarlierBucketIncrement() throws Exception {
        try (RawFixture fixture = new RawFixture(table(), "partial-prepare", false)) {
            RecordWriter<Object> first = mock(RecordWriter.class);
            RecordWriter<Object> second = mock(RecordWriter.class);
            DataFileMeta compactedFile = mock(DataFileMeta.class);
            CommitIncrement compacted = new CommitIncrement(
                    DataIncrement.emptyIncrement(),
                    new CompactIncrement(Collections.emptyList(),
                            Collections.singletonList(compactedFile), Collections.emptyList()),
                    null);
            CommitIncrement empty = new CommitIncrement(
                    DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement(), null);
            IOException secondBucketFailure = new IOException("SS01 第二个 bucket 准备失败");
            when(first.prepareCommit(true)).thenReturn(compacted, empty);
            when(second.prepareCommit(true)).thenThrow(secondBucketFailure).thenReturn(empty);

            // Paimon 1.3.2 提供公开的 writers()/WriterContainer 接缝；原生 prepare 按桶
            // 逐个消费，第二桶失败不会回滚第一桶已经 drain 的结果，不能提交部分成功列表。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L185-L267
            AbstractFileStoreWrite<Object> nativeWrite = nativeWrite(fixture.writer);
            Map<Integer, AbstractFileStoreWrite.WriterContainer<Object>> orderedBuckets =
                    new LinkedHashMap<>();
            orderedBuckets.put(0, new TestWriterContainer(first));
            orderedBuckets.put(1, new TestWriterContainer(second));
            nativeWrite.writers().put(BinaryRow.EMPTY_ROW, orderedBuckets);

            assertSame(secondBucketFailure,
                    assertThrows(IOException.class, () -> fixture.writer.prepareCommit(true, 0L)));
            List<CommitMessage> retry = fixture.writer.prepareCommit(true, 0L);
            assertEquals(2, retry.size());
            assertTrue(retry.stream().map(PaimonNativeStopContractTest::nativeMessage)
                    .allMatch(CommitMessageImpl::isEmpty),
                    "再次 prepare 已无法找回第一桶的 Compaction increment，必须放弃整表尝试");
        }
    }

    @Test
    void contextStopMustCommitCompletedCompactionWithoutNewBusinessRows() throws Exception {
        FileStoreTable table = table();
        CountDownLatch spillEntered = new CountDownLatch(1);
        CountDownLatch releaseSpill = new CountDownLatch(0);
        BlockingSpillIOManager io = new BlockingSpillIOManager(ioTmpDir, spillEntered, releaseSpill);
        PaimonCompactionLifecycle lifecycle = PaimonCompactionLifecycle.forTable(TABLE_KEY);
        TableWriteImpl<?> rawWriter = null;
        StreamTableCommit rawCommitter = null;
        PaimonTableWriteContext context = null;
        List<String> spillDirectories = Collections.emptyList();
        try {
            String commitUser = "connector-final-stop";
            StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(commitUser);
            rawWriter = (TableWriteImpl<?>) builder.newWrite().withIOManager(io);
            rawWriter.withCompactExecutor(lifecycle.compactionExecutor());
            rawCommitter = builder.newCommit();
            PaimonWriteSemanticContract contract =
                    PaimonWriteSemanticContractResolver.resolve(TABLE_KEY, table);
            PaimonBucketWriterStrategy strategy = PaimonBucketWriterStrategyFactory.create(
                    new PaimonBucketWriterStrategyContext(
                            TABLE_KEY, table, rawWriter, commitUser, io, contract),
                    DefaultPaimonBucketWriterRuntimeFactory.INSTANCE);
            spillDirectories = PaimonSpillDirCleaner.registerLiveDirs(io.delegate());
            context = new PaimonTableWriteContext(
                    TABLE_KEY, TABLE_NAME, commitUser, strategy,
                    new PaimonStreamTableCommitter(rawCommitter), io, spillDirectories,
                    0L, PaimonTableWriteContext.CommitStateStore.NOOP, lifecycle,
                    PaimonNativeWriteAccess.of(rawWriter));
            for (int version = 0; version < 3; version++) {
                context.write(row(version));
                context.commit();
            }
            assertEquals(3, activeFileCount(table));
            long businessSnapshotId = latestSnapshot(table).id();
            rawWriter.compact(BinaryRow.EMPTY_ROW, 0, true);
            assertRealSpillEntered(io, spillEntered);
            for (Map<Integer, AbstractFileStoreWrite.WriterContainer<Object>> buckets :
                    nativeWrite(rawWriter).writers().values()) {
                for (AbstractFileStoreWrite.WriterContainer<Object> bucket : buckets.values()) {
                    bucket.writer.sync();
                }
            }

            // Spec S02/S08：业务已确认且 Compaction 已完成，close 必须收集并提交最后的
            // Compaction increment。旧 close 直接清理，没有这个 Snapshot，此断言应先 RED。
            context.closeForStop(true, phase -> {});
            Snapshot finalSnapshot = latestSnapshot(table);
            // StreamWriteBuilderImpl.newCommit:76 设置 ignoreEmptyCommit(false)，纯 Compaction
            // 的同一 envelope 先产生空 APPEND 再 COMPACT；完全无结果时连接器才不调用 commit。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilderImpl.java#L76
            assertEquals(businessSnapshotId + 2L, finalSnapshot.id(),
                    "停止必须提交零业务行的最终 Compaction，不能直接丢弃成功结果");
            assertEquals(Snapshot.CommitKind.COMPACT, finalSnapshot.commitKind());
            assertEquals(1, activeFileCount(table));
            assertEquals(1, io.closeCount());
            for (File directory : io.spillingDirectories()) {
                assertFalse(directory.exists(), "完整停止后释放真实 Spill 目录");
            }
        } finally {
            if (context != null) {
                context.close();
            } else {
                shutdownAndAssertTerminated(lifecycle.compactionExecutor());
                try {
                    if (rawWriter != null) {
                        rawWriter.close();
                    }
                } finally {
                    try {
                        if (rawCommitter != null) {
                            rawCommitter.close();
                        }
                    } finally {
                        try {
                            io.close();
                        } finally {
                            PaimonSpillDirCleaner.releaseAfterClose(spillDirectories, true);
                        }
                    }
                }
            }
        }
    }

    private FileStoreTable table() throws Exception {
        return (FileStoreTable) catalog.getTable(IDENTIFIER);
    }

    private static GenericRow row(int version) {
        return GenericRow.of(1, BinaryString.fromString("v" + version));
    }

    private static void seedThreeCommittedVersions(RawFixture fixture) throws Exception {
        for (int version = 0; version < 3; version++) {
            fixture.writer.write(row(version));
            fixture.committer.commit(version, fixture.writer.prepareCommit(false, version));
        }
        assertEquals(3, activeFileCount(fixture.table), "先证明存在三个真实且已提交的 L0 文件");
    }

    private static int activeFileCount(FileStoreTable table) throws Exception {
        int count = 0;
        for (Split split : table.newReadBuilder().newScan().plan().splits()) {
            count += ((DataSplit) split).dataFiles().size();
        }
        return count;
    }

    private static Snapshot latestSnapshot(FileStoreTable table) {
        return table.latestSnapshot().orElseThrow(() -> new AssertionError("缺少已提交的 Snapshot"));
    }

    private static CommitMessageImpl nativeMessage(CommitMessage message) {
        // 固定依赖接缝必须显式验证类型；CommitMessage 接口并不提供 increment/isEmpty。
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/CommitMessageImpl.java#L79-L89
        assertTrue(message instanceof CommitMessageImpl, message.getClass().getName());
        return (CommitMessageImpl) message;
    }

    @SuppressWarnings("unchecked")
    private static AbstractFileStoreWrite<Object> nativeWrite(TableWriteImpl<?> writer) {
        assertTrue(writer.getWrite() instanceof AbstractFileStoreWrite,
                writer.getWrite().getClass().getName());
        return (AbstractFileStoreWrite<Object>) writer.getWrite();
    }

    private static void assertRealSpillEntered(BlockingSpillIOManager io, CountDownLatch entered)
            throws InterruptedException {
        assertTrue(entered.await(10L, TimeUnit.SECONDS), "没有进入真实 Compaction Spill");
        String stack = io.firstSpillStack();
        assertNotNull(stack);
        assertTrue(stack.contains("org.apache.paimon.mergetree.MergeSorter.spill"), stack);
        assertTrue(stack.contains("org.apache.paimon.mergetree.compact.MergeTreeCompactTask"), stack);
    }

    private static ExecutorService daemonExecutor(String name) {
        return Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, name);
            thread.setDaemon(true);
            return thread;
        });
    }

    private static void shutdownAndAssertTerminated(ExecutorService executor)
            throws InterruptedException {
        executor.shutdown();
        assertTrue(executor.awaitTermination(30L, TimeUnit.SECONDS),
                "测试 finally 已放行全部 latch，必须取得真实线程终止证明");
    }

    private static final class TestWriterContainer
            extends AbstractFileStoreWrite.WriterContainer<Object> {
        TestWriterContainer(RecordWriter<Object> writer) {
            super(writer, 2, null, null, null);
        }
    }

    private final class RawFixture implements AutoCloseable {
        final FileStoreTable table;
        final CountDownLatch spillEntered = new CountDownLatch(1);
        final CountDownLatch releaseSpill;
        final BlockingSpillIOManager io;
        final CapturingExecutor executor = new CapturingExecutor();
        TableWriteImpl<?> writer;
        StreamTableCommit committer;
        boolean writerClosed;

        RawFixture(FileStoreTable table, String commitUser, boolean blockSpill) throws Exception {
            this.table = table;
            releaseSpill = new CountDownLatch(blockSpill ? 1 : 0);
            io = new BlockingSpillIOManager(ioTmpDir, spillEntered, releaseSpill);
            try {
                StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(commitUser);
                writer = (TableWriteImpl<?>) builder.newWrite().withIOManager(io);
                writer.withCompactExecutor(executor);
                committer = builder.newCommit();
            } catch (Exception creationFailure) {
                try {
                    close();
                } catch (Exception cleanupFailure) {
                    creationFailure.addSuppressed(cleanupFailure);
                }
                throw creationFailure;
            }
        }

        @Override
        public void close() throws Exception {
            releaseSpill.countDown();
            shutdownAndAssertTerminated(executor);
            try {
                if (writer != null && !writerClosed) {
                    writer.close();
                    writerClosed = true;
                }
            } finally {
                try {
                    if (committer != null) {
                        committer.close();
                    }
                } finally {
                    io.close();
                }
            }
        }
    }

    /** 只观察真实任务状态，异常注入仅用于证明原生失败 Future 的消费契约。 */
    private static final class CapturingExecutor extends AbstractExecutorService {
        final ExecutorService delegate = daemonExecutor("native-stop-compaction");
        final AtomicInteger submitted = new AtomicInteger();
        final AtomicReference<Future<?>> lastSubmitted = new AtomicReference<>();
        final AtomicReference<Exception> failNextTask = new AtomicReference<>();
        final AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        final CountDownLatch workerFinished = new CountDownLatch(1);

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            submitted.incrementAndGet();
            Future<T> future = delegate.submit(() -> {
                try {
                    Exception failure = failNextTask.getAndSet(null);
                    if (failure != null) {
                        throw failure;
                    }
                    return task.call();
                } catch (Exception | Error failure) {
                    workerFailure.compareAndSet(null, failure);
                    throw failure;
                } finally {
                    workerFinished.countDown();
                }
            });
            lastSubmitted.set(future);
            return future;
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(command);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }
    }
}
