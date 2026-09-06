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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** S05/S06：通过真实 MergeSorter Spill 故障验证最终提交豁免与整表隔离边界。 */
class PaimonFinalCompactionIntegrationTest {
    private static final String TABLE_NAME = "final_compaction";
    private static final String TABLE_KEY = "default." + TABLE_NAME;
    private static final Identifier IDENTIFIER = Identifier.create("default", TABLE_NAME);

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void failureInSecondRealBucketMustDiscardTheWholeFinalAttemptAfterFirstBucketWasPrepared()
            throws Exception {
        try (Fixture fixture = new Fixture(2, 2)) {
            fixture.seedThreeVersionsInEveryBucket();
            long businessSnapshot = latestSnapshotId(fixture.table);
            Map<Integer, String> committedRows = readRows(fixture.table);
            assertEquals(2, committedRows.size());

            fixture.rawWriter.compact(BinaryRow.EMPTY_ROW, 0, true);
            fixture.rawWriter.compact(BinaryRow.EMPTY_ROW, 1, true);
            assertTrue(fixture.io.failureEntered.await(10L, TimeUnit.SECONDS),
                    "第二个真实 Compaction 必须进入被注入故障的 Spill 调用");
            fixture.io.assertRealSpills(2);

            // 单线程 executor 开始第二桶前，第一桶任务已实际返回。公开 writers() 接缝
            // 只替换遍历容器，保留原生 WriterContainer 与 RecordWriter 的全部状态。
            // 确定遍历顺序后，prepare 等待第二桶的 Future 即证明第一桶已成功 drainIncrement。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L207-L267
            AbstractFileStoreWrite<Object> nativeWrite = nativeWrite(fixture.rawWriter);
            Map<Integer, AbstractFileStoreWrite.WriterContainer<Object>> buckets =
                    nativeWrite.writers().get(BinaryRow.EMPTY_ROW);
            assertEquals(2, buckets.size());
            Map<Integer, AbstractFileStoreWrite.WriterContainer<Object>> ordered = new LinkedHashMap<>();
            ordered.put(0, buckets.get(0));
            ordered.put(1, buckets.get(1));
            nativeWrite.writers().put(BinaryRow.EMPTY_ROW, ordered);

            fixture.startFinalStop();
            assertTrue(fixture.finalPrepareEntered.await(5L, TimeUnit.SECONDS));
            awaitFinalPrepareWaitingForFuture(fixture.closer);
            assertEquals(1L, fixture.closeReturned.getCount());
            assertEquals(0, fixture.io.closeCount.get());
            for (String directory : fixture.spillDirectories) {
                assertTrue(new File(directory).isDirectory());
            }

            fixture.io.releaseFailure.countDown();
            assertTrue(fixture.closeReturned.await(30L, TimeUnit.SECONDS));
            joinAndAssertStopped(fixture.closer);
            assertNull(fixture.closeFailure.get());
            assertEquals(PaimonTableWriteContext.StopOutcome.SUCCESS_COMPACTION_DISCARDED,
                    fixture.stopOutcome.get());
            assertTrue(fixture.phases.stream().anyMatch(phase -> phase.startsWith("COMPACTION_DISCARDED")));
            assertFalse(fixture.phases.contains("FINAL_COMMIT"), "部分成功不能进入外部提交");
            assertUnchangedBusinessAndCompleteCleanup(fixture, businessSnapshot, committedRows);
            assertEquals(PaimonTableWriteContext.StopOutcome.SUCCESS_COMPACTION_DISCARDED,
                    fixture.context.closeForStop(true, ignored -> { }));
            assertEquals(1, fixture.io.closeCount.get(), "重复停止不能重复关闭 IOManager");
        }
    }

    @Test
    void theSameRealSpillFailureDuringOrdinaryPrepareMustRemainAHardBusinessFailure()
            throws Exception {
        try (Fixture fixture = new Fixture(1, 1)) {
            fixture.seedThreeVersionsInEveryBucket();
            long businessSnapshot = latestSnapshotId(fixture.table);
            Map<Integer, String> committedRows = readRows(fixture.table);
            fixture.rawWriter.compact(BinaryRow.EMPTY_ROW, 0, true);
            assertTrue(fixture.io.failureEntered.await(10L, TimeUnit.SECONDS));
            fixture.io.assertRealSpills(1);
            fixture.io.releaseFailure.countDown();

            // 普通 prepare(false) 只消费已经 done 的 Future。先 graceful shutdown/await，
            // 使本次真实故障确定可见；尚未消费失败 Future 时调用 prepare 会先抛错，不再调度。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java#L47-L68
            fixture.compaction.compactionExecutor().shutdown();
            assertTrue(fixture.compaction.compactionExecutor().awaitTermination(30L, TimeUnit.SECONDS));
            ExecutionException businessFailure = assertThrows(ExecutionException.class, fixture.context::commit);
            assertTrue(PaimonCompactionExecutor.isNativeCompactionFailure(businessFailure),
                    "同一个原生 CompactTask 标记不能越过业务阶段的失败边界");
            assertSame(fixture.io.injectedFailure, rootCause(businessFailure));
            assertThrows(IllegalStateException.class,
                    () -> fixture.context.write(row(0, "不应接收的新业务")));

            IllegalStateException stopFailure = assertThrows(IllegalStateException.class,
                    () -> fixture.context.closeForStop(true, fixture.phases::add));
            fixture.expectedCloseFailure = stopFailure;
            assertTrue(stopFailure.getMessage().contains("business barrier"));
            assertFalse(fixture.phases.stream().anyMatch(phase -> phase.startsWith("COMPACTION_DISCARDED")),
                    "业务已经失败后不能输出 Compaction 放弃并成功退出");
            assertUnchangedBusinessAndCompleteCleanup(fixture, businessSnapshot, committedRows);
        }
    }

    @Test
    void wholeTableDiscardMustWaitForThirdRealBucketAfterSecondBucketFailed() throws Exception {
        try (Fixture fixture = new Fixture(3, 2, 3)) {
            fixture.seedThreeVersionsInEveryBucket();
            long businessSnapshot = latestSnapshotId(fixture.table);
            Map<Integer, String> committedRows = readRows(fixture.table);
            for (int bucket = 0; bucket < 3; bucket++) {
                fixture.rawWriter.compact(BinaryRow.EMPTY_ROW, bucket, true);
            }
            assertTrue(fixture.io.failureEntered.await(10L, TimeUnit.SECONDS));
            AbstractFileStoreWrite<Object> nativeWrite = nativeWrite(fixture.rawWriter);
            Map<Integer, AbstractFileStoreWrite.WriterContainer<Object>> buckets =
                    nativeWrite.writers().get(BinaryRow.EMPTY_ROW);
            Map<Integer, AbstractFileStoreWrite.WriterContainer<Object>> ordered = new LinkedHashMap<>();
            for (int bucket = 0; bucket < 3; bucket++) { ordered.put(bucket, buckets.get(bucket)); }
            nativeWrite.writers().put(BinaryRow.EMPTY_ROW, ordered);
            fixture.startFinalStop();
            assertTrue(fixture.finalPrepareEntered.await(5L, TimeUnit.SECONDS));
            awaitFinalPrepareWaitingForFuture(fixture.closer);

            fixture.io.releaseFailure.countDown();
            assertTrue(fixture.discardedObserved.await(5L, TimeUnit.SECONDS),
                    "先证明整表最终尝试已经被标记为放弃");
            assertTrue(fixture.io.otherSpillEntered.await(10L, TimeUnit.SECONDS),
                    "第三个 CompactTask 必须真正进入 Spill，而不是停留在执行队列");
            fixture.io.assertRealSpills(3);
            assertFalse(fixture.closeReturned.await(250L, TimeUnit.MILLISECONDS),
                    "已经放弃提交也必须等第三桶实际退出");
            assertFalse(fixture.compaction.compactionExecutor().isTerminated());
            assertEquals(0, fixture.io.closeCount.get());
            assertEquals(3, fixture.commitCalls.get(), "不能提交第一桶已经 prepare 的部分结果");
            assertEquals(0, fixture.recoveryCalls.get(), "没有提交尝试，也不能制造恢复提交");
            assertEquals(3, fixture.stateSaves.get());
            assertEquals(businessSnapshot, latestSnapshotId(fixture.table));
            for (String directory : fixture.spillDirectories) {
                assertTrue(new File(directory).isDirectory());
            }

            fixture.io.releaseOtherSpill.countDown();
            assertTrue(fixture.closeReturned.await(30L, TimeUnit.SECONDS));
            joinAndAssertStopped(fixture.closer);
            assertNull(fixture.closeFailure.get());
            assertEquals(PaimonTableWriteContext.StopOutcome.SUCCESS_COMPACTION_DISCARDED,
                    fixture.stopOutcome.get());
            assertEquals(3, fixture.commitCalls.get());
            assertEquals(0, fixture.recoveryCalls.get());
            assertUnchangedBusinessAndCompleteCleanup(fixture, businessSnapshot, committedRows);
        }
    }

    private static void assertUnchangedBusinessAndCompleteCleanup(
            Fixture fixture, long businessSnapshot, Map<Integer, String> committedRows) throws Exception {
        assertEquals(businessSnapshot, latestSnapshotId(fixture.table),
                "放弃最终尝试不得产生空 APPEND 或部分 COMPACT Snapshot");
        assertEquals(committedRows, readRows(fixture.table), "已确认业务文件必须继续可读");
        for (int bucket = 0; bucket < fixture.bucketCount; bucket++) {
            assertEquals(Integer.valueOf(3), activeFilesByBucket(fixture.table).get(bucket));
        }
        assertEquals(3L, fixture.savedNextIdentifier.get(), "放弃不能推进 next identifier");
        assertEquals(3, fixture.stateSaves.get(), "只允许原来三轮业务的状态保存");
        assertFalse(fixture.context.hasPendingCommit(), "尚未提交的最终失败不能制造 pending envelope");
        assertTrue(fixture.compaction.compactionExecutor().isTerminated());
        assertTrue(fixture.context.cleanupComplete());
        assertEquals(1, fixture.io.closeCount.get());
        for (String spillDirectory : fixture.spillDirectories) {
            File directory = new File(spillDirectory);
            assertFalse(directory.exists(), "实际 termination 后必须删除 Spill 目录");
            assertFalse(new File(directory.getParentFile(),
                    "." + directory.getName() + ".tapdata-owner.lock").exists());
        }
    }

    private static long latestSnapshotId(FileStoreTable table) {
        return table.latestSnapshot().orElseThrow(() -> new AssertionError("缺少业务 Snapshot")).id();
    }

    private static GenericRow row(int key, String value) {
        return GenericRow.of(key, BinaryString.fromString(value));
    }

    private static Map<Integer, Integer> activeFilesByBucket(FileStoreTable table) throws Exception {
        Map<Integer, Integer> counts = new HashMap<>();
        for (Split split : table.newReadBuilder().newScan().plan().splits()) {
            DataSplit data = (DataSplit) split;
            counts.merge(data.bucket(), data.dataFiles().size(), Integer::sum);
        }
        return counts;
    }

    private static Map<Integer, String> readRows(FileStoreTable table) throws Exception {
        Map<Integer, String> rows = new HashMap<>();
        ReadBuilder builder = table.newReadBuilder();
        try (RecordReader<InternalRow> reader = builder.newRead().createReader(builder.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        String previous = rows.put(row.getInt(0), row.getString(1).toString());
                        assertNull(previous, "同一已提交主键只能读到一个最终值");
                    }
                } finally {
                    batch.releaseBatch();
                }
            }
        }
        return rows;
    }

    @SuppressWarnings("unchecked")
    private static AbstractFileStoreWrite<Object> nativeWrite(TableWriteImpl<?> writer) {
        assertTrue(writer.getWrite() instanceof AbstractFileStoreWrite);
        return (AbstractFileStoreWrite<Object>) writer.getWrite();
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null) { root = root.getCause(); }
        return root;
    }

    private static void awaitFinalPrepareWaitingForFuture(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (thread.isAlive() && System.nanoTime() < deadline) {
            boolean nativePrepare = false;
            boolean futureGet = false;
            for (StackTraceElement frame : thread.getStackTrace()) {
                nativePrepare |= frame.getClassName().equals(
                        "org.apache.paimon.operation.AbstractFileStoreWrite")
                        && frame.getMethodName().equals("prepareCommit");
                futureGet |= frame.getClassName().equals("org.apache.paimon.compact.CompactFutureManager")
                        && frame.getMethodName().equals("obtainCompactResult");
            }
            if (nativePrepare && futureGet && thread.getState() == Thread.State.WAITING) {
                return;
            }
            Thread.sleep(1L);
        }
        throw new AssertionError("最终 prepare 没有进入第二桶的真实 Future.get 等待");
    }

    private static void joinAndAssertStopped(Thread thread) throws InterruptedException {
        if (thread != null) {
            thread.join(TimeUnit.SECONDS.toMillis(30L));
            assertFalse(thread.isAlive(), "放行故障 latch 后线程仍未结束：" + thread.getName());
        }
    }

    private final class Fixture implements AutoCloseable {
        final int bucketCount;
        final AtomicLong savedNextIdentifier = new AtomicLong();
        final AtomicInteger stateSaves = new AtomicInteger();
        final AtomicInteger commitCalls = new AtomicInteger();
        final AtomicInteger recoveryCalls = new AtomicInteger();
        final List<String> phases = new CopyOnWriteArrayList<>();
        final CountDownLatch finalPrepareEntered = new CountDownLatch(1);
        final CountDownLatch discardedObserved = new CountDownLatch(1);
        final CountDownLatch closeReturned = new CountDownLatch(1);
        final AtomicReference<PaimonTableWriteContext.StopOutcome> stopOutcome = new AtomicReference<>();
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Catalog catalog;
        FileStoreTable table;
        FailingSpillIO io;
        PaimonCompactionLifecycle compaction;
        TableWriteImpl<?> rawWriter;
        StreamTableCommit rawCommitter;
        PaimonTableWriteContext context;
        List<String> spillDirectories = Collections.emptyList();
        Thread closer;
        Throwable expectedCloseFailure;

        Fixture(int bucketCount, int failingSpillOrdinal) throws Exception {
            this(bucketCount, failingSpillOrdinal, -1);
        }

        Fixture(int bucketCount, int failingSpillOrdinal, int blockedSpillOrdinal) throws Exception {
            this.bucketCount = bucketCount;
            try {
                String tmpDir = Files.createDirectory(tempDir.resolve("io")).toString();
                catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tempDir.toUri())));
                catalog.createDatabase("default", true);
                Map<String, String> options = new HashMap<>();
                options.put("bucket", Integer.toString(bucketCount));
                options.put("num-levels", "2");
                options.put("num-sorted-run.compaction-trigger", "100");
                options.put("sort-spill-threshold", "2");
                options.put("write-buffer-size", "1mb");
                options.put("snapshot.expire.execution-mode", "SYNC");
                catalog.createTable(IDENTIFIER, Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id").options(options).build(), false);
                table = (FileStoreTable) catalog.getTable(IDENTIFIER);
                compaction = PaimonCompactionLifecycle.forTable(TABLE_KEY);
                io = new FailingSpillIO(tmpDir, failingSpillOrdinal, blockedSpillOrdinal);
                String commitUser = "final-compaction-user";
                StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(commitUser);
                rawWriter = (TableWriteImpl<?>) builder.newWrite().withIOManager(io);
                rawWriter.withCompactExecutor(compaction.compactionExecutor());
                rawCommitter = builder.newCommit();
                PaimonWriteSemanticContract contract =
                        PaimonWriteSemanticContractResolver.resolve(TABLE_KEY, table);
                PaimonBucketWriterStrategy strategy = PaimonBucketWriterStrategyFactory.create(
                        new PaimonBucketWriterStrategyContext(
                                TABLE_KEY, table, rawWriter, commitUser, io, contract),
                        DefaultPaimonBucketWriterRuntimeFactory.INSTANCE);
                spillDirectories = PaimonSpillDirCleaner.registerLiveDirs(io.delegate);
                PaimonTableCommitter delegate = new PaimonStreamTableCommitter(rawCommitter);
                PaimonTableCommitter countedCommitter = new PaimonTableCommitter() {
                    @Override
                    public void commit(long identifier, List<CommitMessage> messages) {
                        commitCalls.incrementAndGet();
                        delegate.commit(identifier, messages);
                    }

                    @Override
                    public int filterAndCommit(Map<Long, List<CommitMessage>> pending) {
                        recoveryCalls.incrementAndGet();
                        return delegate.filterAndCommit(pending);
                    }

                    @Override
                    public void close() throws Exception { delegate.close(); }
                };
                context = new PaimonTableWriteContext(
                        TABLE_KEY, TABLE_NAME, commitUser, strategy,
                        countedCommitter, io, spillDirectories,
                        0L, nextIdentifier -> {
                            savedNextIdentifier.set(nextIdentifier);
                            stateSaves.incrementAndGet();
                        }, compaction, PaimonNativeWriteAccess.of(rawWriter));
            } catch (Exception failure) {
                try { close(); }
                catch (Exception cleanupFailure) { failure.addSuppressed(cleanupFailure); }
                throw failure;
            }
        }

        void seedThreeVersionsInEveryBucket() throws Exception {
            Map<Integer, Integer> keysByBucket = new HashMap<>();
            for (int key = 0; key < 256 && keysByBucket.size() < bucketCount; key++) {
                keysByBucket.putIfAbsent(rawWriter.getBucket(row(key, "route-probe")), key);
            }
            assertEquals(bucketCount, keysByBucket.size(), "必须根据原生 hash 找到每个实际桶的 key");
            for (int version = 0; version < 3; version++) {
                for (int bucket = 0; bucket < bucketCount; bucket++) {
                    assertNotNull(keysByBucket.get(bucket));
                    context.write(row(keysByBucket.get(bucket), "bucket-" + bucket + "-v" + version));
                }
                context.commit();
            }
            Map<Integer, Integer> files = activeFilesByBucket(table);
            assertEquals(bucketCount, files.size());
            for (int bucket = 0; bucket < bucketCount; bucket++) {
                assertEquals(Integer.valueOf(3), files.get(bucket), "每桶必须有三个真实已提交 L0");
            }
            Map<Integer, String> expected = new HashMap<>();
            keysByBucket.forEach((bucket, key) -> expected.put(key, "bucket-" + bucket + "-v2"));
            assertEquals(expected, readRows(table));
            assertEquals(0, io.spillCalls.get(), "小业务 fixture 不应提前触发缓冲区 Spill");
        }

        void startFinalStop() {
            closer = new Thread(() -> {
                try {
                    stopOutcome.set(context.closeForStop(true, new PaimonTableWriteContext.CloseObserver() {
                        @Override
                        public void phase(String phase) {
                            phases.add(phase);
                            if ("FINAL_PREPARE".equals(phase)) { finalPrepareEntered.countDown(); }
                        }

                        @Override
                        public void compactionDiscarded(long identifier, Throwable failure) {
                            assertTrue(PaimonCompactionExecutor.isNativeCompactionFailure(failure));
                            phases.add("COMPACTION_DISCARDED");
                            discardedObserved.countDown();
                        }
                    }));
                } catch (Throwable failure) {
                    closeFailure.set(failure);
                } finally {
                    closeReturned.countDown();
                }
            }, "S06-final-multi-bucket-prepare");
            closer.setDaemon(true);
            closer.start();
        }

        @Override
        public void close() throws Exception {
            if (io != null) {
                io.releaseFailure.countDown();
                io.releaseOtherSpill.countDown();
            }
            try {
                joinAndAssertStopped(closer);
            } finally {
                if (compaction != null) {
                    compaction.compactionExecutor().shutdown();
                    assertTrue(compaction.compactionExecutor().awaitTermination(30L, TimeUnit.SECONDS),
                            "必须先证明 Compaction 终止，再关闭测试资源");
                }
            }
            try {
                if (context != null) {
                    try {
                        context.closeForStop(false, ignored -> { });
                    } catch (Exception failure) {
                        if (failure != expectedCloseFailure || !context.cleanupComplete()) {
                            throw failure;
                        }
                    }
                } else {
                    try {
                        if (rawWriter != null) { rawWriter.close(); }
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

    private static final class FailingSpillIO implements IOManager {
        final IOManagerImpl delegate;
        final int failingOrdinal;
        final int blockedOrdinal;
        final IOException injectedFailure = new IOException("S06 第二桶真实 MergeSorter Spill 打开失败");
        final AtomicInteger spillCalls = new AtomicInteger();
        final AtomicInteger closeCount = new AtomicInteger();
        final CountDownLatch failureEntered = new CountDownLatch(1);
        final CountDownLatch releaseFailure = new CountDownLatch(1);
        final CountDownLatch otherSpillEntered = new CountDownLatch(1);
        final CountDownLatch releaseOtherSpill = new CountDownLatch(1);
        final List<String> spillStacks = new CopyOnWriteArrayList<>();

        FailingSpillIO(String tmpDir, int failingOrdinal, int blockedOrdinal) {
            delegate = (IOManagerImpl) IOManager.create(tmpDir);
            this.failingOrdinal = failingOrdinal;
            this.blockedOrdinal = blockedOrdinal;
        }

        @Override
        public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channel) throws IOException {
            StringBuilder stack = new StringBuilder();
            for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
                stack.append(frame.getClassName()).append('.').append(frame.getMethodName()).append('\n');
            }
            spillStacks.add(stack.toString());
            int ordinal = spillCalls.incrementAndGet();
            if (ordinal == failingOrdinal) {
                failureEntered.countDown();
                while (true) {
                    try { releaseFailure.await(); break; }
                    catch (InterruptedException ignored) {
                        // 只用于 fixture 的不可中断文件打开，不让取消制造假终止。
                    }
                }
                // 故障发生在真实 CompactTask -> MergeSorter.spill -> createOutputView 调用内，
                // 由连接器自有 executor 标记；不是直接从 prepare 或测试策略伪造的异常。
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeSorter.java#L159-L173
                throw injectedFailure;
            }
            if (ordinal == blockedOrdinal) {
                otherSpillEntered.countDown();
                while (true) {
                    try { releaseOtherSpill.await(); break; }
                    catch (InterruptedException ignored) {
                        // 第二桶已失败时，第三桶仍不可把取消当成任务结束。
                    }
                }
            }
            return delegate.createBufferFileWriter(channel);
        }

        void assertRealSpills(int expectedCount) {
            assertEquals(expectedCount, spillCalls.get());
            assertEquals(expectedCount, spillStacks.size());
            for (String stack : spillStacks) {
                assertTrue(stack.contains("org.apache.paimon.mergetree.MergeSorter.spill"), stack);
                assertTrue(stack.contains("org.apache.paimon.mergetree.compact.MergeTreeCompactTask"), stack);
            }
        }

        @Override
        public BufferFileReader createBufferFileReader(FileIOChannel.ID channel) throws IOException {
            return delegate.createBufferFileReader(channel);
        }

        @Override
        public FileIOChannel.ID createChannel() { return delegate.createChannel(); }

        @Override
        public FileIOChannel.ID createChannel(String prefix) { return delegate.createChannel(prefix); }

        @Override
        public String[] tempDirs() { return delegate.tempDirs(); }

        @Override
        public FileIOChannel.Enumerator createChannelEnumerator() { return delegate.createChannelEnumerator(); }

        @Override
        public void close() throws Exception {
            closeCount.incrementAndGet();
            delegate.close();
        }
    }
}
