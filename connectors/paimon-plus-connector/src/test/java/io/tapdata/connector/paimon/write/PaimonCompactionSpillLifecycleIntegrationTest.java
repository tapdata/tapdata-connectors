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
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** 保留原生不安全关闭的 FNF 复现，并验证新协议在真实 Spill 结束后才释放目录。 */
class PaimonCompactionSpillLifecycleIntegrationTest {
    private static final String DATABASE = "default";
    private static final String TABLE = "spill_lifecycle";
    @TempDir java.nio.file.Path tempDir;
    private Catalog catalog;
    private String ioTmpDir;
    private int ownerLocksBefore;

    @BeforeEach
    void setUp() throws Exception {
        ownerLocksBefore = ownerLockCount();
        ioTmpDir = Files.createDirectory(tempDir.resolve("io")).toString();
        catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tempDir.toUri())));
        catalog.createDatabase(DATABASE, true);
        Map<String, String> options = new HashMap<>();
        options.put("bucket", "1");
        options.put("num-levels", "2");
        options.put("num-sorted-run.compaction-trigger", "100");
        options.put("sort-spill-threshold", "2");
        options.put("write-buffer-size", "1mb");
        options.put("snapshot.expire.execution-mode", "SYNC");
        catalog.createTable(Identifier.create(DATABASE, TABLE), Schema.newBuilder()
                .column("id", DataTypes.INT()).column("value", DataTypes.STRING())
                .primaryKey("id").options(options).build(), false);
    }

    @AfterEach
    void tearDown() throws Exception {
        assertAll(
                () -> { if (catalog != null) { catalog.close(); } },
                () -> assertEquals(ownerLocksBefore, ownerLockCount(),
                        "删除临时目录不能代替释放 owner 文件锁及通道"));
    }

    private static int ownerLockCount() throws ReflectiveOperationException {
        java.lang.reflect.Field field = PaimonSpillDirCleaner.class.getDeclaredField("OWNER_LOCKS");
        field.setAccessible(true);
        return ((Map<?, ?>) field.get(null)).size();
    }

    private FileStoreTable table() throws Exception {
        return (FileStoreTable) catalog.getTable(Identifier.create(DATABASE, TABLE));
    }

    private TableWriteImpl<?> writeThreeCommittedVersions(StreamWriteBuilder builder, StreamTableWrite writer)
            throws Exception {
        TableWriteImpl<?> raw = (TableWriteImpl<?>) writer;
        try (StreamTableCommit committer = builder.newCommit()) {
            for (int version = 0; version < 3; version++) {
                raw.write(GenericRow.of(1, BinaryString.fromString("v" + version)));
                committer.commit(version, raw.prepareCommit(false, version));
            }
        }
        return raw;
    }

    private static void assertThreeActiveFilesInBucketZero(FileStoreTable table) throws Exception {
        assertEquals(3, activeFileCount(table), "显式 Compaction 前必须有三个真实 L0 文件");
    }

    private static int activeFileCount(FileStoreTable table) throws Exception {
        int count = 0;
        for (Split split : table.newReadBuilder().newScan().plan().splits()) {
            count += ((DataSplit) split).dataFiles().size();
        }
        return count;
    }

    // 原生机制反例：普通 executor 只观测实际错误，不使用连接器 typedexecutor。
    // Paimon cancel 不等待任务，而 IOManager.close 递归删除目录，二者错误排序会复现 FNF。
    // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java#L34-L68
    @Test
    void rawPaimonCloseCanReturnBeforeExternalWorkerAndProduceFileNotFound() throws Exception {
        FileStoreTable fileStoreTable = table();
        CountDownLatch spillEntered = new CountDownLatch(1);
        CountDownLatch releaseSpill = new CountDownLatch(1);
        BlockingSpillIOManager ioManager =
                new BlockingSpillIOManager(ioTmpDir, spillEntered, releaseSpill);

        // Future 被取消后只能报告 CancellationException，记录真实任务的异常。
        AtomicReference<Throwable> workerOutcome = new AtomicReference<>();
        CountDownLatch workerFinished = new CountDownLatch(1);
        ExecutorService capturingExecutor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "raw-compaction-worker");
                            thread.setDaemon(true);
                            return thread;
                        });
        ExecutorService recording = new ForwardingExecutor(
                capturingExecutor, t -> workerOutcome.compareAndSet(null, t), workerFinished);
        TableWriteImpl<?> tableWrite = null;
        boolean writerClosed = false;
        try {
            StreamWriteBuilder builder =
                    fileStoreTable.newStreamWriteBuilder().withCommitUser("raw-mechanism-user");
            tableWrite = (TableWriteImpl<?>) builder.newWrite().withIOManager(ioManager);
            tableWrite.withCompactExecutor(recording);
            writeThreeCommittedVersions(builder, tableWrite);
            assertThreeActiveFilesInBucketZero(fileStoreTable);

            tableWrite.compact(tableWrite.getPartition(keyRow()), tableWrite.getBucket(keyRow()), true);
            assertTrue(spillEntered.await(10L, TimeUnit.SECONDS), "spill worker never entered");
            assertRealSpillStack(ioManager);

            // Paimon CompactFutureManager.cancelCompaction 只取消 Future，IOManagerImpl.close
            // 直接删除目录；故意保留旧关闭顺序以复现生产事故，不能用取消状态代替退出证明。
            tableWrite.close();
            writerClosed = true;
            ioManager.close();
            for (File dir : spillingDirsOf(ioManager)) {
                assertFalse(dir.exists(), "IOManager close must have deleted " + dir);
            }

            releaseSpill.countDown();
            assertTrue(workerFinished.await(30L, TimeUnit.SECONDS),
                    "spill worker did not finish after release");
            Throwable workerFailure = workerOutcome.get();
            assertTrue(workerFailure != null, "spill worker must report the deleted directory");
            Throwable root = workerFailure;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            assertEquals(
                    java.io.FileNotFoundException.class,
                    root.getClass(),
                    "expected the incident's FNF at the root, got: " + root);
        } finally {
            // 断言失败也必须放行真实 worker，先证明退出再回收测试资源。
            releaseSpill.countDown();
            capturingExecutor.shutdown();
            assertTrue(capturingExecutor.awaitTermination(30L, TimeUnit.SECONDS),
                    "raw spill worker must terminate during teardown");
            try {
                if (tableWrite != null && !writerClosed) {
                    tableWrite.close();
                }
            } finally {
                if (ioManager.closeCount() == 0) {
                    ioManager.close();
                }
            }
        }
    }

    @Test
    void finalStopMustCommitAndDeleteDirectoryOnlyAfterRealSpillTerminates() throws Exception {
        verifyRealSpillWait(true);
    }

    @Test
    void plainResourceCloseMustAlsoWaitWithoutCancellingRealSpill() throws Exception {
        verifyRealSpillWait(false);
    }

    private void verifyRealSpillWait(boolean finalizeCompaction) throws Exception {
        FileStoreTable table = table();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        BlockingSpillIOManager io = new BlockingSpillIOManager(ioTmpDir, entered, release);
        try (Fixture fixture = new Fixture(table, "wait-" + finalizeCompaction, io, release)) {
            for (int version = 0; version < 3; version++) {
                fixture.context.write(GenericRow.of(1, BinaryString.fromString("v" + version)));
                fixture.context.commit();
            }
            assertThreeActiveFilesInBucketZero(table);
            long businessSnapshot = table.latestSnapshot().orElseThrow(AssertionError::new).id();
            fixture.writer.compact(fixture.writer.getPartition(keyRow()),
                    fixture.writer.getBucket(keyRow()), true);
            assertTrue(entered.await(10L, TimeUnit.SECONDS));
            assertRealSpillStack(io);
            fixture.startClose(finalizeCompaction);
            assertTrue(fixture.closePhaseEntered.await(5L, TimeUnit.SECONDS));
            assertFalse(fixture.closeReturned.await(250L, TimeUnit.MILLISECONDS),
                    "真实 Spill latch 未放行，close 必须完整等待");
            assertFalse(fixture.context.cleanupComplete());
            assertEquals(0, io.closeCount());
            for (File directory : spillingDirsOf(io)) {
                assertTrue(directory.isDirectory());
                assertTrue(ownerMarker(directory).isFile());
            }

            release.countDown();
            assertTrue(fixture.closeReturned.await(30L, TimeUnit.SECONDS));
            joinAndAssertStopped(fixture.closer);
            assertNull(fixture.closeFailure.get());
            assertEquals(PaimonTableWriteContext.StopOutcome.SUCCESS, fixture.outcome.get());
            assertTrue(fixture.lifecycle.compactionExecutor().isTerminated());
            assertTrue(fixture.context.cleanupComplete());
            assertEquals(1, io.closeCount());

            Snapshot finalSnapshot = table.latestSnapshot().orElseThrow(AssertionError::new);
            // 原生 stream commit 保留空 APPEND，再写 COMPACT；纯资源 close 不提交结果。
            assertEquals(businessSnapshot + (finalizeCompaction ? 2L : 0L), finalSnapshot.id());
            if (finalizeCompaction) {
                assertEquals(Snapshot.CommitKind.COMPACT, finalSnapshot.commitKind());
                assertEquals(1, activeFileCount(table));
            } else {
                assertEquals(3, activeFileCount(table));
            }
        }
    }

    private static GenericRow keyRow() {
        return GenericRow.of(1, BinaryString.fromString("v"));
    }

    private static void assertRealSpillStack(BlockingSpillIOManager io) {
        String stack = io.firstSpillStack();
        assertNotNull(stack);
        assertTrue(stack.contains("org.apache.paimon.mergetree.MergeSorter.spill"), stack);
        assertTrue(stack.contains("org.apache.paimon.mergetree.compact.MergeTreeCompactTask"), stack);
    }

    private static File[] spillingDirsOf(BlockingSpillIOManager io) {
        return io.spillingDirectories();
    }

    private static File ownerMarker(File directory) {
        return new File(directory.getParentFile(), "." + directory.getName() + ".tapdata-owner.lock");
    }

    private static void joinAndAssertStopped(Thread thread) throws InterruptedException {
        if (thread != null) {
            thread.join(TimeUnit.SECONDS.toMillis(30L));
            assertFalse(thread.isAlive(), "放行真实 Spill 后关闭线程必须退出");
        }
    }

    private static final class Fixture implements AutoCloseable {
        final PaimonCompactionLifecycle lifecycle;
        final BlockingSpillIOManager io;
        final CountDownLatch release;
        final CountDownLatch closePhaseEntered = new CountDownLatch(1);
        final CountDownLatch closeReturned = new CountDownLatch(1);
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        final AtomicReference<PaimonTableWriteContext.StopOutcome> outcome = new AtomicReference<>();
        TableWriteImpl<?> writer;
        StreamTableCommit rawCommitter;
        PaimonTableWriteContext context;
        List<String> directories = Collections.emptyList();
        Thread closer;

        Fixture(FileStoreTable table, String user, BlockingSpillIOManager io, CountDownLatch release)
                throws Exception {
            this.lifecycle = PaimonCompactionLifecycle.forTable("default." + TABLE);
            this.io = io;
            this.release = release;
            try {
                StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(user);
                writer = (TableWriteImpl<?>) builder.newWrite().withIOManager(io);
                // 直接提交真实 CompactTask，不在 typedexecutor 前包装成 lambda。
                writer.withCompactExecutor(lifecycle.compactionExecutor());
                rawCommitter = builder.newCommit();
                PaimonWriteSemanticContract contract =
                        PaimonWriteSemanticContractResolver.resolve("default." + TABLE, table);
                PaimonBucketWriterStrategy strategy = PaimonBucketWriterStrategyFactory.create(
                        new PaimonBucketWriterStrategyContext(
                                "default." + TABLE, table, writer, user, io, contract),
                        DefaultPaimonBucketWriterRuntimeFactory.INSTANCE);
                directories = PaimonSpillDirCleaner.registerLiveDirs(io.delegate());
                context = new PaimonTableWriteContext("default." + TABLE, TABLE, user, strategy,
                        new PaimonStreamTableCommitter(rawCommitter), io, directories, 0L,
                        PaimonTableWriteContext.CommitStateStore.NOOP, lifecycle,
                        PaimonNativeWriteAccess.of(writer));
            } catch (Exception failure) {
                try { close(); }
                catch (Exception cleanupFailure) { failure.addSuppressed(cleanupFailure); }
                throw failure;
            }
        }

        void startClose(boolean finalizeCompaction) {
            closer = new Thread(() -> {
                try {
                    outcome.set(context.closeForStop(finalizeCompaction, phase -> closePhaseEntered.countDown()));
                } catch (Throwable failure) {
                    closeFailure.set(failure);
                } finally {
                    closeReturned.countDown();
                }
            }, "real-spill-complete-close");
            closer.setDaemon(true);
            closer.start();
        }

        @Override
        public void close() throws Exception {
            release.countDown();
            try {
                joinAndAssertStopped(closer);
                if (context != null) { context.close(); }
            } finally {
                lifecycle.compactionExecutor().shutdown();
                assertTrue(lifecycle.compactionExecutor().awaitTermination(30L, TimeUnit.SECONDS));
                if (context == null) {
                    try {
                        if (writer != null) { writer.close(); }
                    } finally {
                        try {
                            if (rawCommitter != null) { rawCommitter.close(); }
                        } finally {
                            try { io.close(); }
                            finally { PaimonSpillDirCleaner.releaseAfterClose(directories, true); }
                        }
                    }
                }
            }
            if (context == null) { return; }
            assertTrue(context.cleanupComplete());
            assertEquals(1, io.closeCount(), "最终收尾只能关闭 IOManager 一次");
            for (File directory : spillingDirsOf(io)) {
                assertFalse(directory.exists());
                assertFalse(ownerMarker(directory).exists());
            }
        }
    }

    /** 仅用于原生反例，普通 executor 记录实际任务结果，不适配连接器任务类型。 */
    private static final class ForwardingExecutor extends AbstractExecutorService {
        private final ExecutorService delegate;
        private final java.util.function.Consumer<Throwable> outcomeSink;
        private final CountDownLatch taskFinished;

        ForwardingExecutor(ExecutorService delegate, java.util.function.Consumer<Throwable> outcomeSink,
                CountDownLatch taskFinished) {
            this.delegate = delegate;
            this.outcomeSink = outcomeSink;
            this.taskFinished = taskFinished;
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return delegate.submit(() -> {
                try { return task.call(); }
                catch (Exception | Error failure) {
                    outcomeSink.accept(failure);
                    throw failure;
                } finally {
                    taskFinished.countDown();
                }
            });
        }

        @Override public void execute(Runnable command) { delegate.execute(command); }
        @Override public void shutdown() { delegate.shutdown(); }
        @Override public List<Runnable> shutdownNow() { return delegate.shutdownNow(); }
        @Override public boolean isShutdown() { return delegate.isShutdown(); }
        @Override public boolean isTerminated() { return delegate.isTerminated(); }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }
    }
}
