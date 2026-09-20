package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.commit.PaimonServiceLifecycle;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractResolver;
import io.tapdata.connector.paimon.util.PaimonSpillDirCleaner;
import io.tapdata.connector.paimon.write.PaimonCompactionLifecycle;
import io.tapdata.connector.paimon.write.PaimonNativeWriteAccess;
import io.tapdata.connector.paimon.write.PaimonStreamTableCommitter;
import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.connector.paimon.write.bucket.DefaultPaimonBucketWriterRuntimeFactory;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategyContext;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategyFactory;
import io.tapdata.entity.logger.Log;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.mock;

/** S03/S13：使用本地文件表、真实 Compaction Spill 和 Service.close 验证完整同步停止。 */
class PaimonServiceSyncStopIntegrationTest {
    private static final String TABLE_NAME = "service_sync_stop";
    private static final String TABLE_KEY = "default." + TABLE_NAME;
    private static final Identifier IDENTIFIER = Identifier.create("default", TABLE_NAME);

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void realSpillMustKeepCloseAndOwnerWaitingBeyondThirtySecondsWithInfoProgress()
            throws Exception {
        try (Fixture fixture = new Fixture(true, false)) {
            fixture.seedAndStartRealCompaction();
            long businessSnapshot = latestSnapshot(fixture.table).id();
            fixture.startClose();
            awaitStopping(fixture.service, fixture.closeReturned);
            long observationStarted = System.nanoTime();
            long observationDeadline = observationStarted + TimeUnit.SECONDS.toNanos(35L);

            // S03 刻意使用真实 35 秒，不用伪时钟把旧 30 秒 deadline 缩短成另一个短超时。
            // Spill latch 在这整个窗口始终未放行，检查的是 Service 调用者的实际返回行为。
            while (System.nanoTime() < observationDeadline) {
                long remaining = observationDeadline - System.nanoTime();
                assertFalse(fixture.closeReturned.await(
                                Math.min(TimeUnit.SECONDS.toNanos(5L), Math.max(1L, remaining)),
                                TimeUnit.NANOSECONDS),
                        "真实 Compaction 尚未退出，Service.close 不得在 30 秒或其他期限返回");
                assertTrue(fixture.closer.isAlive());
                assertNull(fixture.closeFailure.get());
                assertFalse(fixture.compaction.compactionExecutor().isTerminated());
                assertEquals(0, fixture.io.closeCount.get());
                String[] roots = fixture.spillDirectories.stream()
                        .map(dir -> new File(dir).getParent()).distinct().toArray(String[]::new);
                assertEquals(0, PaimonSpillDirCleaner.cleanupStaleSpillDirs(roots, 0L,
                        (dir, bytes) -> { throw new AssertionError("活跃 Spill 被 stale cleaner 删除：" + dir); }));
                assertSpillProtectionPresent(fixture.spillDirectories);
                assertSecondOwnerRejected(fixture.contender, fixture.table);
                assertEquals(0L, normalExitCount(fixture.infos), "等待期间不能打印正常退出");
            }
            assertTrue(System.nanoTime() - observationStarted >= TimeUnit.SECONDS.toNanos(35L));

            List<InfoEvent> progress = fixture.infos.stream()
                    .filter(event -> event.atNanos >= observationStarted)
                    .filter(event -> event.message.contains(TABLE_KEY))
                    .filter(event -> event.message.contains("phase="))
                    .filter(event -> event.message.contains("elapsedMs="))
                    .collect(Collectors.toList());
            assertTrue(progress.size() >= 5, "35 秒等待必须持续输出 INFO 进度，实际=" + progress);
            assertTrue(progress.stream().allMatch(event -> event.message.contains("owner=")));
            assertTrue(progress.stream().allMatch(event -> event.message.contains("phaseElapsedMs=")));
            long previousProgress = observationStarted;
            for (InfoEvent event : progress) {
                assertTrue(event.atNanos - previousProgress <= TimeUnit.SECONDS.toNanos(10L),
                        "每 5 秒心跳允许调度余量，但不允许长时间静默：" + progress);
                previousProgress = event.atNanos;
            }
            assertTrue(progress.get(progress.size() - 1).atNanos
                            >= observationDeadline - TimeUnit.SECONDS.toNanos(10L),
                    "长时间等待末段仍必须有 INFO，不能只在开始打印一次");

            fixture.io.releaseSpill.countDown();
            assertTrue(fixture.closeReturned.await(30L, TimeUnit.SECONDS), "放行后完整停止没有结束");
            joinAndAssertStopped(fixture.closer);
            assertNull(fixture.closeFailure.get());
            assertCompletedSnapshotAndCleanup(fixture, businessSnapshot);
            assertEquals(1L, normalExitCount(fixture.infos), "只有唯一终态发布者打印正常退出");
            assertDoesNotThrow(fixture.service::close);
            assertDoesNotThrow(fixture.service::close);
            assertEquals(1L, normalExitCount(fixture.infos), "重复 close 不得重复打印正常退出");
            assertDoesNotThrow(() -> registerOwner(fixture.contender, fixture.table),
                    "旧 Context 完整清理后，同 JVM 新代才能取得物理表 owner");
        }
    }

    @Test
    void runtimeExceptionFromInfoLoggerMustNotBreakSnapshotCommitOrResourceCleanup()
            throws Exception {
        try (Fixture fixture = new Fixture(false, true)) {
            fixture.seedAndStartRealCompaction();
            long businessSnapshot = latestSnapshot(fixture.table).id();
            fixture.startClose();
            assertTrue(fixture.closeReturned.await(30L, TimeUnit.SECONDS),
                    "日志组件 RuntimeException 不得让关闭操作停在未发布的状态");
            joinAndAssertStopped(fixture.closer);
            assertNull(fixture.closeFailure.get(), "INFO 错误不能替代真实提交或资源关闭结果");
            assertTrue(fixture.infoFailures.get() > 0, "必须真正注入 INFO 日志异常");
            assertCompletedSnapshotAndCleanup(fixture, businessSnapshot);
            assertDoesNotThrow(fixture.service::close);
            assertDoesNotThrow(() -> registerOwner(fixture.contender, fixture.table));
        }
    }

    private static void assertCompletedSnapshotAndCleanup(Fixture fixture, long businessSnapshot)
            throws Exception {
        assertTrue(fixture.compaction.compactionExecutor().isTerminated(),
                "Service 返回前必须取得真实 executor termination");
        Snapshot finalSnapshot = latestSnapshot(fixture.table);
        // Paimon 1.3.2 StreamWriteBuilderImpl.newCommit 使用 ignoreEmptyCommit(false)：
        // 仅 Compaction 的一次 commit 原生生成空 APPEND，再生成 COMPACT，identifier 相同。
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/StreamWriteBuilderImpl.java#L76
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L323-L405
        assertEquals(businessSnapshot + 2L, finalSnapshot.id());
        Snapshot emptyAppend = fixture.table.snapshotManager().snapshot(businessSnapshot + 1L);
        assertEquals(Snapshot.CommitKind.APPEND, emptyAppend.commitKind());
        assertEquals(3L, emptyAppend.commitIdentifier());
        assertEquals(emptyAppend.commitIdentifier(), finalSnapshot.commitIdentifier());
        assertEquals(emptyAppend.commitUser(), finalSnapshot.commitUser());
        assertTrue(fixture.table.manifestListReader().read(emptyAppend.deltaManifestList()).isEmpty(),
                "最终 APPEND 不能携带任何新业务 data delta");
        assertEquals(Long.valueOf(0L), emptyAppend.deltaRecordCount());
        assertEquals(Snapshot.CommitKind.COMPACT, finalSnapshot.commitKind());
        assertEquals(1, activeFiles(fixture.table));
        assertEquals(1, fixture.io.closeCount.get());
        assertEquals(4L, fixture.savedIdentifier.get(), "最终 envelope 只推进原生 commit identifier");
        assertEquals(0, fixture.offsetCallbacks.get(), "纯 Compaction 最终提交不得产生 PDK offset callback");
        for (String spillDirectory : fixture.spillDirectories) {
            File directory = new File(spillDirectory);
            assertFalse(directory.exists(), "结束后 Spill 目录必须删除：" + directory);
            assertFalse(ownerMarker(directory).exists(), "结束后 owner marker 必须删除");
        }
        assertEquals(PaimonServiceLifecycle.State.CLOSED, serviceLifecycle(fixture.service).state());
    }

    private static void assertSpillProtectionPresent(List<String> directories) {
        assertFalse(directories.isEmpty(), "fixture 必须拥有真实 Spill 目录");
        for (String spillDirectory : directories) {
            File directory = new File(spillDirectory);
            assertTrue(directory.isDirectory(), "真实任务尚未结束：" + directory);
            assertTrue(ownerMarker(directory).isFile(), "等待期间 owner marker 必须保留");
        }
    }

    private static File ownerMarker(File directory) {
        return new File(directory.getParentFile(), "." + directory.getName() + ".tapdata-owner.lock");
    }

    private static long normalExitCount(List<InfoEvent> events) {
        return events.stream().filter(event -> event.message.contains("正常退出")).count();
    }

    private static int activeFiles(FileStoreTable table) throws Exception {
        int count = 0;
        for (Split split : table.newReadBuilder().newScan().plan().splits()) {
            count += ((DataSplit) split).dataFiles().size();
        }
        return count;
    }

    private static Snapshot latestSnapshot(FileStoreTable table) {
        return table.latestSnapshot().orElseThrow(() -> new AssertionError("缺少已提交 Snapshot"));
    }

    private static PaimonConfig config() {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(100);
        config.setCommitIntervalMs(30_000);
        config.setEnableAsyncCommit(false);
        return config;
    }

    private static void awaitStopping(PaimonService service, CountDownLatch returned) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (serviceLifecycle(service).state() != PaimonServiceLifecycle.State.STOPPING
                && returned.getCount() != 0L && System.nanoTime() < deadline) {
            Thread.sleep(1L);
        }
        assertEquals(PaimonServiceLifecycle.State.STOPPING, serviceLifecycle(service).state());
    }

    private static PaimonServiceLifecycle serviceLifecycle(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("lifecycle");
        field.setAccessible(true);
        return (PaimonServiceLifecycle) field.get(service);
    }

    private static PaimonStreamTableCommitter commitAdapter(StreamTableCommit rawCommitter)
            throws Exception {
        // 保持生产构造器的包可见性；fixture 使用真实适配器，不复制提交恢复语义。
        Constructor<PaimonStreamTableCommitter> constructor =
                PaimonStreamTableCommitter.class.getDeclaredConstructor(StreamTableCommit.class);
        constructor.setAccessible(true);
        return constructor.newInstance(rawCommitter);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, PaimonTableWriteContext> contexts(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("tableWriteContexts");
        field.setAccessible(true);
        return (Map<String, PaimonTableWriteContext>) field.get(service);
    }

    private static void registerOwner(PaimonService service, FileStoreTable table) throws Exception {
        Method register = PaimonService.class.getDeclaredMethod(
                "registerPhysicalTableOwner", String.class, FileStoreTable.class);
        register.setAccessible(true);
        register.invoke(service, TABLE_KEY, table);
    }

    private static void unregisterOwner(PaimonService service) throws Exception {
        if (service != null) {
            Method unregister = PaimonService.class.getDeclaredMethod(
                    "unregisterPhysicalTableOwner", String.class);
            unregister.setAccessible(true);
            unregister.invoke(service, TABLE_KEY);
        }
    }

    private static void assertSecondOwnerRejected(PaimonService contender, FileStoreTable table) {
        InvocationTargetException rejected = assertThrows(
                InvocationTargetException.class, () -> registerOwner(contender, table));
        assertInstanceOf(IllegalStateException.class, rejected.getCause());
    }

    private static Thread closeWorker(PaimonService service) throws Exception {
        Field operationField = PaimonService.class.getDeclaredField("closeOperation");
        operationField.setAccessible(true);
        Object operation = operationField.get(service);
        if (operation == null) {
            return null;
        }
        Field workerField = operation.getClass().getDeclaredField("worker");
        workerField.setAccessible(true);
        return (Thread) workerField.get(operation);
    }

    private static void joinAndAssertStopped(Thread thread) throws InterruptedException {
        if (thread != null) {
            thread.join(TimeUnit.SECONDS.toMillis(30L));
            assertFalse(thread.isAlive(), "finally 放行后测试线程仍未结束：" + thread.getName());
        }
    }

    /** 同时接受 Log.info(String) 和 Log.info(String, Object...)，不依赖某个日志格式化实现。 */
    private static String renderedMessage(Object[] arguments) {
        if (arguments.length == 0) {
            return "";
        }
        String message = String.valueOf(arguments[0]);
        List<Object> values = new ArrayList<>();
        for (int i = 1; i < arguments.length; i++) {
            if (arguments[i] instanceof Object[]) {
                Collections.addAll(values, (Object[]) arguments[i]);
            } else {
                values.add(arguments[i]);
            }
        }
        for (Object value : values) {
            int position = message.indexOf("{}");
            if (position < 0) {
                break;
            }
            message = message.substring(0, position) + value + message.substring(position + 2);
        }
        return message;
    }

    private static final class InfoEvent {
        final long atNanos = System.nanoTime();
        final String message;

        InfoEvent(String message) { this.message = message; }

        @Override
        public String toString() { return message; }
    }

    private final class Fixture implements AutoCloseable {
        final List<InfoEvent> infos = new CopyOnWriteArrayList<>();
        final AtomicInteger infoFailures = new AtomicInteger();
        final AtomicInteger offsetCallbacks = new AtomicInteger();
        final java.util.concurrent.atomic.AtomicLong savedIdentifier = new java.util.concurrent.atomic.AtomicLong(-1L);
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        final CountDownLatch closeReturned = new CountDownLatch(1);
        Catalog catalog;
        FileStoreTable table;
        PaimonService service;
        PaimonService contender;
        PaimonCompactionLifecycle compaction;
        BlockingSpillIO io;
        TableWriteImpl<?> rawWriter;
        StreamTableCommit rawCommitter;
        PaimonTableWriteContext context;
        List<String> spillDirectories = Collections.emptyList();
        Thread closer;

        Fixture(boolean blockSpill, boolean throwInfo) throws Exception {
            try {
                String ioTmpDir = Files.createDirectory(tempDir.resolve("io")).toString();
                catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tempDir.toUri())));
                catalog.createDatabase("default", true);
                Map<String, String> options = new HashMap<>();
                options.put("bucket", "1");
                options.put("num-levels", "2");
                options.put("num-sorted-run.compaction-trigger", "100");
                options.put("sort-spill-threshold", "2");
                options.put("write-buffer-size", "1mb");
                options.put("snapshot.expire.execution-mode", "SYNC");
                catalog.createTable(IDENTIFIER,
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("value", DataTypes.STRING())
                                .primaryKey("id")
                                .options(options)
                                .build(), false);
                table = (FileStoreTable) catalog.getTable(IDENTIFIER);
                Log log = mock(Log.class, invocation -> {
                    if ("info".equals(invocation.getMethod().getName())) {
                        infos.add(new InfoEvent(renderedMessage(invocation.getArguments())));
                        if (throwInfo) {
                            infoFailures.incrementAndGet();
                            throw new IllegalStateException("S13 INFO 日志组件故障");
                        }
                    }
                    return RETURNS_DEFAULTS.answer(invocation);
                });
                service = new PaimonService(config(), log, System::currentTimeMillis, () -> { });
                service.setFlushOffsetCallback(ignored -> offsetCallbacks.incrementAndGet());
                service.startForTest();
                contender = new PaimonService(config(), mock(Log.class), System::currentTimeMillis, () -> { });
                compaction = PaimonCompactionLifecycle.forTable(TABLE_KEY);
                io = new BlockingSpillIO(ioTmpDir, blockSpill);
                String commitUser = "service-sync-stop-user";
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
                context = new PaimonTableWriteContext(
                        TABLE_KEY, TABLE_NAME, commitUser, strategy,
                        commitAdapter(rawCommitter), io, spillDirectories,
                        0L, savedIdentifier::set, compaction,
                        PaimonNativeWriteAccess.of(rawWriter));
                registerOwner(service, table);
                contexts(service).put(TABLE_KEY, context);
            } catch (Exception failure) {
                try {
                    close();
                } catch (Exception cleanupFailure) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
        }

        void seedAndStartRealCompaction() throws Exception {
            for (int version = 0; version < 3; version++) {
                context.write(GenericRow.of(1, BinaryString.fromString("v" + version)));
                context.commit();
            }
            assertEquals(3, activeFiles(table), "必须先证明三个真实且已提交的 L0 文件");
            // Paimon 1.3.2 MergeSorter 仅在读入 runs 超过阈值时真正 Spill；显式 full
            // compact 保证此 fixture 不会因为自动 compaction 尚未触发而产生假阳性。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeSorter.java#L104-L173
            rawWriter.compact(BinaryRow.EMPTY_ROW, 0, true);
            assertTrue(io.spillEntered.await(10L, TimeUnit.SECONDS), "没有进入真实 Spill");
            assertNotNull(io.spillStack.get());
            assertTrue(io.spillStack.get().contains("org.apache.paimon.mergetree.MergeSorter.spill"));
            assertTrue(io.spillStack.get().contains("org.apache.paimon.mergetree.compact.MergeTreeCompactTask"));
        }

        void startClose() {
            closer = new Thread(() -> {
                try {
                    service.close();
                } catch (Throwable failure) {
                    closeFailure.set(failure);
                } finally {
                    closeReturned.countDown();
                }
            }, "S03-real-spill-service-close");
            closer.setDaemon(true);
            closer.start();
        }

        @Override
        public void close() throws Exception {
            if (io != null) {
                io.releaseSpill.countDown();
            }
            try {
                joinAndAssertStopped(closer);
                if (service != null) {
                    joinAndAssertStopped(closeWorker(service));
                }
            } finally {
                if (compaction != null) {
                    compaction.compactionExecutor().shutdown();
                    assertTrue(compaction.compactionExecutor().awaitTermination(30L, TimeUnit.SECONDS),
                            "fixture 放行后必须证明真实 Compaction executor 终止");
                }
            }
            try {
                if (context != null) {
                    context.close();
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
                try {
                    unregisterOwner(service);
                    unregisterOwner(contender);
                } finally {
                    if (catalog != null) { catalog.close(); }
                }
            }
        }
    }

    /** 仅阻塞真实 Spill 的文件打开；目录和 channel 仍由原生 IOManagerImpl 管理。 */
    private static final class BlockingSpillIO implements IOManager {
        final IOManagerImpl delegate;
        final CountDownLatch spillEntered = new CountDownLatch(1);
        final CountDownLatch releaseSpill;
        final AtomicReference<String> spillStack = new AtomicReference<>();
        final AtomicInteger closeCount = new AtomicInteger();

        BlockingSpillIO(String tmpDir, boolean blockSpill) {
            delegate = (IOManagerImpl) IOManager.create(tmpDir);
            releaseSpill = new CountDownLatch(blockSpill ? 1 : 0);
        }

        @Override
        public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channel) throws IOException {
            StringBuilder stack = new StringBuilder();
            for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
                stack.append(frame.getClassName()).append('.').append(frame.getMethodName()).append('\n');
            }
            spillStack.compareAndSet(null, stack.toString());
            spillEntered.countDown();
            while (true) {
                try {
                    releaseSpill.await();
                    break;
                } catch (InterruptedException ignored) {
                    // 模拟文件系统不可中断阻塞；cancel 不能被当成实际执行结束。
                }
            }
            return delegate.createBufferFileWriter(channel);
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
