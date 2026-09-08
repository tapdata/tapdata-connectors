package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.commit.PaimonServiceLifecycle;
import io.tapdata.connector.paimon.config.PaimonConfig;
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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.operation.PartitionExpire;
import org.apache.paimon.partition.PartitionValuesTimeExpireStrategy;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

/** S12：真实同步维护仍可发布 OVERWRITE，必须留在 Service 的关闭与 owner 生命周期内。 */
class PaimonServiceNativeMaintenanceIntegrationTest {
    private static final String TABLE_NAME = "native_sync_maintenance";
    private static final String TABLE_KEY = "default." + TABLE_NAME;
    private static final String COMMIT_USER = "native-sync-maintenance-user";
    private static final String EXPIRED_PARTITION = "2000-01-01";
    private static final String LIVE_PARTITION = "2999-01-01";
    private static final Identifier IDENTIFIER = Identifier.create("default", TABLE_NAME);

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void synchronousPartitionOverwriteMustFinishBeforeCloseReleasesSpillAndPhysicalOwner()
            throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            fixture.startClose();
            assertTrue(fixture.dropEntered.await(15L, TimeUnit.SECONDS),
                    "必须进入真实 PartitionExpire 的 dropPartitions 接缝");
            String stack = fixture.maintenanceStack.get();
            assertNotNull(stack);
            assertTrue(stack.contains("org.apache.paimon.operation.PartitionExpire.doBatchExpire"), stack);
            assertTrue(stack.contains("org.apache.paimon.table.sink.TableCommitImpl.maintain"), stack);
            assertTrue(stack.contains("io.tapdata.connector.paimon.service.PaimonService"), stack);
            assertEquals(PaimonServiceLifecycle.State.STOPPING, lifecycle(fixture.service).state());
            assertFalse(fixture.closeReturned.await(200L, TimeUnit.MILLISECONDS),
                    "SYNC 维护仍能提交 OVERWRITE 时，Service.close 必须仍在等待");
            assertNull(fixture.closeFailure.get());
            assertFalse(fixture.context.cleanupComplete());
            assertEquals(0L, fixture.normalExitCount());
            assertSpillPresent(fixture.spillDirectories);
            assertOwnerRejected(fixture.contender, fixture.table);

            Snapshot beforeOverwrite = fixture.table.latestSnapshot().orElseThrow(AssertionError::new);
            assertEquals(Snapshot.CommitKind.COMPACT, beforeOverwrite.commitKind());
            assertEquals(3L, beforeOverwrite.commitIdentifier());
            assertEquals(Arrays.asList(EXPIRED_PARTITION + ":v2", LIVE_PARTITION + ":v2"),
                    readRows(fixture.table), "过期维护尚未放行时，两份已确认业务仍然可读");

            fixture.releaseDrop.countDown();
            fixture.awaitClose();

            Snapshot afterOverwrite = fixture.table.latestSnapshot().orElseThrow(AssertionError::new);
            assertEquals(beforeOverwrite.id() + 1L, afterOverwrite.id());
            assertEquals(Snapshot.CommitKind.OVERWRITE, afterOverwrite.commitKind());
            assertEquals(beforeOverwrite.commitIdentifier(), afterOverwrite.commitIdentifier());
            assertEquals(COMMIT_USER, afterOverwrite.commitUser());
            assertEquals(Collections.singletonList(LIVE_PARTITION + ":v2"), readRows(fixture.table),
                    "真实过期策略仅移除旧分区，保留未过期分区的最新业务值");
            assertEquals(1, fixture.dropCalls.get());
            fixture.assertSafeNormalCompletion();
        }
    }

    @Test
    void lastNativeMaintenanceErrorMustKeepNativeHiddenErrorAndCloseSemantics()
            throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.startClose();
            fixture.awaitClose();

            // 原生 SYNC 的 Runnable 自身 catch(Throwable)，仅记录 maintainError；最后一次
            // 调用成功返回后 close 不读取该私有状态。Connector 不得额外探测或重跑维护。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java#L348-L400
            assertEquals(1, fixture.hiddenErrorCalls.get(), "必须实际执行一次原生维护故障注入");
            assertEquals(0, fixture.dropCalls.get(), "快照维护失败后原生不会继续执行分区过期");
            Snapshot last = fixture.table.latestSnapshot().orElseThrow(AssertionError::new);
            assertEquals(Snapshot.CommitKind.COMPACT, last.commitKind());
            assertEquals(3L, last.commitIdentifier());
            assertEquals(Arrays.asList(EXPIRED_PARTITION + ":v2", LIVE_PARTITION + ":v2"),
                    readRows(fixture.table));
            fixture.assertSafeNormalCompletion();
        }
    }

    private final class Fixture implements AutoCloseable {
        final CountDownLatch dropEntered = new CountDownLatch(1);
        final CountDownLatch releaseDrop = new CountDownLatch(1);
        final CountDownLatch closeReturned = new CountDownLatch(1);
        final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        final AtomicReference<String> maintenanceStack = new AtomicReference<>();
        final AtomicInteger dropCalls = new AtomicInteger();
        final AtomicInteger hiddenErrorCalls = new AtomicInteger();
        final AtomicInteger callbacks = new AtomicInteger();
        final AtomicLong savedIdentifier = new AtomicLong(-1L);
        final List<String> infoMessages = new CopyOnWriteArrayList<>();
        Catalog catalog;
        FileStoreTable table;
        PaimonService service;
        PaimonService contender;
        PaimonCompactionLifecycle compaction;
        IOManagerImpl io;
        TableWriteImpl<?> writer;
        TableCommitImpl nativeCommitter;
        FileStoreCommit partitionCommit;
        PaimonTableWriteContext context;
        List<String> spillDirectories = Collections.emptyList();
        Thread closer;

        Fixture(boolean failLastMaintenance) throws Exception {
            try {
                catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(tempDir.toUri())));
                catalog.createDatabase("default", true);
                Map<String, String> options = new HashMap<>();
                options.put("bucket", "1");
                options.put("num-levels", "2");
                options.put("num-sorted-run.compaction-trigger", "100");
                options.put("snapshot.expire.execution-mode", "SYNC");
                options.put("partition.expiration-time", "1 d");
                options.put("partition.expiration-check-interval", "0 ms");
                catalog.createTable(IDENTIFIER, Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id", "dt")
                        .partitionKeys("dt")
                        .options(options).build(), false);
                table = (FileStoreTable) catalog.getTable(IDENTIFIER);
                io = new IOManagerImpl(Files.createDirectory(tempDir.resolve("io")).toString());
                spillDirectories = PaimonSpillDirCleaner.registerLiveDirs(io);
                compaction = PaimonCompactionLifecycle.forTable(TABLE_KEY);
                writer = (TableWriteImpl<?>) table.newStreamWriteBuilder()
                        .withCommitUser(COMMIT_USER).newWrite().withIOManager(io);
                writer.withCompactExecutor(compaction.compactionExecutor());
                PaimonBucketWriterStrategy strategy = PaimonBucketWriterStrategyFactory.create(
                        new PaimonBucketWriterStrategyContext(TABLE_KEY, table, writer, COMMIT_USER,
                                io, PaimonWriteSemanticContractResolver.resolve(TABLE_KEY, table)),
                        DefaultPaimonBucketWriterRuntimeFactory.INSTANCE);

                // 预置已确认文件时暂不运行维护，否则历史分区会在测试的停止阶段前被删除。
                // writer / FileStoreCommit 都是真实实现；只有种子 committer 的维护参数为 null。
                try (TableCommitImpl seed = newNativeCommitter(null, null)) {
                    for (long identifier = 0L; identifier < 3L; identifier++) {
                        strategy.write(GenericRow.of(1, BinaryString.fromString(EXPIRED_PARTITION),
                                BinaryString.fromString("v" + identifier)));
                        strategy.write(GenericRow.of(1, BinaryString.fromString(LIVE_PARTITION),
                                BinaryString.fromString("v" + identifier)));
                        seed.commit(identifier, strategy.prepareCommit(identifier));
                    }
                }
                assertEquals(2, readRows(table).size());

                // 对真实 FileStoreCommit 只加闩锁，不替换过期决策、manifest 或提交实现。
                // 该接缝位于 native PartitionExpire -> dropPartitions，放行后 callRealMethod
                // 会实际发布 OVERWRITE，覆盖“维护任务已经具有提交能力”的危险窗口。
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/PartitionExpire.java#L170-L182
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L601-L637
                partitionCommit = spy(table.store().newCommit(COMMIT_USER, table));
                doAnswer(invocation -> {
                    dropCalls.incrementAndGet();
                    maintenanceStack.set(Arrays.toString(Thread.currentThread().getStackTrace()));
                    dropEntered.countDown();
                    if (!releaseDrop.await(30L, TimeUnit.SECONDS)) {
                        throw new AssertionError("测试未在 finally 之前放行真实分区过期");
                    }
                    return invocation.callRealMethod();
                }).when(partitionCommit).dropPartitions(anyList(), anyLong());
                PartitionExpire partitionExpire = new PartitionExpire(Duration.ofDays(1L),
                        Duration.ZERO,
                        new PartitionValuesTimeExpireStrategy(table.coreOptions(), table.store().partitionType()),
                        table.store().newScan(), partitionCommit, null, false, 100, 100);
                Runnable lastMaintenance = failLastMaintenance ? () -> {
                    hiddenErrorCalls.incrementAndGet();
                    throw new AssertionError("S12 原生最后一次维护隐藏的故障");
                } : null;
                nativeCommitter = newNativeCommitter(lastMaintenance, partitionExpire);
                context = new PaimonTableWriteContext(TABLE_KEY, TABLE_NAME, COMMIT_USER, strategy,
                        commitAdapter(nativeCommitter), io, spillDirectories, 3L, savedIdentifier::set,
                        compaction, PaimonNativeWriteAccess.of(writer));

                Log log = mock(Log.class, invocation -> {
                    if ("info".equals(invocation.getMethod().getName())) {
                        infoMessages.add(renderedMessage(invocation.getArguments()));
                    }
                    return RETURNS_DEFAULTS.answer(invocation);
                });
                service = new PaimonService(config(), log, System::currentTimeMillis, () -> { });
                service.setFlushOffsetCallback(ignored -> callbacks.incrementAndGet());
                service.startForTest();
                contender = new PaimonService(config(), mock(Log.class), System::currentTimeMillis, () -> { });
                registerOwner(service, table);
                contexts(service).put(TABLE_KEY, context);

                // 显式触发当前分区的原生 Compaction，使 clean STOP 的 final prepare 确实
                // 有纯 Compaction envelope，从而进入真实 TableCommitImpl.maintain。
                BinaryRow livePartition = null;
                for (Split split : table.newReadBuilder().newScan().plan().splits()) {
                    BinaryRow partition = ((DataSplit) split).partition();
                    if (LIVE_PARTITION.equals(partition.getString(0).toString())) {
                        livePartition = partition;
                        break;
                    }
                }
                assertNotNull(livePartition);
                writer.compact(livePartition, 0, true);
            } catch (Exception | Error failure) {
                try { close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        }

        private TableCommitImpl newNativeCommitter(Runnable expireSnapshots, PartitionExpire partitionExpire) {
            // 1.3.2 的 SYNC 使用 direct executor，维护在 commit 调用栈中完成。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java#L94-L134
            return new TableCommitImpl(table.store().newCommit(COMMIT_USER, table), expireSnapshots,
                    partitionExpire, null, null, new ConsumerManager(table.fileIO(), table.location()),
                    CoreOptions.ExpireExecutionMode.SYNC, TABLE_KEY, false, 1).ignoreEmptyCommit(false);
        }

        void startClose() {
            closer = new Thread(() -> {
                try { service.close(); } catch (Throwable failure) { closeFailure.set(failure); }
                finally { closeReturned.countDown(); }
            }, "S12-native-sync-maintenance-close");
            closer.setDaemon(true);
            closer.start();
        }

        void awaitClose() throws Exception {
            assertTrue(closeReturned.await(30L, TimeUnit.SECONDS), "真实维护放行后 Service 未结束关闭");
            join(closer);
            assertNull(closeFailure.get());
        }

        long normalExitCount() {
            return infoMessages.stream().filter(message -> message.contains("正常退出")).count();
        }

        void assertSafeNormalCompletion() throws Exception {
            assertTrue(compaction.compactionExecutor().isTerminated());
            assertTrue(nativeCommitter.getMaintainExecutor().isTerminated());
            assertTrue(context.cleanupComplete());
            assertEquals(PaimonServiceLifecycle.State.CLOSED, lifecycle(service).state());
            assertEquals(4L, savedIdentifier.get());
            assertEquals(0, callbacks.get(), "仅最终 Compaction 不得产生业务 offset callback");
            assertEquals(1L, normalExitCount());
            for (String directory : spillDirectories) {
                assertFalse(new File(directory).exists());
                assertFalse(ownerMarker(new File(directory)).exists());
            }
            assertDoesNotThrow(service::close);
            assertEquals(1L, normalExitCount());
            assertDoesNotThrow(() -> registerOwner(contender, table));
        }

        @Override
        public void close() throws Exception {
            releaseDrop.countDown();
            try {
                join(closer);
                if (service != null) { join(closeWorker(service)); }
                if (compaction != null) {
                    compaction.compactionExecutor().shutdown();
                    assertTrue(compaction.compactionExecutor().awaitTermination(30L, TimeUnit.SECONDS));
                }
                if (context != null) {
                    context.close();
                } else {
                    try { if (writer != null) { writer.close(); } }
                    finally {
                        try { if (nativeCommitter != null) { nativeCommitter.close(); } }
                        finally {
                            if (io != null) { io.close(); }
                            PaimonSpillDirCleaner.releaseAfterClose(spillDirectories, true);
                        }
                    }
                }
            } finally {
                try { if (partitionCommit != null) { partitionCommit.close(); } }
                finally {
                    try { unregisterOwner(service); unregisterOwner(contender); }
                    finally { if (catalog != null) { catalog.close(); } }
                }
            }
        }
    }

    private static List<String> readRows(FileStoreTable table) throws Exception {
        List<String> rows = new ArrayList<>();
        ReadBuilder builder = table.newReadBuilder();
        try (RecordReader<InternalRow> reader = builder.newRead().createReader(builder.newScan().plan())) {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                try {
                    InternalRow row;
                    while ((row = batch.next()) != null) {
                        rows.add(row.getString(1) + ":" + row.getString(2));
                    }
                } finally { batch.releaseBatch(); }
            }
        }
        Collections.sort(rows);
        return rows;
    }

    private static PaimonConfig config() {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(100);
        config.setCommitIntervalMs(30_000);
        config.setEnableAsyncCommit(false);
        return config;
    }

    private static void assertSpillPresent(List<String> paths) {
        assertFalse(paths.isEmpty());
        for (String path : paths) {
            File directory = new File(path);
            assertTrue(directory.isDirectory());
            assertTrue(ownerMarker(directory).isFile());
        }
    }

    private static File ownerMarker(File directory) {
        return new File(directory.getParentFile(), "." + directory.getName() + ".tapdata-owner.lock");
    }

    private static PaimonStreamTableCommitter commitAdapter(StreamTableCommit committer) throws Exception {
        Constructor<PaimonStreamTableCommitter> constructor =
                PaimonStreamTableCommitter.class.getDeclaredConstructor(StreamTableCommit.class);
        constructor.setAccessible(true);
        return constructor.newInstance(committer);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, PaimonTableWriteContext> contexts(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("tableWriteContexts");
        field.setAccessible(true);
        return (Map<String, PaimonTableWriteContext>) field.get(service);
    }

    private static PaimonServiceLifecycle lifecycle(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("lifecycle");
        field.setAccessible(true);
        return (PaimonServiceLifecycle) field.get(service);
    }

    private static void registerOwner(PaimonService service, FileStoreTable table) throws Exception {
        Method method = PaimonService.class.getDeclaredMethod(
                "registerPhysicalTableOwner", String.class, FileStoreTable.class);
        method.setAccessible(true);
        method.invoke(service, TABLE_KEY, table);
    }

    private static void unregisterOwner(PaimonService service) throws Exception {
        if (service == null) { return; }
        Method method = PaimonService.class.getDeclaredMethod("unregisterPhysicalTableOwner", String.class);
        method.setAccessible(true);
        method.invoke(service, TABLE_KEY);
    }

    private static void assertOwnerRejected(PaimonService service, FileStoreTable table) {
        InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                () -> registerOwner(service, table));
        assertInstanceOf(IllegalStateException.class, failure.getCause());
    }

    private static Thread closeWorker(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("closeOperation");
        field.setAccessible(true);
        Object operation = field.get(service);
        if (operation == null) { return null; }
        Field worker = operation.getClass().getDeclaredField("worker");
        worker.setAccessible(true);
        return (Thread) worker.get(operation);
    }

    private static void join(Thread thread) throws InterruptedException {
        if (thread == null) { return; }
        thread.join(TimeUnit.SECONDS.toMillis(30L));
        assertFalse(thread.isAlive(), "finally 放行后测试线程仍存活：" + thread.getName());
    }

    private static String renderedMessage(Object[] arguments) {
        if (arguments.length == 0) { return ""; }
        String message = String.valueOf(arguments[0]);
        List<Object> values = new ArrayList<>();
        for (int i = 1; i < arguments.length; i++) {
            if (arguments[i] instanceof Object[]) { Collections.addAll(values, (Object[]) arguments[i]); }
            else { values.add(arguments[i]); }
        }
        for (Object value : values) {
            int position = message.indexOf("{}");
            if (position < 0) { break; }
            message = message.substring(0, position) + value + message.substring(position + 2);
        }
        return message;
    }
}
