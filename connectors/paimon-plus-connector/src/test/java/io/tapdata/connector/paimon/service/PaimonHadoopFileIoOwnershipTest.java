package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.fs.PaimonS3AFileSystem;
import io.tapdata.entity.logger.Log;
import com.sun.net.httpserver.HttpServer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.ClientNamenodeProtocolServerSideTranslatorPB;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.hadoop.HadoopFileIO;
import org.apache.paimon.fs.hadoop.HadoopSecuredFileSystem;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atLeast;

class PaimonHadoopFileIoOwnershipTest {

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void s3AdapterMustPreserveCustomImplementationAndCachePolicy() throws Exception {
        PaimonConfig config = s3Config();
        config.setS3Properties(new ArrayList<>());
        Configuration defaults = hadoopConfiguration(config);
        assertTrue(defaults.getBoolean("fs.s3a.impl.disable.cache", false));
        assertEquals(PaimonS3AFileSystem.class, FileSystem.getFileSystemClass("s3a", defaults));

        config.getS3Properties().add(property("fs.s3a.impl.disable.cache", "false"));
        config.getS3Properties().add(property("fs.s3a.impl", CustomS3AFileSystem.class.getName()));
        Configuration custom = hadoopConfiguration(config);
        assertFalse(custom.getBoolean("fs.s3a.impl.disable.cache", true));
        assertEquals(CustomS3AFileSystem.class, FileSystem.getFileSystemClass("s3a", custom));
    }

    @Test
    @SuppressWarnings("removal")
    void deserializedFileIoMustProtectLazyFilesystemInitialization() throws Exception {
        PaimonConfig config = s3Config();
        HadoopFileIO original = new HadoopFileIO();
        original.configure(CatalogContext.create(new Options(), hadoopConfiguration(config)));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(original);
        }
        HadoopFileIO restored;
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (HadoopFileIO) input.readObject();
        }
        ThreadGroup task = new ThreadGroup("paimon-deserialized-fileio-task");
        S3AFileSystem fs = null;
        try {
            inTaskGroup(task, () -> assertTrue(restored.exists(new Path(config.getFullWarehousePath()))));
            fs = (S3AFileSystem) hadoopFileSystem(restored);
            assertTrue(fs instanceof PaimonS3AFileSystem);
            task.destroy();
            ThreadPoolExecutor pool = s3Pool(fs);
            assertNotSame(task, pool.submit(
                    () -> Thread.currentThread().getThreadGroup()).get(5L, TimeUnit.SECONDS));
        } finally {
            closeTestResources(restored, original, fs, () -> destroyTaskGroup(task));
        }
    }

    public static final class CustomS3AFileSystem extends S3AFileSystem {}

    private static Configuration hadoopConfiguration(PaimonConfig config) throws Exception {
        Method method = PaimonService.class.getDeclaredMethod("buildHadoopConfiguration");
        method.setAccessible(true);
        return (Configuration) method.invoke(new PaimonService(config, mock(Log.class)));
    }

    /** 真实 DFSClient/RPC + 本地 NameNode 协议桩；覆盖元数据 IO，不冒充 DataNode 数据读写。 */
    @Test
    @SuppressWarnings("removal")
    void serviceRestartMustReuseHdfsAfterOldTaskGroupWasDestroyed() throws Exception {
        Configuration serverConfig = new Configuration();
        RPC.setProtocolEngine(serverConfig, ClientNamenodeProtocolPB.class, ProtobufRpcEngine2.class);
        ClientProtocol namenode = mock(ClientProtocol.class);
        HdfsFileStatus root = new HdfsFileStatus.Builder()
                .isdir(true).perm(FsPermission.getDirDefault()).owner("test").group("test")
                .path(new byte[0]).fileId(1L).children(0).build();
        when(namenode.getFileInfo("/")).thenReturn(root);
        RPC.Server server = new RPC.Builder(serverConfig)
                .setProtocol(ClientNamenodeProtocolPB.class)
                .setInstance(ClientNamenodeProtocolProtos.ClientNamenodeProtocol.newReflectiveBlockingService(
                        new ClientNamenodeProtocolServerSideTranslatorPB(namenode)))
                .setBindAddress("127.0.0.1").setPort(0).setNumHandlers(1).build();
        server.start();
        PaimonConfig config = new PaimonConfig();
        config.setStorageType("hdfs");
        config.setHdfsHost("127.0.0.1");
        config.setHdfsPort(server.getListenerAddress().getPort());
        config.setWarehouse("hdfs://127.0.0.1:" + server.getListenerAddress().getPort() + "/");
        config.setDiskTmpDir(tempDir.toString());
        PaimonService first = new PaimonService(config, mock(Log.class));
        PaimonService second = new PaimonService(config, mock(Log.class));
        ThreadGroup oldTask = new ThreadGroup("paimon-old-hdfs-task");
        ThreadGroup newTask = new ThreadGroup("paimon-new-hdfs-task");
        DistributedFileSystem shared = null;
        try {
            inTaskGroup(oldTask, first::init);
            shared = (DistributedFileSystem) hadoopFileSystem(first);
            assertEquals(DistributedFileSystem.class, shared.getClass());
            inTaskGroup(oldTask, first::close);
            destroyTaskGroup(oldTask);
            assertTrue(oldTask.isDestroyed());

            inTaskGroup(newTask, second::init);
            assertSame(shared, hadoopFileSystem(second));
            DistributedFileSystem active = shared;
            inTaskGroup(newTask, () -> assertTrue(
                    active.getFileStatus(new org.apache.hadoop.fs.Path("/")).isDirectory()));
            verify(namenode, atLeast(3)).getFileInfo("/");
            inTaskGroup(newTask, second::close);
        } finally {
            closeTestResources(first, second, shared, server::stop,
                    () -> destroyTaskGroup(oldTask), () -> destroyTaskGroup(newTask));
        }
    }

    /**
     * 使用真实 Service 和真实 S3A，localhost HTTP 桩提供权限表探测和一个只读对象。
     * 公开 openFile 路径必须在线程池空闲退出后重新创建 worker，并成功读取对象内容。
     */
    @Test
    @SuppressWarnings("removal")
    void serviceRestartMustReuseS3AAfterOldTaskGroupWasDestroyed() throws Exception {
        PaimonConfig config = s3Config();
        byte[] content = "s3a-restart-fixture".getBytes(StandardCharsets.UTF_8);
        HttpServer server = s3Server(content);
        config.setS3Endpoint("http://127.0.0.1:" + server.getAddress().getPort());
        PaimonService first = new PaimonService(config, mock(Log.class));
        PaimonService second = new PaimonService(config, mock(Log.class));
        ThreadGroup oldTask = new ThreadGroup("paimon-old-s3a-task");
        ThreadGroup newTask = new ThreadGroup("paimon-new-s3a-task");
        S3AFileSystem shared = null;
        try {
            inTaskGroup(oldTask, first::init);
            shared = (S3AFileSystem) hadoopFileSystem(first);
            assertTrue(shared instanceof PaimonS3AFileSystem);
            ThreadPoolExecutor pool = s3Pool(shared);
            assertNotSame(oldTask, pool.submit(
                    () -> Thread.currentThread().getThreadGroup()).get(5L, TimeUnit.SECONDS));
            awaitIdlePool(pool);

            inTaskGroup(oldTask, first::close);
            assertFalse(pool.isShutdown(), "单个 Service 不拥有共享 S3A 的关闭权");
            oldTask.destroy();
            assertTrue(oldTask.isDestroyed());

            inTaskGroup(newTask, second::init);
            assertSame(shared, hadoopFileSystem(second), "缓存打开时，新代必须沿用原 Hadoop FS 身份");
            assertTrue(shared.exists(new org.apache.hadoop.fs.Path(config.getFullWarehousePath())));
            S3AFileSystem active = shared;
            try (org.apache.hadoop.fs.FSDataInputStream input = active.openFile(
                    new org.apache.hadoop.fs.Path(config.getFullWarehousePath() + "fixture"))
                    .build().get(5L, TimeUnit.SECONDS)) {
                byte[] actual = new byte[content.length];
                input.readFully(actual);
                org.junit.jupiter.api.Assertions.assertArrayEquals(content, actual);
            }
            assertNotSame(newTask, pool.submit(
                    () -> Thread.currentThread().getThreadGroup()).get(5L, TimeUnit.SECONDS));
            inTaskGroup(newTask, second::close);
            assertFalse(pool.isShutdown());
        } finally {
            // 测试创建的唯一 bucket/cache identity 在所有 Service 退出后由测试回收。
            closeTestResources(first, second, shared, () -> server.stop(0),
                    () -> destroyTaskGroup(oldTask), () -> destroyTaskGroup(newTask));
        }
    }

    private static HttpServer s3Server(byte[] fixture) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                String method = exchange.getRequestMethod();
                String path = exchange.getRequestURI().getPath();
                if (path.endsWith("/fixture")) {
                    exchange.getResponseHeaders().set("Content-Length", String.valueOf(fixture.length));
                    exchange.getResponseHeaders().set("ETag", "\"fixture-etag\"");
                    exchange.sendResponseHeaders(200, "HEAD".equals(method) ? -1 : fixture.length);
                    if (!"HEAD".equals(method)) {
                        exchange.getResponseBody().write(fixture);
                    }
                } else if ("GET".equals(method)) {
                    byte[] listing = ("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
                            + "<Name>test</Name><Prefix></Prefix><KeyCount>0</KeyCount>"
                            + "<MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>")
                            .getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/xml");
                    exchange.sendResponseHeaders(200, listing.length);
                    exchange.getResponseBody().write(listing);
                } else {
                    exchange.sendResponseHeaders(404, -1);
                }
            } finally {
                exchange.close();
            }
        });
        server.start();
        return server;
    }

    private PaimonConfig s3Config() {
        PaimonConfig config = new PaimonConfig();
        config.setStorageType("s3");
        config.setWarehouse("s3a://paimon-ownership-" + UUID.randomUUID() + "/");
        config.setS3Endpoint("http://127.0.0.1:9");
        config.setS3Region("us-east-1");
        config.setS3AccessKey("local-test");
        config.setS3SecretKey("local-test");
        config.setDiskTmpDir(tempDir.toString());
        List<LinkedHashMap<String, String>> properties = new ArrayList<>();
        properties.add(property("fs.s3a.impl.disable.cache", "false"));
        properties.add(property("fs.s3a.bucket.probe", "0"));
        properties.add(property("fs.s3a.threads.keepalivetime", "1"));
        properties.add(property("fs.s3a.endpoint.region", "us-east-1"));
        config.setS3Properties(properties);
        return config;
    }

    private static LinkedHashMap<String, String> property(String key, String value) {
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        result.put("propKey", key);
        result.put("propValue", value);
        return result;
    }

    private static FileSystem hadoopFileSystem(PaimonService service) throws Exception {
        Field catalogField = PaimonService.class.getDeclaredField("catalog");
        catalogField.setAccessible(true);
        Catalog catalog = (Catalog) catalogField.get(service);
        HadoopFileIO fileIO = (HadoopFileIO) ((AbstractCatalog) DelegateCatalog.rootCatalog(catalog)).fileIO();
        return hadoopFileSystem(fileIO);
    }

    private static FileSystem hadoopFileSystem(HadoopFileIO fileIO) throws Exception {
        Field cacheField = HadoopFileIO.class.getDeclaredField("fsMap");
        cacheField.setAccessible(true);
        Map<?, ?> cache = (Map<?, ?>) cacheField.get(fileIO);
        assertEquals(1, cache.size());
        FileSystem fs = (FileSystem) cache.values().iterator().next();
        if (fs instanceof HadoopSecuredFileSystem) {
            Field delegate = HadoopSecuredFileSystem.class.getDeclaredField("fileSystem");
            delegate.setAccessible(true);
            fs = (FileSystem) delegate.get(fs);
        }
        return fs;
    }

    private static ThreadPoolExecutor s3Pool(S3AFileSystem fs) throws Exception {
        Field field = S3AFileSystem.class.getDeclaredField("unboundedThreadPool");
        field.setAccessible(true);
        return (ThreadPoolExecutor) field.get(fs);
    }

    private static void awaitIdlePool(ThreadPoolExecutor pool) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (pool.getPoolSize() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(0, pool.getPoolSize(), "后续操作必须重新创建池线程，不能复用旧 worker 掩盖问题");
    }

    private static void inTaskGroup(ThreadGroup group, ThrowingRunnable action) throws Exception {
        FutureTask<Void> task = new FutureTask<>(() -> {
            action.run();
            return null;
        });
        Thread worker = new Thread(group, task, "paimon-task-lifecycle-test");
        worker.setDaemon(true);
        worker.start();
        try {
            task.get(10L, TimeUnit.SECONDS);
        } finally {
            if (!task.isDone()) {
                worker.interrupt();
            }
            worker.join(10_000L);
        }
        assertFalse(worker.isAlive());
    }

    @SuppressWarnings("removal")
    private static void destroyTaskGroup(ThreadGroup group) throws Exception {
        if (group.isDestroyed()) {
            return;
        }
        // 模拟宿主销毁任务组。HDFS 首次 RPC 的 Connection 线程是懒创建的，允许其响应中断退出。
        group.interrupt();
        Thread[] threads = new Thread[Math.max(16, group.activeCount() * 2 + 1)];
        int count = group.enumerate(threads, true);
        for (int index = 0; index < count; index++) {
            threads[index].join(5_000L);
            assertFalse(threads[index].isAlive(), "Task group still owns " + threads[index].getName());
        }
        group.destroy();
    }

    private static void closeTestResources(AutoCloseable... resources) throws Exception {
        Throwable failure = null;
        for (AutoCloseable resource : resources) {
            if (resource == null) {
                continue;
            }
            try {
                resource.close();
            } catch (Throwable error) {
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        if (failure != null) {
            throw (Exception) failure;
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * 本地表的基础回归：直接关闭 A 的 Context/Catalog 后，B 仍可读写。
     * Service 与 Hadoop 共享客户端、任务线程组的协议由上面的 S3A/HDFS 用例覆盖。
     */
    @Test
    void closingOneOwnerMustNotAffectTheOtherOwnersFileIo() throws Exception {
        Path warehouse = new Path(tempDir.toUri());
        Catalog first = CatalogFactory.createCatalog(CatalogContext.create(warehouse));
        Catalog second = CatalogFactory.createCatalog(CatalogContext.create(warehouse));
        try {
            first.createDatabase("default", true);
            first.createTable(
                    Identifier.create("default", "shared"),
                    Schema.newBuilder()
                            .column("id", DataTypes.INT())
                            .column("value", DataTypes.STRING())
                            .primaryKey("id")
                            .build(),
                    false);

            // Owner A writes and closes completely (its catalog close included).
            Table tableA = first.getTable(Identifier.create("default", "shared"));
            try (PaimonTableWriteContext contextA =
                    PaimonTableWriteContext.create(
                            "default.shared", "shared", tableA, "owner-a", null)) {
                contextA.write(GenericRow.of(1, BinaryString.fromString("a")));
                contextA.commit();
            }
            first.close();
        } finally {
            // first.close() must not break second; nothing else to clean here.
        }

        // Owner B writes and reads the same physical table through its own FileIO.
        Table tableB = second.getTable(Identifier.create("default", "shared"));
        try (PaimonTableWriteContext contextB =
                PaimonTableWriteContext.create(
                        "default.shared", "shared", tableB, "owner-b", null)) {
            contextB.write(GenericRow.of(2, BinaryString.fromString("b")));
            contextB.commit();
        }
        assertEquals(2, readRowCount(second, "default", "shared"));
        second.close();
    }

    /** Spec I5 / H3: the fsMap reflection close path must stay deleted. */
    @Test
    void productionCodeMustNotReflectIntoHadoopFileIoFsMap() throws Exception {
        java.nio.file.Path source =
                java.nio.file.Paths.get(
                        "src/main/java/io/tapdata/connector/paimon/service/PaimonService.java");
        assertTrue(Files.exists(source), "Cannot locate PaimonService.java from " + source);
        String content = new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
        assertFalse(
                content.contains("closeHadoopFileIOCachedFileSystems"),
                "The fsMap reflection close must not be reintroduced");
        assertFalse(
                content.contains("getDeclaredField(\"fsMap\")"),
                "Reflection into HadoopFileIO.fsMap must not be reintroduced");
    }

    private static int readRowCount(Catalog catalog, String database, String table)
            throws Exception {
        Table tableHandle = catalog.getTable(Identifier.create(database, table));
        try (var reader = tableHandle.newReadBuilder().newRead().createReader(
                tableHandle.newReadBuilder().newScan().plan())) {
            int count = 0;
            var batch = reader.readBatch();
            while (batch != null) {
                while (batch.next() != null) {
                    count++;
                }
                batch.releaseBatch();
                batch = reader.readBatch();
            }
            return count;
        }
    }
}
