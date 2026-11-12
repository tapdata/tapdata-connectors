package io.tapdata.connector.paimon.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * T11：同一 JVM 内多个隔离 ClassLoader 的验收（AC-02、AC-04、AC-05、AC-15）。
 *
 * <p>跨 loader 测试只使用纯 Java 的 Paimon IOManager，不加载 RocksDB JNI（同一 native 库不能被两个
 * ClassLoader 加载）。
 */
@DisabledOnOs(OS.WINDOWS)
class PaimonSpillIsolatedClassLoaderIntegrationTest {

    /** AC-02（跨 ClassLoader）：loader B 回收时，对 loader A 命名空间中任何文件的打开次数为 0。 */
    @Test
    void foreignLoaderNeverOpensFilesInTheSharedJvmNamespace(@TempDir Path root) throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult owner =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(root.toString(), "loader-a.orders");
        Path lockFile = SpillTestSupport.ownerLockFile(owner.spillDirs().get(0));
        Path namespace = lockFile.getParent().getParent();
        List<Path> opened = new CopyOnWriteArrayList<>();
        try (URLClassLoader loaderB = IsolatedConnectorLoader.create()) {
            assertEquals(PaimonJvmIdentity.current(), invoke(loaderB, "jvmId", new Class<?>[0]));
            Consumer<Path> recorder = opened::add;
            assertEquals(0, invoke(loaderB, "reclaim", new Class<?>[] {Collection.class, Consumer.class},
                    Collections.singletonList(root.toString()), recorder));
        } finally {
            assertTrue(opened.stream().noneMatch(path -> path.startsWith(namespace)),
                    "loader B 打开了本 JVM 命名空间中的文件: " + opened);
            assertFalse(SpillProbeProcess.otherProcessAcquires(lockFile), "owner 的 OS 锁必须仍被持有");
            owner.ioManager().close();
            assertTrue(PaimonSpillDirCleaner.releaseAfterClose(owner.spillDirs(), true).complete());
        }
    }

    /** AC-04：owner 停在“已加锁、未返回”时，同 ClassLoader 与其他 ClassLoader 的并发回收都不打开其锁文件。 */
    @Test
    void reclaimDuringProvisioningWindowDoesNotTouchTheNewLease(@TempDir Path root) throws Exception {
        List<Path> opened = new CopyOnWriteArrayList<>();
        AtomicReference<Integer> sameLoader = new AtomicReference<>();
        AtomicReference<Object> otherLoader = new AtomicReference<>();
        try (URLClassLoader loaderB = IsolatedConnectorLoader.create()) {
            PaimonSpillLease.ProvisionHook window = new PaimonSpillLease.ProvisionHook() {
                @Override
                public void afterOwnerLock(Path lease) {
                    sameLoader.set(SpillLoaderDriver.reclaim(Collections.singletonList(root.toString()), opened::add));
                    try {
                        Consumer<Path> recorder = opened::add;
                        otherLoader.set(invoke(loaderB, "reclaim", new Class<?>[] {Collection.class, Consumer.class},
                                Collections.singletonList(root.toString()), recorder));
                    } catch (Exception failure) {
                        throw new IllegalStateException(failure);
                    }
                }
            };
            PaimonSpillLease lease = PaimonSpillLease.provision(Collections.singletonList(root.toString()),
                    "window.orders", PaimonJvmIdentity::current, PaimonSpillAudit.defaultAudit(), window);
            try {
                assertEquals(Integer.valueOf(0), sameLoader.get());
                assertEquals(0, otherLoader.get());
                assertTrue(opened.isEmpty(), "Provision 窗口内的回收打开了文件: " + opened);
                assertFalse(SpillProbeProcess.otherProcessAcquires(lease.roots().get(0).lockFile));
            } finally {
                assertNull(lease.roots().get(0).retire());
            }
        }
    }

    /** AC-05（ClassLoader）：两个 loader 同时回收同一 ORPHANED lease，无异常逃逸、恰好一次成功。 */
    @Test
    void twoLoadersReclaimTheSameOrphanAtMostOnce(@TempDir Path root) throws Exception {
        Path lease = Files.createDirectories(root.resolve("tapdata-paimon-v2")
                .resolve(PaimonJvmIdentity.derive(7L, 7L)).resolve("0123456789ab-" + UUID.randomUUID()));
        Files.write(lease.resolve("owner.lock"), new byte[0]);
        Files.write(Files.createDirectories(lease.resolve("io").resolve("paimon-io-x")).resolve("a.channel"), new byte[] {1});
        CyclicBarrier bothOpened = new CyclicBarrier(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try (URLClassLoader loaderB = IsolatedConnectorLoader.create();
             URLClassLoader loaderC = IsolatedConnectorLoader.create()) {
            Future<Object> b = pool.submit(() -> invoke(loaderB, "reclaimAfterBarrier",
                    new Class<?>[] {Collection.class, CyclicBarrier.class}, Collections.singletonList(root.toString()), bothOpened));
            Future<Object> c = pool.submit(() -> invoke(loaderC, "reclaimAfterBarrier",
                    new Class<?>[] {Collection.class, CyclicBarrier.class}, Collections.singletonList(root.toString()), bothOpened));
            assertEquals(1, (Integer) b.get(30, TimeUnit.SECONDS) + (Integer) c.get(30, TimeUnit.SECONDS));
            assertFalse(Files.exists(lease));
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * AC-15：loader A 中仍有活跃的 spill 使用者线程时，丢弃 PDK 侧对 loader A 的全部引用并施加 GC 压力，
     * owner 的 OS 锁仍被持有；使用者按生产顺序退出后租约被退役。所有引用都丢弃后锁被释放属于允许的行为，不在此断言。
     */
    @Test
    void ownerLockSurvivesLoaderDereferenceWhileAUserIsAlive(@TempDir Path root) throws Exception {
        CountDownLatch ready = new CountDownLatch(1);
        CountDownLatch stop = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<String> lockFile = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        WeakReference<URLClassLoader> loaderRef = startUserInIsolatedLoader(root, ready, lockFile, stop, done, failure);
        assertTrue(ready.await(30, TimeUnit.SECONDS));
        assertNull(failure.get());

        for (int i = 0; i < 5; i++) {
            System.gc();
            Thread.sleep(50);
        }

        assertFalse(SpillProbeProcess.otherProcessAcquires(Paths.get(lockFile.get())),
                "使用者仍在运行时，丢弃 loader 引用后 OS 锁必须仍被持有");
        assertTrue(loaderRef.get() != null, "活跃使用者线程使 loader A 保持可达");
        stop.countDown();
        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertNull(failure.get());
        assertFalse(Files.exists(Paths.get(lockFile.get()).getParent()), "使用者退出后租约按生产顺序退役");
    }

    /** 在独立方法中创建 loader 并启动使用者，返回后测试不再持有对 loader A 的强引用。 */
    private static WeakReference<URLClassLoader> startUserInIsolatedLoader(Path root, CountDownLatch ready,
            AtomicReference<String> lockFile, CountDownLatch stop, CountDownLatch done,
            AtomicReference<Throwable> failure) throws Exception {
        URLClassLoader loaderA = IsolatedConnectorLoader.create();
        invoke(loaderA, "startSpillUser", new Class<?>[] {String.class, CountDownLatch.class, AtomicReference.class,
                CountDownLatch.class, CountDownLatch.class, AtomicReference.class},
                root.toString(), ready, lockFile, stop, done, failure);
        return new WeakReference<>(loaderA);
    }

    private static Object invoke(URLClassLoader loader, String method, Class<?>[] types, Object... args)
            throws Exception {
        Class<?> driver = loader.loadClass(SpillLoaderDriver.class.getName());
        if (driver == SpillLoaderDriver.class) {
            throw new IOException("driver was not isolated");
        }
        Method target = driver.getMethod(method, types);
        return target.invoke(null, args);
    }
}
