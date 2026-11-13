package io.tapdata.connector.paimon.util;

import org.apache.paimon.disk.IOManager;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * 在隔离 ClassLoader 内执行的驱动（Plan T11）。测试经反射调用这些静态方法，参数与返回值只使用 JDK 类型，
 * 因此任何 Connector 或 Paimon 类型都不会跨越 ClassLoader 边界。
 */
public final class SpillLoaderDriver {

    private SpillLoaderDriver() {
    }

    /** 当前 loader 中的回收者对 roots 执行一次回收；opened 记录本 loader 打开过的锁文件。返回回收数。 */
    public static int reclaim(Collection<String> roots, Consumer<Path> opened) {
        PaimonSpillReclaimer reclaimer = new PaimonSpillReclaimer(PaimonJvmIdentity::current,
                PaimonSpillReclaimer.DEFAULT_TYPE_PROBE, lockFile -> {
                    opened.accept(lockFile);
                    return PaimonSpillReclaimer.DEFAULT_OPENER.open(lockFile);
                }, PaimonSpillFiles.FILES_DELETE, PaimonSpillAudit.defaultAudit(), PaimonSpillReclaimer.DEFAULT_BUDGET);
        return reclaimer.reclaim(roots).reclaimed.size();
    }

    /** 与 {@link #reclaim} 相同，但在打开锁文件后于 barrier 处会合，用于制造两个 loader 的并发回收。 */
    public static int reclaimAfterBarrier(Collection<String> roots, CyclicBarrier bothOpened) {
        PaimonSpillReclaimer reclaimer = new PaimonSpillReclaimer(PaimonJvmIdentity::current,
                PaimonSpillReclaimer.DEFAULT_TYPE_PROBE, lockFile -> {
                    FileChannel channel = PaimonSpillReclaimer.DEFAULT_OPENER.open(lockFile);
                    try {
                        bothOpened.await(10, TimeUnit.SECONDS);
                    } catch (Exception barrier) {
                        channel.close();
                        throw new java.io.IOException(barrier);
                    }
                    return channel;
                }, PaimonSpillFiles.FILES_DELETE, PaimonSpillAudit.defaultAudit(), PaimonSpillReclaimer.DEFAULT_BUDGET);
        return reclaimer.reclaim(roots).reclaimed.size();
    }

    public static String jvmId() {
        return PaimonJvmIdentity.current();
    }

    /**
     * 在本 loader 中启动一个真实的 spill 使用者线程：经生产外观建立租约与 IOManager，循环写入 channel，
     * 直到 stop 计数归零，再按生产顺序关闭 IOManager 并退役租约。lockFile 发布锁文件路径；done 在退役后计数。
     */
    public static Thread startSpillUser(String root, CountDownLatch ready, AtomicReference<String> lockFile,
            CountDownLatch stop, CountDownLatch done, AtomicReference<Throwable> failure) {
        Thread user = new Thread(() -> {
            PaimonSpillDirCleaner.IOManagerBuildResult built = null;
            try {
                built = PaimonSpillDirCleaner.resolveAndCreateIOManager(root, "loader-a.user");
                List<String> dirs = built.spillDirs();
                lockFile.set(SpillTestSupport.ownerLockFile(dirs.get(0)).toString());
                ready.countDown();
                IOManager io = built.ioManager();
                while (!stop.await(20, TimeUnit.MILLISECONDS)) {
                    io.createBufferFileWriter(io.createChannel()).closeAndDelete();
                }
                io.close();
                if (!PaimonSpillDirCleaner.releaseAfterClose(dirs, true).complete()) {
                    throw new IllegalStateException("lease was not retired");
                }
            } catch (Throwable error) {
                failure.compareAndSet(null, error);
                ready.countDown();
            } finally {
                done.countDown();
            }
        }, "loader-a-spill-user");
        user.setDaemon(true);
        user.start();
        return user;
    }
}
