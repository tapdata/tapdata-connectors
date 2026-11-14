package io.tapdata.connector.paimon.util;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;

/**
 * 测试访问 Spill 所有权的唯一入口（Plan T07）。
 *
 * <p>测试只经本类构造“已登记”的 IOManager、定位 owner 锁文件、观察登记状态与执行启动清理，不直接依赖
 * 生产代码的目录布局或 static 字段。布局切换（Plan T08/T10）时只改本类内部实现，测试断言保持不变。
 * 当前实现对应 Spec V0.3 的租约布局：{@code <root>/tapdata-paimon-v2/<jvmId>/<lease>/{owner.lock, io/paimon-io-*}}。
 */
public final class SpillTestSupport {

    private static final Map<IOManager, List<String>> OWNED = Collections.synchronizedMap(new IdentityHashMap<>());

    private SpillTestSupport() {
    }

    /** 创建并登记一个属于测试的 IOManager；目录在返回前已物化。 */
    public static IOManagerImpl ownedIOManager(String root) {
        PaimonSpillDirCleaner.IOManagerBuildResult built =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(root, "test-owned");
        IOManagerImpl ioManager = (IOManagerImpl) built.ioManager();
        OWNED.put(ioManager, built.spillDirs());
        return ioManager;
    }

    /** {@link #ownedIOManager(String)} 登记的 spill 目录（规范路径），供 Context 与 releaseAfterClose 使用。 */
    public static List<String> spillDirsOf(IOManager ioManager) {
        List<String> dirs = OWNED.get(ioManager);
        if (dirs == null) {
            throw new AssertionError("IOManager was not created through SpillTestSupport.ownedIOManager");
        }
        return dirs;
    }

    /** spill 目录对应的 owner 锁文件。 */
    public static Path ownerLockFile(String spillDir) {
        // <lease>/io/paimon-io-<uuid> → <lease>/owner.lock
        return Paths.get(spillDir).getParent().getParent().resolve(PaimonSpillLease.LOCK_FILE);
    }

    public static File ownerLockFile(File spillDir) {
        return ownerLockFile(spillDir.getPath()).toFile();
    }

    /** 以另一个进程探测：该 spill 目录所属租约的 OS 记录锁是否仍被本进程持有（同进程探测会释放锁，见 C1）。 */
    public static boolean ownerLockHeld(String spillDir) {
        try {
            return !SpillProbeProcess.otherProcessAcquires(ownerLockFile(spillDir));
        } catch (Exception failure) {
            throw new AssertionError("Cannot probe owner lock of " + spillDir, failure);
        }
    }

    /** 在另一个 JVM 中对该 spill 目录所在的根执行一次新协议回收，返回回收数。 */
    public static int reclaimInOtherJvm(String spillDir) {
        Path root = Paths.get(spillDir).getParent().getParent().getParent().getParent().getParent();
        try {
            String line = SpillProbeProcess.reclaimInOtherJvm(root);
            return Integer.parseInt(line.substring("reclaimed=".length()));
        } catch (Exception failure) {
            throw new AssertionError("Cannot run reclaim in another JVM for " + root, failure);
        }
    }

    /** 该目录是否仍处于本 ClassLoader 的 owner 登记中（存活保护与锁对象均在）。 */
    public static boolean ownerRegistered(String spillDir) {
        return staticField("OWNERS", Map.class).containsKey(spillDir);
    }

    /** 当前 ClassLoader 中仍被持有的 owner 登记数量，用于检测泄漏。 */
    public static int ownerRegistrationCount() {
        return staticField("OWNERS", Map.class).size();
    }

    /** 执行一次生产启动回收（PaimonService 的同一入口），覆盖 spill 目录所在的根；返回回收的租约数。 */
    public static int runStartupCleanup(List<String> spillDirs, BiConsumer<String, Long> onDeleted) {
        // <root>/tapdata-paimon-v2/<jvmId>/<lease>/io/paimon-io-<uuid> 向上 5 级即配置的根目录。
        String[] roots = spillDirs.stream()
                .map(dir -> Paths.get(dir).getParent().getParent().getParent().getParent().getParent().toString())
                .distinct().toArray(String[]::new);
        return PaimonSpillDirCleaner.reclaimOrphanedLeases(java.util.Arrays.asList(roots), onDeleted).reclaimed();
    }

    /**
     * 拦截生产代码对 {@code IOManager.create(String[])} 的调用，按实际传入的根构造真实 {@link IOManagerImpl}
     * 的 spy，并交给 configurer 注入故障。用于在 mockStatic 作用域内观察生产代码创建的 IOManager。
     */
    public static CreatedIOManagers spyOnIOManagerCreation(MockedStatic<IOManager> factory,
            Consumer<IOManagerImpl> configurer) {
        CreatedIOManagers created = new CreatedIOManagers();
        factory.when(() -> IOManager.create(any(String[].class))).thenAnswer(call -> {
            String[] roots = call.getArgument(0);
            IOManagerImpl ioManager = Mockito.spy(new IOManagerImpl(roots));
            configurer.accept(ioManager);
            created.ioManagers.add(ioManager);
            return ioManager;
        });
        return created;
    }

    public static final class CreatedIOManagers {
        private final List<IOManagerImpl> ioManagers = new CopyOnWriteArrayList<>();

        public boolean isEmpty() {
            return ioManagers.isEmpty();
        }

        public IOManagerImpl only() {
            if (ioManagers.size() != 1) {
                throw new AssertionError("Expected exactly one IOManager.create call but saw " + ioManagers.size());
            }
            return ioManagers.get(0);
        }

        /** 唯一 IOManager 的 spill 目录（规范路径）。 */
        public List<String> spillDirs() throws IOException {
            List<String> dirs = new ArrayList<>();
            for (File dir : only().getSpillingDirectories()) {
                dirs.add(dir.getCanonicalPath());
            }
            return dirs;
        }
    }

    private static <T> T staticField(String name, Class<T> type) {
        try {
            Field field = PaimonSpillDirCleaner.class.getDeclaredField(name);
            field.setAccessible(true);
            return type.cast(field.get(null));
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError("Cannot read PaimonSpillDirCleaner." + name, failure);
        }
    }
}
