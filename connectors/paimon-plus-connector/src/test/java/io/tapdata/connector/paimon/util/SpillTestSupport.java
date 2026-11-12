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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;

/**
 * 测试访问 Spill 所有权的唯一入口（Plan T07）。
 *
 * <p>测试只经本类构造“已登记”的 IOManager、定位 owner 锁文件、观察登记状态与执行启动清理，不直接依赖
 * 生产代码的目录布局或 static 字段。布局切换（Plan T08/T10）时只改本类内部实现，测试断言保持不变。
 * 当前实现对应 develop e3f68298 的平铺布局。
 */
public final class SpillTestSupport {

    private static final Map<IOManager, List<String>> OWNED = Collections.synchronizedMap(new IdentityHashMap<>());

    private SpillTestSupport() {
    }

    /** 创建并登记一个属于测试的 IOManager；目录在返回前已物化。 */
    public static IOManagerImpl ownedIOManager(String root) {
        IOManagerImpl ioManager = (IOManagerImpl) IOManager.create(root);
        OWNED.put(ioManager, PaimonSpillDirCleaner.registerLiveDirs(ioManager));
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
        Path path = Paths.get(spillDir);
        return path.resolveSibling("." + path.getFileName() + PaimonSpillDirCleaner.OWNER_LOCK_SUFFIX);
    }

    public static File ownerLockFile(File spillDir) {
        return ownerLockFile(spillDir.getPath()).toFile();
    }

    /** 该目录是否仍处于本 ClassLoader 的 owner 登记中（存活保护与锁对象均在）。 */
    public static boolean ownerRegistered(String spillDir) {
        Set<?> live = staticField("LIVE_DIRS", Set.class);
        Map<?, ?> owners = staticField("OWNER_LOCKS", Map.class);
        boolean registered = live.contains(spillDir);
        if (registered != owners.containsKey(spillDir)) {
            throw new AssertionError("Spill live registration and owner lock diverged for " + spillDir);
        }
        return registered;
    }

    /** 当前 ClassLoader 中仍被持有的 owner 登记数量，用于检测泄漏。 */
    public static int ownerRegistrationCount() {
        return staticField("OWNER_LOCKS", Map.class).size();
    }

    /** 以零宽限期执行一次启动清理扫描，覆盖 spill 目录所在的根；返回删除数。 */
    public static int runStartupCleanup(List<String> spillDirs, BiConsumer<String, Long> onDeleted) {
        String[] roots = spillDirs.stream().map(dir -> new File(dir).getParent()).distinct().toArray(String[]::new);
        return PaimonSpillDirCleaner.cleanupStaleSpillDirs(roots, 0L, onDeleted);
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
