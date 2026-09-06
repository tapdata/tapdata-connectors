package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractTestFactory;
import io.tapdata.connector.paimon.util.PaimonSpillDirCleaner;
import io.tapdata.connector.paimon.write.PaimonTableWriteContextFactory.IncompleteCleanupException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.crosspartition.IndexBootstrap;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** HASH_DYNAMIC 预检失败时，真实 Spill 目录保护与 Service owner 必须共享清理证明。 */
class PaimonDynamicBucketPreflightCleanupTest {

    @ParameterizedTest(name = "HASH_DYNAMIC bootstrap Error 后 {0} 的目录和 owner 证明")
    @EnumSource(CleanupFailure.class)
    void preflightMustReleaseOwnerOnlyAfterPositiveResourceCleanupProof(
            CleanupFailure scenario, @TempDir Path tempDir) throws Exception {
        String tableKey = "default.orders";
        FileStoreTable table = table(tempDir.resolve("orders"));
        Map<String, Object> persisted = new HashMap<>();
        KVMap<Object> state = stateMap(persisted);
        PaimonService first = service(tempDir, state);
        PaimonService second = service(tempDir, state);
        IOManagerImpl io = spy(new IOManagerImpl(new String[] {tempDir.toString()}));
        List<String> dirs = new ArrayList<>();
        for (File dir : io.getSpillingDirectories()) {
            dirs.add(dir.getCanonicalPath());
        }
        assertFalse(dirs.isEmpty());
        AssertionError bootstrapFailure = new AssertionError("原生 bootstrap Error");
        Throwable cleanupFailure = scenario.error
                ? new AssertionError("预检关闭 Error: " + scenario)
                : new IOException("预检关闭失败: " + scenario);
        if (scenario.io) {
            doThrow(cleanupFailure).when(io).close();
        }

        // 只在原生边界注入失败；Service admission、commit-state binding、Spill 注册和删除均真实执行。
        // Paimon 1.3.2 GlobalIndexAssigner.close 先关闭 stateFactory，再删除自身路径；失败并非访问者退出证明。
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java#L276
        // IOManagerImpl.close 最终删除 FileChannelManagerImpl 管理的全部 Spill 目录。
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java#L125
        try (MockedStatic<IOManager> factory = mockStatic(IOManager.class);
             MockedConstruction<GlobalIndexAssigner> checkers = mockConstruction(
                     GlobalIndexAssigner.class, (checker, ignored) -> {
                         if (scenario.checker) {
                             doThrow(cleanupFailure).when(checker).close();
                         }
                     });
             MockedConstruction<IndexBootstrap> bootstraps = mockConstruction(
                     IndexBootstrap.class, (bootstrap, ignored) ->
                             when(bootstrap.bootstrap(1, 0)).thenThrow(bootstrapFailure))) {
            factory.when(() -> IOManager.create(any(String[].class))).thenReturn(io);

            InvocationTargetException invocation = assertThrows(InvocationTargetException.class,
                    () -> admissionMethod().invoke(first, tableKey, "orders",
                            Identifier.create("default", "orders"), null, table,
                            PaimonWriteSemanticContractTestFactory.forMode(BucketMode.HASH_DYNAMIC)));
            assertEquals(1, checkers.constructed().size());
            org.mockito.ArgumentCaptor<IOManager> view = org.mockito.ArgumentCaptor.forClass(IOManager.class);
            verify(checkers.constructed().get(0)).open(org.mockito.ArgumentMatchers.eq(0L),
                    view.capture(), org.mockito.ArgumentMatchers.eq(1), org.mockito.ArgumentMatchers.eq(0), any());
            org.junit.jupiter.api.Assertions.assertArrayEquals(dirs.toArray(new String[0]), view.getValue().tempDirs());
            org.junit.jupiter.api.Assertions.assertNotSame(io, view.getValue());
            assertEquals(1, bootstraps.constructed().size());
            verify(checkers.constructed().get(0), times(1)).close();
            assertTrue(((Map<?, ?>) field(first, "tableWriteContexts")).isEmpty());
            assertTrue(persisted.keySet().stream().noneMatch(
                    key -> key.startsWith("paimon.hash-dynamic-preflight-v1.")),
                    "失败预检不得写入已验证标记");
            verify(table, never()).newStreamWriteBuilder();

            if (scenario == CleanupFailure.NONE) {
                assertSame(bootstrapFailure, invocation.getCause(), "完整清理后必须原样抛出 Error");
                assertEquals(0, bootstrapFailure.getSuppressed().length);
                assertTrue(((Set<?>) field(first, "unsafeResourceOwners")).isEmpty());
                assertTrue(((Map<?, ?>) field(first, "physicalTableByLogicalTable")).isEmpty());
                assertDoesNotThrow(first::close);
                assertDoesNotThrow(() -> registerMethod().invoke(second, tableKey, table),
                        "完整清理后下一 Service 应能获得物理表 owner");
            } else {
                IncompleteCleanupException failure = assertInstanceOf(
                        IncompleteCleanupException.class, invocation.getCause());
                assertSame(bootstrapFailure, failure.getCause());
                assertEquals(Collections.singletonList(cleanupFailure),
                        Arrays.asList(bootstrapFailure.getSuppressed()));
                assertTrue(((Set<?>) field(first, "unsafeResourceOwners")).contains(tableKey));
                assertTrue(((Map<?, ?>) field(first, "physicalTableByLogicalTable")).containsKey(tableKey));
                assertThrows(Exception.class, first::close,
                        "Service close 不得将未清理完成的预检转为正常退出");
                InvocationTargetException duplicate = assertThrows(InvocationTargetException.class,
                        () -> registerMethod().invoke(second, tableKey, table));
                assertInstanceOf(IllegalStateException.class, duplicate.getCause());
            }

            if (scenario.checker) {
                verify(io, never()).close();
            } else {
                verify(io, times(1)).close();
            }
            for (String dir : dirs) {
                assertEquals(scenario != CleanupFailure.NONE, Files.isDirectory(Paths.get(dir)),
                        "清理失败必须保留真实 Spill 目录");
                assertEquals(scenario != CleanupFailure.NONE, Files.exists(ownerMarker(dir)),
                        "清理失败必须保留目录 owner marker");
                assertEquals(scenario.checker, liveDirs().contains(dir),
                        "checker 未证明关闭时仍须保持 live 保护；IO 删除失败可交给 stale cleaner 重试");
                assertEquals(scenario.checker, ownerLocks().containsKey(dir),
                        "checker 未证明关闭时不能释放目录文件锁");
            }
        } finally {
            // 注入的 checker 没有真实后台访问者；退出构造 mock 作用域后才允许测试回收保留的资源。
            // 不使用生产 unregister 伪造成功证明，先执行真实 IO close，再释放测试持有的 owner。
            doCallRealMethod().when(io).close();
            io.close();
            PaimonSpillDirCleaner.releaseAfterClose(dirs, true);
            for (String dir : dirs) {
                Files.deleteIfExists(ownerMarker(dir));
            }
            unregisterMethod().invoke(first, tableKey);
            unregisterMethod().invoke(second, tableKey);
            try {
                first.close();
            } catch (Exception expectedStickyFailure) {
                assertTrue(scenario != CleanupFailure.NONE,
                        "只有清理失败场景允许缓存的关闭错误");
            } finally {
                second.close();
            }
        }
    }

    private enum CleanupFailure {
        CHECKER_EXCEPTION(true, false, false),
        CHECKER_ERROR(true, false, true),
        IO_EXCEPTION(false, true, false),
        IO_ERROR(false, true, true),
        NONE(false, false, false);

        final boolean checker;
        final boolean io;
        final boolean error;

        CleanupFailure(boolean checker, boolean io, boolean error) {
            this.checker = checker;
            this.io = io;
            this.error = error;
        }
    }

    private static PaimonService service(Path tempDir, KVMap<Object> state) throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setWarehouse(tempDir.toUri().toString());
        config.setDiskTmpDir(tempDir.toString());
        config.setBatchAccumulationSize(100);
        config.setCommitIntervalMs(30_000);
        config.setEnableAsyncCommit(false);
        PaimonService service = new PaimonService(config, mock(Log.class), () -> 100L, () -> { });
        Field boundState = PaimonService.class.getDeclaredField("boundTaskStateMap");
        boundState.setAccessible(true);
        boundState.set(service, state);
        return service;
    }

    private static FileStoreTable table(Path location) {
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.location()).thenReturn(new org.apache.paimon.fs.Path(location.toUri()));
        when(table.uuid()).thenReturn("preflight-cleanup-table");
        when(table.bucketMode()).thenReturn(BucketMode.HASH_DYNAMIC);
        when(table.rowType()).thenReturn(RowType.of(DataTypes.INT()));
        when(table.options()).thenReturn(Collections.emptyMap());
        when(table.coreOptions()).thenReturn(CoreOptions.fromMap(Collections.emptyMap()));
        SnapshotManager snapshots = mock(SnapshotManager.class);
        when(snapshots.latestSnapshotOfUserFromFilesystem(anyString())).thenReturn(Optional.empty());
        when(table.snapshotManager()).thenReturn(snapshots);
        return table;
    }

    @SuppressWarnings("unchecked")
    private static KVMap<Object> stateMap(Map<String, Object> values) {
        KVMap<Object> state = mock(KVMap.class);
        when(state.get(anyString())).thenAnswer(call -> values.get(call.getArgument(0)));
        when(state.putIfAbsent(anyString(), any())).thenAnswer(
                call -> values.putIfAbsent(call.getArgument(0), call.getArgument(1)));
        return state;
    }

    private static Path ownerMarker(String dir) {
        Path path = Paths.get(dir);
        return path.resolveSibling("." + path.getFileName() + ".tapdata-owner.lock");
    }

    private static Object field(PaimonService service, String name) throws Exception {
        Field field = PaimonService.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(service);
    }

    private static Set<?> liveDirs() throws Exception {
        Field field = PaimonSpillDirCleaner.class.getDeclaredField("LIVE_DIRS");
        field.setAccessible(true);
        return (Set<?>) field.get(null);
    }

    private static Map<?, ?> ownerLocks() throws Exception {
        Field field = PaimonSpillDirCleaner.class.getDeclaredField("OWNER_LOCKS");
        field.setAccessible(true);
        return (Map<?, ?>) field.get(null);
    }

    private static Method admissionMethod() throws Exception {
        return method("getOrCreateTableWriteContext", String.class, String.class, Identifier.class,
                TapConnectorContext.class, FileStoreTable.class, PaimonWriteSemanticContract.class);
    }

    private static Method registerMethod() throws Exception {
        return method("registerPhysicalTableOwner", String.class, FileStoreTable.class);
    }

    private static Method unregisterMethod() throws Exception {
        return method("unregisterPhysicalTableOwner", String.class);
    }

    private static Method method(String name, Class<?>... types) throws Exception {
        Method method = PaimonService.class.getDeclaredMethod(name, types);
        method.setAccessible(true);
        return method;
    }
}
