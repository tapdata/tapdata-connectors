package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;
import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.connector.paimon.write.PaimonTableWriteContextFactory;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonServiceIncompleteCleanupTest {
    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void incompleteConstructionMustFenceSubsequentPublicWriteAndDdlAndKeepOwner() throws Exception {
        Identifier identifier = Identifier.create("default", "orders");
        Catalog catalog = spy(CatalogFactory.createCatalog(CatalogContext.create(new Path(tempDir.toUri()))));
        catalog.createDatabase("default", true);
        catalog.createTable(identifier, Schema.newBuilder().column("id", DataTypes.INT())
                .primaryKey("id").option("bucket", "1")
                .option("snapshot.expire.execution-mode", "SYNC").build(), false);
        FileStoreTable table = spy((FileStoreTable) catalog.getTable(identifier));
        doReturn(table).when(catalog).getTable(identifier);
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse(tempDir.toUri().toString());
        config.setDatabase("default");
        config.setDiskTmpDir(tempDir.resolve("spill").toString());
        config.setEnableAsyncCommit(false);
        PaimonService service = new PaimonService(config, mock(Log.class), () -> 100L, () -> { });
        PaimonService contender = new PaimonService(config, mock(Log.class), () -> 100L, () -> { });
        Field catalogField = PaimonService.class.getDeclaredField("catalog");
        catalogField.setAccessible(true);
        catalogField.set(service, catalog);
        service.startForTest();
        Map<String, Object> states = new HashMap<>();
        @SuppressWarnings("unchecked")
        KVMap<Object> stateMap = mock(KVMap.class);
        when(stateMap.get(anyString())).thenAnswer(call -> states.get(call.getArgument(0)));
        when(stateMap.putIfAbsent(anyString(), any())).thenAnswer(call ->
                states.putIfAbsent(call.getArgument(0), call.getArgument(1)));
        TapConnectorContext context = mock(TapConnectorContext.class);
        when(context.getStateMap()).thenReturn(stateMap);
        when(context.getLog()).thenReturn(mock(Log.class));
        TapTable source = new TapTable("orders").add(new TapField("id", "INT").primaryKeyPos(1));
        TapInsertRecordEvent event = new TapInsertRecordEvent().init().table("orders")
                .after(Collections.singletonMap("id", 1));
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "INITIAL_SYNC");
        PaimonTableWriteContextFactory.IncompleteCleanupException incomplete =
                new PaimonTableWriteContextFactory.IncompleteCleanupException(
                        "default.orders", new IOException("半构造 writer 未完整关闭"));
        Method register = PaimonService.class.getDeclaredMethod("registerPhysicalTableOwner",
                String.class, FileStoreTable.class);
        Method unregister = PaimonService.class.getDeclaredMethod("unregisterPhysicalTableOwner", String.class);
        register.setAccessible(true);
        unregister.setAccessible(true);
        try (MockedStatic<PaimonTableWriteContextFactory> factory = mockStatic(PaimonTableWriteContextFactory.class)) {
            MockedStatic.Verification creation = () -> PaimonTableWriteContextFactory.create(
                    eq("default.orders"), eq("orders"), same(table), anyString(), anyString(), anyLong(),
                    any(PaimonTableWriteContext.CommitStateStore.class),
                    any(io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterRuntimeFactory.class),
                    any(PaimonWriteSemanticContract.class), any(PaimonStopResources.Scope.class));
            factory.when(creation).thenThrow(incomplete);

            assertSame(incomplete, assertThrows(Exception.class, () -> service.writeRecords(
                    Collections.singletonList(event), source, context)));
            assertSame(incomplete, assertThrows(Exception.class, () -> service.writeRecords(
                    Collections.singletonList(event), source, context)));
            assertSame(incomplete, assertThrows(Exception.class, () -> service.clearTable("orders")));
            assertSame(incomplete, assertThrows(Exception.class, () -> service.dropTable("orders")));

            factory.verify(creation, times(1));
            verify(table, never()).newStreamWriteBuilder();
            verify(table, never()).newBatchWriteBuilder();
            verify(catalog, never()).dropTable(identifier, true);
            org.junit.jupiter.api.Assertions.assertNotNull(catalog.getTable(identifier));
            Field unsafe = PaimonService.class.getDeclaredField("unsafeResourceOwners");
            unsafe.setAccessible(true);
            assertTrue(((Set<?>) unsafe.get(service)).contains("default.orders"));
            InvocationTargetException owned = assertThrows(InvocationTargetException.class,
                    () -> register.invoke(contender, "default.orders", table));
            assertInstanceOf(IllegalStateException.class, owned.getCause());
            assertSame(incomplete, assertThrows(Exception.class, service::close));
            assertThrows(InvocationTargetException.class,
                    () -> register.invoke(contender, "default.orders", table));
        } finally {
            try { service.close(); } catch (Exception expectedIncompleteCleanup) { /* 故障终态保持。 */ }
            // 本 fixture 的 Incomplete 信号来自故障注入，未创建实际 writer；仅测试收尾清除其登记。
            PaimonStopTestSupport.releaseInjectedOwner(service, "default.orders");
            PaimonStopTestSupport.releaseInjectedOwner(contender, "default.orders");
            contender.close();
            catalog.close();
        }
    }
}
