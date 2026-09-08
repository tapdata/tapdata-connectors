package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonSyncExpireMode;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class PaimonAutoSyncExpireModeTest {
    @TempDir java.nio.file.Path tempDir;
    private static final Identifier ID = Identifier.create("default", "orders");

    @Test void asyncMustAlterInvalidateAndReloadBeforeReturningFreshTableWithWarnings() throws Exception {
        Fixture f = new Fixture();
        assertSame(f.sync, f.ensure(f.async));
        org.mockito.InOrder order = inOrder(f.catalog);
        order.verify(f.catalog).alterTable(eq(ID), anyList(), eq(false));
        order.verify(f.catalog).invalidateTable(ID);
        order.verify(f.catalog).getTable(ID);
        verify(f.log).warn(contains("准备 ALTER 为 SYNC"), eq(ID.getFullName()));
        verify(f.log).warn(contains("已持久修改并回读确认"), eq(ID.getFullName()));
        assertTrue(f.owners().isEmpty());
        // 重复检查已同步的实例不重复修改或打印转换 WARN。
        assertSame(f.sync, f.ensure(f.sync));
        verifyNoMoreInteractions(f.catalog);
        f.stop.service.close();
    }

    @Test void unchangedAsyncReadbackMustFailWithoutCreatingWriterOrCommitter() throws Exception {
        Fixture f = new Fixture(); when(f.catalog.getTable(ID)).thenReturn(f.async);
        assertThrows(IllegalArgumentException.class, () -> f.ensure(f.async));
        verify(f.async, never()).newStreamWriteBuilder(); verify(f.async, never()).newBatchWriteBuilder();
        verify(f.log, never()).warn(contains("已持久修改并回读确认"), any());
        assertTrue(f.owners().isEmpty()); f.stop.service.close();
    }

    @Test void failedReadbackMustNotClaimConversionSuccess() throws Exception {
        Fixture f = new Fixture(); Catalog.TableNotExistException absent = new Catalog.TableNotExistException(ID);
        when(f.catalog.getTable(ID)).thenThrow(absent);
        assertSame(absent, assertThrows(Catalog.TableNotExistException.class, () -> f.ensure(f.async)));
        verify(f.log, never()).warn(contains("已持久修改并回读确认"), any());
        assertTrue(f.owners().isEmpty()); f.stop.service.close();
    }

    @Test void anotherServiceOwnerMustRejectAlterBeforeCatalogMutation() throws Exception {
        Fixture f = new Fixture();
        PaimonBoundedStopTest.Fixture other = new PaimonBoundedStopTest.Fixture(180, 120, 30);
        Method register = PaimonService.class.getDeclaredMethod("registerPhysicalTableOwner", String.class, FileStoreTable.class);
        Method release = PaimonService.class.getDeclaredMethod("unregisterPhysicalTableOwner", String.class);
        register.setAccessible(true); release.setAccessible(true);
        register.invoke(other.service, ID.getFullName(), f.async);
        try {
            assertThrows(IllegalStateException.class, () -> f.ensure(f.async));
            verifyNoInteractions(f.catalog);
        } finally { release.invoke(other.service, ID.getFullName()); other.service.close(); f.stop.service.close(); }
    }

    @Test void truncateMustUseReloadedSyncCommitter() throws Exception {
        Fixture f = new Fixture();
        when(f.catalog.getTable(ID)).thenReturn(f.async, f.async, f.sync);
        BatchWriteBuilder builder = mock(BatchWriteBuilder.class); BatchTableCommit commit = mock(BatchTableCommit.class);
        when(f.sync.newBatchWriteBuilder()).thenReturn(builder); when(builder.newCommit()).thenReturn(commit);
        f.stop.service.clearTable(ID.getObjectName());
        org.mockito.InOrder order = inOrder(f.catalog, f.sync, builder, commit);
        order.verify(f.catalog, times(2)).getTable(ID);
        order.verify(f.catalog).alterTable(eq(ID), anyList(), eq(false));
        order.verify(f.catalog).invalidateTable(ID); order.verify(f.catalog).getTable(ID);
        order.verify(f.sync).newBatchWriteBuilder(); order.verify(builder).newCommit();
        order.verify(commit).truncateTable(); order.verify(commit).close();
        verify(f.async, never()).newBatchWriteBuilder(); assertTrue(f.owners().isEmpty());
        f.stop.service.close();
    }

    @Test void stopDuringBlockedAlterMustRetainOwnerAndRejectLateReadback() throws Exception {
        Fixture f = new Fixture(); when(f.catalog.getTable(ID)).thenReturn(f.async);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        doAnswer(i -> { entered.countDown(); assertTrue(release.await(5, TimeUnit.SECONDS)); return null; })
                .when(f.catalog).alterTable(eq(ID), anyList(), eq(false));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread create = new Thread(() -> { try { f.stop.service.createTable(new TapTable(ID.getObjectName())); }
                catch (Throwable e) { failure.set(e); } });
        create.setDaemon(true); create.start();
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS)); f.stop.start();
            long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (!f.stop.control().isStarted() && System.nanoTime() < limit) { Thread.sleep(5); }
            assertTrue(f.stop.control().isStarted()); f.stop.advance(180); f.stop.joinFailedRetained();
            assertFalse(f.owners().isEmpty()); release.countDown(); create.join(3000); f.stop.worker().join(3000);
            assertFalse(create.isAlive()); assertNotNull(failure.get());
            verify(f.catalog, never()).invalidateTable(ID); verify(f.catalog, times(1)).getTable(ID);
            verify(f.catalog, never()).close(); assertFalse(f.owners().isEmpty());
        } finally { release.countDown(); create.join(3000); }
    }

    private final class Fixture {
        final Log log = mock(Log.class);
        final PaimonBoundedStopTest.Fixture stop = new PaimonBoundedStopTest.Fixture(180, 120, 30, log);
        final Catalog catalog = mock(Catalog.class);
        final FileStoreTable async = table("ASYNC"), sync = table("SYNC");
        Fixture() throws Exception {
            Field field = PaimonService.class.getDeclaredField("catalog"); field.setAccessible(true); field.set(stop.service, catalog);
            when(catalog.getTable(ID)).thenReturn(sync);
        }
        FileStoreTable table(String mode) {
            FileStoreTable table = mock(FileStoreTable.class);
            Map<String, String> options = Collections.singletonMap(PaimonSyncExpireMode.KEY, mode);
            when(table.options()).thenReturn(options); when(table.coreOptions()).thenReturn(CoreOptions.fromMap(options));
            when(table.location()).thenReturn(new Path(tempDir.resolve("orders").toUri())); return table;
        }
        FileStoreTable ensure(Table table) throws Exception {
            Method method = PaimonService.class.getDeclaredMethod("ensureSyncExpireMode", Identifier.class, Table.class);
            method.setAccessible(true);
            try { return (FileStoreTable) method.invoke(stop.service, ID, table); }
            catch (InvocationTargetException e) { throw (Exception) e.getCause(); }
        }
        Map<?, ?> owners() throws Exception {
            Field field = PaimonService.class.getDeclaredField("physicalTableByLogicalTable"); field.setAccessible(true);
            return (Map<?, ?>) field.get(stop.service);
        }
    }
}
