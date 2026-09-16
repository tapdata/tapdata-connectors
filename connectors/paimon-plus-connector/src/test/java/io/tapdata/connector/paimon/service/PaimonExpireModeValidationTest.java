package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonExpireMode;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/** 合法模式原样通过，禁止持久转换。 */
class PaimonExpireModeValidationTest {
    @TempDir java.nio.file.Path tempDir;
    private static final Identifier ID = Identifier.create("default", "orders");

    @Test
    void asyncValidationMustNotMutateOrReloadTable() throws Exception {
        Fixture f = new Fixture();
        try {
            assertSame(f.async, f.validate(f.async));
            assertEquals("ASYNC", f.async.options().get(PaimonExpireMode.KEY));
            verifyNoInteractions(f.catalog, f.log);
            verify(f.async, never()).newStreamWriteBuilder();
            verify(f.async, never()).newBatchWriteBuilder();
            assertTrue(f.owners().isEmpty());
        } finally { f.stop.service.close(); }
    }

    @Test
    void syncValidationMustReturnOriginalWithoutCatalogAccess() throws Exception {
        Fixture f = new Fixture();
        try {
            assertSame(f.sync, f.validate(f.sync));
            verifyNoInteractions(f.catalog, f.log);
            assertTrue(f.owners().isEmpty());
        } finally { f.stop.service.close(); }
    }

    @Test
    void unsupportedTableTypeMustFailWithoutMutation() throws Exception {
        Fixture f = new Fixture();
        try {
            assertThrows(IllegalArgumentException.class, () -> f.validate(mock(Table.class)));
            verifyNoInteractions(f.catalog);
        } finally { f.stop.service.close(); }
    }

    @Test
    void existingAsyncCreateCheckMustNotAlterTable() throws Exception {
        Fixture f = new Fixture();
        when(f.catalog.getTable(ID)).thenReturn(f.async);
        try {
            assertFalse(f.stop.service.createTable(new TapTable(ID.getObjectName())));
            verify(f.catalog, never()).alterTable(any(), anyList(), anyBoolean());
            verify(f.catalog, never()).invalidateTable(any());
            verify(f.catalog, never()).createTable(any(), any(), anyBoolean());
            assertTrue(f.owners().isEmpty());
        } finally { f.stop.service.close(); }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"SYNC", "ASYNC"})
    void truncateMustKeepNativeLifecycleWithoutAlter(String mode) throws Exception {
        Fixture f = new Fixture();
        BatchWriteBuilder builder = mock(BatchWriteBuilder.class);
        org.apache.paimon.table.sink.TableCommitImpl commit = mock(org.apache.paimon.table.sink.TableCommitImpl.class);
        when(commit.getMaintainExecutor()).thenReturn(java.util.concurrent.Executors.newSingleThreadExecutor());
        FileStoreTable table = mode.equals("ASYNC") ? f.async : f.sync;
        when(f.catalog.getTable(ID)).thenReturn(table);
        when(table.newBatchWriteBuilder()).thenReturn(builder);
        when(builder.newCommit()).thenReturn(commit);
        try {
            f.stop.service.clearTable(ID.getObjectName());
            org.mockito.InOrder order = inOrder(commit);
            order.verify(commit).truncateTable();
            order.verify(commit).close();
            verify(f.catalog, never()).alterTable(any(), anyList(), anyBoolean());
            assertTrue(f.owners().isEmpty());
        } finally { f.stop.service.close(); }
    }

    private final class Fixture {
        final Log log = mock(Log.class);
        final PaimonBoundedStopTest.Fixture stop = new PaimonBoundedStopTest.Fixture(180, 120, 30, log);
        final Catalog catalog = mock(Catalog.class);
        final FileStoreTable async = table("ASYNC"), sync = table("SYNC");

        Fixture() throws Exception {
            Field field = PaimonService.class.getDeclaredField("catalog");
            field.setAccessible(true);
            field.set(stop.service, catalog);
            when(catalog.getTable(ID)).thenReturn(sync);
        }

        FileStoreTable table(String mode) {
            FileStoreTable table = mock(FileStoreTable.class);
            Map<String, String> options = Collections.singletonMap(PaimonExpireMode.KEY, mode);
            org.apache.paimon.schema.TableSchema schema = org.apache.paimon.schema.TableSchema.create(0L,
                    org.apache.paimon.schema.Schema.newBuilder().column("id", org.apache.paimon.types.DataTypes.INT())
                            .primaryKey("id").option("bucket", "1").build());
            when(table.schema()).thenReturn(schema);
            when(table.bucketMode()).thenReturn(org.apache.paimon.table.BucketMode.HASH_FIXED);
            when(table.options()).thenReturn(options);
            when(table.coreOptions()).thenReturn(CoreOptions.fromMap(options));
            when(table.location()).thenReturn(new Path(tempDir.resolve("orders").toUri()));
            return table;
        }

        FileStoreTable validate(Table table) throws Exception {
            Method method = PaimonService.class.getDeclaredMethod("requireWriteTable", Identifier.class, Table.class);
            method.setAccessible(true);
            try { return (FileStoreTable) method.invoke(stop.service, ID, table); }
            catch (InvocationTargetException e) { throw (Exception) e.getCause(); }
        }

        Map<?, ?> owners() throws Exception {
            Field field = PaimonService.class.getDeclaredField("physicalTableByLogicalTable");
            field.setAccessible(true);
            return (Map<?, ?>) field.get(stop.service);
        }
    }
}
