package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.commit.PaimonMicroBatchCoordinator;

import io.tapdata.connector.paimon.write.PaimonTableCommitter;
import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractTestFactory;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonServiceTableDdlCacheInvalidationTest {

    private static final String TABLE_KEY = "default.target";
    private static final String TABLE_NAME = "target";
    private static final String OTHER_TABLE_KEY = "default.other";
    private static final String OTHER_TABLE_NAME = "other";

    @Test
    void successfulDropMustInvalidateOnlyCurrentTableDerivedCaches() throws Exception {
        Fixture fixture = fixture();
        seedDerivedCaches(fixture.service);
        PaimonBucketWriterStrategy targetWriter = mock(PaimonBucketWriterStrategy.class);
        PaimonBucketWriterStrategy otherWriter = mock(PaimonBucketWriterStrategy.class);
        PaimonTableWriteContext otherContext =
                context(OTHER_TABLE_KEY, OTHER_TABLE_NAME, otherWriter);
        map(fixture.service, "tableWriteContexts")
                .put(TABLE_KEY, context(TABLE_KEY, TABLE_NAME, targetWriter));
        map(fixture.service, "tableWriteContexts").put(OTHER_TABLE_KEY, otherContext);

        // Paimon 1.3.1 CachingCatalog#dropTable invalidates the Catalog's own table cache.
        // Connector policy separately invalidates only the matching Connector-derived cache
        // keys while retaining the other table generation and writer context.
        // Source:
        // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/catalog/CachingCatalog.java#L184-L197
        fixture.service.dropTable(TABLE_NAME);

        verify(fixture.catalog)
                .dropTable(Identifier.create("default", TABLE_NAME), true);
        assertCurrentTableCachesRemoved(fixture.service);
        assertOtherTableCachesPreserved(fixture.service);
        assertSame(
                otherContext,
                map(fixture.service, "tableWriteContexts").get(OTHER_TABLE_KEY));
        verify(targetWriter).close();
        verify(otherWriter, never()).close();
    }

    @Test
    void catalogFailureMustPreserveOriginalExceptionAndInvalidateCaches() throws Exception {
        Fixture fixture = fixture();
        seedDerivedCaches(fixture.service);
        IllegalStateException actionFailure = new IllegalStateException("catalog-action-failure");
        doThrow(actionFailure)
                .when(fixture.catalog)
                .dropTable(Identifier.create("default", TABLE_NAME), true);

        IllegalStateException thrown =
                assertThrows(
                        IllegalStateException.class,
                        () -> fixture.service.dropTable(TABLE_NAME));

        assertSame(actionFailure, thrown);
        assertSame(actionFailure, stickyFailure(fixture.service).get());
        assertCurrentTableCachesRemoved(fixture.service);
        assertOtherTableCachesPreserved(fixture.service);
    }

    @Test
    void flushFailureMustFenceWritesSkipCatalogActionAndInvalidateCaches() throws Exception {
        Fixture fixture = fixture();
        seedDerivedCaches(fixture.service);
        IOException flushFailure = new IOException("flush-failure");
        PaimonBucketWriterStrategy writerStrategy = mock(PaimonBucketWriterStrategy.class);
        when(writerStrategy.prepareCommit(0L)).thenThrow(flushFailure);
        PaimonTableWriteContext context = context(writerStrategy);
        map(fixture.service, "tableWriteContexts").put(TABLE_KEY, context);
        coordinator(fixture.service).acceptInitial(TABLE_KEY, 1);

        Exception thrown =
                assertThrows(Exception.class, () -> fixture.service.dropTable(TABLE_NAME));

        assertSame(flushFailure, thrown);
        assertSame(flushFailure, stickyFailure(fixture.service).get());
        verify(fixture.catalog, never()).dropTable(any(Identifier.class), eq(true));
        assertCurrentTableCachesRemoved(fixture.service);
        assertOtherTableCachesPreserved(fixture.service);
    }

    @Test
    void contextCloseFailureMustSkipCatalogActionAndInvalidateCaches() throws Exception {
        Fixture fixture = fixture();
        seedDerivedCaches(fixture.service);
        IOException closeFailure = new IOException("context-close-failure");
        PaimonBucketWriterStrategy writerStrategy = mock(PaimonBucketWriterStrategy.class);
        doThrow(closeFailure).when(writerStrategy).close();
        map(fixture.service, "tableWriteContexts")
                .put(TABLE_KEY, context(writerStrategy));

        Exception thrown =
                assertThrows(Exception.class, () -> fixture.service.dropTable(TABLE_NAME));

        assertSame(closeFailure, thrown);
        assertSame(closeFailure, stickyFailure(fixture.service).get());
        verify(fixture.catalog, never()).dropTable(any(Identifier.class), eq(true));
        assertCurrentTableCachesRemoved(fixture.service);
        assertOtherTableCachesPreserved(fixture.service);
    }

    @Test
    void serviceCleanupMustClearTheSameGlobalDerivedCacheSet() throws Exception {
        Fixture fixture = fixture();
        seedDerivedCaches(fixture.service);

        fixture.service.close();

        assertTrue(map(fixture.service, "paimonFieldCache").isEmpty());
        assertTrue(map(fixture.service, "computeHashKey").isEmpty());
        assertTrue(map(fixture.service, "primaryKeyMap").isEmpty());
    }

    @Test
    void failedWriteContextCreationMustNotPublishFieldCache() throws Exception {
        Fixture fixture = fixture();
        FileStoreTable physicalTable = mock(FileStoreTable.class);
        RowType rowType = mock(RowType.class);
        when(rowType.getFields())
                .thenReturn(Collections.singletonList(new DataField(0, "id", DataTypes.INT())));
        when(physicalTable.rowType()).thenReturn(rowType);
        when(physicalTable.location())
                .thenReturn(new Path("file:/tmp/paimon-b3-cache-publication-test"));
        Method createContext =
                PaimonService.class.getDeclaredMethod(
                        "getOrCreateTableWriteContext",
                        String.class,
                        String.class,
                        Identifier.class,
                        TapConnectorContext.class,
                        FileStoreTable.class,
                        PaimonWriteSemanticContract.class);
        createContext.setAccessible(true);

        InvocationTargetException thrown =
                assertThrows(
                        InvocationTargetException.class,
                        () ->
                                createContext.invoke(
                                        fixture.service,
                                        TABLE_KEY,
                                        TABLE_NAME,
                                        Identifier.create("default", TABLE_NAME),
                                        mock(TapConnectorContext.class),
                                        physicalTable,
                                        PaimonWriteSemanticContractTestFactory.forMode(
                                                BucketMode.HASH_FIXED)));

        assertTrue(thrown.getCause() instanceof IllegalStateException);
        assertEquals(
                "Tap task state map is required for stable Paimon commits",
                thrown.getCause().getMessage());
        assertFalse(map(fixture.service, "tableWriteContexts").containsKey(TABLE_KEY));
        assertFalse(map(fixture.service, "paimonFieldCache").containsKey(TABLE_KEY));
    }

    @Test
    void sourceDerivedStateMustReuseOnePrimaryKeySnapshot() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setHashKey(true);
        PaimonService service = new PaimonService(config, mock(Log.class));
        TapTable table = mock(TapTable.class);
        Collection<String> primaryKeys =
                Arrays.asList("pk_1", "pk_2", "pk_3", "pk_4", "pk_5", "pk_6");
        when(table.primaryKeys(true)).thenReturn(primaryKeys);
        Method cacheSourceDerivedState =
                PaimonService.class.getDeclaredMethod(
                        "cacheSourceDerivedState", String.class, TapTable.class);
        cacheSourceDerivedState.setAccessible(true);

        cacheSourceDerivedState.invoke(service, TABLE_NAME, table);

        verify(table, times(1)).primaryKeys(true);
        assertEquals(Boolean.TRUE, map(service, "computeHashKey").get(TABLE_NAME));
        assertSame(primaryKeys, map(service, "primaryKeyMap").get(TABLE_NAME));
    }

    @Test
    @SuppressWarnings("unchecked")
    void sourceDerivedStateMustNotBePopulatedOutsideTheDdlLifecycleLock()
            throws Exception {
        Fixture fixture = fixture();
        FileStoreTable physicalTable = mock(FileStoreTable.class);
        when(physicalTable.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        when(fixture.catalog.getTable(Identifier.create("default", TABLE_NAME)))
                .thenReturn(physicalTable);
        CountDownLatch ddlActionEntered = new CountDownLatch(1);
        CountDownLatch releaseDdlAction = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            ddlActionEntered.countDown();
                            if (!releaseDdlAction.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("Timed out waiting to release DDL action");
                            }
                            return null;
                        })
                .when(fixture.catalog)
                .dropTable(Identifier.create("default", TABLE_NAME), true);

        CountDownLatch writeReachedInternalPreLock = new CountDownLatch(1);
        TapConnectorContext connectorContext = mock(TapConnectorContext.class);
        when(connectorContext.getStateMap()).thenReturn(mock(KVMap.class));
        when(connectorContext.getLog()).thenReturn(mock(Log.class));
        doAnswer(
                        invocation -> {
                            writeReachedInternalPreLock.countDown();
                            return null;
                        })
                .when(connectorContext)
                .configContext();
        TapTable tapTable =
                new TapTable(TABLE_NAME)
                        .add(new TapField("value", "STRING"))
                        .add(new TapField("id", "INT").primaryKeyPos(1));
        TapInsertRecordEvent event =
                new TapInsertRecordEvent()
                        .init()
                        .table(TABLE_NAME)
                        .after(Collections.singletonMap("id", 1));
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "INITIAL_SYNC");

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<?> ddlFuture = null;
        Future<?> writeFuture = null;
        try {
            ddlFuture =
                    executor.submit(
                            () -> {
                                fixture.service.dropTable(TABLE_NAME);
                                return null;
                            });
            assertTrue(ddlActionEntered.await(5, TimeUnit.SECONDS));
            writeFuture =
                    executor.submit(
                            () -> {
                                fixture.service.writeRecords(
                                        Collections.singletonList(event),
                                        tapTable,
                                        connectorContext);
                                return null;
                            });
            assertTrue(writeReachedInternalPreLock.await(5, TimeUnit.SECONDS));

            // The write has completed all work before the lifecycle lock. It must not publish
            // source-derived state from the old generation while CachingCatalog#dropTable still
            // owns that lock.
            // Source:
            // https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/catalog/CachingCatalog.java#L184-L197
            assertFalse(map(fixture.service, "computeHashKey").containsKey(TABLE_NAME));
            assertFalse(map(fixture.service, "primaryKeyMap").containsKey(TABLE_NAME));
        } finally {
            releaseDdlAction.countDown();
            if (ddlFuture != null) {
                ddlFuture.get(5, TimeUnit.SECONDS);
            }
            if (writeFuture != null) {
                try {
                    writeFuture.get(5, TimeUnit.SECONDS);
                } catch (ExecutionException expected) {
                    // The mock table intentionally has no schema; only pre-lock ordering is under
                    // test after the DDL action is released.
                }
            }
            executor.shutdownNow();
        }
    }

    private Fixture fixture() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setWarehouse("/tmp/paimon-ddl-cache-test");
        PaimonService service = new PaimonService(config, mock(Log.class));
        service.startForTest();
        Catalog catalog = mock(Catalog.class);
        setField(service, "catalog", catalog);
        return new Fixture(service, catalog);
    }

    private static PaimonTableWriteContext context(
            PaimonBucketWriterStrategy writerStrategy) {
        return context(TABLE_KEY, TABLE_NAME, writerStrategy);
    }

    private static PaimonTableWriteContext context(
            String tableKey,
            String tableName,
            PaimonBucketWriterStrategy writerStrategy) {
        return new PaimonTableWriteContext(
                tableKey,
                tableName,
                "ddl-cache-test",
                writerStrategy,
                mock(PaimonTableCommitter.class),
                null,
                Collections.emptyList(),
                0L);
    }

    private static void seedDerivedCaches(PaimonService service) throws Exception {
        map(service, "paimonFieldCache").put(TABLE_KEY, Collections.emptyList());
        map(service, "paimonFieldCache").put(OTHER_TABLE_KEY, Collections.emptyList());
        map(service, "computeHashKey").put(TABLE_NAME, Boolean.TRUE);
        map(service, "computeHashKey").put(OTHER_TABLE_NAME, Boolean.FALSE);
        map(service, "primaryKeyMap").put(TABLE_NAME, Collections.singleton("old_pk"));
        map(service, "primaryKeyMap").put(OTHER_TABLE_NAME, Collections.singleton("other_pk"));
    }

    private static void assertCurrentTableCachesRemoved(PaimonService service)
            throws Exception {
        assertFalse(map(service, "paimonFieldCache").containsKey(TABLE_KEY));
        assertFalse(map(service, "computeHashKey").containsKey(TABLE_NAME));
        assertFalse(map(service, "primaryKeyMap").containsKey(TABLE_NAME));
    }

    private static void assertOtherTableCachesPreserved(PaimonService service)
            throws Exception {
        assertTrue(map(service, "paimonFieldCache").containsKey(OTHER_TABLE_KEY));
        assertEquals(Boolean.FALSE, map(service, "computeHashKey").get(OTHER_TABLE_NAME));
        assertEquals(
                Collections.singleton("other_pk"),
                map(service, "primaryKeyMap").get(OTHER_TABLE_NAME));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(PaimonService service, String fieldName)
            throws Exception {
        Field field = PaimonService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Map<String, Object>) field.get(service);
    }

    @SuppressWarnings("unchecked")
    private static AtomicReference<Throwable> stickyFailure(PaimonService service)
            throws Exception {
        Field field = PaimonService.class.getDeclaredField("stickyWriteFailure");
        field.setAccessible(true);
        return (AtomicReference<Throwable>) field.get(service);
    }

    private static void setField(PaimonService service, String fieldName, Object value)
            throws Exception {
        Field field = PaimonService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(service, value);
    }

    private static PaimonMicroBatchCoordinator coordinator(PaimonService service)
            throws Exception {
        Field field = PaimonService.class.getDeclaredField("microBatchCoordinator");
        field.setAccessible(true);
        return (PaimonMicroBatchCoordinator) field.get(service);
    }

    private static final class Fixture {
        private final PaimonService service;
        private final Catalog catalog;

        private Fixture(PaimonService service, Catalog catalog) {
            this.service = service;
            this.catalog = catalog;
        }
    }
}
