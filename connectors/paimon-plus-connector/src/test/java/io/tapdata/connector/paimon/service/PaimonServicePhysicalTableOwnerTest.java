package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.commit.PaimonAsyncCommitScheduler;
import io.tapdata.connector.paimon.commit.PaimonMicroBatchCoordinator;
import io.tapdata.connector.paimon.commit.PaimonServiceLifecycle;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.write.PaimonCompactionLifecycle;
import io.tapdata.connector.paimon.write.PaimonTableCommitter;
import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;
import io.tapdata.entity.logger.Log;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PaimonServicePhysicalTableOwnerTest {

    @Test
    void sameJvmMustRejectASecondOwnerUntilTheFirstOwnerReleasesTheTable() throws Exception {
        PaimonService first = service();
        PaimonService second = service();
        String tableKey = "default.orders";
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.coreOptions()).thenReturn(org.apache.paimon.CoreOptions.fromMap(Collections.emptyMap()));
        when(table.location())
                .thenReturn(new Path("file:///tmp/paimon-owner-" + UUID.randomUUID()));

        Method register = method("registerPhysicalTableOwner", String.class, FileStoreTable.class);
        Method unregister = method("unregisterPhysicalTableOwner", String.class);

        try {
            register.invoke(first, tableKey, table);

            InvocationTargetException duplicate =
                    assertThrows(
                            InvocationTargetException.class,
                            () -> register.invoke(second, tableKey, table));
            assertInstanceOf(IllegalStateException.class, duplicate.getCause());

            unregister.invoke(first, tableKey);
            assertDoesNotThrow(() -> register.invoke(second, tableKey, table));
        } finally {
            unregister.invoke(first, tableKey);
            unregister.invoke(second, tableKey);
        }
    }

    @Test
    void failedAsyncAlterMustReleaseTemporaryOwnerBeforeWriterAllocation() throws Exception {
        PaimonService service = service();
        FileStoreTable table = physicalTable("async-admission");
        when(table.coreOptions()).thenReturn(org.apache.paimon.CoreOptions.fromMap(
                Collections.singletonMap("snapshot.expire.execution-mode", "ASYNC")));
        io.tapdata.entity.utils.cache.KVMap<Object> state = mock(io.tapdata.entity.utils.cache.KVMap.class);
        Field stateField = PaimonService.class.getDeclaredField("boundTaskStateMap");
        stateField.setAccessible(true);
        stateField.set(service, state);
        org.apache.paimon.catalog.Catalog catalog = mock(org.apache.paimon.catalog.Catalog.class);
        Field catalogField = PaimonService.class.getDeclaredField("catalog");
        catalogField.setAccessible(true); catalogField.set(service, catalog);
        IllegalStateException denied = new IllegalStateException("ALTER permission denied");
        org.mockito.Mockito.doThrow(denied).when(catalog).alterTable(
                org.mockito.ArgumentMatchers.any(Identifier.class), org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.eq(false));
        Method admission = contextAdmissionMethod();

        InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                () -> admission.invoke(service, "default.orders", "orders",
                        Identifier.create("default", "orders"), null, table, null));

        assertSame(denied, failure.getCause());
        assertTrue(((Map<?, ?>) serviceField(service, "physicalTableByLogicalTable")).isEmpty());
        assertTrue(unsafeResourceOwners(service).isEmpty());
        assertTrue(tableContexts(service).isEmpty());
        verifyNoInteractions(state);
        verify(table, never()).rowType();
        verify(table, never()).snapshotManager();
        verify(table, never()).newStreamWriteBuilder();
        service.close();
    }

    @Test
    void constructionErrorAfterCompleteRollbackMustReleaseOwnerForNextService() throws Exception {
        PaimonService first = service();
        PaimonService second = service();
        String tableKey = "default.orders";
        FileStoreTable table = physicalTable("construction-error");
        when(table.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        when(table.rowType()).thenReturn(org.apache.paimon.types.RowType.of(org.apache.paimon.types.DataTypes.INT()));
        org.apache.paimon.table.sink.StreamWriteBuilder builder =
                mock(org.apache.paimon.table.sink.StreamWriteBuilder.class);
        org.apache.paimon.table.sink.TableWriteImpl<?> writer =
                mock(org.apache.paimon.table.sink.TableWriteImpl.class);
        when(table.newStreamWriteBuilder()).thenReturn(builder);
        when(builder.withCommitUser("construction-error-owner")).thenReturn(builder);
        when(builder.newWrite()).thenReturn(writer);
        when(writer.getWrite()).thenReturn(mock(org.apache.paimon.operation.AbstractFileStoreWrite.class));
        org.mockito.Mockito.doReturn(writer).when(writer).withCompactExecutor(any());
        org.mockito.Mockito.doReturn(writer).when(writer).withIOManager(any());
        AssertionError original = new AssertionError("native committer 初始化 Error");
        when(builder.newCommit()).thenThrow(original);
        io.tapdata.connector.paimon.commit.PaimonCommitStateStore.Binding binding =
                mock(io.tapdata.connector.paimon.commit.PaimonCommitStateStore.Binding.class);
        when(binding.commitUser()).thenReturn("construction-error-owner");
        when(binding.store()).thenReturn(mock(io.tapdata.connector.paimon.commit.PaimonCommitStateStore.class));
        ((PaimonConfig) serviceField(first, "config")).setWarehouse("file:///tmp/paimon-error-owner-warehouse");
        String physicalHash = io.tapdata.connector.paimon.commit.PaimonCommitStateStore.physicalTableHash(
                table.location().toUri().toString());
        Method register = method("registerPhysicalTableOwner", String.class, FileStoreTable.class);
        Method unregister = method("unregisterPhysicalTableOwner", String.class);
        try (org.mockito.MockedStatic<io.tapdata.connector.paimon.commit.PaimonCommitStateStore> state =
                org.mockito.Mockito.mockStatic(io.tapdata.connector.paimon.commit.PaimonCommitStateStore.class)) {
            state.when(() -> io.tapdata.connector.paimon.commit.PaimonCommitStateStore.physicalTableHash(
                    org.mockito.ArgumentMatchers.anyString())).thenReturn(physicalHash);
            state.when(() -> io.tapdata.connector.paimon.commit.PaimonCommitStateStore.bind(
                    any(), any(), org.mockito.ArgumentMatchers.eq(table), any(PaimonStopController.class))).thenReturn(binding);
            InvocationTargetException thrown = assertThrows(InvocationTargetException.class,
                    () -> contextAdmissionMethod().invoke(first, tableKey, "orders",
                            Identifier.create("default", "orders"), null, table,
                            io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractTestFactory.forMode(
                                    BucketMode.HASH_FIXED)));

            assertSame(original, thrown.getCause());
            verify(writer, times(1)).close();
            assertTrue(tableContexts(first).isEmpty());
            assertTrue(unsafeResourceOwners(first).isEmpty());
            assertTrue(((Map<?, ?>) serviceField(first, "physicalTableByLogicalTable")).isEmpty());
            assertDoesNotThrow(() -> register.invoke(second, tableKey, table));
        } finally {
            unregister.invoke(first, tableKey);
            unregister.invoke(second, tableKey);
            first.close();
            second.close();
        }
    }

    private static Method contextAdmissionMethod() throws Exception {
        return method("getOrCreateTableWriteContext", String.class, String.class, Identifier.class,
                io.tapdata.pdk.apis.context.TapConnectorContext.class, FileStoreTable.class,
                io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract.class);
    }

    @Test
    void closeMustNotReturnAndMustRetainOwnerUntilForegroundFinishes() throws Exception {
        PaimonService first = service();
        PaimonService second = service();
        first.startForTest();
        String tableKey = "default.orders";
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.coreOptions()).thenReturn(org.apache.paimon.CoreOptions.fromMap(Collections.emptyMap()));
        when(table.location())
                .thenReturn(new Path("file:///tmp/paimon-owner-close-" + UUID.randomUUID()));
        Method register = method("registerPhysicalTableOwner", String.class, FileStoreTable.class);
        Method unregister = method("unregisterPhysicalTableOwner", String.class);
        PaimonServiceLifecycle lifecycle = lifecycle(first);
        PaimonServiceLifecycle.Ingress blockedIngress = lifecycle.enter("owner-close-wait");
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer =
                new Thread(
                        () -> {
                            try {
                                first.close();
                            } catch (Throwable failure) {
                                closeFailure.set(failure);
                            }
                        },
                        "paimon-owner-close-wait");
        closer.setDaemon(true);

        try {
            register.invoke(first, tableKey, table);
            closer.start();
            // The foreground drain has no deadline escape: the caller keeps waiting while the
            // blocked ingress runs, and the physical owner stays registered the whole time.
            closer.join(800L);
            org.junit.jupiter.api.Assertions.assertTrue(
                    closer.isAlive(),
                    "close() must not return while the foreground ingress is running");

            InvocationTargetException stillOwned =
                    assertThrows(
                            InvocationTargetException.class,
                            () -> register.invoke(second, tableKey, table));
            assertInstanceOf(IllegalStateException.class, stillOwned.getCause());

            blockedIngress.close();
            closer.join(3_000L);
            org.junit.jupiter.api.Assertions.assertFalse(closer.isAlive());
            org.junit.jupiter.api.Assertions.assertNull(closeFailure.get());
            awaitClosed(lifecycle);
            assertDoesNotThrow(() -> register.invoke(second, tableKey, table));
        } finally {
            blockedIngress.close();
            unregister.invoke(first, tableKey);
            unregister.invoke(second, tableKey);
        }
    }

    @Test
    void blockedCompactionMustKeepOwnerUntilCompleteResourceCleanup() throws Exception {
        try (DdlOwnerFixture fixture = new DdlOwnerFixture()) {
            fixture.startCompaction();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread close = closeInThread(fixture.first, failure);
            try {
                close.join(500L);
                assertTrue(close.isAlive(), "Compaction 尚未退出时 close 不能返回");
                assertFalse(fixture.context.cleanupComplete());
                fixture.assertOwnerRejected();
                fixture.assertResourcesNotClosed();
            } finally {
                fixture.releaseCompaction.countDown();
            }
            close.join(3_000L);
            assertFalse(close.isAlive());
            assertNull(failure.get());
            assertTrue(fixture.context.cleanupComplete());
            assertTrue(fixture.lifecycle.compactionExecutor().isTerminated());
            fixture.assertResourcesClosedOnce();
            assertDoesNotThrow(() -> fixture.register.invoke(
                    fixture.second, DdlOwnerFixture.TABLE_KEY, fixture.physicalTable));
            // 旧 token 的迟到清理不能误释放第二代的所有权。
            fixture.unregister.invoke(fixture.first, DdlOwnerFixture.TABLE_KEY);
            PaimonService third = service();
            try {
                InvocationTargetException rejected = assertThrows(InvocationTargetException.class,
                        () -> fixture.register.invoke(third, DdlOwnerFixture.TABLE_KEY, fixture.physicalTable));
                assertInstanceOf(IllegalStateException.class, rejected.getCause());
            } finally {
                third.close();
            }
        }
    }

    @Test
    void synchronousMaintenanceInsideBusinessCommitMustKeepOwnerAndBlockStop() throws Exception {
        try (DdlOwnerFixture fixture = new DdlOwnerFixture()) {
            CountDownLatch commitEntered = new CountDownLatch(1);
            CountDownLatch releaseCommit = new CountDownLatch(1);
            doAnswer(invocation -> {
                commitEntered.countDown();
                awaitUninterruptibly(releaseCommit);
                return null;
            }).when(fixture.committer).commit(0L, fixture.messages);
            fixture.coordinator.acceptInitial(DdlOwnerFixture.TABLE_KEY, 1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread close = closeInThread(fixture.first, failure);
            try {
                assertTrue(commitEntered.await(5L, TimeUnit.SECONDS));
                close.join(400L);
                assertTrue(close.isAlive());
                fixture.assertOwnerRejected();
                fixture.assertResourcesNotClosed();
            } finally {
                releaseCommit.countDown();
            }
            close.join(3_000L);
            assertFalse(close.isAlive());
            assertNull(failure.get());
            fixture.assertResourcesClosedOnce();
            assertDoesNotThrow(() -> fixture.register.invoke(
                    fixture.second, DdlOwnerFixture.TABLE_KEY, fixture.physicalTable));
        }
    }

    @ParameterizedTest(name = "{0}: {1} 必须保留旧上下文及 owner")
    @CsvSource({
            "CLEAR, PREPARE_FAILURE",
            "DROP, PREPARE_FAILURE",
            "CLEAR, PENDING_RETRY_EXHAUSTED",
            "DROP, PENDING_RETRY_EXHAUSTED"
    })
    void failedDdlDrainMustRetainContextPendingAndOwnerUntilNormalClose(
            DdlOperation operation, DrainFailure failureMode) throws Exception {
        try (DdlOwnerFixture fixture = new DdlOwnerFixture()) {
            fixture.startCompaction();
            fixture.coordinator.acceptInitial(DdlOwnerFixture.TABLE_KEY, 1);
            Exception expected;
            if (failureMode == DrainFailure.PREPARE_FAILURE) {
                expected = new IOException("DDL prepare 失败");
                when(fixture.strategy.prepareCommit(0L)).thenThrow(expected);
            } else {
                RuntimeException directFailure = new RuntimeException("直接提交结果未知");
                RuntimeException pendingFailure = new RuntimeException("pending 确认持续失败");
                doThrow(directFailure)
                        .when(fixture.committer).commit(0L, fixture.messages);
                when(fixture.committer.filterAndCommit(anyMap()))
                        .thenAnswer(ignored -> {
                            throw new RuntimeException("pending 确认持续失败");
                        });
                assertThrows(RuntimeException.class, fixture.context::commit);
                AtomicInteger pendingAttempts = new AtomicInteger();
                doAnswer(ignored -> {
                    if (pendingAttempts.getAndIncrement() == 0) {
                        throw pendingFailure;
                    }
                    throw new RuntimeException("pending 重试持续失败");
                }).when(fixture.committer).filterAndCommit(anyMap());
                fixture.coordinator.markPendingCommit(
                        fixture.coordinator.captureCommitTarget(DdlOwnerFixture.TABLE_KEY));
                expected = pendingFailure;
            }

            Map<Long, List<CommitMessage>> pendingBefore = pendingCommits(fixture.context);
            PaimonMicroBatchCoordinator.CommitTarget targetBefore =
                    fixture.coordinator.pendingCommitTarget(DdlOwnerFixture.TABLE_KEY);

            Exception thrown = assertThrows(
                    Exception.class, () -> operation.run(fixture.first));

            assertSame(expected, thrown);
            assertSame(fixture.context, tableContexts(fixture.first).get(DdlOwnerFixture.TABLE_KEY));
            assertEquals(pendingBefore, pendingCommits(fixture.context));
            assertSame(targetBefore,
                    fixture.coordinator.pendingCommitTarget(DdlOwnerFixture.TABLE_KEY));
            assertEquals(1L,
                    fixture.coordinator.tableSnapshot(DdlOwnerFixture.TABLE_KEY).bufferedRecordCount());
            assertEquals(failureMode == DrainFailure.PENDING_RETRY_EXHAUSTED,
                    fixture.context.hasPendingCommit());
            assertEquals(failureMode == DrainFailure.PENDING_RETRY_EXHAUSTED ? 3 : 0,
                    fixture.retryWaits.get(), "pending 必须耗尽现有的三次重试后才退出 DDL");
            verifyNoInteractions(fixture.catalog);
            assertFalse(fixture.lifecycle.compactionExecutor().isShutdown(), "DDL drain 失败不能停止旧 Compaction");
            assertFalse(fixture.context.cleanupComplete(), "DDL drain 失败不能提前关闭 Context");
            fixture.assertOwnerRejected();
            fixture.assertResourcesNotClosed();

            // 业务 FAILED 与清理证明独立：保持原错误，但完整等待任务并关闭后允许下一代恢复。
            AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            Thread close = closeInThread(fixture.first, closeFailure);
            try {
                close.join(500L);
                assertTrue(close.isAlive());
                fixture.assertOwnerRejected();
                fixture.assertResourcesNotClosed();
            } finally {
                fixture.releaseCompaction.countDown();
            }
            close.join(3_000L);
            assertFalse(close.isAlive());
            assertSame(expected, closeFailure.get());
            assertTrue(fixture.context.cleanupComplete());
            assertTrue(fixture.lifecycle.compactionExecutor().isTerminated());
            assertDoesNotThrow(() -> fixture.register.invoke(
                    fixture.second, DdlOwnerFixture.TABLE_KEY, fixture.physicalTable));
            fixture.assertResourcesClosedOnce();
        }
    }

    @ParameterizedTest(name = "{0}: action 完成之前不得释放 owner")
    @EnumSource(DdlOperation.class)
    void successfulDdlMustHoldOwnerThroughoutCatalogAction(DdlOperation operation) throws Exception {
        try (DdlOwnerFixture fixture = new DdlOwnerFixture()) {
            CountDownLatch actionEntered = new CountDownLatch(1);
            CountDownLatch releaseAction = new CountDownLatch(1);
            operation.blockAction(fixture, actionEntered, releaseAction);
            ExecutorService ddlExecutor = daemonExecutor("paimon-owner-ddl-action");
            try {
                Future<?> ddl = ddlExecutor.submit(() -> {
                    operation.run(fixture.first);
                    return null;
                });
                assertTrue(actionEntered.await(5L, TimeUnit.SECONDS));
                assertTrue(fixture.context.cleanupComplete());
                fixture.assertResourcesClosedOnce();
                fixture.assertOwnerRejected();
                assertFalse(ddl.isDone(), "阻塞的 DDL action 仍未完成");

                releaseAction.countDown();
                ddl.get(5L, TimeUnit.SECONDS);
                assertDoesNotThrow(() -> fixture.register.invoke(
                        fixture.second, DdlOwnerFixture.TABLE_KEY, fixture.physicalTable));
                operation.verifyAction(fixture);
            } finally {
                releaseAction.countDown();
                ddlExecutor.shutdown();
                assertTrue(ddlExecutor.awaitTermination(5L, TimeUnit.SECONDS), "DDL worker 必须退出");
            }
        }
    }

    @ParameterizedTest(name = "truncateFailure={0}, closeFailure={1}")
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void clearTableMustCloseTemporaryCommitterOnceAndPreserveFailures(
            boolean truncateFails, boolean closeFails) throws Exception {
        try (DdlOwnerFixture fixture = new DdlOwnerFixture()) {
            DdlOperation.CLEAR.blockAction(fixture, new CountDownLatch(1), new CountDownLatch(0));
            RuntimeException actionFailure = new RuntimeException("truncate 失败");
            IOException closeFailure = new IOException("临时 committer close 失败");
            if (truncateFails) {
                doThrow(actionFailure).when(fixture.truncateCommitter).truncateTable();
            }
            if (closeFails) {
                doThrow(closeFailure).when(fixture.truncateCommitter).close();
            }

            if (truncateFails || closeFails) {
                Exception failure = assertThrows(Exception.class, () -> DdlOperation.CLEAR.run(fixture.first));
                assertSame(truncateFails ? actionFailure : closeFailure, failure);
                if (truncateFails && closeFails) {
                    assertEquals(Collections.singletonList(closeFailure),
                            java.util.Arrays.asList(failure.getSuppressed()));
                }
            } else {
                assertDoesNotThrow(() -> DdlOperation.CLEAR.run(fixture.first));
            }
            verify(fixture.truncateCommitter, times(1)).truncateTable();
            verify(fixture.truncateCommitter, times(1)).close();
            if (closeFails) {
                fixture.assertOwnerRejected();
                assertTrue(unsafeResourceOwners(fixture.first).contains(DdlOwnerFixture.TABLE_KEY));
            } else {
                assertDoesNotThrow(() -> fixture.register.invoke(
                        fixture.second, DdlOwnerFixture.TABLE_KEY, fixture.physicalTable));
            }
        }
    }

    @Test
    void temporarySynchronousCommitterCloseMustKeepOwnerUntilItReturns() throws Exception {
        try (DdlOwnerFixture fixture = new DdlOwnerFixture()) {
            DdlOperation.CLEAR.blockAction(fixture, new CountDownLatch(1), new CountDownLatch(0));
            CountDownLatch closeEntered = new CountDownLatch(1);
            CountDownLatch releaseClose = new CountDownLatch(1);
            doAnswer(invocation -> {
                closeEntered.countDown();
                awaitUninterruptibly(releaseClose);
                return null;
            }).when(fixture.truncateCommitter).close();
            ExecutorService executor = daemonExecutor("paimon-sync-ddl-close");
            try {
                Future<?> ddl = executor.submit(() -> {
                    DdlOperation.CLEAR.run(fixture.first);
                    return null;
                });
                assertTrue(closeEntered.await(5L, TimeUnit.SECONDS));
                assertFalse(ddl.isDone());
                fixture.assertOwnerRejected();
                releaseClose.countDown();
                ddl.get(5L, TimeUnit.SECONDS);
                verify(fixture.truncateCommitter, times(1)).truncateTable();
                verify(fixture.truncateCommitter, times(1)).close();
                assertDoesNotThrow(() -> fixture.register.invoke(
                        fixture.second, DdlOwnerFixture.TABLE_KEY, fixture.physicalTable));
            } finally {
                releaseClose.countDown();
                executor.shutdown();
                assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
            }
        }
    }

    @ParameterizedTest(name = "{0}: Writer 关闭失败必须保留 owner")
    @EnumSource(DdlOperation.class)
    void resourceCloseFailureMustRetainOwnerAndSkipDdlAction(DdlOperation operation)
            throws Exception {
        try (DdlOwnerFixture fixture = new DdlOwnerFixture()) {
            IOException closeFailure = new IOException("Writer 资源关闭失败");
            doThrow(closeFailure).when(fixture.strategy).close();

            Exception thrown = assertThrows(Exception.class, () -> operation.run(fixture.first));

            assertSame(closeFailure, thrown);
            assertFalse(fixture.context.cleanupComplete());
            verify(fixture.strategy, times(1)).close();
            verify(fixture.committer, never()).close();
            verify(fixture.ioManager, never()).close();
            verifyNoInteractions(fixture.catalog);
            fixture.assertOwnerRejected();
            assertTrue(unsafeResourceOwners(fixture.first).contains(DdlOwnerFixture.TABLE_KEY));
        }
    }

    @ParameterizedTest(name = "{0}: Compaction 完成之前不得执行 DDL 或释放 owner")
    @EnumSource(DdlOperation.class)
    void ddlMustWaitForCompactionThenCleanResourcesBeforeAction(DdlOperation operation)
            throws Exception {
        try (DdlOwnerFixture fixture = new DdlOwnerFixture()) {
            fixture.startCompaction();
            CountDownLatch actionEntered = new CountDownLatch(1);
            operation.blockAction(fixture, actionEntered, new CountDownLatch(0));
            ExecutorService executor = daemonExecutor("paimon-ddl-wait-compaction");
            try {
                Future<?> ddl = executor.submit(() -> {
                    operation.run(fixture.first);
                    return null;
                });
                assertFalse(actionEntered.await(400L, TimeUnit.MILLISECONDS));
                assertFalse(ddl.isDone());
                fixture.assertOwnerRejected();
                fixture.assertResourcesNotClosed();
                fixture.releaseCompaction.countDown();
                ddl.get(5L, TimeUnit.SECONDS);
                assertTrue(fixture.context.cleanupComplete());
                fixture.assertResourcesClosedOnce();
                operation.verifyAction(fixture);
                verify(fixture.strategy, never()).prepareFinalCommit(anyLong());
                assertDoesNotThrow(() -> fixture.register.invoke(
                        fixture.second, DdlOwnerFixture.TABLE_KEY, fixture.physicalTable));
            } finally {
                fixture.releaseCompaction.countDown();
                executor.shutdown();
                assertTrue(executor.awaitTermination(5L, TimeUnit.SECONDS));
            }
        }
    }

    @ParameterizedTest(name = "{0}: 未知关闭结果不能释放 owner")
    @EnumSource(DdlOperation.class)
    void unknownCleanupResultMustKeepOwnerAndSkipDdlAction(DdlOperation operation) throws Exception {
        try (DdlOwnerFixture fixture = new DdlOwnerFixture()) {
            IOException unknown = new IOException("关闭结果未知");
            PaimonTableWriteContext unknownContext = mock(PaimonTableWriteContext.class);
            when(unknownContext.tableKey()).thenReturn(DdlOwnerFixture.TABLE_KEY);
            when(unknownContext.tableName()).thenReturn(DdlOwnerFixture.TABLE_NAME);
            doThrow(unknown).when(unknownContext).close();
            when(unknownContext.closeForStop(org.mockito.ArgumentMatchers.anyBoolean(), any()))
                    .thenThrow(unknown);
            tableContexts(fixture.first).put(DdlOwnerFixture.TABLE_KEY, unknownContext);

            assertSame(unknown, assertThrows(Exception.class, () -> operation.run(fixture.first)));

            verifyNoInteractions(fixture.catalog);
            fixture.assertOwnerRejected();
            assertTrue(unsafeResourceOwners(fixture.first).contains(DdlOwnerFixture.TABLE_KEY));
        }
    }

    @ParameterizedTest(name = "{0}: DDL action Error 必须传播并禁止 STOP 正常退出")
    @EnumSource(DdlOperation.class)
    void ddlActionErrorMustBeStickyWithoutHoldingSafelyClosedOwner(DdlOperation operation)
            throws Exception {
        try (DdlOwnerFixture fixture = new DdlOwnerFixture()) {
            operation.blockAction(fixture, new CountDownLatch(1), new CountDownLatch(0));
            AssertionError original = new AssertionError("DDL action Error: " + operation);
            if (operation == DdlOperation.CLEAR) {
                doThrow(original).when(fixture.truncateCommitter).truncateTable();
            } else {
                doThrow(original).when(fixture.catalog).dropTable(DdlOwnerFixture.IDENTIFIER, true);
            }

            assertSame(original, assertThrows(AssertionError.class, () -> operation.run(fixture.first)));
            assertSame(original, assertThrows(AssertionError.class, fixture.first::close));

            assertTrue(fixture.context.cleanupComplete());
            fixture.assertResourcesClosedOnce();
            verify(fixture.strategy, never()).prepareFinalCommit(anyLong());
            if (operation == DdlOperation.CLEAR) {
                verify(fixture.truncateCommitter, times(1)).close();
            }
            assertFalse(org.mockito.Mockito.mockingDetails(fixture.auditLog).getInvocations().stream()
                    .flatMap(invocation -> java.util.Arrays.stream(invocation.getArguments()))
                    .filter(String.class::isInstance).map(String.class::cast)
                    .anyMatch(message -> message.contains("正常退出")));
            assertDoesNotThrow(() -> fixture.register.invoke(
                    fixture.second, DdlOwnerFixture.TABLE_KEY, fixture.physicalTable));
        }
    }

    @ParameterizedTest(name = "{0}: 无 Context 的第二个 Service 不得越过旧 writer owner")
    @EnumSource(DdlOperation.class)
    void secondServiceWithoutContextMustRejectDdlAgainstOwnedPhysicalTable(DdlOperation operation)
            throws Exception {
        try (DdlOwnerFixture fixture = new DdlOwnerFixture()) {
            fixture.second.startForTest();
            Catalog secondCatalog = mock(Catalog.class);
            when(secondCatalog.getTable(DdlOwnerFixture.IDENTIFIER)).thenReturn(fixture.physicalTable);
            BatchWriteBuilder builder = mock(BatchWriteBuilder.class);
            when(fixture.physicalTable.newBatchWriteBuilder()).thenReturn(builder);
            Field catalogField = PaimonService.class.getDeclaredField("catalog");
            catalogField.setAccessible(true);
            catalogField.set(fixture.second, secondCatalog);
            assertTrue(tableContexts(fixture.second).isEmpty());

            IllegalStateException rejected = assertThrows(IllegalStateException.class,
                    () -> operation.run(fixture.second));

            assertTrue(rejected.getMessage().contains("owns the target physical table"));
            verify(secondCatalog, never()).dropTable(DdlOwnerFixture.IDENTIFIER, true);
            verify(fixture.physicalTable, never()).newBatchWriteBuilder();
            verify(builder, never()).newCommit();
            assertFalse(fixture.context.cleanupComplete());
            fixture.assertResourcesNotClosed();
            fixture.assertOwnerRejected();
            assertTrue(((Map<?, ?>) serviceField(fixture.second, "physicalTableByLogicalTable")).isEmpty());
            assertSame(rejected, assertThrows(IllegalStateException.class, fixture.second::close));
        }
    }

    private enum DrainFailure {
        PREPARE_FAILURE,
        PENDING_RETRY_EXHAUSTED
    }

    private enum DdlOperation {
        CLEAR,
        DROP;

        void run(PaimonService service) throws Exception {
            if (this == CLEAR) {
                service.clearTable(DdlOwnerFixture.TABLE_NAME);
            } else {
                service.dropTable(DdlOwnerFixture.TABLE_NAME);
            }
        }

        void blockAction(DdlOwnerFixture fixture, CountDownLatch entered, CountDownLatch release)
                throws Exception {
            org.mockito.stubbing.Answer<Void> action = ignored -> {
                entered.countDown();
                awaitUninterruptibly(release);
                return null;
            };
            if (this == CLEAR) {
                BatchWriteBuilder builder = mock(BatchWriteBuilder.class);
                when(fixture.catalog.getTable(DdlOwnerFixture.IDENTIFIER))
                        .thenReturn(fixture.physicalTable);
                when(fixture.physicalTable.newBatchWriteBuilder()).thenReturn(builder);
                when(builder.newCommit()).thenReturn(fixture.truncateCommitter);
                doAnswer(action).when(fixture.truncateCommitter).truncateTable();
            } else {
                doAnswer(action).when(fixture.catalog).dropTable(DdlOwnerFixture.IDENTIFIER, true);
            }
        }

        void verifyAction(DdlOwnerFixture fixture) throws Exception {
            if (this == CLEAR) {
                verify(fixture.truncateCommitter, times(1)).truncateTable();
                verify(fixture.truncateCommitter, times(1)).close();
            } else {
                verify(fixture.catalog, times(1)).dropTable(DdlOwnerFixture.IDENTIFIER, true);
            }
        }
    }

    private static final class DdlOwnerFixture implements AutoCloseable {
        private static final String TABLE_NAME = "orders";
        private static final String TABLE_KEY = "default." + TABLE_NAME;
        private static final Identifier IDENTIFIER = Identifier.create("default", TABLE_NAME);

        private final AtomicInteger retryWaits = new AtomicInteger();
        private final Log auditLog = mock(Log.class);
        private final PaimonService first;
        private final PaimonService second = service();
        private final Catalog catalog = mock(Catalog.class);
        private final FileStoreTable physicalTable = physicalTable("ddl-owner");
        private final Method register =
                method("registerPhysicalTableOwner", String.class, FileStoreTable.class);
        private final Method unregister = method("unregisterPhysicalTableOwner", String.class);
        private final TableCommitImpl truncateCommitter = mock(TableCommitImpl.class);
        private final PaimonCompactionLifecycle lifecycle = PaimonCompactionLifecycle.forTable(TABLE_KEY);
        private final CountDownLatch releaseCompaction = new CountDownLatch(1);
        private final PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        private final PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        private final IOManager ioManager = mock(IOManager.class);
        private final List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
        private final List<String> spillDirs = Collections.singletonList(
                "/tmp/paimon-io-ddl-owner-" + UUID.randomUUID());
        private final PaimonTableWriteContext context;
        private final PaimonMicroBatchCoordinator coordinator;

        private DdlOwnerFixture() throws Exception {
            PaimonConfig config = new PaimonConfig();
            config.setDatabase("default");
            config.setBatchAccumulationSize(100);
            config.setCommitIntervalMs(30_000);
            config.setEnableAsyncCommit(false);
            first = new PaimonService(config, auditLog, () -> 100L,
                    retryWaits::incrementAndGet, PaimonAsyncCommitScheduler::newDaemonExecutor);
            first.startForTest();
            Field catalogField = PaimonService.class.getDeclaredField("catalog");
            catalogField.setAccessible(true);
            catalogField.set(first, catalog);
            coordinator = (PaimonMicroBatchCoordinator) serviceField(first, "microBatchCoordinator");

            when(strategy.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
            when(strategy.prepareCommit(0L)).thenReturn(messages);
            when(strategy.prepareFinalCommit(anyLong())).thenReturn(Collections.emptyList());
            context = new PaimonTableWriteContext(TABLE_KEY, TABLE_NAME, "ddl-owner-test",
                    strategy, committer, ioManager, spillDirs, 0L,
                    PaimonTableWriteContext.CommitStateStore.NOOP, lifecycle);
            register.invoke(first, TABLE_KEY, physicalTable);
            tableContexts(first).put(TABLE_KEY, context);
        }

        private void startCompaction() throws Exception {
            CountDownLatch started = new CountDownLatch(1);
            lifecycle.compactionExecutor().submit(new CompactTask(null) {
                @Override
                protected CompactResult doCompact() {
                    started.countDown();
                    awaitUninterruptibly(releaseCompaction);
                    return new CompactResult();
                }
            });
            assertTrue(started.await(5L, TimeUnit.SECONDS));
        }

        private void assertOwnerRejected() {
            InvocationTargetException rejected = assertThrows(InvocationTargetException.class,
                    () -> register.invoke(second, TABLE_KEY, physicalTable));
            assertInstanceOf(IllegalStateException.class, rejected.getCause());
        }

        private void assertResourcesNotClosed() throws Exception {
            verify(strategy, never()).close();
            verify(committer, never()).close();
            verify(ioManager, never()).close();
        }

        private void assertResourcesClosedOnce() throws Exception {
            verify(strategy, times(1)).close();
            verify(committer, times(1)).close();
            verify(ioManager, times(1)).close();
        }

        @Override
        public void close() throws Exception {
            releaseCompaction.countDown();
            try {
                first.close();
            } catch (Exception | Error expectedAfterInjectedFailure) {
                // 故障用例的 Service 保留原始失败；资源退出由下面的终止断言独立验证。
            } finally {
                lifecycle.compactionExecutor().shutdown();
                assertTrue(lifecycle.compactionExecutor().awaitTermination(5L, TimeUnit.SECONDS),
                        "旧 compaction 必须退出");
                awaitServiceCloseWorker(first);
                PaimonStopTestSupport.releaseInjectedOwner(first, TABLE_KEY);
                PaimonStopTestSupport.releaseInjectedOwner(second, TABLE_KEY);
                try { second.close(); }
                catch (Exception | Error expectedAfterInjectedFailure) {
                    // 第二个 Service 的 DDL 拒绝也保留原始失败，断言在用例主体中完成。
                }
            }
        }
    }

    private static Object serviceField(PaimonService service, String name) throws Exception {
        Field field = PaimonService.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(service);
    }

    @SuppressWarnings("unchecked")
    private static Map<Long, List<CommitMessage>> pendingCommits(PaimonTableWriteContext context)
            throws Exception {
        Field field = PaimonTableWriteContext.class.getDeclaredField("pendingCommits");
        field.setAccessible(true);
        return new LinkedHashMap<>((Map<Long, List<CommitMessage>>) field.get(context));
    }

    private static Set<?> unsafeResourceOwners(PaimonService service) throws Exception {
        return (Set<?>) serviceField(service, "unsafeResourceOwners");
    }

    private static void awaitServiceCloseWorker(PaimonService service) throws Exception {
        Object operation = serviceField(service, "closeOperation");
        if (operation != null) {
            Field workerField = operation.getClass().getDeclaredField("worker");
            workerField.setAccessible(true);
            Thread worker = (Thread) workerField.get(operation);
            if (worker != null) {
                worker.join(5_000L);
                assertFalse(worker.isAlive(), "Service close worker 必须退出");
            }
        }
    }

    private static PaimonService service() {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(100);
        config.setCommitIntervalMs(30_000);
        config.setEnableAsyncCommit(false);
        return new PaimonService(config, mock(Log.class), () -> 100L, () -> { });
    }

    private static PaimonServiceLifecycle lifecycle(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("lifecycle");
        field.setAccessible(true);
        return (PaimonServiceLifecycle) field.get(service);
    }

    private static void awaitClosed(PaimonServiceLifecycle lifecycle) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            if (lifecycle.state() == PaimonServiceLifecycle.State.CLOSED) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("Timed out waiting for deferred close");
    }

    private static Method method(String name, Class<?>... parameterTypes) throws Exception {
        Method method = PaimonService.class.getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        return method;
    }

    private static FileStoreTable physicalTable(String suffix) {
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.coreOptions()).thenReturn(org.apache.paimon.CoreOptions.fromMap(Collections.emptyMap()));
        when(table.location())
                .thenReturn(new Path("file:///tmp/paimon-owner-" + suffix + '-' + UUID.randomUUID()));
        return table;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, PaimonTableWriteContext> tableContexts(PaimonService service)
            throws Exception {
        Field field = PaimonService.class.getDeclaredField("tableWriteContexts");
        field.setAccessible(true);
        return (Map<String, PaimonTableWriteContext>) field.get(service);
    }

    private static ExecutorService daemonExecutor(String threadName) {
        return Executors.newSingleThreadExecutor(
                runnable -> {
                    Thread thread = new Thread(runnable, threadName);
                    thread.setDaemon(true);
                    return thread;
                });
    }

    private static Thread closeInThread(PaimonService service, AtomicReference<Throwable> failure) {
        Thread thread = new Thread(() -> {
            try {
                service.close();
            } catch (Throwable caught) {
                failure.set(caught);
            }
        }, "paimon-owner-stop-test");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    latch.await();
                    return;
                } catch (InterruptedException interruption) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
