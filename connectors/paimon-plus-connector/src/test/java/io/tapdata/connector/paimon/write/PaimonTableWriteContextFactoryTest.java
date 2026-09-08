package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterRuntimeFactory;
import io.tapdata.connector.paimon.util.PaimonSpillDirCleaner;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContract;
import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractResolver;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import java.util.Collections;
import java.util.Optional;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonTableWriteContextFactoryTest {

    private static final String COMMIT_USER = "factory-test-user";

    @Test
    void asyncTableMustFailBeforeWriterOrIoAllocation() {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        when(fixture.coreOptions.snapshotExpireExecutionMode())
                .thenReturn(CoreOptions.ExpireExecutionMode.ASYNC);
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, fixture::create);
        assertTrue(failure.getMessage().contains("snapshot.expire.execution-mode"));
        verify(fixture.table, never()).newStreamWriteBuilder();
        verify(fixture.builder, never()).newWrite();
    }

    @Test
    void successfulContextMustOwnAndCloseWriterBeforeCommitterExactlyOnce() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);

        PaimonTableWriteContext context = fixture.create();
        org.junit.jupiter.api.Assertions.assertEquals(
                BucketMode.HASH_FIXED, context.writeSemanticContract().bucketMode());
        context.close();
        context.close();

        InOrder order = inOrder(fixture.writer, fixture.committer);
        order.verify(fixture.writer).close();
        order.verify(fixture.committer).close();
        verify(fixture.writer).close();
        verify(fixture.committer).close();
    }

    @Test
    void preResolvedContractMustBeSharedByContextAndStrategy() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        PaimonWriteSemanticContract contract =
                PaimonWriteSemanticContractResolver.resolve("default.t", fixture.table);

        try (PaimonTableWriteContext context =
                PaimonTableWriteContextFactory.create(
                        "default.t",
                        "t",
                        fixture.table,
                        COMMIT_USER,
                        null,
                        0L,
                        PaimonTableWriteContext.CommitStateStore.NOOP,
                        fixture.runtimeFactory,
                        contract)) {
            assertSame(contract, context.writeSemanticContract());
        }
    }

    @Test
    void committerCreationFailureMustCloseAlreadyCreatedWriter() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        RuntimeException failure = new RuntimeException("committer creation failed");
        when(fixture.builder.newCommit()).thenThrow(failure);

        RuntimeException thrown = assertThrows(RuntimeException.class, fixture::create);

        assertSame(failure, thrown);
        verify(fixture.writer).close();
    }

    @Test
    void strategyConstructionFailureMustCloseWriterThenCommitter() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_DYNAMIC);
        TableSchema schema =
                TableSchema.create(
                        0L,
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("value", DataTypes.STRING())
                                .primaryKey("id")
                                .option("bucket", "-1")
                                .build());
        when(fixture.table.schema()).thenReturn(schema);
        when(fixture.table.primaryKeys()).thenReturn(Collections.singletonList("id"));
        when(fixture.table.partitionKeys()).thenReturn(Collections.emptyList());
        RuntimeException failure = new RuntimeException("assigner creation failed");
        when(fixture.runtimeFactory.createHashBucketAssigner(fixture.table, COMMIT_USER))
                .thenThrow(failure);

        RuntimeException thrown = assertThrows(RuntimeException.class, fixture::create);

        assertSame(failure, thrown);
        InOrder order = inOrder(fixture.writer, fixture.committer);
        order.verify(fixture.writer).close();
        order.verify(fixture.committer).close();
    }

    @Test
    void invalidIdentifierMustFailBeforeAllocatingPaimonResources() {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        PaimonTableWriteContextFactory.create(
                                "default.t",
                                "t",
                                fixture.table,
                                COMMIT_USER,
                                null,
                                -1L,
                                PaimonTableWriteContext.CommitStateStore.NOOP,
                                fixture.runtimeFactory));

        verify(fixture.table, never()).newStreamWriteBuilder();
    }

    @Test
    void unsupportedCrossPartitionMergeEngineMustFailBeforeAllocatingPaimonResources() {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        TableSchema schema =
                TableSchema.create(
                        0L,
                        Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("pt", DataTypes.STRING())
                                .partitionKeys("pt")
                                .primaryKey("id")
                                .build());
        when(fixture.table.schema()).thenReturn(schema);
        when(fixture.table.rowType()).thenReturn(schema.logicalRowType());
        when(fixture.table.primaryKeys()).thenReturn(schema.primaryKeys());
        when(fixture.table.partitionKeys()).thenReturn(schema.partitionKeys());
        when(fixture.coreOptions.mergeEngine()).thenReturn(MergeEngine.FIRST_ROW);

        PaimonFatalWriteException thrown =
                assertThrows(PaimonFatalWriteException.class, fixture::create);

        org.junit.jupiter.api.Assertions.assertTrue(
                thrown.getMessage()
                        .contains("PAIMON_UNSUPPORTED_CROSS_PARTITION_MERGE_ENGINE"));
        verify(fixture.table, never()).newStreamWriteBuilder();
    }

    @Test
    void spillableWriterMustReceiveIoManagerWhenConfiguredTmpDirsAreBlank() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        when(fixture.coreOptions.writeBufferSpillable()).thenReturn(true);

        try (PaimonTableWriteContext ignored = fixture.create()) {
            verify(fixture.writer).withIOManager(any(IOManager.class));
        }
    }

    @Test
    void writerTypeDriftMustFailClosedBeforeCreatingAnyPaimonResource() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        StreamTableWrite drifted = mock(StreamTableWrite.class);
        when(fixture.builder.newWrite()).thenReturn(drifted);

        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, fixture::create);

        assertTrue(thrown.getMessage().contains("TableWriteImpl"));
        verify(drifted).close();
    }

    @Test
    void committerTypeDriftMustFailClosedAndRollBackTheCreatedWriter() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        StreamTableCommit drifted = mock(StreamTableCommit.class);
        when(fixture.builder.newCommit()).thenReturn(drifted);

        IllegalArgumentException thrown =
                assertThrows(IllegalArgumentException.class, fixture::create);

        assertTrue(thrown.getMessage().contains("TableCommitImpl"));
        verify(fixture.writer).close();
    }

    @Test
    void factoryMustInjectConnectorOwnedCompactionExecutorBeforeIoManager() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        when(fixture.coreOptions.writeBufferSpillable()).thenReturn(true);

        try (PaimonTableWriteContext ignored = fixture.create()) {
            InOrder order = inOrder(fixture.writer);
            order.verify(fixture.writer).withCompactExecutor(any());
            order.verify(fixture.writer).withIOManager(any(IOManager.class));
        }
    }

    @ParameterizedTest(name = "构造阶段 {0} 失败必须仅清理已取得的资源")
    @EnumSource(ConstructionFailureStage.class)
    void constructionFailureMustCloseOwnedResourcesExactlyOnceInSafeOrder(
            ConstructionFailureStage stage, @TempDir java.nio.file.Path tempDir) throws Exception {
        Fixture fixture = new Fixture(stage == ConstructionFailureStage.STRATEGY
                ? BucketMode.HASH_DYNAMIC : BucketMode.HASH_FIXED);
        when(fixture.coreOptions.writeBufferSpillable()).thenReturn(true);
        Throwable original = stage == ConstructionFailureStage.COMMITTER_ERROR
                ? new AssertionError("native committer 构造 Error")
                : new IllegalStateException("构造阶段失败: " + stage);
        if (stage == ConstructionFailureStage.NATIVE_ACCESS) {
            when(fixture.writer.getWrite()).thenThrow(original);
        } else if (stage == ConstructionFailureStage.EXECUTOR_INJECTION) {
            when(fixture.writer.withCompactExecutor(any())).thenThrow(original);
        } else if (stage == ConstructionFailureStage.IO_INJECTION) {
            when(fixture.writer.withIOManager(any(IOManager.class))).thenThrow(original);
        } else if (stage == ConstructionFailureStage.STRATEGY) {
            configureDynamicStrategyFailure(fixture, original);
        } else {
            when(fixture.builder.newCommit()).thenThrow(original);
        }
        org.apache.paimon.disk.IOManagerImpl io = org.mockito.Mockito.spy(
                new org.apache.paimon.disk.IOManagerImpl(new String[] {tempDir.toString()}));
        List<String> dirs = canonicalSpillDirs(io);
        try (org.mockito.MockedStatic<IOManager> factory = org.mockito.Mockito.mockStatic(IOManager.class)) {
            factory.when(() -> IOManager.create(any(String[].class))).thenReturn(io);

            Throwable thrown = assertThrows(Throwable.class, fixture::create);

            assertSame(original, thrown);
            InOrder order = inOrder(fixture.writer, fixture.committer, io);
            order.verify(fixture.writer).close();
            if (stage == ConstructionFailureStage.STRATEGY) {
                order.verify(fixture.committer).close();
            } else {
                verify(fixture.committer, never()).close();
            }
            if (stage.hasIo()) {
                order.verify(io).close();
                for (String dir : dirs) {
                    assertFalse(java.nio.file.Files.exists(java.nio.file.Paths.get(dir)),
                            "本次已拥有的 Spill 目录必须在 writer/committer 关闭后删除");
                    assertFalse(java.nio.file.Files.exists(ownerMarker(dir)));
                }
            } else {
                verify(io, never()).close();
                verify(fixture.writer, never()).withIOManager(any());
            }
            verify(fixture.writer, org.mockito.Mockito.times(1)).close();
        } finally {
            io.close();
            PaimonSpillDirCleaner.releaseAfterClose(dirs, true);
        }
    }

    @ParameterizedTest(name = "回滚资源 {0} 关闭失败必须返回清理未完成证明")
    @EnumSource(RollbackResource.class)
    void rollbackCloseFailureMustRetainCauseAndReportIncompleteCleanup(
            RollbackResource resource, @TempDir java.nio.file.Path tempDir) throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_DYNAMIC);
        when(fixture.coreOptions.writeBufferSpillable()).thenReturn(true);
        IllegalStateException original = new IllegalStateException("assigner 初始化失败");
        configureDynamicStrategyFailure(fixture, original);
        java.io.IOException cleanup = new java.io.IOException("回滚关闭失败: " + resource);
        org.apache.paimon.disk.IOManagerImpl io = org.mockito.Mockito.spy(
                new org.apache.paimon.disk.IOManagerImpl(new String[] {tempDir.toString()}));
        List<String> dirs = canonicalSpillDirs(io);
        if (resource == RollbackResource.WRITER) {
            org.mockito.Mockito.doThrow(cleanup).when(fixture.writer).close();
        } else if (resource == RollbackResource.COMMITTER) {
            org.mockito.Mockito.doThrow(cleanup).when(fixture.committer).close();
        } else {
            org.mockito.Mockito.doThrow(cleanup).when(io).close();
        }
        try (org.mockito.MockedStatic<IOManager> factory = org.mockito.Mockito.mockStatic(IOManager.class)) {
            factory.when(() -> IOManager.create(any(String[].class))).thenReturn(io);

            PaimonTableWriteContextFactory.IncompleteCleanupException failure = assertThrows(
                    PaimonTableWriteContextFactory.IncompleteCleanupException.class, fixture::create);

            assertSame(original, failure.getCause());
            assertTrue(Arrays.asList(original.getSuppressed()).contains(cleanup));
            InOrder order = inOrder(fixture.writer, fixture.committer, io);
            order.verify(fixture.writer).close();
            order.verify(fixture.committer).close();
            if (resource == RollbackResource.IO) {
                order.verify(io).close();
            } else {
                verify(io, never()).close();
            }
            for (String dir : dirs) {
                assertTrue(java.nio.file.Files.isDirectory(java.nio.file.Paths.get(dir)));
                assertTrue(java.nio.file.Files.exists(ownerMarker(dir)),
                        "清理失败不能丢失残留目录的 owner marker");
            }
            verify(fixture.writer, org.mockito.Mockito.times(1)).close();
            verify(fixture.committer, org.mockito.Mockito.times(1)).close();
        } finally {
            org.mockito.Mockito.doCallRealMethod().when(io).close();
            io.close();
            PaimonSpillDirCleaner.releaseAfterClose(dirs, true);
        }
    }

    @ParameterizedTest(name = "KEY_DYNAMIC bootstrap Error 后 assignerCloseFails={0}")
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void keyDynamicBootstrapErrorMustRetainIoWhenAssignerCleanupIsIncomplete(
            boolean assignerCloseFails, @TempDir java.nio.file.Path tempDir) throws Exception {
        Fixture fixture = new Fixture(BucketMode.KEY_DYNAMIC);
        TableSchema schema = TableSchema.create(0L, Schema.newBuilder()
                .column("id", DataTypes.INT()).column("pt", DataTypes.STRING())
                .primaryKey("id").partitionKeys("pt").option("bucket", "-1").build());
        when(fixture.table.schema()).thenReturn(schema);
        when(fixture.table.rowType()).thenReturn(schema.logicalRowType());
        when(fixture.table.primaryKeys()).thenReturn(schema.primaryKeys());
        when(fixture.table.partitionKeys()).thenReturn(schema.partitionKeys());
        org.apache.paimon.utils.SnapshotManager snapshots = mock(org.apache.paimon.utils.SnapshotManager.class);
        when(fixture.table.snapshotManager()).thenReturn(snapshots);
        when(snapshots.latestSnapshotIdFromFileSystem()).thenReturn(10L);
        org.apache.paimon.crosspartition.GlobalIndexAssigner assigner =
                mock(org.apache.paimon.crosspartition.GlobalIndexAssigner.class);
        when(fixture.runtimeFactory.createGlobalIndexAssigner(fixture.table)).thenReturn(assigner);
        @SuppressWarnings("unchecked")
        org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader =
                mock(org.apache.paimon.reader.RecordReader.class);
        when(fixture.runtimeFactory.createIndexBootstrapReader(fixture.table)).thenReturn(reader);
        AssertionError bootstrap = new AssertionError("KEY_DYNAMIC bootstrap Error");
        AssertionError assignerClose = new AssertionError("GlobalIndexAssigner close Error");
        when(reader.readBatch()).thenThrow(bootstrap);
        if (assignerCloseFails) {
            org.mockito.Mockito.doThrow(assignerClose).when(assigner).close();
        }
        org.apache.paimon.disk.IOManagerImpl io = org.mockito.Mockito.spy(
                new org.apache.paimon.disk.IOManagerImpl(new String[] {tempDir.toString()}));
        List<String> dirs = canonicalSpillDirs(io);
        try (org.mockito.MockedStatic<IOManager> factory = org.mockito.Mockito.mockStatic(IOManager.class)) {
            factory.when(() -> IOManager.create(any(String[].class))).thenReturn(io);

            Throwable failure = assertThrows(Throwable.class, fixture::create);

            if (assignerCloseFails) {
                org.junit.jupiter.api.Assertions.assertInstanceOf(
                        PaimonTableWriteContextFactory.IncompleteCleanupException.class, failure);
                assertSame(bootstrap, failure.getCause());
                assertEquals(Collections.singletonList(assignerClose), Arrays.asList(bootstrap.getSuppressed()));
                verify(io, never()).close();
                for (String dir : dirs) {
                    assertTrue(java.nio.file.Files.isDirectory(java.nio.file.Paths.get(dir)));
                    assertTrue(java.nio.file.Files.exists(ownerMarker(dir)));
                }
            } else {
                assertSame(bootstrap, failure);
                verify(io, org.mockito.Mockito.times(1)).close();
                for (String dir : dirs) {
                    assertFalse(java.nio.file.Files.exists(java.nio.file.Paths.get(dir)));
                }
            }
            InOrder order = inOrder(reader, assigner, fixture.writer, fixture.committer, io);
            order.verify(reader).close();
            order.verify(assigner).close();
            order.verify(fixture.writer).close();
            order.verify(fixture.committer).close();
            if (!assignerCloseFails) { order.verify(io).close(); }
            verify(reader, org.mockito.Mockito.times(1)).close();
            verify(assigner, org.mockito.Mockito.times(1)).close();
            verify(fixture.writer, org.mockito.Mockito.times(1)).close();
            verify(fixture.committer, org.mockito.Mockito.times(1)).close();
        } finally {
            io.close();
            PaimonSpillDirCleaner.releaseAfterClose(dirs, true);
        }
    }

    private static java.nio.file.Path ownerMarker(String dir) {
        java.nio.file.Path path = java.nio.file.Paths.get(dir);
        return path.resolveSibling("." + path.getFileName() + ".tapdata-owner.lock");
    }

    private static List<String> canonicalSpillDirs(org.apache.paimon.disk.IOManagerImpl io)
            throws java.io.IOException {
        List<String> dirs = new java.util.ArrayList<>();
        for (java.io.File dir : io.getSpillingDirectories()) {
            dirs.add(dir.getCanonicalPath());
        }
        return dirs;
    }

    private static void configureDynamicStrategyFailure(Fixture fixture, Throwable failure) throws Exception {
        TableSchema schema = TableSchema.create(0L, Schema.newBuilder()
                .column("id", DataTypes.INT()).column("value", DataTypes.STRING())
                .primaryKey("id").option("bucket", "-1").build());
        when(fixture.table.schema()).thenReturn(schema);
        when(fixture.table.rowType()).thenReturn(schema.logicalRowType());
        when(fixture.runtimeFactory.createHashBucketAssigner(fixture.table, COMMIT_USER))
                .thenThrow(failure);
    }

    private enum ConstructionFailureStage {
        NATIVE_ACCESS, EXECUTOR_INJECTION, IO_INJECTION, COMMITTER_CREATION, COMMITTER_ERROR, STRATEGY;

        boolean hasIo() {
            return this != NATIVE_ACCESS && this != EXECUTOR_INJECTION;
        }
    }

    private enum RollbackResource { WRITER, COMMITTER, IO }

    private static final class Fixture {
        private final FileStoreTable table = mock(FileStoreTable.class);
        private final CoreOptions coreOptions = mock(CoreOptions.class);
        private final StreamWriteBuilder builder = mock(StreamWriteBuilder.class);
        @SuppressWarnings("unchecked")
        private final TableWriteImpl<org.apache.paimon.table.sink.CommitMessage> writer =
                mock(TableWriteImpl.class);
        private final TableCommitImpl committer = mock(TableCommitImpl.class);
        private final PaimonBucketWriterRuntimeFactory runtimeFactory =
                mock(PaimonBucketWriterRuntimeFactory.class);

        private Fixture(BucketMode mode) {
            TableSchema schema =
                    TableSchema.create(
                            0L,
                            Schema.newBuilder()
                                    .column("id", DataTypes.INT())
                                    .column("value", DataTypes.STRING())
                                    .primaryKey("id")
                                    .build());
            when(table.bucketMode()).thenReturn(mode);
            when(table.coreOptions()).thenReturn(coreOptions);
            when(coreOptions.snapshotExpireExecutionMode()).thenReturn(CoreOptions.ExpireExecutionMode.SYNC);
            when(table.schema()).thenReturn(schema);
            when(table.rowType()).thenReturn(schema.logicalRowType());
            when(table.primaryKeys()).thenReturn(schema.primaryKeys());
            when(table.partitionKeys()).thenReturn(schema.partitionKeys());
            when(coreOptions.mergeEngine()).thenReturn(MergeEngine.DEDUPLICATE);
            when(coreOptions.changelogProducer()).thenReturn(ChangelogProducer.NONE);
            when(coreOptions.rowkindField()).thenReturn(Optional.empty());
            when(table.newStreamWriteBuilder()).thenReturn(builder);
            when(builder.withCommitUser(COMMIT_USER)).thenReturn(builder);
            when(builder.newWrite()).thenReturn(writer);
            when(writer.getWrite()).thenReturn(mock(org.apache.paimon.operation.AbstractFileStoreWrite.class));
            when(writer.withCompactExecutor(any())).thenReturn(writer);
            when(writer.withIOManager(any(IOManager.class))).thenReturn(writer);
            when(builder.newCommit()).thenReturn(committer);
        }

        private PaimonTableWriteContext create() throws Exception {
            return PaimonTableWriteContextFactory.create(
                    "default.t",
                    "t",
                    table,
                    COMMIT_USER,
                    null,
                    0L,
                    PaimonTableWriteContext.CommitStateStore.NOOP,
                    runtimeFactory);
        }
    }
}
