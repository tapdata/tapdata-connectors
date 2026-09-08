package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.write.PaimonCompactionLifecycle;
import io.tapdata.connector.paimon.write.PaimonNativeWriteAccess;
import io.tapdata.connector.paimon.write.PaimonTableCommitter;
import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;
import io.tapdata.entity.logger.Log;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.compact.CompactResult;
import org.apache.paimon.compact.CompactTask;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.Path;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.utils.RecordWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** S07：相同 typed native task 失败在 DDL 清理阶段必须保持硬失败。 */
class PaimonServiceDdlCompactionFailureTest {
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void ddlMustNotDiscardNativeCompactionFailureOrRunCatalogAction() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setWarehouse("/tmp/paimon-ddl-native-failure");
        Log log = mock(Log.class);
        PaimonService service = new PaimonService(config, log);
        service.startForTest();
        Catalog catalog = mock(Catalog.class);
        field("catalog").set(service, catalog);
        PaimonCompactionLifecycle lifecycle = PaimonCompactionLifecycle.forTable("default.t");
        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        IOManager io = mock(IOManager.class);
        try {
            IOException root = new IOException("native Spill task failure during DDL");
            Future<CompactResult> future = lifecycle.compactionExecutor().submit(new CompactTask(null) {
                @Override protected CompactResult doCompact() throws Exception { throw root; }
            });
            ExecutionException nativeFailure = assertThrows(ExecutionException.class, future::get);
            TableWriteImpl raw = mock(TableWriteImpl.class);
            AbstractFileStoreWrite nativeWrite = mock(AbstractFileStoreWrite.class);
            RecordWriter bucket = mock(RecordWriter.class);
            when(raw.getWrite()).thenReturn(nativeWrite);
            when(nativeWrite.writers()).thenReturn(Collections.singletonMap(BinaryRow.EMPTY_ROW,
                    Collections.singletonMap(0, new Container(bucket))));
            doThrow(nativeFailure).when(bucket).sync();
            PaimonTableWriteContext context = new PaimonTableWriteContext("default.t", "t", "user",
                    strategy, committer, io, Collections.emptyList(), 0L,
                    PaimonTableWriteContext.CommitStateStore.NOOP, lifecycle, PaimonNativeWriteAccess.of(raw));
            ((Map<String, PaimonTableWriteContext>) field("tableWriteContexts").get(service)).put("default.t", context);
            FileStoreTable table = mock(FileStoreTable.class);
            when(table.coreOptions()).thenReturn(CoreOptions.fromMap(Collections.emptyMap()));
            when(table.location()).thenReturn(new Path("file:///tmp/ddl-native-" + UUID.randomUUID()));
            Method register = PaimonService.class.getDeclaredMethod("registerPhysicalTableOwner", String.class, FileStoreTable.class);
            register.setAccessible(true);
            register.invoke(service, "default.t", table);

            assertSame(nativeFailure, assertThrows(ExecutionException.class, () -> service.dropTable("t")));
            assertSame(root, nativeFailure.getCause().getCause());
            verify(catalog, never()).dropTable(Identifier.create("default", "t"), true);
            verify(strategy, never()).prepareFinalCommit(anyLong());
            verify(bucket).sync();
            assertTrue(context.cleanupComplete());
            assertTrue(((Map<?, ?>) field("physicalTableByLogicalTable").get(service)).isEmpty());
            assertSame(nativeFailure, assertThrows(ExecutionException.class, service::close));
            verify(log, never()).info(contains("正常退出"));
            verify(log, never()).info(contains("SUCCESS_COMPACTION_DISCARDED"));
        } finally {
            lifecycle.compactionExecutor().shutdown();
            assertTrue(lifecycle.compactionExecutor().awaitTermination(5, TimeUnit.SECONDS));
            try { service.close(); } catch (Exception expectedHardFailure) { /* 已在主断言验证。 */ }
        }
    }

    private static Field field(String name) throws Exception {
        Field field = PaimonService.class.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static class Container extends AbstractFileStoreWrite.WriterContainer<Object> {
        Container(RecordWriter<Object> writer) { super(writer, 2, null, null, null); }
    }
}
