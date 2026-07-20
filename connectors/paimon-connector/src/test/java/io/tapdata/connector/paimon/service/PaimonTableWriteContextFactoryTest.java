package io.tapdata.connector.paimon.service;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonTableWriteContextFactoryTest {

    private static final String COMMIT_USER = "factory-test-user";

    @Test
    void successfulContextMustOwnAndCloseWriterBeforeCommitterExactlyOnce() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);

        PaimonTableWriteContext context = fixture.create();
        context.close();
        context.close();

        InOrder order = inOrder(fixture.writer, fixture.committer);
        order.verify(fixture.writer).close();
        order.verify(fixture.committer).close();
        verify(fixture.writer).close();
        verify(fixture.committer).close();
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
    void strategyConstructionFailureMustCloseCommitterThenWriter() throws Exception {
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
        InOrder order = inOrder(fixture.committer, fixture.writer);
        order.verify(fixture.committer).close();
        order.verify(fixture.writer).close();
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
    void spillableWriterMustReceiveIoManagerWhenConfiguredTmpDirsAreBlank() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        when(fixture.coreOptions.writeBufferSpillable()).thenReturn(true);

        try (PaimonTableWriteContext ignored = fixture.create()) {
            verify(fixture.writer).withIOManager(any(IOManager.class));
        }
    }

    private static final class Fixture {
        private final FileStoreTable table = mock(FileStoreTable.class);
        private final CoreOptions coreOptions = mock(CoreOptions.class);
        private final StreamWriteBuilder builder = mock(StreamWriteBuilder.class);
        private final StreamTableWrite writer = mock(StreamTableWrite.class);
        private final StreamTableCommit committer = mock(StreamTableCommit.class);
        private final PaimonBucketWriterRuntimeFactory runtimeFactory =
                mock(PaimonBucketWriterRuntimeFactory.class);

        private Fixture(BucketMode mode) {
            when(table.bucketMode()).thenReturn(mode);
            when(table.coreOptions()).thenReturn(coreOptions);
            when(table.newStreamWriteBuilder()).thenReturn(builder);
            when(builder.withCommitUser(COMMIT_USER)).thenReturn(builder);
            when(builder.newWrite()).thenReturn(writer);
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
