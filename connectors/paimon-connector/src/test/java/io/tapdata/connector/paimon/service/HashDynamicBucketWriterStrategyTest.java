package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.RowPartitionKeyExtractor;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HashDynamicBucketWriterStrategyTest {

    @Test
    void requiredFieldsMustBeOrderedPrimaryThenDistinctPartitionKeys() {
        Fixture fixture = new Fixture();

        assertEquals(
                Arrays.asList("id", "pt"),
                new ArrayList<>(fixture.strategy.requiredRoutingFields()));
    }

    @Test
    void requiredFieldsMustBeImmutable() {
        Fixture fixture = new Fixture();

        assertThrows(
                UnsupportedOperationException.class,
                () -> fixture.strategy.requiredRoutingFields().add("later"));
    }

    @Test
    void writeMustAssignUsingExtractedPartitionAndTrimmedPrimaryKeyHash() throws Exception {
        Fixture fixture = new Fixture();
        InternalRow row = fixture.row();
        RowPartitionKeyExtractor expectedExtractor = new RowPartitionKeyExtractor(fixture.schema);
        when(fixture.assigner.assign(any(BinaryRow.class), anyInt())).thenReturn(4);

        fixture.strategy.write(row);

        ArgumentCaptor<BinaryRow> partition = ArgumentCaptor.forClass(BinaryRow.class);
        ArgumentCaptor<Integer> keyHash = ArgumentCaptor.forClass(Integer.class);
        verify(fixture.assigner).assign(partition.capture(), keyHash.capture());
        assertEquals(expectedExtractor.partition(row), partition.getValue());
        assertEquals(expectedExtractor.trimmedPrimaryKey(row).hashCode(), keyHash.getValue());
    }

    @Test
    void writeMustUseReturnedBucketExactlyOnce() throws Exception {
        Fixture fixture = new Fixture();
        InternalRow row = fixture.row();
        when(fixture.assigner.assign(any(BinaryRow.class), anyInt())).thenReturn(6);

        fixture.strategy.write(row);

        verify(fixture.writer).write(org.mockito.ArgumentMatchers.same(row), org.mockito.ArgumentMatchers.eq(6));
        verify(fixture.writer, never()).write(row);
    }

    @Test
    void assignFailureMustSkipRawWrite() throws Exception {
        Fixture fixture = new Fixture();
        RuntimeException failure = new RuntimeException("assign failed");
        when(fixture.assigner.assign(any(BinaryRow.class), anyInt())).thenThrow(failure);

        assertSame(failure, assertThrows(RuntimeException.class, () -> fixture.strategy.write(fixture.row())));
        verify(fixture.writer, never()).write(any(InternalRow.class), anyInt());
    }

    @Test
    void prepareMustCallAssignerBeforeRawWriterWithSameIdentifier() throws Exception {
        Fixture fixture = new Fixture();

        fixture.strategy.prepareCommit(12L);

        InOrder order = inOrder(fixture.assigner, fixture.writer);
        order.verify(fixture.assigner).prepareCommit(12L);
        order.verify(fixture.writer).prepareCommit(false, 12L);
    }

    @Test
    void assignerPrepareFailureMustSkipRawPrepare() throws Exception {
        Fixture fixture = new Fixture();
        RuntimeException failure = new RuntimeException("prepare failed");
        doThrow(failure).when(fixture.assigner).prepareCommit(2L);

        assertSame(failure, assertThrows(RuntimeException.class, () -> fixture.strategy.prepareCommit(2L)));
        verify(fixture.writer, never()).prepareCommit(false, 2L);
    }

    @Test
    void closeMustOnlyCloseRawWriterBecauseBucketAssignerHasNoCloseContract() throws Exception {
        Fixture fixture = new Fixture();

        fixture.strategy.close();

        verify(fixture.writer).close();
    }

    private static final class Fixture {
        private final FileStoreTable table = mock(FileStoreTable.class);
        private final StreamTableWrite writer = mock(StreamTableWrite.class);
        private final BucketAssigner assigner = mock(BucketAssigner.class);
        private final PaimonBucketWriterRuntimeFactory runtime =
                mock(PaimonBucketWriterRuntimeFactory.class);
        private final TableSchema schema = schema();
        private final HashDynamicBucketWriterStrategy strategy;

        private Fixture() {
            when(table.bucketMode()).thenReturn(BucketMode.HASH_DYNAMIC);
            when(table.schema()).thenReturn(schema);
            when(table.primaryKeys()).thenReturn(Arrays.asList("id", "pt"));
            when(table.partitionKeys()).thenReturn(Collections.singletonList("pt"));
            when(runtime.createHashBucketAssigner(table, "user")).thenReturn(assigner);
            strategy =
                    new HashDynamicBucketWriterStrategy(
                            new PaimonBucketWriterStrategyContext(
                                    "default.t", table, writer, "user", null),
                            runtime);
        }

        private InternalRow row() {
            return GenericRow.of(7, 42, BinaryString.fromString("value"));
        }

        private static TableSchema schema() {
            java.util.List<DataField> fields =
                    Arrays.asList(
                            new DataField(0, "pt", new IntType()),
                            new DataField(1, "id", new IntType()),
                            new DataField(2, "v", new VarCharType()));
            return new TableSchema(
                    0,
                    fields,
                    RowType.currentHighestFieldId(fields),
                    Collections.singletonList("pt"),
                    Arrays.asList("id", "pt"),
                    Collections.emptyMap(),
                    "");
        }
    }
}
