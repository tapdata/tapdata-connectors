package io.tapdata.connector.paimon.service;

import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KeyDynamicBucketWriterStrategyTest {

    @Test
    void constructionMustOpenAssignerWithExpectedSingleWriterArguments() throws Exception {
        Fixture fixture = new Fixture();

        fixture.create();

        verify(fixture.assigner)
                .open(eq(0L), org.mockito.ArgumentMatchers.same(fixture.ioManager), eq(1), eq(0), any());
    }

    @Test
    void bootstrapMustVisitEveryRowInEveryBatchInOrder() throws Exception {
        Fixture fixture = new Fixture();
        InternalRow row1 = GenericRow.of(1);
        InternalRow row2 = GenericRow.of(2);
        InternalRow row3 = GenericRow.of(3);
        RecordReader.RecordIterator<InternalRow> batch1 = batch(row1, row2);
        RecordReader.RecordIterator<InternalRow> batch2 = batch(row3);
        when(fixture.reader.readBatch()).thenReturn(batch1, batch2, null);

        fixture.create();

        InOrder order = inOrder(fixture.assigner, batch1, batch2, fixture.reader);
        order.verify(fixture.assigner).bootstrapKey(row1);
        order.verify(fixture.assigner).bootstrapKey(row2);
        order.verify(batch1).releaseBatch();
        order.verify(fixture.assigner).bootstrapKey(row3);
        order.verify(batch2).releaseBatch();
        order.verify(fixture.reader).close();
        order.verify(fixture.assigner).endBoostrap(false);
    }

    @Test
    void everyBatchMustReleaseAndReaderMustCloseWhenIterationFails() throws Exception {
        Fixture fixture = new Fixture();
        RecordReader.RecordIterator<InternalRow> batch = mock(RecordReader.RecordIterator.class);
        IOException failure = new IOException("read failed");
        when(batch.next()).thenThrow(failure);
        when(fixture.reader.readBatch()).thenReturn(batch);

        assertSame(failure, assertThrows(IOException.class, fixture::create));

        verify(batch).releaseBatch();
        verify(fixture.reader).close();
        verify(fixture.assigner, never()).endBoostrap(false);
        verify(fixture.assigner).close();
    }

    @Test
    void endBootstrapMustRunOnceAfterReaderExhausted() throws Exception {
        Fixture fixture = new Fixture();

        fixture.create();

        InOrder order = inOrder(fixture.reader, fixture.assigner);
        order.verify(fixture.reader).close();
        order.verify(fixture.assigner).endBoostrap(false);
    }

    @Test
    void snapshotChangeMustCloseAssignerAndRejectConstruction() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.snapshotManager.latestSnapshotIdFromFileSystem()).thenReturn(1L, 2L);

        assertThrows(IllegalStateException.class, fixture::create);

        verify(fixture.assigner).close();
    }

    @Test
    void openFailureMustCloseAssignerAndPreserveSuppressedCloseFailure() throws Exception {
        Fixture fixture = new Fixture();
        Exception openFailure = new Exception("open failed");
        IOException closeFailure = new IOException("close failed");
        doThrow(openFailure)
                .when(fixture.assigner)
                .open(eq(0L), org.mockito.ArgumentMatchers.same(fixture.ioManager), eq(1), eq(0), any());
        doThrow(closeFailure).when(fixture.assigner).close();

        Exception thrown = assertThrows(Exception.class, fixture::create);

        assertSame(openFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(closeFailure, thrown.getSuppressed()[0]);
        verify(fixture.runtime, never()).createIndexBootstrapReader(fixture.table);
    }

    @Test
    void zeroEmittedRowsMustNotWrite() throws Exception {
        Fixture fixture = new Fixture();
        KeyDynamicBucketWriterStrategy strategy = fixture.create();

        strategy.write(GenericRow.of(1));

        verify(fixture.writer, never()).write(any(InternalRow.class), anyInt());
    }

    @Test
    void oneEmittedRowMustPreserveRowAndBucket() throws Exception {
        Fixture fixture = new Fixture();
        KeyDynamicBucketWriterStrategy strategy = fixture.create();
        InternalRow input = GenericRow.of(1);
        InternalRow emitted = GenericRow.of(2);
        doAnswer(invocation -> {
            fixture.emit(emitted, 4);
            return null;
        }).when(fixture.assigner).processInput(input);

        strategy.write(input);

        verify(fixture.writer).write(org.mockito.ArgumentMatchers.same(emitted), eq(4));
    }

    @Test
    void twoEmittedRowsMustPreserveDeleteThenInsertOrder() throws Exception {
        Fixture fixture = new Fixture();
        KeyDynamicBucketWriterStrategy strategy = fixture.create();
        InternalRow input = GenericRow.of(1);
        InternalRow delete = GenericRow.of(2);
        InternalRow insert = GenericRow.of(3);
        doAnswer(invocation -> {
            fixture.emit(delete, 5);
            fixture.emit(insert, 7);
            return null;
        }).when(fixture.assigner).processInput(input);

        strategy.write(input);

        InOrder order = inOrder(fixture.writer);
        order.verify(fixture.writer).write(org.mockito.ArgumentMatchers.same(delete), eq(5));
        order.verify(fixture.writer).write(org.mockito.ArgumentMatchers.same(insert), eq(7));
    }

    @Test
    void processFailureMustNotWriteBufferedRowsAndMustClearBuffer() throws Exception {
        Fixture fixture = new Fixture();
        KeyDynamicBucketWriterStrategy strategy = fixture.create();
        InternalRow failedInput = GenericRow.of(1);
        InternalRow stale = GenericRow.of(2);
        Exception failure = new Exception("process failed");
        doAnswer(invocation -> {
            fixture.emit(stale, 3);
            throw failure;
        }).when(fixture.assigner).processInput(failedInput);

        assertSame(failure, assertThrows(Exception.class, () -> strategy.write(failedInput)));
        verify(fixture.writer, never()).write(any(InternalRow.class), anyInt());

        InternalRow nextInput = GenericRow.of(4);
        InternalRow next = GenericRow.of(5);
        doAnswer(invocation -> {
            fixture.emit(next, 8);
            return null;
        }).when(fixture.assigner).processInput(nextInput);
        strategy.write(nextInput);
        verify(fixture.writer).write(org.mockito.ArgumentMatchers.same(next), eq(8));
        verify(fixture.writer, never()).write(org.mockito.ArgumentMatchers.same(stale), anyInt());
    }

    @Test
    void secondRawWriteFailureMustStopFurtherWritesAndPropagate() throws Exception {
        Fixture fixture = new Fixture();
        KeyDynamicBucketWriterStrategy strategy = fixture.create();
        InternalRow input = GenericRow.of(1);
        InternalRow first = GenericRow.of(2);
        InternalRow second = GenericRow.of(3);
        InternalRow third = GenericRow.of(4);
        doAnswer(invocation -> {
            fixture.emit(first, 1);
            fixture.emit(second, 2);
            fixture.emit(third, 3);
            return null;
        }).when(fixture.assigner).processInput(input);
        Exception failure = new Exception("raw write failed");
        doThrow(failure).when(fixture.writer).write(second, 2);

        assertSame(failure, assertThrows(Exception.class, () -> strategy.write(input)));

        verify(fixture.writer).write(first, 1);
        verify(fixture.writer).write(second, 2);
        verify(fixture.writer, never()).write(third, 3);
    }

    @Test
    void closeMustCloseAssignerBeforeRawWriter() throws Exception {
        Fixture fixture = new Fixture();
        KeyDynamicBucketWriterStrategy strategy = fixture.create();

        strategy.close();

        InOrder order = inOrder(fixture.assigner, fixture.writer);
        order.verify(fixture.assigner).close();
        order.verify(fixture.writer).close();
    }

    @Test
    void validationMustRequireTargetPrimaryKeyButAllowNullPartition() throws Exception {
        Fixture fixture = new Fixture();
        KeyDynamicBucketWriterStrategy strategy = fixture.create();

        strategy.validateRoutingRow(GenericRow.of(null, 10), "INSERT");
        assertThrows(
                PaimonFatalWriteException.class,
                () -> strategy.validateRoutingRow(GenericRow.of(1, null), "INSERT"));
    }

    private static RecordReader.RecordIterator<InternalRow> batch(InternalRow... rows)
            throws IOException {
        RecordReader.RecordIterator<InternalRow> batch = mock(RecordReader.RecordIterator.class);
        AtomicInteger index = new AtomicInteger();
        when(batch.next())
                .thenAnswer(
                        invocation ->
                                index.get() < rows.length ? rows[index.getAndIncrement()] : null);
        return batch;
    }

    private static final class Fixture {
        private final FileStoreTable table = mock(FileStoreTable.class);
        private final StreamTableWrite writer = mock(StreamTableWrite.class);
        private final IOManager ioManager = mock(IOManager.class);
        private final GlobalIndexAssigner assigner = mock(GlobalIndexAssigner.class);
        private final PaimonBucketWriterRuntimeFactory runtime =
                mock(PaimonBucketWriterRuntimeFactory.class);
        private final RecordReader<InternalRow> reader = mock(RecordReader.class);
        private final SnapshotManager snapshotManager = mock(SnapshotManager.class);
        private final RowType rowType = mock(RowType.class);
        private BiConsumer<InternalRow, Integer> collector;

        private Fixture() throws Exception {
            when(table.bucketMode()).thenReturn(BucketMode.KEY_DYNAMIC);
            when(table.primaryKeys()).thenReturn(Collections.singletonList("id"));
            when(table.partitionKeys()).thenReturn(Collections.singletonList("pt"));
            when(table.rowType()).thenReturn(rowType);
            when(rowType.getFieldNames()).thenReturn(Arrays.asList("pt", "id"));
            when(table.snapshotManager()).thenReturn(snapshotManager);
            when(snapshotManager.latestSnapshotIdFromFileSystem()).thenReturn(10L, 10L);
            when(runtime.createGlobalIndexAssigner(table)).thenReturn(assigner);
            when(runtime.createIndexBootstrapReader(table)).thenReturn(reader);
            when(reader.readBatch()).thenReturn(null);
            doAnswer(invocation -> {
                collector = invocation.getArgument(4);
                return null;
            }).when(assigner)
                    .open(eq(0L), org.mockito.ArgumentMatchers.same(ioManager), eq(1), eq(0), any());
        }

        private KeyDynamicBucketWriterStrategy create() throws Exception {
            return new KeyDynamicBucketWriterStrategy(
                    new PaimonBucketWriterStrategyContext(
                            "default.t", table, writer, "user", ioManager),
                    runtime);
        }

        private void emit(InternalRow row, int bucket) {
            collector.accept(row, bucket);
        }
    }
}
