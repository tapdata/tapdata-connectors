package io.tapdata.connector.paimon.service;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.*;
import org.apache.paimon.types.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class PaimonFiniteReadLifecycleTest {
    @ParameterizedTest @ValueSource(strings = {"getTableCount", "discoverTables", "timestampToStreamOffset"})
    void metadataIngressMustBlockCatalogCleanupAndRejectLateNextCall(String method) throws Exception {
        PaimonBoundedStopTest.Fixture f = new PaimonBoundedStopTest.Fixture(1, 1, 1);
        Catalog catalog = mock(Catalog.class); set(f.service, "catalog", catalog);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        if (method.equals("timestampToStreamOffset")) {
            when(catalog.getTable(any())).thenAnswer(i -> { entered.countDown(); await(release); throw new Catalog.TableNotExistException(i.getArgument(0)); });
        } else {
            when(catalog.getDatabase(anyString())).thenAnswer(i -> { entered.countDown(); await(release); return null; });
        }
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            try {
                if (method.equals("getTableCount")) { f.service.getTableCount(); }
                else if (method.equals("discoverTables")) { f.service.discoverTables(null); }
                else { f.service.timestampToStreamOffset(java.util.Arrays.asList("a", "b"), 1L, mock(Log.class)); }
            } catch (Throwable failure) { error.set(failure); }
        });
        reader.setDaemon(true); reader.start();
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS)); f.start();
            waitStarted(f); f.advance(1); f.joinFailedRetained();
            verify(catalog, never()).close();
            release.countDown(); reader.join(3000); f.worker().join(3000);
            assertNotNull(error.get());
            verify(catalog, never()).listTables(anyString());
            if (method.equals("timestampToStreamOffset")) { verify(catalog, times(1)).getTable(any()); }
            verify(catalog, never()).close();
        } finally { release.countDown(); }
    }

    @ParameterizedTest @ValueSource(strings = {"batchRead", "batchCount", "query"})
    void readerCloseFailureMustRetainReaderAndCatalogEvenAfterIngressReturns(String method) throws Exception {
        ReadFixture r = new ReadFixture();
        IOException expected = new IOException("reader close proof missing");
        doThrow(expected).when(r.reader).close();
        assertSame(expected, assertThrows(IOException.class, () -> r.read(method)));
        assertSame(expected, assertThrows(Exception.class, r.f.service::close));
        assertTrue(r.f.control().isRetained());
        assertSame(r.f.service, PaimonStopResources.retainedRoot(r.f.control()));
        verify(r.catalog, never()).close(); verify(r.reader, times(1)).close();
        assertThrows(Exception.class, () -> r.read(method));
        verify(r.tableRead, times(1)).createReader(any(Split.class));
    }

    @ParameterizedTest @ValueSource(strings = {"batchRead", "batchCount", "query"})
    void readAndCloseFailureMustKeepReadCauseWithSuppressedCleanup(String method) throws Exception {
        ReadFixture r = new ReadFixture();
        IOException read = new IOException("read failed"), close = new IOException("close failed");
        when(r.reader.readBatch()).thenThrow(read); doThrow(close).when(r.reader).close();
        assertSame(read, assertThrows(IOException.class, () -> r.read(method)));
        assertArrayEquals(new Throwable[] {close}, read.getSuppressed());
        assertTrue(r.f.control().isRetained()); verify(r.catalog, never()).close();
        assertSame(r.f.service, PaimonStopResources.retainedRoot(r.f.control()));
    }

    @Test void queryLimitMustReleaseOutstandingBatchBeforeReaderClose() throws Exception {
        ReadFixture r = new ReadFixture();
        RecordReader.RecordIterator<InternalRow> batch = mock(RecordReader.RecordIterator.class);
        when(r.reader.readBatch()).thenReturn(batch);
        when(batch.next()).thenReturn(GenericRow.of(1));
        TapAdvanceFilter filter = new TapAdvanceFilter(); filter.setLimit(0);
        r.f.service.queryByAdvanceFilter(new TapTable("a"), filter, rows -> fail("limit 0"), mock(Log.class));
        org.mockito.InOrder order = inOrder(batch, r.reader);
        order.verify(batch).releaseBatch(); order.verify(r.reader).close();
        r.f.service.close(); assertFalse(r.f.control().isRetained());
    }

    @Test void failedBatchReleaseMustNeverBeHiddenBySuccessfulReaderClose() throws Exception {
        ReadFixture r = new ReadFixture();
        RecordReader.RecordIterator<InternalRow> batch = mock(RecordReader.RecordIterator.class);
        when(r.reader.readBatch()).thenReturn(batch);
        IllegalStateException expected = new IllegalStateException("batch release failed");
        doThrow(expected).when(batch).releaseBatch();
        assertThrows(IllegalStateException.class, () -> r.read("batchCount"));
        assertTrue(r.f.control().isRetained());
        assertSame(expected, assertThrows(Exception.class, r.f.service::close));
        verify(batch, times(1)).releaseBatch();
        verify(r.reader, never()).close(); verify(r.catalog, never()).close();
    }

    @Test void readerCreationReturningAfterTimeoutMustRemainStrongAndCannotCloseOrRead() throws Exception {
        ReadFixture r = new ReadFixture();
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        when(r.tableRead.createReader(any(Split.class))).thenAnswer(i -> { entered.countDown(); await(release); return r.reader; });
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread read = new Thread(() -> { try { r.read("batchCount"); } catch (Throwable error) { failure.set(error); } });
        read.setDaemon(true); read.start();
        try {
            assertTrue(entered.await(3, TimeUnit.SECONDS)); r.f.start(); waitStarted(r.f); r.f.advance(1);
            r.f.joinFailedRetained(); release.countDown(); read.join(3000);
            assertNotNull(failure.get()); assertTrue(r.f.resources().size() > 0);
            verify(r.reader, never()).readBatch(); verify(r.reader, never()).close(); verify(r.catalog, never()).close();
        } finally { release.countDown(); }
    }

    private static void waitStarted(PaimonBoundedStopTest.Fixture f) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!f.control().isStarted() && System.nanoTime() < end) { Thread.sleep(5); }
        assertTrue(f.control().isStarted());
    }
    private static void await(CountDownLatch latch) throws InterruptedException { assertTrue(latch.await(5, TimeUnit.SECONDS)); }
    private static void set(Object owner, String name, Object value) throws Exception {
        Field field = owner.getClass().getDeclaredField(name); field.setAccessible(true); field.set(owner, value);
    }
    private static class ReadFixture {
        final PaimonBoundedStopTest.Fixture f = new PaimonBoundedStopTest.Fixture(1, 1, 1);
        final Catalog catalog = mock(Catalog.class);
        final TableRead tableRead = mock(TableRead.class);
        final RecordReader<InternalRow> reader = mock(RecordReader.class);
        ReadFixture() throws Exception {
            set(f.service, "catalog", catalog);
            Table table = mock(Table.class); ReadBuilder builder = mock(ReadBuilder.class);
            TableScan scan = mock(TableScan.class); TableScan.Plan plan = mock(TableScan.Plan.class);
            when(catalog.getTable(any())).thenReturn(table);
            when(table.rowType()).thenReturn(RowType.of(DataTypes.INT()));
            when(table.newReadBuilder()).thenReturn(builder); when(builder.newScan()).thenReturn(scan);
            when(scan.plan()).thenReturn(plan); when(plan.splits()).thenReturn(Collections.singletonList(mock(Split.class)));
            when(builder.newRead()).thenReturn(tableRead); when(tableRead.createReader(any(Split.class))).thenReturn(reader);
        }
        void read(String method) throws Exception {
            if (method.equals("batchCount")) { f.service.batchCount(new TapTable("a"), mock(Log.class)); }
            else if (method.equals("query")) { f.service.queryByAdvanceFilter(new TapTable("a"), null, rows -> {}, mock(Log.class)); }
            else { TapConnectorContext context = mock(TapConnectorContext.class); when(context.getLog()).thenReturn(mock(Log.class));
                f.service.batchRead(new TapTable("a"), null, 10, (events, offset) -> {}, context); }
        }
    }
}
