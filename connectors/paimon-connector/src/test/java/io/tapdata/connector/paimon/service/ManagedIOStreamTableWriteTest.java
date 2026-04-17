package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.FileIOChannel;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.io.BundleRecords;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableWrite;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ManagedIOStreamTableWriteTest {

    @Test
    void closeShouldCloseWriteThenIoManager() throws Exception {
        List<String> order = new ArrayList<>();
        StreamTableWrite delegate = new FakeStreamTableWrite(order, null);
        IOManager ioManager = new FakeIOManager(order, null);
        ManagedIOStreamTableWrite writer = new ManagedIOStreamTableWrite(delegate, ioManager);

        writer.close();

        assertEquals(2, order.size());
        assertEquals("write.close", order.get(0));
        assertEquals("io.close", order.get(1));
    }

    @Test
    void closeShouldStillCloseIoManagerWhenWriteCloseFails() {
        List<String> order = new ArrayList<>();
        Exception writeError = new Exception("write close failed");
        StreamTableWrite delegate = new FakeStreamTableWrite(order, writeError);
        IOManager ioManager = new FakeIOManager(order, null);

        ManagedIOStreamTableWrite writer = new ManagedIOStreamTableWrite(delegate, ioManager);

        Exception thrown = assertThrows(Exception.class, writer::close);
        assertSame(writeError, thrown);
        assertEquals(2, order.size());
        assertEquals("write.close", order.get(0));
        assertEquals("io.close", order.get(1));
    }

    @Test
    void closeShouldAttachIoManagerErrorAsSuppressedWhenBothFail() {
        List<String> order = new ArrayList<>();
        Exception writeError = new Exception("write close failed");
        Exception ioError = new Exception("io close failed");
        StreamTableWrite delegate = new FakeStreamTableWrite(order, writeError);
        IOManager ioManager = new FakeIOManager(order, ioError);

        ManagedIOStreamTableWrite writer = new ManagedIOStreamTableWrite(delegate, ioManager);

        Exception thrown = assertThrows(Exception.class, writer::close);
        assertSame(writeError, thrown);
        assertArrayEquals(new Throwable[] {ioError}, thrown.getSuppressed());
        assertEquals(2, order.size());
        assertEquals("write.close", order.get(0));
        assertEquals("io.close", order.get(1));
    }

    @Test
    void closeShouldWorkWithoutIoManager() throws Exception {
        List<String> order = new ArrayList<>();
        StreamTableWrite delegate = new FakeStreamTableWrite(order, null);
        ManagedIOStreamTableWrite writer = new ManagedIOStreamTableWrite(delegate, null);

        writer.close();

        assertEquals(Collections.singletonList("write.close"), order);
    }

    private static class FakeStreamTableWrite implements StreamTableWrite {

        private final List<String> order;
        private final Exception closeError;

        private FakeStreamTableWrite(List<String> order, Exception closeError) {
            this.order = order;
            this.closeError = closeError;
        }

        @Override
        public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier) {
            return Collections.emptyList();
        }

        @Override
        public TableWrite withIOManager(IOManager ioManager) {
            return this;
        }

        @Override
        public TableWrite withWriteType(RowType writeType) {
            return this;
        }

        @Override
        public TableWrite withMemoryPoolFactory(MemoryPoolFactory memoryPoolFactory) {
            return this;
        }

        @Override
        public BinaryRow getPartition(InternalRow row) {
            return null;
        }

        @Override
        public int getBucket(InternalRow row) {
            return 0;
        }

        @Override
        public void write(InternalRow row) {
        }

        @Override
        public void write(InternalRow row, int bucket) {
        }

        @Override
        public void writeBundle(BinaryRow partition, int bucket, BundleRecords bundle) {
        }

        @Override
        public void compact(BinaryRow partition, int bucket, boolean fullCompaction) {
        }

        @Override
        public TableWrite withMetricRegistry(MetricRegistry registry) {
            return this;
        }

        @Override
        public void close() throws Exception {
            order.add("write.close");
            if (closeError != null) {
                throw closeError;
            }
        }
    }

    private static class FakeIOManager implements IOManager {

        private final List<String> order;
        private final Exception closeError;

        private FakeIOManager(List<String> order, Exception closeError) {
            this.order = order;
            this.closeError = closeError;
        }

        @Override
        public FileIOChannel.ID createChannel() {
            return null;
        }

        @Override
        public FileIOChannel.ID createChannel(String prefix) {
            return null;
        }

        @Override
        public String[] tempDirs() {
            return new String[0];
        }

        @Override
        public FileIOChannel.Enumerator createChannelEnumerator() {
            return null;
        }

        @Override
        public BufferFileWriter createBufferFileWriter(FileIOChannel.ID channelID) {
            return null;
        }

        @Override
        public BufferFileReader createBufferFileReader(FileIOChannel.ID channelID) {
            return null;
        }

        @Override
        public void close() throws Exception {
            order.add("io.close");
            if (closeError != null) {
                throw closeError;
            }
        }
    }
}



