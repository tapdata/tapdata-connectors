package io.tapdata.connector.paimon.write;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.utils.RecordWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PaimonNativeWriteAccessTest {
    @Test
    void unknownWriterAndCommitMessageImplementationsMustFailClosed() {
        assertThrows(IllegalArgumentException.class,
                () -> PaimonNativeWriteAccess.of(mock(StreamTableWrite.class)));
        TableWriteImpl raw = mock(TableWriteImpl.class);
        when(raw.getWrite()).thenReturn(mock(FileStoreWrite.class));
        assertThrows(IllegalArgumentException.class, () -> PaimonNativeWriteAccess.of(raw));
        assertThrows(IllegalArgumentException.class,
                () -> PaimonNativeWriteAccess.hasFinalCompaction(Collections.singletonList(mock(CommitMessage.class))));
    }

    @Test
    void finalMessagesMustRejectAnyUnconfirmedDataIncrement() {
        DataIncrement data = mock(DataIncrement.class);
        when(data.isEmpty()).thenReturn(false);
        CommitMessage mixed = new CommitMessageImpl(BinaryRow.EMPTY_ROW, 0, 1, data, CompactIncrement.emptyIncrement());
        assertThrows(IllegalStateException.class,
                () -> PaimonNativeWriteAccess.hasFinalCompaction(Collections.singletonList(mixed)));
        CommitMessage empty = new CommitMessageImpl(BinaryRow.EMPTY_ROW, 0, 1,
                DataIncrement.emptyIncrement(), CompactIncrement.emptyIncrement());
        assertFalse(PaimonNativeWriteAccess.hasFinalCompaction(Collections.singletonList(empty)));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void syncMustVisitEveryBucketAndPreserveAllErrors() throws Exception {
        TableWriteImpl raw = mock(TableWriteImpl.class);
        AbstractFileStoreWrite store = mock(AbstractFileStoreWrite.class);
        when(raw.getWrite()).thenReturn(store);
        RecordWriter<Object> first = mock(RecordWriter.class);
        RecordWriter<Object> second = mock(RecordWriter.class);
        IOException firstError = new IOException("first bucket");
        IOException secondError = new IOException("second bucket");
        doThrow(firstError).when(first).sync();
        doThrow(secondError).when(second).sync();
        Map<Integer, AbstractFileStoreWrite.WriterContainer<Object>> buckets = new LinkedHashMap<>();
        buckets.put(0, new Container(first));
        buckets.put(1, new Container(second));
        when(store.writers()).thenReturn(Collections.singletonMap(BinaryRow.EMPTY_ROW, buckets));
        List<Throwable> failures = PaimonNativeWriteAccess.of(raw).syncAll();
        assertEquals(Arrays.asList(firstError, secondError), failures);
        verify(first).sync();
        verify(second).sync();
        assertEquals(2, buckets.size(), "适配器不能清空或篡改内核 writers map");
    }

    private static class Container extends AbstractFileStoreWrite.WriterContainer<Object> {
        Container(RecordWriter<Object> writer) { super(writer, 2, null, null, null); }
    }
}
