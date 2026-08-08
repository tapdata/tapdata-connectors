package io.tapdata.connector.paimon.write;

import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonStreamTableCommitterTest {

    @Test
    void directCommitMustDelegateTheExactIdentifierAndMessages() {
        StreamTableCommit delegate = mock(StreamTableCommit.class);
        PaimonStreamTableCommitter committer = new PaimonStreamTableCommitter(delegate);
        List<CommitMessage> messages =
                Collections.singletonList(mock(CommitMessage.class));

        committer.commit(7L, messages);

        verify(delegate).commit(eq(7L), same(messages));
        verify(delegate, never()).filterAndCommit(org.mockito.ArgumentMatchers.anyMap());
    }

    @Test
    void filterAndCommitMustDelegateSameMapAndReturnSameCount() {
        StreamTableCommit delegate = mock(StreamTableCommit.class);
        PaimonStreamTableCommitter committer = new PaimonStreamTableCommitter(delegate);
        Map<Long, List<CommitMessage>> pending = pendingMap();
        when(delegate.filterAndCommit(pending)).thenReturn(1);

        assertEquals(1, committer.filterAndCommit(pending));

        verify(delegate).filterAndCommit(same(pending));
        assertEquals(1, pending.size());
    }

    @Test
    void delegateFailureMustNotModifyInput() {
        StreamTableCommit delegate = mock(StreamTableCommit.class);
        PaimonStreamTableCommitter committer = new PaimonStreamTableCommitter(delegate);
        Map<Long, List<CommitMessage>> pending = pendingMap();
        Map<Long, List<CommitMessage>> before = new LinkedHashMap<>(pending);
        RuntimeException failure = new RuntimeException("ambiguous outcome");
        when(delegate.filterAndCommit(pending)).thenThrow(failure);

        assertSame(
                failure,
                assertThrows(RuntimeException.class, () -> committer.filterAndCommit(pending)));
        assertEquals(before, pending);
    }

    @Test
    void closeMustBeIdempotentAndRejectLaterCommit() throws Exception {
        StreamTableCommit delegate = mock(StreamTableCommit.class);
        PaimonStreamTableCommitter committer = new PaimonStreamTableCommitter(delegate);
        Map<Long, List<CommitMessage>> pending = pendingMap();

        committer.close();
        committer.close();

        verify(delegate).close();
        assertThrows(
                IllegalStateException.class,
                () -> committer.commit(7L, pending.get(7L)));
        assertThrows(IllegalStateException.class, () -> committer.filterAndCommit(pending));
        verify(delegate, never()).commit(anyLong(), anyList());
        verify(delegate, never()).filterAndCommit(pending);
    }

    @Test
    void closeFailureMustStillMakeAdapterClosed() throws Exception {
        StreamTableCommit delegate = mock(StreamTableCommit.class);
        PaimonStreamTableCommitter committer = new PaimonStreamTableCommitter(delegate);
        Exception failure = new Exception("close failed");
        doThrow(failure).when(delegate).close();

        assertSame(failure, assertThrows(Exception.class, committer::close));
        committer.close();
        verify(delegate).close();
        assertThrows(IllegalStateException.class, () -> committer.filterAndCommit(pendingMap()));
    }

    private static Map<Long, List<CommitMessage>> pendingMap() {
        Map<Long, List<CommitMessage>> pending = new LinkedHashMap<>();
        pending.put(7L, Collections.singletonList(mock(CommitMessage.class)));
        return pending;
    }
}
