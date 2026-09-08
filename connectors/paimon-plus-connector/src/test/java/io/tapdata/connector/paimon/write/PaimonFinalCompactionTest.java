package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PaimonFinalCompactionTest {
    @Test
    void confirmedFinalCommitWithStateSaveFailureMustRemainHardWithoutReprepare() throws Exception {
        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        IOException failure = new IOException("identifier persistence failed");
        PaimonTableWriteContext context = new PaimonTableWriteContext("default.t", "t", "user",
                strategy, committer, null, Collections.emptyList(), 7,
                identifier -> { assertEquals(8, identifier); throw failure; });
        List<CommitMessage> messages = Collections.singletonList(compactionMessage());
        when(strategy.prepareFinalCommit(7)).thenReturn(messages);

        assertSame(failure, assertThrows(IOException.class, () -> context.closeForStop(true, phase -> {})));
        assertFalse(context.hasPendingCommit(), "已确认提交不能作为未知 pending 重放");
        assertTrue(context.cleanupComplete());
        assertSame(failure, assertThrows(IOException.class, () -> context.closeForStop(true, phase -> {})));
        verify(committer).commit(7, messages);
        verify(committer, never()).filterAndCommit(anyMap());
        verify(strategy).prepareFinalCommit(7);
    }

    @Test
    void emptyFinalPrepareMustNotCommitOrAdvanceIdentifier() throws Exception {
        Fixture f = new Fixture();
        when(f.strategy.prepareFinalCommit(7)).thenReturn(Collections.emptyList());
        f.context.closeForStop(true, phase -> {});
        verify(f.committer, never()).commit(anyLong(), anyList());
        assertEquals(-1, f.saved.get());
        assertTrue(f.context.cleanupComplete());
        f.context.closeForStop(true, phase -> {});
        verify(f.strategy).prepareFinalCommit(7);
        verify(f.strategy).close();
    }

    @Test
    void finalCommitMustRecoverTheExactEnvelopeWithoutPreparingAgain() throws Exception {
        Fixture f = new Fixture();
        List<CommitMessage> messages = Collections.singletonList(compactionMessage());
        when(f.strategy.prepareFinalCommit(7)).thenReturn(messages);
        doThrow(new RuntimeException("response lost")).when(f.committer).commit(7, messages);
        when(f.committer.filterAndCommit(anyMap())).thenAnswer(call -> {
            java.util.Map<Long, List<CommitMessage>> envelope = call.getArgument(0);
            assertSame(messages, envelope.get(7L));
            return 0;
        });
        f.context.closeForStop(true, phase -> {});
        verify(f.strategy).prepareFinalCommit(7);
        assertEquals(8, f.saved.get());
        assertFalse(f.context.hasPendingCommit());
    }

    @Test
    void onlyTypedFinalTaskFailureMayBeDiscarded() throws Exception {
        Fixture f = new Fixture();
        ExecutionException taskFailure;
        PaimonCompactionExecutor executor = new PaimonCompactionExecutor("typed-test");
        try {
            java.util.concurrent.Future<?> task = executor.submit(PaimonCompactionExecutorTest.task(
                    () -> { throw new IOException("spill"); }));
            taskFailure = assertThrows(ExecutionException.class, task::get);
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
        }
        when(f.strategy.prepareFinalCommit(7)).thenThrow(taskFailure);
        assertEquals(PaimonTableWriteContext.StopOutcome.SUCCESS_COMPACTION_DISCARDED,
                f.context.closeForStop(true, phase -> {}));
        verify(f.committer, never()).commit(anyLong(), anyList());
        assertEquals(-1, f.saved.get());
        assertTrue(f.context.cleanupComplete());

        Fixture hard = new Fixture();
        IOException ordinary = new IOException("prepare application failure");
        when(hard.strategy.prepareFinalCommit(7)).thenThrow(ordinary);
        assertSame(ordinary, assertThrows(IOException.class,
                () -> hard.context.closeForStop(true, phase -> {})));
        assertTrue(hard.context.cleanupComplete(), "业务结果与资源清理证明必须独立");
    }

    @Test
    void unknownFinalCommitMustStayPendingAndMustNotBeDiscarded() throws Exception {
        Fixture f = new Fixture();
        List<CommitMessage> messages = Collections.singletonList(compactionMessage());
        when(f.strategy.prepareFinalCommit(7)).thenReturn(messages);
        RuntimeException direct = new RuntimeException("commit unknown");
        RuntimeException recovery = new RuntimeException("confirmation unavailable");
        doThrow(direct).when(f.committer).commit(7, messages);
        when(f.committer.filterAndCommit(anyMap())).thenThrow(recovery);
        assertSame(recovery, assertThrows(RuntimeException.class,
                () -> f.context.closeForStop(true, phase -> {})));
        assertTrue(f.context.hasPendingCommit());
        assertEquals(-1, f.saved.get());
        assertTrue(f.context.cleanupComplete());
        assertSame(recovery, assertThrows(RuntimeException.class,
                () -> f.context.closeForStop(true, phase -> {})));
    }

    static CommitMessage compactionMessage() {
        return new CommitMessageImpl(BinaryRow.EMPTY_ROW, 0, 1, DataIncrement.emptyIncrement(),
                new CompactIncrement(Collections.singletonList(mock(DataFileMeta.class)),
                        Collections.singletonList(mock(DataFileMeta.class)), Collections.emptyList()));
    }

    private static class Fixture {
        final PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        final PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        final AtomicLong saved = new AtomicLong(-1);
        final PaimonTableWriteContext context = new PaimonTableWriteContext("default.t", "t", "user",
                strategy, committer, null, Collections.emptyList(), 7, saved::set);
    }
}
