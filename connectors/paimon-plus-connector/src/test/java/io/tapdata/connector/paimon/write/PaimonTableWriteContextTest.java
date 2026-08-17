package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.CommitMessage;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonTableWriteContextTest {

    @Test
    void writeMustOnlyCallStrategy() throws Exception {
        Fixture fixture = new Fixture(0L);
        InternalRow row = GenericRow.of(1);

        fixture.context.write(row);

        verify(fixture.strategy).write(org.mockito.ArgumentMatchers.same(row));
        verify(fixture.committer, never()).commit(anyLong(), anyList());
        verify(fixture.committer, never()).filterAndCommit(anyMap());
    }

    @Test
    void routingValidationMustUseMaterializedTargetRow() {
        Fixture fixture = new Fixture(0L);
        InternalRow row = GenericRow.of(1, 2);

        fixture.context.validateRoutingRow(row, "UPDATE_AFTER");

        verify(fixture.strategy)
                .validateRoutingRow(
                        org.mockito.ArgumentMatchers.same(row),
                        org.mockito.ArgumentMatchers.eq("UPDATE_AFTER"));
    }

    @Test
    void firstAttemptMustPersistPendingThenCallDirectCommitOnly() throws Exception {
        Fixture fixture = new Fixture(7L);
        CommitMessage message = mock(CommitMessage.class);
        List<CommitMessage> messages = Collections.singletonList(message);
        when(fixture.strategy.prepareCommit(7L)).thenReturn(messages);
        doAnswer(
                        invocation -> {
                            assertTrue(
                                    fixture.context.hasPendingCommit(),
                                    "pending must exist before Paimon commit I/O starts");
                            assertSame(messages, invocation.getArgument(1));
                            return null;
                        })
                .when(fixture.committer)
                .commit(eq(7L), same(messages));

        assertEquals(7L, fixture.context.commit());

        verify(fixture.strategy).prepareCommit(7L);
        verify(fixture.committer).commit(7L, messages);
        verify(fixture.committer, never()).filterAndCommit(anyMap());
        assertFalse(fixture.context.hasPendingCommit());
    }

    @Test
    void directFailureAndRecoveryFailureMustRetainExactPendingForFilterOnlyRetry()
            throws Exception {
        Fixture fixture = new Fixture(3L);
        List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
        when(fixture.strategy.prepareCommit(3L)).thenReturn(messages);
        RuntimeException directFailure = new RuntimeException("direct outcome unknown");
        RuntimeException recoveryFailure = new RuntimeException("recovery unavailable");
        doThrow(directFailure).when(fixture.committer).commit(3L, messages);
        when(fixture.committer.filterAndCommit(anyMap()))
                .thenThrow(recoveryFailure)
                .thenReturn(0);

        RuntimeException thrown = assertThrows(RuntimeException.class, fixture.context::commit);
        assertSame(recoveryFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(directFailure, thrown.getSuppressed()[0]);
        assertTrue(fixture.context.hasPendingCommit());
        assertThrows(IllegalStateException.class, () -> fixture.context.write(GenericRow.of(2)));
        assertEquals(
                3L,
                fixture.context.commit(),
                "commit with pending must confirm the old envelope instead of preparing again");

        verify(fixture.strategy).prepareCommit(3L);
        verify(fixture.committer).commit(3L, messages);
        ArgumentCaptor<Map<Long, List<CommitMessage>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(fixture.committer, times(2)).filterAndCommit(captor.capture());
        assertEquals(3L, captor.getAllValues().get(0).keySet().iterator().next());
        assertSame(messages, captor.getAllValues().get(0).get(3L));
        assertSame(messages, captor.getAllValues().get(1).get(3L));
    }

    @Test
    void failureAfterSnapshotMustBeConfirmedByFilterWithoutApplyingMessagesAgain()
            throws Exception {
        assertDirectFailureRecovered(0);
    }

    @Test
    void failureBeforeSnapshotMustLetFilterCommitTheOriginalMessages() throws Exception {
        assertDirectFailureRecovered(1);
    }

    private static void assertDirectFailureRecovered(int filterResult) throws Exception {
        PaimonTableWriteContext.CommitStateStore stateStore =
                mock(PaimonTableWriteContext.CommitStateStore.class);
        Fixture fixture = new Fixture(1L, stateStore);
        List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
        when(fixture.strategy.prepareCommit(1L)).thenReturn(messages);
        doThrow(new RuntimeException("direct outcome unknown"))
                .when(fixture.committer)
                .commit(1L, messages);
        when(fixture.committer.filterAndCommit(anyMap())).thenReturn(filterResult);

        assertEquals(1L, fixture.context.commit());

        ArgumentCaptor<Map<Long, List<CommitMessage>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(fixture.committer).filterAndCommit(captor.capture());
        assertEquals(Collections.singleton(1L), captor.getValue().keySet());
        assertSame(messages, captor.getValue().get(1L));
        verify(stateStore).save(2L);
        assertFalse(fixture.context.hasPendingCommit());
    }

    @Test
    void invalidNegativeCommitCountMustFenceAndRetainPending() throws Exception {
        assertInvalidCommitCount(-1);
    }

    @Test
    void commitCountGreaterThanPendingMustFenceAndRetainPending() throws Exception {
        assertInvalidCommitCount(2);
    }

    private static void assertInvalidCommitCount(int count) throws Exception {
        PaimonTableWriteContext.CommitStateStore stateStore =
                mock(PaimonTableWriteContext.CommitStateStore.class);
        Fixture fixture = new Fixture(1L, stateStore);
        when(fixture.strategy.prepareCommit(1L)).thenReturn(Collections.emptyList());
        doThrow(new RuntimeException("direct outcome unknown"))
                .when(fixture.committer)
                .commit(1L, Collections.emptyList());
        when(fixture.committer.filterAndCommit(anyMap())).thenReturn(count);

        assertThrows(IllegalStateException.class, fixture.context::commit);

        assertTrue(fixture.context.hasPendingCommit());
        assertThrows(IllegalStateException.class, fixture.context::commit);
        verify(stateStore, never()).save(org.mockito.ArgumentMatchers.anyLong());
    }

    @Test
    void strategyWriteFailureMustPoisonContext() throws Exception {
        Fixture fixture = new Fixture(0L);
        InternalRow row = GenericRow.of(1);
        RuntimeException failure = new RuntimeException("writer failed");
        doThrow(failure).when(fixture.strategy).write(row);

        assertSame(failure, assertThrows(RuntimeException.class, () -> fixture.context.write(row)));
        assertThrows(IllegalStateException.class, () -> fixture.context.write(GenericRow.of(2)));
        assertThrows(IllegalStateException.class, fixture.context::commit);
    }

    @Test
    void strategyPrepareFailureMustPoisonContextAndSkipCommitter() throws Exception {
        Fixture fixture = new Fixture(5L);
        RuntimeException failure = new RuntimeException("assigner prepare failed");
        when(fixture.strategy.prepareCommit(5L)).thenThrow(failure);

        assertSame(failure, assertThrows(RuntimeException.class, fixture.context::commit));
        assertFalse(fixture.context.hasPendingCommit());
        assertThrows(IllegalStateException.class, fixture.context::commit);
        verify(fixture.committer, never()).filterAndCommit(anyMap());
        verify(fixture.committer, never()).commit(anyLong(), anyList());
    }

    @Test
    void nullPreparedMessagesMustFenceBeforePublishingPendingOrCallingCommitter()
            throws Exception {
        Fixture fixture = new Fixture(5L);
        when(fixture.strategy.prepareCommit(5L)).thenReturn(null);

        assertThrows(NullPointerException.class, fixture.context::commit);

        assertFalse(fixture.context.hasPendingCommit());
        assertThrows(IllegalStateException.class, fixture.context::commit);
        verify(fixture.committer, never()).commit(anyLong(), anyList());
        verify(fixture.committer, never()).filterAndCommit(anyMap());
    }

    @Test
    void confirmedCommitMustPersistNextIdentifier() throws Exception {
        PaimonTableWriteContext.CommitStateStore stateStore =
                mock(PaimonTableWriteContext.CommitStateStore.class);
        Fixture fixture = new Fixture(9L, stateStore);
        when(fixture.strategy.prepareCommit(9L)).thenReturn(Collections.emptyList());

        assertEquals(9L, fixture.context.commit());

        verify(fixture.committer).commit(9L, Collections.emptyList());
        verify(fixture.committer, never()).filterAndCommit(anyMap());
        verify(stateStore).save(10L);
    }

    @Test
    void stateSaveFailureMustNotRecommitConfirmedMessages() throws Exception {
        PaimonTableWriteContext.CommitStateStore stateStore =
                mock(PaimonTableWriteContext.CommitStateStore.class);
        doThrow(new RuntimeException("state unavailable")).when(stateStore).save(2L);
        Fixture fixture = new Fixture(1L, stateStore);
        when(fixture.strategy.prepareCommit(1L)).thenReturn(Collections.emptyList());

        assertThrows(RuntimeException.class, fixture.context::commit);

        assertFalse(fixture.context.hasPendingCommit());
        assertThrows(IllegalStateException.class, fixture.context::commit);
        verify(fixture.committer).commit(1L, Collections.emptyList());
        verify(fixture.committer, never()).filterAndCommit(anyMap());
    }

    @Test
    void maximumIdentifierMustFailBeforePrepare() throws Exception {
        Fixture fixture = new Fixture(Long.MAX_VALUE);

        assertThrows(IllegalStateException.class, fixture.context::commit);

        verify(fixture.strategy, never()).prepareCommit(org.mockito.ArgumentMatchers.anyLong());
        verify(fixture.committer, never()).commit(anyLong(), anyList());
        verify(fixture.committer, never()).filterAndCommit(anyMap());
    }

    @Test
    void closeMustRunStrategyThenCommitterThenIoAndSuppressLaterFailures() throws Exception {
        IOManager ioManager = mock(IOManager.class);
        Fixture fixture = new Fixture(0L, PaimonTableWriteContext.CommitStateStore.NOOP, ioManager);
        Exception strategyError = new Exception("strategy close");
        Exception committerError = new Exception("committer close");
        Exception ioError = new Exception("io close");
        doThrow(strategyError).when(fixture.strategy).close();
        doThrow(committerError).when(fixture.committer).close();
        doThrow(ioError).when(ioManager).close();

        Exception thrown = assertThrows(Exception.class, fixture.context::close);

        assertSame(strategyError, thrown);
        assertEquals(2, thrown.getSuppressed().length);
        assertSame(committerError, thrown.getSuppressed()[0]);
        assertSame(ioError, thrown.getSuppressed()[1]);
        InOrder order = inOrder(fixture.strategy, fixture.committer, ioManager);
        order.verify(fixture.strategy).close();
        order.verify(fixture.committer).close();
        order.verify(ioManager).close();
    }

    @Test
    void closeMustBeIdempotent() throws Exception {
        IOManager ioManager = mock(IOManager.class);
        Fixture fixture = new Fixture(0L, PaimonTableWriteContext.CommitStateStore.NOOP, ioManager);

        fixture.context.close();
        fixture.context.close();

        verify(fixture.strategy).close();
        verify(fixture.committer).close();
        verify(ioManager).close();
    }

    private static final class Fixture {
        private final PaimonBucketWriterStrategy strategy =
                mock(PaimonBucketWriterStrategy.class);
        private final PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        private final PaimonTableWriteContext context;

        private Fixture(long nextIdentifier) {
            this(nextIdentifier, PaimonTableWriteContext.CommitStateStore.NOOP);
        }

        private Fixture(
                long nextIdentifier,
                PaimonTableWriteContext.CommitStateStore commitStateStore) {
            this(nextIdentifier, commitStateStore, null);
        }

        private Fixture(
                long nextIdentifier,
                PaimonTableWriteContext.CommitStateStore commitStateStore,
                IOManager ioManager) {
            when(strategy.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
            context =
                    new PaimonTableWriteContext(
                            "default.t",
                            "t",
                            "user",
                            strategy,
                            committer,
                            ioManager,
                            Collections.emptyList(),
                            nextIdentifier,
                            commitStateStore);
        }
    }
}
