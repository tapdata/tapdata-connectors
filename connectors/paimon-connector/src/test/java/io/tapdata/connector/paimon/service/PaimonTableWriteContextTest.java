package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.CommitMessage;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
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
        verify(fixture.committer, never()).filterAndCommit(anyMap());
    }

    @Test
    void routingValidationMustUseStrategyRequiredFields() {
        Fixture fixture = new Fixture(0L);
        when(fixture.strategy.requiredRoutingFields())
                .thenReturn(new LinkedHashSet<>(Arrays.asList("id", "pt")));
        Map<String, Object> data = new HashMap<>();
        data.put("id", 1);
        data.put("pt", 2);

        fixture.context.validateRequiredRoutingFields(data, "UPDATE_AFTER");
        data.remove("pt");
        assertThrows(
                PaimonFatalWriteException.class,
                () -> fixture.context.validateRequiredRoutingFields(data, "UPDATE_AFTER"));
        data.put("pt", null);
        assertThrows(
                PaimonFatalWriteException.class,
                () -> fixture.context.validateRequiredRoutingFields(data, "DELETE"));
    }

    @Test
    void commitMustPrepareOnceThenFilterAndCommitOnce() throws Exception {
        Fixture fixture = new Fixture(7L);
        CommitMessage message = mock(CommitMessage.class);
        List<CommitMessage> messages = Collections.singletonList(message);
        when(fixture.strategy.prepareCommit(7L)).thenReturn(messages);
        when(fixture.committer.filterAndCommit(anyMap())).thenReturn(1);

        assertEquals(7L, fixture.context.commit());

        verify(fixture.strategy).prepareCommit(7L);
        ArgumentCaptor<Map<Long, List<CommitMessage>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(fixture.committer).filterAndCommit(captor.capture());
        assertSame(messages, captor.getValue().get(7L));
        assertFalse(fixture.context.hasPendingCommit());
    }

    @Test
    void ambiguousCommitRetryMustNotPrepareAgain() throws Exception {
        Fixture fixture = new Fixture(3L);
        List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
        when(fixture.strategy.prepareCommit(3L)).thenReturn(messages);
        when(fixture.committer.filterAndCommit(anyMap()))
                .thenThrow(new RuntimeException("unknown outcome"))
                .thenReturn(1);

        assertThrows(RuntimeException.class, fixture.context::commit);
        assertTrue(fixture.context.hasPendingCommit());
        assertThrows(IllegalStateException.class, () -> fixture.context.write(GenericRow.of(2)));
        assertEquals(3L, fixture.context.retryPendingCommit());

        verify(fixture.strategy).prepareCommit(3L);
        ArgumentCaptor<Map<Long, List<CommitMessage>>> captor = ArgumentCaptor.forClass(Map.class);
        verify(fixture.committer, times(2)).filterAndCommit(captor.capture());
        assertEquals(3L, captor.getAllValues().get(0).keySet().iterator().next());
        assertSame(messages, captor.getAllValues().get(0).get(3L));
        assertSame(messages, captor.getAllValues().get(1).get(3L));
    }

    @Test
    void commitFailureMustRetainPendingAndBlockNewWrites() throws Exception {
        Fixture fixture = new Fixture(1L);
        when(fixture.strategy.prepareCommit(1L)).thenReturn(Collections.emptyList());
        when(fixture.committer.filterAndCommit(anyMap()))
                .thenThrow(new RuntimeException("ambiguous"));

        assertThrows(RuntimeException.class, fixture.context::commit);

        assertTrue(fixture.context.hasPendingCommit());
        assertThrows(IllegalStateException.class, () -> fixture.context.write(GenericRow.of(1)));
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
    }

    @Test
    void confirmedCommitMustPersistNextIdentifier() throws Exception {
        PaimonTableWriteContext.CommitStateStore stateStore =
                mock(PaimonTableWriteContext.CommitStateStore.class);
        Fixture fixture = new Fixture(9L, stateStore);
        when(fixture.strategy.prepareCommit(9L)).thenReturn(Collections.emptyList());
        when(fixture.committer.filterAndCommit(anyMap())).thenReturn(1);

        assertEquals(9L, fixture.context.commit());

        verify(stateStore).save(10L);
    }

    @Test
    void stateSaveFailureMustNotRecommitConfirmedMessages() throws Exception {
        PaimonTableWriteContext.CommitStateStore stateStore =
                mock(PaimonTableWriteContext.CommitStateStore.class);
        doThrow(new RuntimeException("state unavailable")).when(stateStore).save(2L);
        Fixture fixture = new Fixture(1L, stateStore);
        when(fixture.strategy.prepareCommit(1L)).thenReturn(Collections.emptyList());
        when(fixture.committer.filterAndCommit(anyMap())).thenReturn(1);

        assertThrows(RuntimeException.class, fixture.context::commit);

        assertFalse(fixture.context.hasPendingCommit());
        assertThrows(IllegalStateException.class, fixture.context::commit);
        verify(fixture.committer).filterAndCommit(anyMap());
    }

    @Test
    void maximumIdentifierMustFailBeforePrepare() throws Exception {
        Fixture fixture = new Fixture(Long.MAX_VALUE);

        assertThrows(IllegalStateException.class, fixture.context::commit);

        verify(fixture.strategy, never()).prepareCommit(org.mockito.ArgumentMatchers.anyLong());
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
            when(strategy.requiredRoutingFields()).thenReturn(Collections.emptySet());
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
