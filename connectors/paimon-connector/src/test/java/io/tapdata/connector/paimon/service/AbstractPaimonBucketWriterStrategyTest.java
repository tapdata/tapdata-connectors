package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AbstractPaimonBucketWriterStrategyTest {

    @Test
    void modeMismatchMustFailConstruction() {
        Fixture fixture = new Fixture(BucketMode.KEY_DYNAMIC);

        assertThrows(
                IllegalArgumentException.class,
                () -> fixture.strategy(BucketMode.HASH_DYNAMIC, Collections.emptyList()));
    }

    @Test
    void requiredRoutingFieldsMustBeOrderedAndImmutable() {
        Fixture fixture = new Fixture(BucketMode.HASH_DYNAMIC);
        List<String> fields = new ArrayList<>(Arrays.asList("id", "pt", "id"));
        TestStrategy strategy = fixture.strategy(BucketMode.HASH_DYNAMIC, fields);
        fields.clear();

        assertEquals(Arrays.asList("id", "pt"), new ArrayList<>(strategy.requiredRoutingFields()));
        assertThrows(
                UnsupportedOperationException.class,
                () -> strategy.requiredRoutingFields().add("later"));
    }

    @Test
    void prepareMustRunModeHookBeforeRawWriterWithSameIdentifier() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_DYNAMIC);
        TestStrategy strategy = fixture.strategy(BucketMode.HASH_DYNAMIC, Collections.emptyList());
        List<CommitMessage> messages = Collections.singletonList(mock(CommitMessage.class));
        when(fixture.writer.prepareCommit(false, 17L)).thenReturn(messages);

        assertSame(messages, strategy.prepareCommit(17L));

        InOrder order = inOrder(fixture.lifecycle, fixture.writer);
        order.verify(fixture.lifecycle).beforePrepare(17L);
        order.verify(fixture.writer).prepareCommit(false, 17L);
    }

    @Test
    void hookFailureMustSkipRawPrepare() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_DYNAMIC);
        TestStrategy strategy = fixture.strategy(BucketMode.HASH_DYNAMIC, Collections.emptyList());
        Exception failure = new Exception("assigner prepare failed");
        doThrow(failure).when(fixture.lifecycle).beforePrepare(3L);

        assertSame(failure, assertThrows(Exception.class, () -> strategy.prepareCommit(3L)));
        verify(fixture.writer, never()).prepareCommit(false, 3L);
    }

    @Test
    void rawPrepareFailureMustPropagateUnchanged() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        TestStrategy strategy = fixture.strategy(BucketMode.HASH_FIXED, Collections.emptyList());
        Exception failure = new Exception("raw prepare failed");
        when(fixture.writer.prepareCommit(false, 9L)).thenThrow(failure);

        assertSame(failure, assertThrows(Exception.class, () -> strategy.prepareCommit(9L)));
    }

    @Test
    void closeMustRunModeResourcesThenWriterAndSuppressLaterFailure() throws Exception {
        Fixture fixture = new Fixture(BucketMode.KEY_DYNAMIC);
        TestStrategy strategy = fixture.strategy(BucketMode.KEY_DYNAMIC, Collections.emptyList());
        Exception modeFailure = new Exception("mode close failed");
        Exception writerFailure = new Exception("writer close failed");
        doThrow(modeFailure).when(fixture.lifecycle).closeModeResources();
        doThrow(writerFailure).when(fixture.writer).close();

        Exception thrown = assertThrows(Exception.class, strategy::close);

        assertSame(modeFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(writerFailure, thrown.getSuppressed()[0]);
        InOrder order = inOrder(fixture.lifecycle, fixture.writer);
        order.verify(fixture.lifecycle).closeModeResources();
        order.verify(fixture.writer).close();

        strategy.close();
        verify(fixture.lifecycle).closeModeResources();
        verify(fixture.writer).close();
    }

    @Test
    void writeAndPrepareMustRejectCallsAfterClose() throws Exception {
        Fixture fixture = new Fixture(BucketMode.HASH_FIXED);
        TestStrategy strategy = fixture.strategy(BucketMode.HASH_FIXED, Collections.emptyList());
        strategy.close();

        assertThrows(IllegalStateException.class, () -> strategy.write(GenericRow.of(1)));
        assertThrows(IllegalStateException.class, () -> strategy.prepareCommit(1L));
        verify(fixture.lifecycle, never()).write(org.mockito.ArgumentMatchers.any());
        verify(fixture.writer, never()).prepareCommit(false, 1L);
    }

    private interface ModeLifecycle {
        void write(InternalRow row) throws Exception;

        void beforePrepare(long identifier) throws Exception;

        void closeModeResources() throws Exception;
    }

    private static final class Fixture {
        private final FileStoreTable table = mock(FileStoreTable.class);
        private final StreamTableWrite writer = mock(StreamTableWrite.class);
        private final ModeLifecycle lifecycle = mock(ModeLifecycle.class);
        private final PaimonBucketWriterStrategyContext context;

        private Fixture(BucketMode actualMode) {
            when(table.bucketMode()).thenReturn(actualMode);
            context = new PaimonBucketWriterStrategyContext("default.t", table, writer, "user", null);
        }

        private TestStrategy strategy(BucketMode expectedMode, List<String> fields) {
            return new TestStrategy(context, expectedMode, fields, lifecycle);
        }
    }

    private static final class TestStrategy extends AbstractPaimonBucketWriterStrategy {
        private final ModeLifecycle lifecycle;

        private TestStrategy(
                PaimonBucketWriterStrategyContext context,
                BucketMode expectedMode,
                List<String> fields,
                ModeLifecycle lifecycle) {
            super(context, expectedMode, fields);
            this.lifecycle = lifecycle;
        }

        @Override
        protected void doWrite(InternalRow row) throws Exception {
            lifecycle.write(row);
        }

        @Override
        protected void beforePrepareCommit(long commitIdentifier) throws Exception {
            lifecycle.beforePrepare(commitIdentifier);
        }

        @Override
        protected void closeModeResources() throws Exception {
            lifecycle.closeModeResources();
        }
    }
}
