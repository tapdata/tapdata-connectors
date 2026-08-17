package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.commit.PaimonMicroBatchCoordinator;

import io.tapdata.connector.paimon.write.PaimonTableCommitter;
import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractTestFactory;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonMicroBatchCommitTest {

    @Test
    void threeSmallCdcCallsMustAccumulateAndCommitExactlyOnceAtSizeThreshold()
            throws Exception {
        Fixture fixture = fixture(3, 30_000);

        fixture.write(cdcEvent(1));
        fixture.write(cdcEvent(2));
        verify(fixture.strategy, never()).prepareCommit(anyLong());

        fixture.write(cdcEvent(3));

        verify(fixture.strategy, times(3)).write(any());
        verify(fixture.strategy, times(1)).prepareCommit(0L);
        verify(fixture.committer, times(1)).commit(anyLong(), anyList());
        verify(fixture.committer, never()).filterAndCommit(anyMap());
        PaimonMicroBatchCoordinator.TableSnapshot state = fixture.state();
        assertEquals(3L, state.acceptedGeneration());
        assertEquals(3L, state.committedGeneration());
        assertEquals(0L, state.bufferedRecordCount());
        assertEquals(0L, state.accumulatedRecordCount());
    }

    @Test
    void callTimeThresholdMustCommitWithoutWaitingForBackgroundScheduler() throws Exception {
        Fixture fixture = fixture(100, 1_000);
        fixture.clock.set(100L);
        fixture.write(cdcEvent(1));
        verify(fixture.strategy, never()).prepareCommit(anyLong());

        fixture.clock.set(1_100L);
        fixture.write(cdcEvent(2));

        verify(fixture.strategy, times(1)).prepareCommit(0L);
        verify(fixture.committer, times(1)).commit(anyLong(), anyList());
        verify(fixture.committer, never()).filterAndCommit(anyMap());
        assertEquals(Long.valueOf(1_100L), fixture.state().commitIntervalBaseTimeMs());
    }

    @Test
    void initialRowsMustOnlyAffectBufferedCountAndAfterInitialSyncCommitsOnce()
            throws Exception {
        Fixture fixture = fixture(1, 1);
        fixture.clock.set(10L);

        fixture.write(initialEvent(1));
        fixture.write(initialEvent(2));

        verify(fixture.strategy, never()).prepareCommit(anyLong());
        assertEquals(2L, fixture.state().bufferedRecordCount());
        assertEquals(0L, fixture.state().accumulatedRecordCount());
        assertEquals(0L, fixture.state().acceptedGeneration());

        fixture.clock.set(20L);
        fixture.service.afterInitialSync(fixture.connectorContext, fixture.tapTable);

        verify(fixture.strategy, times(1)).prepareCommit(0L);
        assertEquals(0L, fixture.state().bufferedRecordCount());
        assertEquals(0L, fixture.state().accumulatedRecordCount());
        assertEquals(Long.valueOf(20L), fixture.state().commitIntervalBaseTimeMs());
    }

    @Test
    void malformedStageMustFailBeforeWriterAndFenceTheService() throws Exception {
        Fixture fixture = fixture(3, 30_000);
        TapInsertRecordEvent missingStage =
                new TapInsertRecordEvent()
                        .init()
                        .table("t")
                        .after(Collections.singletonMap("id", 1));

        PaimonFatalWriteException first = assertThrows(
                PaimonFatalWriteException.class, () -> fixture.write(missingStage));
        Exception second = assertThrows(Exception.class, () -> fixture.write(cdcEvent(2)));

        assertSame(first, second);
        verify(fixture.strategy, never()).write(any());
    }

    @Test
    void cdcWithoutSourceLaneMustFailBeforeWriter() throws Exception {
        Fixture fixture = fixture(3, 30_000);
        TapInsertRecordEvent event = initialEvent(1);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");

        assertThrows(PaimonFatalWriteException.class, () -> fixture.write(event));

        verify(fixture.strategy, never()).write(any());
    }

    @Test
    void ambiguousCommitMustUseExactlyThreePendingConfirmationsAndKeepGenerationBlocked()
            throws Exception {
        Fixture fixture = fixture(1, 30_000);
        RuntimeException first = new RuntimeException("first-ambiguous");
        RuntimeException direct = new RuntimeException("direct-ambiguous");
        RuntimeException retry1 = new RuntimeException("retry-1");
        RuntimeException retry2 = new RuntimeException("retry-2");
        RuntimeException retry3 = new RuntimeException("retry-3");
        doThrow(direct).when(fixture.committer).commit(anyLong(), anyList());
        when(fixture.committer.filterAndCommit(anyMap()))
                .thenThrow(first, retry1, retry2, retry3);

        RuntimeException thrown =
                assertThrows(RuntimeException.class, () -> fixture.write(cdcEvent(1)));

        assertSame(first, thrown);
        assertEquals(4, thrown.getSuppressed().length);
        assertSame(direct, thrown.getSuppressed()[0]);
        assertSame(retry1, thrown.getSuppressed()[1]);
        assertSame(retry2, thrown.getSuppressed()[2]);
        assertSame(retry3, thrown.getSuppressed()[3]);
        verify(fixture.strategy, times(1)).write(any());
        verify(fixture.strategy, times(1)).prepareCommit(0L);
        verify(fixture.committer, times(1)).commit(anyLong(), anyList());
        verify(fixture.committer, times(4)).filterAndCommit(anyMap());
        assertEquals(1L, fixture.state().acceptedGeneration());
        assertEquals(0L, fixture.state().committedGeneration());
        assertEquals(1L, fixture.state().bufferedRecordCount());
        assertTrue(fixture.state().hasPendingCommit());
    }

    private static TapInsertRecordEvent cdcEvent(int id) {
        TapInsertRecordEvent event = initialEvent(id);
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
        event.addInfo("nodeIds", Collections.singletonList("source-a"));
        return event;
    }

    private static TapInsertRecordEvent initialEvent(int id) {
        TapInsertRecordEvent event =
                new TapInsertRecordEvent()
                        .init()
                        .table("t")
                        .after(Collections.singletonMap("id", id));
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "INITIAL_SYNC");
        return event;
    }

    private static Fixture fixture(int batchSize, int intervalMs) throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(batchSize);
        config.setCommitIntervalMs(intervalMs);
        config.setEnableAsyncCommit(false);
        AtomicLong clock = new AtomicLong();
        PaimonService service =
                new PaimonService(config, mock(Log.class), clock::get, () -> { });
        service.setFlushOffsetCallback(ignored -> { });
        service.startForTest();

        PaimonBucketWriterStrategy strategy = mock(PaimonBucketWriterStrategy.class);
        PaimonTableCommitter committer = mock(PaimonTableCommitter.class);
        when(strategy.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        when(strategy.writeSemanticContract())
                .thenReturn(PaimonWriteSemanticContractTestFactory.forMode(BucketMode.HASH_FIXED));
        when(strategy.prepareCommit(anyLong())).thenReturn(Collections.emptyList());
        when(committer.filterAndCommit(anyMap())).thenReturn(0);
        PaimonTableWriteContext context =
                new PaimonTableWriteContext(
                        "default.t",
                        "t",
                        "stable-user",
                        strategy,
                        committer,
                        null,
                        Collections.emptyList(),
                        0L);
        tableContexts(service).put("default.t", context);
        fieldCache(service).put(
                "default.t", Collections.singletonList(new DataField(0, "id", DataTypes.INT())));

        TapTable tapTable = mock(TapTable.class);
        when(tapTable.getName()).thenReturn("t");
        when(tapTable.primaryKeys(true)).thenReturn(Collections.emptyList());
        TapConnectorContext connectorContext = mock(TapConnectorContext.class);
        when(connectorContext.getStateMap()).thenReturn(mock(KVMap.class));
        when(connectorContext.getLog()).thenReturn(mock(Log.class));
        return new Fixture(
                service, strategy, committer, context, tapTable, connectorContext, clock);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, PaimonTableWriteContext> tableContexts(PaimonService service)
            throws Exception {
        Field field = PaimonService.class.getDeclaredField("tableWriteContexts");
        field.setAccessible(true);
        return (Map<String, PaimonTableWriteContext>) field.get(service);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<DataField>> fieldCache(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("paimonFieldCache");
        field.setAccessible(true);
        return (Map<String, List<DataField>>) field.get(service);
    }

    private static PaimonMicroBatchCoordinator coordinator(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("microBatchCoordinator");
        field.setAccessible(true);
        return (PaimonMicroBatchCoordinator) field.get(service);
    }

    private static final class Fixture {
        private final PaimonService service;
        private final PaimonBucketWriterStrategy strategy;
        private final PaimonTableCommitter committer;
        private final PaimonTableWriteContext context;
        private final TapTable tapTable;
        private final TapConnectorContext connectorContext;
        private final AtomicLong clock;

        private Fixture(
                PaimonService service,
                PaimonBucketWriterStrategy strategy,
                PaimonTableCommitter committer,
                PaimonTableWriteContext context,
                TapTable tapTable,
                TapConnectorContext connectorContext,
                AtomicLong clock) {
            this.service = service;
            this.strategy = strategy;
            this.committer = committer;
            this.context = context;
            this.tapTable = tapTable;
            this.connectorContext = connectorContext;
            this.clock = clock;
        }

        private void write(TapInsertRecordEvent event) throws Exception {
            service.writeRecords(Collections.singletonList(event), tapTable, connectorContext);
        }

        private PaimonMicroBatchCoordinator.TableSnapshot state() throws Exception {
            return coordinator(service).tableSnapshot("default.t");
        }
    }
}
