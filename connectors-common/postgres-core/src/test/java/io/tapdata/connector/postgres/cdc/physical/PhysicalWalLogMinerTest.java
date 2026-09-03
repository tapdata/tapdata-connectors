package io.tapdata.connector.postgres.cdc.physical;

import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.cdc.NormalRedo;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.logger.Log;
import io.tapdata.exception.TapPdkRetryableEx;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.MockedStatic;

/**
 * Unit tests for the deterministic helper logic of {@link PhysicalWalLogMiner}:
 * WAL segment-size parsing, commit-timestamp (TimestampTz) extraction and LSN
 * string round-tripping. The streaming/decoding path is covered by the page,
 * record and heap-rmgr decoder tests.
 */
public class PhysicalWalLogMinerTest {

    /* PostgreSQL epoch (2000-01-01) expressed as epoch micros, mirrored in the miner. */
    private static final long PG_EPOCH_MILLIS = 946684800000L;

    @Test
    public void testParseSizeUnits() {
        assertEquals(16L * 1024 * 1024, PhysicalWalLogMiner.parseSize("16MB"));
        assertEquals(1024L * 1024 * 1024, PhysicalWalLogMiner.parseSize("1GB"));
        assertEquals(64L * 1024, PhysicalWalLogMiner.parseSize("64kB"));
        assertEquals(8192L, PhysicalWalLogMiner.parseSize("8192B"));
        assertEquals(8192L, PhysicalWalLogMiner.parseSize("8192"));
    }

    @Test
    public void testParseSizeCaseAndSpaces() {
        assertEquals(16L * 1024 * 1024, PhysicalWalLogMiner.parseSize("  16mb "));
        assertEquals(2L * 1024 * 1024 * 1024, PhysicalWalLogMiner.parseSize("2Gb"));
    }

    @Test
    public void testParseSizeInvalid() {
        assertEquals(0L, PhysicalWalLogMiner.parseSize(null));
        assertEquals(0L, PhysicalWalLogMiner.parseSize(""));
        assertEquals(0L, PhysicalWalLogMiner.parseSize("garbage"));
    }

    @Test
    public void testParseCapacityValid() {
        assertEquals(50000, PhysicalWalLogMiner.parseCapacity("50000", 4096));
        assertEquals(8192, PhysicalWalLogMiner.parseCapacity("  8192 ", 4096));
    }

    @Test
    public void testParseCapacityFallback() {
        assertEquals(4096, PhysicalWalLogMiner.parseCapacity(null, 4096));
        assertEquals(4096, PhysicalWalLogMiner.parseCapacity("", 4096));
        assertEquals(4096, PhysicalWalLogMiner.parseCapacity("garbage", 4096));
        assertEquals(4096, PhysicalWalLogMiner.parseCapacity("0", 4096));
        assertEquals(4096, PhysicalWalLogMiner.parseCapacity("-1", 4096));
    }

    @Test
    public void testReadCommitMillisAtEpoch() {
        // TimestampTz == 0 -> exactly 2000-01-01T00:00:00Z
        assertEquals(PG_EPOCH_MILLIS, PhysicalWalLogMiner.readCommitMillis(le64(0L)));
    }

    @Test
    public void testReadCommitMillisOffset() {
        // 5_000_000 micros == 5 seconds after the PG epoch
        assertEquals(PG_EPOCH_MILLIS + 5000L, PhysicalWalLogMiner.readCommitMillis(le64(5_000_000L)));
    }

    @Test
    public void testReadCommitMillisShortDataFallsBack() {
        long before = System.currentTimeMillis();
        long ts = PhysicalWalLogMiner.readCommitMillis(new byte[]{1, 2, 3});
        assertTrue(ts >= before);
        assertEquals(System.currentTimeMillis(), ts, 5000L);
    }

    @Test
    public void testReadSubxactsNoInfoFlag() {
        // Without XLOG_XACT_HAS_INFO there is no xinfo word, hence no subxacts.
        byte[] body = le64(5_000_000L);
        assertEquals(0, PhysicalWalLogMiner.readSubxacts(body, 0).length);
        assertEquals(0, PhysicalWalLogMiner.readSubxacts(null, WalConstants.XLOG_XACT_HAS_INFO).length);
    }

    @Test
    public void testReadSubxactsInfoButNoSubxacts() {
        byte[] body = concat(le64(0L), le32(0)); // xact_time + xinfo=0
        assertEquals(0, PhysicalWalLogMiner.readSubxacts(body, WalConstants.XLOG_XACT_HAS_INFO).length);
    }

    @Test
    public void testReadSubxactsParsesList() {
        byte[] body = concat(le64(0L), le32(WalConstants.XACT_XINFO_HAS_SUBXACTS),
                le32(3), le32(101), le32(102), le32(103));
        long[] subs = PhysicalWalLogMiner.readSubxacts(body, WalConstants.XLOG_XACT_HAS_INFO);
        assertArrayEquals(new long[]{101L, 102L, 103L}, subs);
    }

    @Test
    public void testReadSubxactsSkipsDbInfoBeforeSubxacts() {
        int xinfo = WalConstants.XACT_XINFO_HAS_DBINFO | WalConstants.XACT_XINFO_HAS_SUBXACTS;
        byte[] body = concat(le64(0L), le32(xinfo),
                le32(1234), le32(5678),       // xl_xact_dbinfo: dbId + tsId
                le32(2), le32(55), le32(66)); // nsubxacts + subxids
        long[] subs = PhysicalWalLogMiner.readSubxacts(body, WalConstants.XLOG_XACT_HAS_INFO);
        assertArrayEquals(new long[]{55L, 66L}, subs);
    }

    @Test
    public void testReadAssignmentParsesTopAndSubxids() {
        byte[] body = concat(le32(1000), le32(3), le32(1001), le32(1002), le32(1003));
        PhysicalWalLogMiner.XactAssignment assignment = PhysicalWalLogMiner.readAssignment(body);
        assertEquals(1000L, assignment.topXid);
        assertArrayEquals(new long[]{1001L, 1002L, 1003L}, assignment.subxids);
    }

    @Test
    public void testReadAssignmentMalformedFallsBackToEmpty() {
        PhysicalWalLogMiner.XactAssignment assignment = PhysicalWalLogMiner.readAssignment(new byte[]{1, 2, 3});
        assertEquals(0L, assignment.topXid);
        assertEquals(0, assignment.subxids.length);
    }

    @Test
    public void testMergeSubxidsIncludesAssignmentOnlyChildren() {
        Map<Long, Long> assignments = new LinkedHashMap<>();
        assignments.put(1002L, 1000L);
        assignments.put(1003L, 1000L);
        assignments.put(2001L, 2000L);

        long[] merged = PhysicalWalLogMiner.mergeSubxids(1000L, new long[]{1001L, 1002L}, assignments);

        assertArrayEquals(new long[]{1001L, 1002L, 1003L}, merged);
    }

    @Test
    public void testMergeSubxidsCoversCommitRecordWithoutSubxacts() {
        Map<Long, Long> assignments = new LinkedHashMap<>();
        assignments.put(1003L, 1000L);

        long[] merged = PhysicalWalLogMiner.mergeSubxids(1000L, new long[0], assignments);

        assertArrayEquals(new long[]{1003L}, merged);
    }

    @Test
    public void testCommittedXidsOnlyUseCommitRecordSubxids() {
        Set<Long> committed = PhysicalWalLogMiner.committedXids(1000L, new long[]{1001L});

        assertTrue(committed.contains(1000L));
        assertTrue(committed.contains(1001L));
        assertFalse(committed.contains(1002L));
    }

    @Test
    public void testRedoCommitFilterSkipsAssignmentOnlyRolledBackSubxid() {
        Set<Long> committed = PhysicalWalLogMiner.committedXids(1000L, new long[]{1001L});
        NormalRedo top = redoFromXid(1000L, 10L);
        NormalRedo committedSub = redoFromXid(1001L, 20L);
        NormalRedo rolledBackSub = redoFromXid(1002L, 30L);

        assertTrue(PhysicalWalLogMiner.isCommittedRedo(top, committed));
        assertTrue(PhysicalWalLogMiner.isCommittedRedo(committedSub, committed));
        assertFalse(PhysicalWalLogMiner.isCommittedRedo(rolledBackSub, committed));
    }

    @Test
    public void testRedoCommitFilterKeepsLegacyRedoWithoutSourceXid() {
        NormalRedo redo = redoFromXid(null, 10L);

        assertTrue(PhysicalWalLogMiner.isCommittedRedo(redo,
                PhysicalWalLogMiner.committedXids(1000L, new long[0])));
    }

    @Test
    public void testSourceXidSurvivesSpillSerialization() throws Exception {
        NormalRedo redo = redoFromXid(1002L, 30L);
        redo.setOperation(NormalRedo.OperationEnum.INSERT.name());
        redo.setNameSpace("public");
        redo.setTableName("t");

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(redo);
        }

        NormalRedo restored;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            restored = (NormalRedo) in.readObject();
        }

        assertEquals(1002L, restored.getSourceXid().longValue());
        assertEquals(30L, restored.getCdcSequenceId().longValue());
        assertFalse(PhysicalWalLogMiner.isCommittedRedo(restored,
                PhysicalWalLogMiner.committedXids(1000L, new long[]{1001L})));
    }

    @Test
    public void testLsnStrRoundTrip() {
        String[] samples = {"0/0", "0/16B6A50", "16/B374D848", "FF/FFFFFFFF"};
        for (String s : samples) {
            long asLong = org.postgresql.replication.LogSequenceNumber.valueOf(s).asLong();
            assertEquals(normalize(s), PhysicalWalLogMiner.lsnStr(asLong));
        }
    }

    @Test
    public void testTimelineHistoryFindsSavedTimelineForkPoint() {
        String history = "94\t117/6A000010\tbefore 95\n"
                + "95\t117/6A000AF0\tbefore 96\n"
                + "96\t117/6B000020\tbefore 97\n";

        long switchPoint = PhysicalWalLogMiner.parseTimelineSwitchPoint(history, 95);

        assertEquals(org.postgresql.replication.LogSequenceNumber.valueOf("117/6A000AF0").asLong(), switchPoint);
    }

    @Test
    public void testTimelineHistoryDoesNotUseLastLineBlindly() {
        String history = "95\t117/6A000AF0\tbefore 96\n"
                + "96\t117/6B000020\tbefore 97\n";

        long switchPoint = PhysicalWalLogMiner.parseTimelineSwitchPoint(history, 95);

        assertNotEquals(org.postgresql.replication.LogSequenceNumber.valueOf("117/6B000020").asLong(), switchPoint);
        assertEquals(org.postgresql.replication.LogSequenceNumber.valueOf("117/6A000AF0").asLong(), switchPoint);
    }

    @Test
    public void testTimelineHistorySkipsMalformedNonTargetLines() {
        String history = "# generated history\n"
                + "bad-line\n"
                + "95\t117/6A000AF0\tbefore 96\n";

        long switchPoint = PhysicalWalLogMiner.parseTimelineSwitchPoint(history, 95);

        assertEquals(org.postgresql.replication.LogSequenceNumber.valueOf("117/6A000AF0").asLong(), switchPoint);
    }

    @Test
    public void testTimelineHistoryFindsCurrentTimelineStartPoint() {
        String history = "1\t0/404ADD8\tbefore 2\n"
                + "2\t0/6022198\tbefore 3\n"
                + "3\t0/842A910\tbefore 4\n"
                + "4\t0/8855250\tbefore 5\n"
                + "5\t0/B000578\tbefore 6\n";

        long switchPoint = PhysicalWalLogMiner.parseLastTimelineSwitchPoint(history);

        assertEquals(org.postgresql.replication.LogSequenceNumber.valueOf("0/B000578").asLong(), switchPoint);
    }

    @Test
    public void testTimelineSourcesPreferCurrentNodeAndDeduplicateEntries() {
        PostgresConfig config = new PostgresConfig();
        config.setHost("postgres-master");
        config.setPort(5433);
        config.setDeploymentMode("master-slave");
        ArrayList<LinkedHashMap<String, Object>> nodes = new ArrayList<>();
        nodes.add(node("postgres-slave1", 5434));
        nodes.add(node("postgres-master", 5433));
        nodes.add(node("postgres-slave2", 5435));
        config.setMasterSlaveAddress((ArrayList) nodes);

        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);

        @SuppressWarnings("unchecked")
        List<Object> sources = (List<Object>) ReflectionTestUtils.invokeMethod(miner, "timelineSources");

        assertEquals(3, sources.size());
        assertEquals("postgres-master", ReflectionTestUtils.getField(sources.get(0), "host"));
        assertEquals(5433, ReflectionTestUtils.getField(sources.get(0), "port"));
        assertEquals("postgres-slave1", ReflectionTestUtils.getField(sources.get(1), "host"));
        assertEquals("postgres-slave2", ReflectionTestUtils.getField(sources.get(2), "host"));
    }

    @Test
    public void testRemovedSegmentErrorMatchesNestedCauseChain() {
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        RuntimeException nested = new RuntimeException("requested WAL segment 000000060000000000000008 has already been removed");
        RuntimeException wrapper = new RuntimeException("outer", nested);

        assertTrue((Boolean) ReflectionTestUtils.invokeMethod(miner, "isRemovedSegmentError", wrapper));
    }

    @Test
    public void testMissingReplicationSlotErrorMatchesNestedCauseChain() {
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        RuntimeException nested = new RuntimeException("ERROR: replication slot \"tapdata_cdc\" does not exist");
        RuntimeException wrapper = new RuntimeException("outer", nested);

        assertTrue((Boolean) ReflectionTestUtils.invokeMethod(miner, "isMissingReplicationSlotError", wrapper));
    }

    @Test
    public void testSourceUnavailableBeforeStreamingMatchesNestedConnectException() {
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        RuntimeException wrapper = new RuntimeException("outer",
                new java.net.ConnectException("Connection refused"));

        assertTrue((Boolean) ReflectionTestUtils.invokeMethod(miner, "isSourceUnavailableBeforeStreaming", wrapper));
    }

    @Test
    public void testSourceUnavailableBeforeStreamingDoesNotMatchCopyWriteFailure() {
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        PSQLException copyFailure = new PSQLException("Database connection failed when writing to copy",
                PSQLState.CONNECTION_FAILURE);

        assertFalse((Boolean) ReflectionTestUtils.invokeMethod(miner, "isSourceUnavailableBeforeStreaming", copyFailure));
    }

    @Test
    public void testParseOffsetLsnKeepsExactLsnWithTimelineAnnotation() {
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));

        long parsed = (Long) ReflectionTestUtils.invokeMethod(miner, "parseOffsetLsn", "0/1900A123,timeline=9");

        assertEquals(org.postgresql.replication.LogSequenceNumber.valueOf("0/1900A123").asLong(), parsed);
    }

    @Test
    public void testBuildStartPhysicalReplicationQueryIncludesTimeline() {
        String query = PhysicalWalLogMiner.buildStartPhysicalReplicationQuery("tapdata_cdc",
                org.postgresql.replication.LogSequenceNumber.valueOf("0/3E038628"), 19);

        assertEquals("START_REPLICATION SLOT tapdata_cdc PHYSICAL 0/3E038628 TIMELINE 19", query);
    }

    @Test
    public void testStopWithExceptionStopsConsumerLoopCondition() {
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        assertTrue((Boolean) ReflectionTestUtils.invokeMethod(miner, "shouldContinueConsuming",
                (java.util.function.Supplier<Boolean>) () -> true));

        RuntimeException retryable = new RuntimeException("switch to slave");
        miner.stopWithException(retryable);

        assertFalse((Boolean) ReflectionTestUtils.invokeMethod(miner, "shouldContinueConsuming",
                (java.util.function.Supplier<Boolean>) () -> true));
    }

    @Test
    public void testTimelineHistoryLastSwitchPointSkipsMalformedLines() {
        String history = "bad-line\n"
                + "4\t0/8855250\tbefore 5\n"
                + "# comment\n"
                + "5\t0/B000578\tbefore 6\n";

        long switchPoint = PhysicalWalLogMiner.parseLastTimelineSwitchPoint(history);

        assertEquals(org.postgresql.replication.LogSequenceNumber.valueOf("0/B000578").asLong(), switchPoint);
    }

    @Test
    public void testTimelineForLsnFollowsHistoryChain() {
        String history = "4\t0/8855250\tbefore 5\n"
                + "5\t0/B000578\tbefore 6\n";
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "timelineChanged", true);
        ReflectionTestUtils.setField(miner, "savedTimeline", 4);
        ReflectionTestUtils.setField(miner, "currentTimeline", 6);
        ReflectionTestUtils.setField(miner, "timelineHistoryChain", PhysicalWalLogMiner.parseTimelineHistoryChain(history));

        int beforeFirstFork = (Integer) ReflectionTestUtils.invokeMethod(miner, "timelineForLsn",
                org.postgresql.replication.LogSequenceNumber.valueOf("0/8854000").asLong());
        int betweenForks = (Integer) ReflectionTestUtils.invokeMethod(miner, "timelineForLsn",
                org.postgresql.replication.LogSequenceNumber.valueOf("0/A000000").asLong());
        int currentTimeline = (Integer) ReflectionTestUtils.invokeMethod(miner, "timelineForLsn",
                org.postgresql.replication.LogSequenceNumber.valueOf("0/B000578").asLong());

        assertEquals(4, beforeFirstFork);
        assertEquals(5, betweenForks);
        assertEquals(6, currentTimeline);
    }

    @Test
    public void testTimelineCatchupOnStandbySchedulesPrimaryRestoreWhenSlavePreferredDisabled() {
        PostgresConfig config = new PostgresConfig();
        config.setHost("postgres-master");
        config.setPort(5433);
        config.setDeploymentMode("master-slave");
        config.setCheckCdcSlave(false);
        ArrayList<LinkedHashMap<String, Object>> nodes = new ArrayList<>();
        nodes.add(node("postgres-slave1", 5434));
        config.setMasterSlaveAddress((ArrayList) nodes);
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);
        ReflectionTestUtils.setField(miner, "timelineChanged", true);
        ReflectionTestUtils.setField(miner, "savedTimeline", 5);
        ReflectionTestUtils.setField(miner, "currentTimeline", 6);
        ReflectionTestUtils.setField(miner, "timelineHistoryChain",
                PhysicalWalLogMiner.parseTimelineHistoryChain("5\t0/B000578\tbefore 6\n"));
        ReflectionTestUtils.setField(miner, "currentTimelineStartPoint",
                org.postgresql.replication.LogSequenceNumber.valueOf("0/B000578").asLong());
        @SuppressWarnings("unchecked")
        List<Object> sources = (List<Object>) ReflectionTestUtils.invokeMethod(miner, "timelineSources");
        Object primary = sources.get(0);
        Object standby = sources.get(1);

        ReflectionTestUtils.invokeMethod(miner, "configurePrimaryRestoreAfterTimelineCatchup", standby, primary,
                org.postgresql.replication.LogSequenceNumber.valueOf("0/A000000").asLong());

        assertEquals(true, ReflectionTestUtils.getField(miner, "restorePrimaryAfterTimelineCatchup"));
        assertEquals("postgres-slave1:5434", ReflectionTestUtils.getField(miner, "timelineCatchupSourceId"));
    }

    @Test
    public void testTimelineCatchupKeepsStandbyWhenSlavePreferredEnabled() {
        PostgresConfig config = new PostgresConfig();
        config.setHost("postgres-master");
        config.setPort(5433);
        config.setDeploymentMode("master-slave");
        config.setCheckCdcSlave(true);
        ArrayList<LinkedHashMap<String, Object>> nodes = new ArrayList<>();
        nodes.add(node("postgres-slave1", 5434));
        config.setMasterSlaveAddress((ArrayList) nodes);
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);
        ReflectionTestUtils.setField(miner, "timelineChanged", true);
        ReflectionTestUtils.setField(miner, "savedTimeline", 5);
        ReflectionTestUtils.setField(miner, "currentTimeline", 6);
        ReflectionTestUtils.setField(miner, "timelineHistoryChain",
                PhysicalWalLogMiner.parseTimelineHistoryChain("5\t0/B000578\tbefore 6\n"));
        ReflectionTestUtils.setField(miner, "currentTimelineStartPoint",
                org.postgresql.replication.LogSequenceNumber.valueOf("0/B000578").asLong());
        @SuppressWarnings("unchecked")
        List<Object> sources = (List<Object>) ReflectionTestUtils.invokeMethod(miner, "timelineSources");
        Object primary = sources.get(0);
        Object standby = sources.get(1);

        ReflectionTestUtils.invokeMethod(miner, "configurePrimaryRestoreAfterTimelineCatchup", standby, primary,
                org.postgresql.replication.LogSequenceNumber.valueOf("0/A000000").asLong());

        assertEquals(true, ReflectionTestUtils.getField(miner, "restorePrimaryAfterTimelineCatchup"));
        assertEquals("postgres-slave1:5434", ReflectionTestUtils.getField(miner, "timelineCatchupSourceId"));
    }

    @Test
    public void testTimelineCatchupRetryMessageUsesStandbyWhenSlavePreferredEnabled() {
        PostgresConfig config = new PostgresConfig();
        config.setCheckCdcSlave(true);
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);
        ReflectionTestUtils.setField(miner, "restorePrimaryAfterTimelineCatchup", true);
        ReflectionTestUtils.setField(miner, "restorePrimaryAtLsn",
                org.postgresql.replication.LogSequenceNumber.valueOf("0/B000578").asLong());
        ReflectionTestUtils.setField(miner, "timelineCatchupSourceId", "postgres-slave1:5434");

        TapPdkRetryableEx retryable = assertThrows(TapPdkRetryableEx.class,
                () -> ReflectionTestUtils.invokeMethod(miner, "maybeRestorePrimaryAfterTimelineCatchup",
                        "0/B000578,timeline=6"));

        assertEquals("TimelineCatchupRetryException", retryable.getCause().getClass().getSimpleName());
        assertTrue(retryable.getCause().getMessage().contains("current-timeline standby node"));
    }

    @Test
    public void testSwitchToCurrentTimelineResumeClearsAncestorTimelineState() {
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        long resumeLsn = org.postgresql.replication.LogSequenceNumber.valueOf("0/B000578").asLong();
        ReflectionTestUtils.setField(miner, "timelineChanged", true);
        ReflectionTestUtils.setField(miner, "savedTimeline", 5);
        ReflectionTestUtils.setField(miner, "currentTimeline", 6);
        ReflectionTestUtils.setField(miner, "timelineHistoryChain",
                PhysicalWalLogMiner.parseTimelineHistoryChain("5\t0/B000578\tbefore 6\n"));
        ReflectionTestUtils.setField(miner, "currentTimelineStartPoint", resumeLsn);
        ReflectionTestUtils.setField(miner, "restorePrimaryAfterTimelineCatchup", true);
        ReflectionTestUtils.setField(miner, "restorePrimaryAtLsn", resumeLsn);
        ReflectionTestUtils.setField(miner, "timelineCatchupSourceId", "postgres-slave1:5434");
        ReflectionTestUtils.setField(miner, "emitFromLsn",
                org.postgresql.replication.LogSequenceNumber.valueOf("0/A000000").asLong());

        ReflectionTestUtils.invokeMethod(miner, "switchToCurrentTimelineResume", resumeLsn);

        assertEquals(false, ReflectionTestUtils.getField(miner, "timelineChanged"));
        assertEquals(6, ReflectionTestUtils.getField(miner, "savedTimeline"));
        assertEquals(Collections.emptyList(), ReflectionTestUtils.getField(miner, "timelineHistoryChain"));
        assertEquals(0L, ReflectionTestUtils.getField(miner, "currentTimelineStartPoint"));
        assertEquals(false, ReflectionTestUtils.getField(miner, "restorePrimaryAfterTimelineCatchup"));
        assertEquals(resumeLsn, ReflectionTestUtils.getField(miner, "emitFromLsn"));
    }

    @Test
    public void testOffsetParsesTimelineAnnotation() {
        PostgresJdbcContext ctx = mock(PostgresJdbcContext.class);
        when(ctx.getConfig()).thenReturn(new PostgresConfig());
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, null);

        miner.offset("117/6A000AF0,timeline=96");

        assertEquals("117/6A000AF0", ReflectionTestUtils.getField(miner, "startLsn"));
        assertEquals(96, ReflectionTestUtils.getField(miner, "savedTimeline"));
    }

    @Test
    public void testKeyChangingUpdateSplitsIntoDeleteThenInsert() {
        NormalRedo u = update(map("id", 1, "v", "a"), map("id", 2, "v", "a"));
        RelationInfo rel = rel("id");
        List<NormalRedo> out = PhysicalWalLogMiner.expandKeyUpdates(Arrays.asList(u), rel);
        assertEquals(2, out.size());
        assertEquals("DELETE", out.get(0).getOperation());
        assertEquals(1, out.get(0).getUndoRecord().get("id"));
        assertEquals("INSERT", out.get(1).getOperation());
        assertEquals(2, out.get(1).getRedoRecord().get("id"));
        // metadata and source LSN are carried onto both halves so ordering holds
        assertEquals(u.getCdcSequenceId(), out.get(0).getCdcSequenceId());
        assertEquals(u.getCdcSequenceId(), out.get(1).getCdcSequenceId());
        assertEquals(u.getSourceXid(), out.get(0).getSourceXid());
        assertEquals(u.getSourceXid(), out.get(1).getSourceXid());
        assertEquals(u.getTableName(), out.get(1).getTableName());
    }

    @Test
    public void testNonKeyUpdateIsNotSplit() {
        NormalRedo u = update(map("id", 1, "v", "a"), map("id", 1, "v", "b"));
        List<NormalRedo> in = Arrays.asList(u);
        List<NormalRedo> out = PhysicalWalLogMiner.expandKeyUpdates(in, rel("id"));
        assertSame(in, out);
        assertFalse(PhysicalWalLogMiner.isKeyChangingUpdate(u, Collections.singletonList("id")));
    }

    @Test
    public void testNullBeforeImageKeepsUpdate() {
        NormalRedo u = update(null, map("id", 2, "v", "a"));
        assertFalse(PhysicalWalLogMiner.isKeyChangingUpdate(u, Collections.singletonList("id")));
        assertSame(u, PhysicalWalLogMiner.expandKeyUpdates(Arrays.asList(u), rel("id")).get(0));
    }

    @Test
    public void testBeforeImageMissingKeyColumnKeepsUpdate() {
        // before-image recovered but without the key column -> cannot decide, keep UPDATE
        NormalRedo u = update(map("v", "a"), map("id", 2, "v", "a"));
        assertFalse(PhysicalWalLogMiner.isKeyChangingUpdate(u, Collections.singletonList("id")));
    }

    @Test
    public void testCompositeKeyAnyColumnChangeSplits() {
        NormalRedo u = update(map("a", 1, "b", 9), map("a", 1, "b", 10));
        assertTrue(PhysicalWalLogMiner.isKeyChangingUpdate(u, Arrays.asList("a", "b")));
    }

    @Test
    public void testNoKeyColumnsKeepsUpdate() {
        NormalRedo u = update(map("id", 1), map("id", 2));
        List<NormalRedo> in = Arrays.asList(u);
        assertSame(in, PhysicalWalLogMiner.expandKeyUpdates(in, rel()));
    }

    private static NormalRedo update(Map<String, Object> before, Map<String, Object> after) {
        NormalRedo r = new NormalRedo();
        r.setOperation(NormalRedo.OperationEnum.UPDATE.name());
        r.setNameSpace("public");
        r.setTableName("t");
        r.setTransactionId("42");
        r.setCdcSequenceId(100L);
        r.setSourceXid(1000L);
        r.setUndoRecord(before);
        r.setRedoRecord(after);
        return r;
    }

    private static NormalRedo redoFromXid(Long sourceXid, Long lsn) {
        NormalRedo r = new NormalRedo();
        r.setSourceXid(sourceXid);
        r.setCdcSequenceId(lsn);
        return r;
    }

    private static LinkedHashMap<String, Object> node(String host, int port) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put("host", host);
        map.put("port", port);
        return map;
    }

    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    private static RelationInfo rel(String... keys) {
        return new RelationInfo("public", "t", Collections.<ColumnInfo>emptyList(), Arrays.asList(keys), false);
    }

    /* LogSequenceNumber.asString() emits upper-case hex with no leading zeros in
     * each half except the trailing half is zero-padded to 8 hex digits. */
    private static String normalize(String s) {
        return org.postgresql.replication.LogSequenceNumber.valueOf(s).asString();
    }

    private static byte[] le64(long v) {
        byte[] b = new byte[8];
        for (int i = 0; i < 8; i++) {
            b[i] = (byte) (v >>> (8 * i));
        }
        return b;
    }

    private static byte[] le32(int v) {
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            b[i] = (byte) (v >>> (8 * i));
        }
        return b;
    }

    private static byte[] concat(byte[]... parts) {
        int len = 0;
        for (byte[] p : parts) {
            len += p.length;
        }
        byte[] out = new byte[len];
        int pos = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }

    /* ── stalled-stream detection (failover auto-recovery) ───────────────── */

    /* PostgresJdbcContext whose timeline probe (queryCurrentTimeline) reports the
     * given WAL file-name prefix, e.g. "00000002" → timeline 2. */
    private PostgresJdbcContext timelineProbeContext(String walfileName) throws Exception {
        PostgresJdbcContext ctx = mock(PostgresJdbcContext.class);
        doAnswer(inv -> {
            ResultSetConsumer consumer = inv.getArgument(1);
            ResultSet rs = mock(ResultSet.class);
            when(rs.getString(1)).thenReturn(walfileName);
            consumer.accept(rs);
            return null;
        }).when(ctx).queryWithNext(anyString(), any(ResultSetConsumer.class));
        return ctx;
    }

    @SuppressWarnings("unchecked")
    private Throwable stallSignal(PhysicalWalLogMiner miner) {
        AtomicReference<Throwable> ref =
                (AtomicReference<Throwable>) ReflectionTestUtils.getField(miner, "threadException");
        return ref == null ? null : ref.get();
    }

    @Test
    public void testStallDetectionTimelineChangedSignalsRecovery() throws Exception {
        // Source timeline moved 1 -> 2 while the stream was connected (failover):
        // the probe must raise a retryable stall signal the recovery loop handles.
        PostgresJdbcContext ctx = timelineProbeContext("00000002");
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 1);

        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);

        Throwable signal = stallSignal(miner);
        assertNotNull(signal);
        assertTrue(signal instanceof TapPdkRetryableEx);
        assertTrue((Boolean) ReflectionTestUtils.invokeMethod(miner, "isStallRecoverySignal", signal));
    }

    @Test
    public void testStallDetectionTimelineUnchangedKeepsStreamOpen() throws Exception {
        // Idle but healthy cluster: same timeline, no writes — keep waiting,
        // never raise a signal.
        PostgresJdbcContext ctx = timelineProbeContext("00000001");
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 1);

        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);

        assertNull(stallSignal(miner));
        assertEquals(0, ReflectionTestUtils.getField(miner, "stallProbeFailures"));
    }

    @Test
    public void testStallDetectionNotDueWhileDataIsFlowing() throws Exception {
        // lastDataMs is current → within the stall timeout, no probe at all,
        // even though a probe would report a timeline change.
        PostgresJdbcContext ctx = timelineProbeContext("00000002");
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 1);

        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis());

        assertNull(stallSignal(miner));
    }

    @Test
    public void testStallDetectionProbeFailuresEscalateToRestart() throws Exception {
        // Source unreachable (timeline probe fails): tolerate a few failures,
        // then raise the stall signal so the framework can restart the task.
        PostgresJdbcContext ctx = mock(PostgresJdbcContext.class);
        doThrow(new SQLException("connection refused"))
                .when(ctx).queryWithNext(anyString(), any(ResultSetConsumer.class));
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 1);

        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);
        assertNull(stallSignal(miner)); // 1st failure tolerated
        ReflectionTestUtils.setField(miner, "lastStallProbeMs", 0L);
        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);
        assertNull(stallSignal(miner)); // 2nd failure tolerated
        ReflectionTestUtils.setField(miner, "lastStallProbeMs", 0L);
        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);
        assertNotNull(stallSignal(miner)); // 3rd failure -> restart signal
    }

    @Test
    public void testStallDetectionProbeIntervalThrottlesRepeatedChecks() throws Exception {
        // Two calls within the probe interval must probe the source only once,
        // even though the stream stays stalled between them.
        AtomicInteger probes = new AtomicInteger();
        PostgresJdbcContext ctx = mock(PostgresJdbcContext.class);
        doAnswer(inv -> {
            probes.incrementAndGet();
            ResultSetConsumer consumer = inv.getArgument(1);
            ResultSet rs = mock(ResultSet.class);
            when(rs.getInt(1)).thenReturn(2);
            when(rs.getString(1)).thenReturn("00000002");
            consumer.accept(rs);
            return null;
        }).when(ctx).queryWithNext(anyString(), any(ResultSetConsumer.class));
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 1);

        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);
        assertNotNull(stallSignal(miner));
        assertEquals(1, probes.get());
        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);
        assertEquals(1, probes.get()); // throttled: no second probe within the interval
    }

    @Test
    public void testIsStallRecoverySignalRejectsUnrelatedRetryable() {
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        TapPdkRetryableEx unrelated = new TapPdkRetryableEx("postgres", new RuntimeException("switch to slave"));

        assertFalse((Boolean) ReflectionTestUtils.invokeMethod(miner, "isStallRecoverySignal", unrelated));
    }

    @Test
    public void testDetectTimelineChangeNoChangeReturnsFalseAndResetsState() {
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 1);
        ReflectionTestUtils.setField(miner, "currentTimeline", 1);
        ReflectionTestUtils.setField(miner, "timelineChanged", true); // stale from an earlier cycle

        boolean changed = (Boolean) ReflectionTestUtils.invokeMethod(miner, "detectTimelineChange");

        assertFalse(changed);
        assertEquals(false, ReflectionTestUtils.getField(miner, "timelineChanged"));
    }

    @Test
    public void testDetectTimelineChangeReportsChangeWithHistory() throws Exception {
        // 1 -> 2 with a readable history file: must populate the switch point.
        PostgresJdbcContext ctx = mock(PostgresJdbcContext.class);
        doAnswer(inv -> {
            ResultSetConsumer consumer = inv.getArgument(1);
            ResultSet rs = mock(ResultSet.class);
            when(rs.getString(1)).thenReturn("1\t0/B000578\tbefore 2\n");
            consumer.accept(rs);
            return null;
        }).when(ctx).queryWithNext(anyString(), any(ResultSetConsumer.class));
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 1);
        ReflectionTestUtils.setField(miner, "currentTimeline", 2);
        ReflectionTestUtils.setField(miner, "startLsn", "0/9000000");

        boolean changed = (Boolean) ReflectionTestUtils.invokeMethod(miner, "detectTimelineChange");

        assertTrue(changed);
        assertEquals(true, ReflectionTestUtils.getField(miner, "timelineChanged"));
        assertTrue((Long) ReflectionTestUtils.getField(miner, "currentTimelineSwitchPoint") > 0L);
    }

    @Test
    public void testDetectTimelineChangeReadsHistoryFromActiveStreamSource() throws Exception {
        // EFM/VIP failover can leave the base JDBC context on the old primary
        // while the WAL stream is active on a standby/current node. The history
        // lookup must follow the active stream source so switch-point parsing
        // still succeeds.
        PostgresConfig config = new PostgresConfig();
        config.setHost("postgres-master");
        config.setPort(5433);
        config.setDatabase("postgres");
        config.setUser("postgres");
        config.setPassword("postgres");
        config.setDeploymentMode("master-slave");
        ArrayList<LinkedHashMap<String, Object>> nodes = new ArrayList<>();
        nodes.add(node("postgres-slave1", 5434));
        config.setMasterSlaveAddress((ArrayList) nodes);

        PostgresJdbcContext ctx = mock(PostgresJdbcContext.class);
        doThrow(new SQLException("old primary cannot read history"))
                .when(ctx).queryWithNext(anyString(), any(ResultSetConsumer.class));
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);
        ReflectionTestUtils.setField(miner, "savedTimeline", 1);
        ReflectionTestUtils.setField(miner, "currentTimeline", 2);
        ReflectionTestUtils.setField(miner, "startLsn", "0/9000000");

        @SuppressWarnings("unchecked")
        List<Object> sources = (List<Object>) ReflectionTestUtils.invokeMethod(miner, "timelineSources");
        ReflectionTestUtils.setField(miner, "activeTimelineSource", sources.get(1)); // slave1

        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            Connection conn = mock(Connection.class);
            PreparedStatement ps = mock(PreparedStatement.class);
            ResultSet rs = mock(ResultSet.class);
            when(rs.next()).thenReturn(true);
            when(rs.getString(1)).thenReturn("1\t0/B000578\tbefore 2\n");
            when(ps.executeQuery()).thenReturn(rs);
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            dm.when(() -> DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(conn);

            boolean changed = (Boolean) ReflectionTestUtils.invokeMethod(miner, "detectTimelineChange");

            assertTrue(changed);
            assertEquals(true, ReflectionTestUtils.getField(miner, "timelineChanged"));
            assertEquals(0x0B000578L, ReflectionTestUtils.getField(miner, "currentTimelineSwitchPoint"));
        }
        verify(ctx, never()).queryWithNext(anyString(), any(ResultSetConsumer.class));
    }

    @Test
    public void testStallDetectionFailbackToOffsetTimelineDetected() throws Exception {
        // Right after a recovery the stream runs on the new timeline while the
        // persisted offset keeps its old-timeline tag until the first commit; a
        // failback to the offset's original timeline must still be detected by
        // comparing against the current (expected) timeline.
        PostgresJdbcContext ctx = timelineProbeContext("00000001"); // live timeline 1
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 1);
        ReflectionTestUtils.setField(miner, "currentTimeline", 2); // stream expected on 2

        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);

        Throwable signal = stallSignal(miner);
        assertNotNull(signal);
        assertTrue(signal instanceof TapPdkRetryableEx);
    }

    @Test
    public void testRecoveryRecreatesPageCaches() throws Exception {
        // A failover moves the WAL source onto a new timeline; the page overlays
        // must be recreated (mirroring a task restart) so old-timeline page
        // states cannot feed wrong before-images on the new timeline.
        PostgresJdbcContext ctx = mock(PostgresJdbcContext.class);
        doAnswer(inv -> null).when(ctx).query(anyString(), any(ResultSetConsumer.class));
        doAnswer(inv -> null).when(ctx).queryWithNext(anyString(), any(ResultSetConsumer.class));
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", new PostgresConfig());
        // Seed stale old-timeline overlays so the test proves replacement, not
        // just creation (both fields start null on a fresh miner).
        ReflectionTestUtils.setField(miner, "pageCache", new PageStateCache(100));
        ReflectionTestUtils.setField(miner, "decodeCtx", new HeapRmgrDecoder.Ctx(new PageStateCache(100), false, null));

        Object beforePageCache = ReflectionTestUtils.getField(miner, "pageCache");
        Object beforeDecodeCtx = ReflectionTestUtils.getField(miner, "decodeCtx");
        ReflectionTestUtils.invokeMethod(miner, "resetCachesForRecovery");
        Object afterPageCache = ReflectionTestUtils.getField(miner, "pageCache");
        Object afterDecodeCtx = ReflectionTestUtils.getField(miner, "decodeCtx");

        assertNotNull(afterPageCache);
        assertNotNull(afterDecodeCtx);
        assertNotSame(beforePageCache, afterPageCache);
        assertNotSame(beforeDecodeCtx, afterDecodeCtx);
    }

    @Test
    public void testRecoveryClearsSpillState() throws Exception {
        // The stalled stream may leave in-flight spill writers; recovery must
        // close and drop them so a re-delivered xid starts a fresh bucket.
        PostgresJdbcContext ctx = mock(PostgresJdbcContext.class);
        doAnswer(inv -> null).when(ctx).query(anyString(), any(ResultSetConsumer.class));
        doAnswer(inv -> null).when(ctx).queryWithNext(anyString(), any(ResultSetConsumer.class));
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", new PostgresConfig());
        @SuppressWarnings("unchecked")
        Map<Long, Object> spillStates =
                (Map<Long, Object>) ReflectionTestUtils.getField(miner, "spillStates");
        Class<?> spillClass = Class.forName(
                "io.tapdata.connector.postgres.cdc.physical.PhysicalWalLogMiner$SpillState");
        java.lang.reflect.Constructor<?> ctor = spillClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        spillStates.put(42L, ctor.newInstance());

        ReflectionTestUtils.invokeMethod(miner, "resetSpillStateForRecovery");

        assertTrue(spillStates.isEmpty());
    }

    /* ── codexcli review fixes ─────────────────────────────────────────── */

    @Test
    public void testSeedSavedTimelineBaselineFillsFreshStartGap() throws Exception {
        // Fresh-start tasks (offsetState==null) and bare-LSN offsets leave
        // savedTimeline==0; without seeding, detectTimelineChange() would never
        // fire on the first failover and the recovery would be handed to the
        // task framework instead of resuming in-process.
        PostgresJdbcContext ctx = timelineProbeContext("00000002"); // live timeline 2
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        miner.offset(null); // fresh start: no timeline tag
        ReflectionTestUtils.setField(miner, "currentTimeline", 2);
        assertEquals(0, ReflectionTestUtils.getField(miner, "savedTimeline"));

        ReflectionTestUtils.invokeMethod(miner, "seedSavedTimelineBaseline");

        assertEquals(2, ReflectionTestUtils.getField(miner, "savedTimeline"));
    }

    @Test
    public void testSeedSavedTimelineBaselineDoesNotOverrideKnownTimeline() throws Exception {
        // A restored offset that carries timeline=N must never be overwritten
        // by the startup baseline — that annotation is the source of truth for
        // detecting a change after the task was down.
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        miner.offset("117/6A000AF0,timeline=96");
        ReflectionTestUtils.setField(miner, "currentTimeline", 96);
        assertEquals(96, ReflectionTestUtils.getField(miner, "savedTimeline"));

        ReflectionTestUtils.invokeMethod(miner, "seedSavedTimelineBaseline");

        assertEquals(96, ReflectionTestUtils.getField(miner, "savedTimeline"));
    }

    @Test
    public void testFreshStartFailoverBecomesDetectableAfterSeed() throws Exception {
        // codexcli: "offsetState=null, first emitted offset has timeline,
        // failover triggers in-process recovery". After seeding, a timeline
        // move 1 -> 2 must be reported by detectTimelineChange() so the
        // recovery loop resumes in-process instead of restarting.
        PostgresJdbcContext ctx = timelineProbeContext("00000002");
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        miner.offset(null);
        ReflectionTestUtils.setField(miner, "currentTimeline", 1); // startup on timeline 1
        ReflectionTestUtils.invokeMethod(miner, "seedSavedTimelineBaseline");

        ReflectionTestUtils.setField(miner, "currentTimeline", 2); // failover
        boolean changed = (Boolean) ReflectionTestUtils.invokeMethod(miner, "detectTimelineChange");

        assertTrue(changed);
        assertEquals(true, ReflectionTestUtils.getField(miner, "timelineChanged"));
    }

    @Test
    public void testQueryActiveTimelinePrefersActiveStreamSource() throws Exception {
        // The live WAL stream may have fallen through to a standby while the
        // base JDBC context still points at the primary. The timeline probe
        // must hit the ACTIVE node, not the base context — otherwise a
        // failover visible only on the streaming node goes undetected.
        PostgresConfig config = new PostgresConfig();
        config.setHost("postgres-master");
        config.setPort(5433);
        config.setDatabase("postgres");
        config.setUser("postgres");
        config.setPassword("postgres");
        config.setDeploymentMode("master-slave");
        ArrayList<LinkedHashMap<String, Object>> nodes = new ArrayList<>();
        nodes.add(node("postgres-slave1", 5434));
        nodes.add(node("postgres-slave2", 5435));
        config.setMasterSlaveAddress((ArrayList) nodes);

        PostgresJdbcContext ctx = timelineProbeContext("00000001"); // base says timeline 1
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);

        @SuppressWarnings("unchecked")
        List<Object> sources = (List<Object>) ReflectionTestUtils.invokeMethod(miner, "timelineSources");
        ReflectionTestUtils.setField(miner, "activeTimelineSource", sources.get(1)); // slave1

        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            Connection conn = mock(Connection.class);
            PreparedStatement ps = mock(PreparedStatement.class);
            ResultSet rs = mock(ResultSet.class);
            when(rs.next()).thenReturn(true);
            when(rs.getString(1)).thenReturn("00000002"); // active node on timeline 2
            when(ps.executeQuery()).thenReturn(rs);
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            dm.when(() -> DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(conn);

            int tli = (Integer) ReflectionTestUtils.invokeMethod(miner, "queryActiveTimeline");

            assertEquals(2, tli);
        }
        // Base context must not have been consulted while the active source answered.
        verify(ctx, never()).queryWithNext(anyString(), any(ResultSetConsumer.class));
    }

    @Test
    public void testQueryActiveTimelineFallsBackToBaseWhenActiveSourceFails() throws Exception {
        // If the active stream node cannot be probed (e.g. it was the old
        // primary and is now unreachable), the probe must degrade to the base
        // JDBC context instead of silently returning "no timeline".
        PostgresConfig config = new PostgresConfig();
        config.setHost("postgres-master");
        config.setPort(5433);
        config.setDatabase("postgres");
        config.setUser("postgres");
        config.setPassword("postgres");
        config.setDeploymentMode("master-slave");
        ArrayList<LinkedHashMap<String, Object>> nodes = new ArrayList<>();
        nodes.add(node("postgres-slave1", 5434));
        config.setMasterSlaveAddress((ArrayList) nodes);

        PostgresJdbcContext ctx = timelineProbeContext("00000001"); // base reports timeline 1
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);

        @SuppressWarnings("unchecked")
        List<Object> sources = (List<Object>) ReflectionTestUtils.invokeMethod(miner, "timelineSources");
        ReflectionTestUtils.setField(miner, "activeTimelineSource", sources.get(1)); // slave1

        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                    .thenAnswer(inv -> {
                        throw new SQLException("connection refused");
                    });

            int tli = (Integer) ReflectionTestUtils.invokeMethod(miner, "queryActiveTimeline");

            assertEquals(1, tli); // fell back to the base context's timeline
        }
    }

    @Test
    public void testStallProbeSignalsWhenActiveStreamNodeTimelineChanged() throws Exception {
        // End-to-end: the stream runs on slave1 (activeTimelineSource) which
        // reports timeline 2 after a failover; the base context (primary) still
        // reports timeline 1. The stall probe must detect the change on the
        // ACTIVE node and raise the recovery signal.
        PostgresConfig config = new PostgresConfig();
        config.setHost("postgres-master");
        config.setPort(5433);
        config.setDatabase("postgres");
        config.setUser("postgres");
        config.setPassword("postgres");
        config.setDeploymentMode("master-slave");
        ArrayList<LinkedHashMap<String, Object>> nodes = new ArrayList<>();
        nodes.add(node("postgres-slave1", 5434));
        config.setMasterSlaveAddress((ArrayList) nodes);

        PostgresJdbcContext ctx = timelineProbeContext("00000001"); // base unchanged
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);
        ReflectionTestUtils.setField(miner, "savedTimeline", 1);
        ReflectionTestUtils.setField(miner, "currentTimeline", 1);

        @SuppressWarnings("unchecked")
        List<Object> sources = (List<Object>) ReflectionTestUtils.invokeMethod(miner, "timelineSources");
        ReflectionTestUtils.setField(miner, "activeTimelineSource", sources.get(1)); // slave1

        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            Connection conn = mock(Connection.class);
            PreparedStatement ps = mock(PreparedStatement.class);
            ResultSet rs = mock(ResultSet.class);
            when(rs.next()).thenReturn(true);
            when(rs.getString(1)).thenReturn("00000002"); // active node on timeline 2
            when(ps.executeQuery()).thenReturn(rs);
            when(conn.prepareStatement(anyString())).thenReturn(ps);
            dm.when(() -> DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(conn);

            ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);
        }

        Throwable signal = stallSignal(miner);
        assertNotNull(signal);
        assertTrue(signal instanceof TapPdkRetryableEx);
        assertTrue((Boolean) ReflectionTestUtils.invokeMethod(miner, "isStallRecoverySignal", signal));
    }

    /* ── review follow-up: advance saved-timeline baseline after recovery ── */

    @Test
    public void testAdvanceSavedTimelineBaselineAfterDirectRecoveryOpen() throws Exception {
        // A stream re-opened directly on the current timeline after a failover
        // must advance the saved-timeline baseline: a second failover resolves
        // its switch point against this timeline (parseTimelineSwitchPoint
        // anchors on savedTimeline), not the pre-failover one.
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 1);
        ReflectionTestUtils.setField(miner, "currentTimeline", 2);
        ReflectionTestUtils.setField(miner, "timelineChanged", true);
        ReflectionTestUtils.setField(miner, "restorePrimaryAfterTimelineCatchup", false);

        ReflectionTestUtils.invokeMethod(miner, "advanceSavedTimelineBaseline");

        assertEquals(2, ReflectionTestUtils.getField(miner, "savedTimeline"));
        assertEquals(false, ReflectionTestUtils.getField(miner, "timelineChanged"));
        assertEquals(0L, ReflectionTestUtils.getField(miner, "currentTimelineSwitchPoint"));
        assertEquals(0L, ReflectionTestUtils.getField(miner, "currentTimelineStartPoint"));
        assertEquals(Collections.emptyList(), ReflectionTestUtils.getField(miner, "timelineHistoryChain"));
    }

    @Test
    public void testAdvanceSavedTimelineBaselineKeepsAncestorCatchupBaseline() throws Exception {
        // While the stream is still catching up on an ancestor timeline, the
        // old baseline must be kept so timelineForLsn() can tag ancestor WAL
        // offsets; switchToCurrentTimelineResume() advances it once the
        // catch-up stream crosses the fork point.
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 1);
        ReflectionTestUtils.setField(miner, "currentTimeline", 2);
        ReflectionTestUtils.setField(miner, "timelineChanged", true);
        ReflectionTestUtils.setField(miner, "restorePrimaryAfterTimelineCatchup", true);

        ReflectionTestUtils.invokeMethod(miner, "advanceSavedTimelineBaseline");

        assertEquals(1, ReflectionTestUtils.getField(miner, "savedTimeline"));
        assertEquals(true, ReflectionTestUtils.getField(miner, "timelineChanged"));
    }

    @Test
    public void testAdvanceStartPastRemovedMovesHalfwayPageAligned() {
        // look-back anchor removed on the new primary: advance halfway toward
        // emitStart, page-aligned, never past it.
        long segSize = 16L * 1024 * 1024; // 16MB segment
        long currentStart = org.postgresql.replication.LogSequenceNumber.valueOf("0/401FE000").asLong();
        long emitStart = org.postgresql.replication.LogSequenceNumber.valueOf("0/4A1FE000").asLong();

        long next = PhysicalWalLogMiner.advanceStartPastRemoved(currentStart, emitStart, segSize);

        assertTrue(next > currentStart);
        assertTrue(next <= emitStart);
        assertEquals(0L, next % 8192L); // page-aligned (XLOG_BLCKSZ = 8KB)
        long remaining = emitStart - currentStart;
        long step = Math.max(remaining / 2, segSize);
        long expect = Math.min(((currentStart + step) / 8192L) * 8192L, emitStart);
        assertEquals(expect, next);
    }

    @Test
    public void testAdvanceStartPastRemovedClampsAtEmitStart() {
        // Less than one segment left before the emit position: clamp there so
        // the re-open reads from (and warms) the readable tail.
        long segSize = 16L * 1024 * 1024;
        long currentStart = org.postgresql.replication.LogSequenceNumber.valueOf("0/4A1FC000").asLong();
        long emitStart = org.postgresql.replication.LogSequenceNumber.valueOf("0/4A1FE000").asLong();

        assertEquals(emitStart, PhysicalWalLogMiner.advanceStartPastRemoved(currentStart, emitStart, segSize));
    }

    @Test
    public void testAdvanceStartPastRemovedAtOrBeyondEmitStartReturnsEmitStart() {
        long segSize = 16L * 1024 * 1024;
        long emitStart = org.postgresql.replication.LogSequenceNumber.valueOf("0/4A1FE000").asLong();

        assertEquals(emitStart, PhysicalWalLogMiner.advanceStartPastRemoved(emitStart, emitStart, segSize));
        assertEquals(emitStart, PhysicalWalLogMiner.advanceStartPastRemoved(emitStart + 8192L, emitStart, segSize));
    }

    @Test
    public void testComputeRecycledResumeResumesFromReadableWhenAhead() {
        // Persisted offset's WAL recycled; node's current readable LSN is well
        // ahead: resume there, page-aligned.
        long savedEmit = org.postgresql.replication.LogSequenceNumber.valueOf("0/5B028970").asLong();
        long readable = org.postgresql.replication.LogSequenceNumber.valueOf("0/5E025BC0").asLong();

        long resume = PhysicalWalLogMiner.computeRecycledResume(readable, savedEmit);

        assertEquals(0L, resume % 8192L); // page-aligned
        assertEquals(org.postgresql.replication.LogSequenceNumber.valueOf("0/5E024000").asLong(), resume);
    }

    @Test
    public void testComputeRecycledResumeReturnsZeroWhenReadableNotAhead() {
        long savedEmit = org.postgresql.replication.LogSequenceNumber.valueOf("0/5B028970").asLong();
        // Node unreachable or its readable position is at/before the saved emit:
        // nothing to gain, the failure must stand.
        assertEquals(0L, PhysicalWalLogMiner.computeRecycledResume(0L, savedEmit));
        assertEquals(0L, PhysicalWalLogMiner.computeRecycledResume(
                org.postgresql.replication.LogSequenceNumber.valueOf("0/5B020000").asLong(), savedEmit));
        assertEquals(0L, PhysicalWalLogMiner.computeRecycledResume(savedEmit, savedEmit));
    }

    @Test
    public void testAdvanceSavedTimelineBaselineNoopOnFreshStart() throws Exception {
        // First startup (no failover) leaves timelineChanged==false; the
        // baseline must not be touched.
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 2);
        ReflectionTestUtils.setField(miner, "currentTimeline", 2);
        ReflectionTestUtils.setField(miner, "timelineChanged", false);
        ReflectionTestUtils.setField(miner, "restorePrimaryAfterTimelineCatchup", false);

        ReflectionTestUtils.invokeMethod(miner, "advanceSavedTimelineBaseline");

        assertEquals(2, ReflectionTestUtils.getField(miner, "savedTimeline"));
        assertEquals(false, ReflectionTestUtils.getField(miner, "timelineChanged"));
    }

    @Test
    public void testStallProbeAncestorCatchupIdleEscalatesToAbandon() throws Exception {
        // Ancestor catch-up mode with an idle stream: the source node's timeline
        // has NOT changed again (so the normal failover branch never fires) but
        // no ancestor WAL is flowing either — the pre-fork segments are missing.
        // After a few idle probes the miner must abandon the catch-up and raise
        // the AncestorCatchupStalledException recovery signal.
        PostgresJdbcContext ctx = timelineProbeContext("00000008");
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 8);
        ReflectionTestUtils.setField(miner, "currentTimeline", 8);
        ReflectionTestUtils.setField(miner, "restorePrimaryAfterTimelineCatchup", true);
        ReflectionTestUtils.setField(miner, "restorePrimaryAtLsn",
                org.postgresql.replication.LogSequenceNumber.valueOf("0/50000A0").asLong());
        ReflectionTestUtils.setField(miner, "currentTimelineStartPoint",
                org.postgresql.replication.LogSequenceNumber.valueOf("0/50000A0").asLong());

        // First two idle probes are tolerated while the count builds up.
        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);
        assertNull(stallSignal(miner));
        ReflectionTestUtils.setField(miner, "lastStallProbeMs", 0L);
        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);
        assertNull(stallSignal(miner));

        // Third idle probe abandons the catch-up.
        ReflectionTestUtils.setField(miner, "lastStallProbeMs", 0L);
        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);

        Throwable signal = stallSignal(miner);
        assertNotNull(signal);
        assertTrue((Boolean) ReflectionTestUtils.invokeMethod(miner, "isAncestorCatchupStallSignal", signal));
        assertTrue((Boolean) ReflectionTestUtils.invokeMethod(miner, "isStallRecoverySignal", signal));
    }

    @Test
    public void testStallProbeAncestorCatchupStillToleratesTimelineChange() throws Exception {
        // Even in ancestor catch-up mode a NEW timeline change must keep using
        // the regular failover signal path (StreamStalledException), not the
        // catch-up-abandon path.
        PostgresJdbcContext ctx = timelineProbeContext("00000009");
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(ctx, mock(Log.class));
        ReflectionTestUtils.setField(miner, "savedTimeline", 8);
        ReflectionTestUtils.setField(miner, "currentTimeline", 8);
        ReflectionTestUtils.setField(miner, "restorePrimaryAfterTimelineCatchup", true);
        ReflectionTestUtils.setField(miner, "restorePrimaryAtLsn",
                org.postgresql.replication.LogSequenceNumber.valueOf("0/50000A0").asLong());

        ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);

        Throwable signal = stallSignal(miner);
        assertNotNull(signal);
        assertFalse((Boolean) ReflectionTestUtils.invokeMethod(miner, "isAncestorCatchupStallSignal", signal));
        assertTrue((Boolean) ReflectionTestUtils.invokeMethod(miner, "isStallRecoverySignal", signal));
    }

    @Test
    public void testSwitchToCurrentTimelineResumeClearsCatchupState() {
        // The recovery-loop path for an abandoned catch-up calls
        // switchToCurrentTimelineResume(resumeLsn): it must clear the catch-up
        // flag, advance the saved baseline to the current timeline and raise the
        // emit point so the re-opened stream runs on the current timeline only.
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        long resumeLsn = org.postgresql.replication.LogSequenceNumber.valueOf("0/502AB00").asLong();
        ReflectionTestUtils.setField(miner, "restorePrimaryAfterTimelineCatchup", true);
        ReflectionTestUtils.setField(miner, "restorePrimaryAtLsn", resumeLsn);
        ReflectionTestUtils.setField(miner, "timelineChanged", true);
        ReflectionTestUtils.setField(miner, "savedTimeline", 7);
        ReflectionTestUtils.setField(miner, "currentTimeline", 10);
        ReflectionTestUtils.setField(miner, "emitFromLsn", 0L);

        ReflectionTestUtils.invokeMethod(miner, "switchToCurrentTimelineResume", resumeLsn);

        assertEquals(false, ReflectionTestUtils.getField(miner, "restorePrimaryAfterTimelineCatchup"));
        assertEquals(false, ReflectionTestUtils.getField(miner, "timelineChanged"));
        assertEquals(10, ReflectionTestUtils.getField(miner, "savedTimeline"));
        assertEquals(resumeLsn, ReflectionTestUtils.getField(miner, "emitFromLsn"));
    }

    @Test
    public void testIsStallRecoverySignalRejectsUnrelatedRetryableAfterAncestorSignal() {
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        TapPdkRetryableEx unrelated = new TapPdkRetryableEx("postgres", new RuntimeException("switch to slave"));

        assertFalse((Boolean) ReflectionTestUtils.invokeMethod(miner, "isAncestorCatchupStallSignal", unrelated));
        assertFalse((Boolean) ReflectionTestUtils.invokeMethod(miner, "isStallRecoverySignal", unrelated));
    }

    @Test
    public void testAncestorCatchupStalledSignalIsRecoverable() {
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "restorePrimaryAfterTimelineCatchup", true);
        ReflectionTestUtils.setField(miner, "restorePrimaryAtLsn",
                org.postgresql.replication.LogSequenceNumber.valueOf("0/4F023478").asLong());

        Throwable removed = new org.postgresql.util.PSQLException(
                "ERROR: requested WAL segment 00000026000000000000004F has already been removed",
                org.postgresql.util.PSQLState.INVALID_CURSOR_STATE);
        TapPdkRetryableEx signal = (TapPdkRetryableEx) ReflectionTestUtils.invokeMethod(
                miner, "ancestorCatchupStalled", removed);

        assertNotNull(signal);
        assertTrue((Boolean) ReflectionTestUtils.invokeMethod(miner, "isAncestorCatchupStallSignal", signal));
        assertTrue((Boolean) ReflectionTestUtils.invokeMethod(miner, "isStallRecoverySignal", signal));
    }

    @Test
    public void testAncestorCatchupStalledSignalCarriesRemovedCause() {
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        Throwable removed = new RuntimeException("requested WAL segment has already been removed");
        TapPdkRetryableEx signal = (TapPdkRetryableEx) ReflectionTestUtils.invokeMethod(
                miner, "ancestorCatchupStalled", removed);

        assertNotNull(signal.getCause());
        assertTrue(signal.getCause().getMessage().contains("already been removed"));
    }

    @Test
    public void testQueryMaxTimelineAcrossSourcesTakesHighest() {
        // After a failover the new primary reports the post-failover timeline while
        // a stranded standby still reports the old one; the probe must return the
        // maximum so currentTimeline reflects what the cluster runs on now,
        // regardless of which node the WAL stream is connected to.
        PostgresConfig config = new PostgresConfig();
        config.setHost("postgres-master");
        config.setPort(6434);
        config.setDatabase("postgres");
        config.setUser("postgres");
        config.setPassword("postgres");
        config.setDeploymentMode("master-slave");
        ArrayList<LinkedHashMap<String, Object>> nodes = new ArrayList<>();
        nodes.add(node("postgres-slave2", 6433));
        config.setMasterSlaveAddress((ArrayList) nodes);
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);

        AtomicInteger call = new AtomicInteger();
        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), any(Properties.class))).thenAnswer(inv -> {
                Connection conn = mock(Connection.class);
                PreparedStatement ps = mock(PreparedStatement.class);
                ResultSet rs = mock(ResultSet.class);
                when(rs.next()).thenReturn(true);
                // base node (6434) reports timeline 40, the standby (6433) already 41
                when(rs.getString(1)).thenReturn(call.incrementAndGet() == 1 ? "00000028" : "00000029");
                when(ps.executeQuery()).thenReturn(rs);
                when(conn.prepareStatement(anyString())).thenReturn(ps);
                return conn;
            });

            int max = (Integer) ReflectionTestUtils.invokeMethod(miner, "queryMaxTimelineAcrossSources");
            assertEquals(41, max);
        }
    }

    @Test
    public void testStallDetectionClusterAdvancedButStreamNodeStrandedSignalsRecovery() throws Exception {
        // Scenario B (native PG + checkCdcSlave): the stream is connected to a
        // standby whose upstream primary was promoted away and stopped. The
        // standby's own timeline never changes, so the node-local probe alone sees
        // no failover — but the cluster has advanced to a higher timeline on
        // another configured node. The probe must detect the stranded node and
        // signal the recovery loop to re-open on the current primary.
        PostgresConfig config = new PostgresConfig();
        config.setHost("postgres-master");
        config.setPort(6434);
        config.setDatabase("postgres");
        config.setUser("postgres");
        config.setPassword("postgres");
        config.setDeploymentMode("master-slave");
        ArrayList<LinkedHashMap<String, Object>> nodes = new ArrayList<>();
        nodes.add(node("postgres-slave2", 6433));
        config.setMasterSlaveAddress((ArrayList) nodes);
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);
        ReflectionTestUtils.setField(miner, "savedTimeline", 40);
        ReflectionTestUtils.setField(miner, "currentTimeline", 40);
        @SuppressWarnings("unchecked")
        List<Object> sources = (List<Object>) ReflectionTestUtils.invokeMethod(miner, "timelineSources");
        ReflectionTestUtils.setField(miner, "activeTimelineSource", sources.get(0)); // stranded standby 6434

        AtomicInteger call = new AtomicInteger();
        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), any(Properties.class))).thenAnswer(inv -> {
                String url = inv.getArgument(0);
                boolean advancedNode = url.contains("postgres-slave2");
                Connection conn = mock(Connection.class);
                when(conn.prepareStatement(anyString())).thenAnswer(psInv -> {
                    String sql = psInv.getArgument(0);
                    PreparedStatement ps = mock(PreparedStatement.class);
                    when(ps.executeQuery()).thenAnswer(qInv -> {
                        call.incrementAndGet();
                        ResultSet rs = mock(ResultSet.class);
                        when(rs.next()).thenReturn(true);
                        if (sql.contains("pg_stat_wal_receiver")) {
                            when(rs.getInt(1)).thenReturn(advancedNode ? 41 : 40);
                        } else if (sql.contains("pg_walfile_name")) {
                            when(rs.getString(1)).thenReturn(advancedNode ? "00000029" : "00000028");
                        } else if (sql.contains("pg_control_checkpoint()")) {
                            when(rs.getInt(1)).thenReturn(advancedNode ? 41 : 40);
                        }
                        return rs;
                    });
                    return ps;
                });
                return conn;
            });

            ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);

            Throwable signal = stallSignal(miner);
            assertNotNull(signal);
            assertTrue(signal instanceof TapPdkRetryableEx);
            assertTrue((Boolean) ReflectionTestUtils.invokeMethod(miner, "isStallRecoverySignal", signal));
        }
    }

    @Test
    public void testStallDetectionClusterNotAdvancedKeepsStreamOpen() throws Exception {
        // Every configured node reports the same timeline the stream expects:
        // a healthy idle cluster, no failover — keep waiting, never signal.
        PostgresConfig config = new PostgresConfig();
        config.setHost("postgres-master");
        config.setPort(6434);
        config.setDatabase("postgres");
        config.setUser("postgres");
        config.setPassword("postgres");
        config.setDeploymentMode("master-slave");
        ArrayList<LinkedHashMap<String, Object>> nodes = new ArrayList<>();
        nodes.add(node("postgres-slave2", 6433));
        config.setMasterSlaveAddress((ArrayList) nodes);
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);
        ReflectionTestUtils.setField(miner, "savedTimeline", 40);
        ReflectionTestUtils.setField(miner, "currentTimeline", 40);
        @SuppressWarnings("unchecked")
        List<Object> sources = (List<Object>) ReflectionTestUtils.invokeMethod(miner, "timelineSources");
        ReflectionTestUtils.setField(miner, "activeTimelineSource", sources.get(0));

        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), any(Properties.class))).thenAnswer(inv -> {
                Connection conn = mock(Connection.class);
                PreparedStatement ps = mock(PreparedStatement.class);
                ResultSet rs = mock(ResultSet.class);
                when(rs.next()).thenReturn(true);
                when(rs.getString(1)).thenReturn("00000028"); // every node on timeline 40
                when(ps.executeQuery()).thenReturn(rs);
                when(conn.prepareStatement(anyString())).thenReturn(ps);
                return conn;
            });

            ReflectionTestUtils.invokeMethod(miner, "probeStallIfDue", System.currentTimeMillis() - 120_000L);

            assertNull(stallSignal(miner));
        }
    }

    @Test
    public void testPrioritizeCurrentTimelineSourcesPutsCurrentTimelineNodeFirst() {
        // After an ancestor catch-up completes on a stranded standby (old
        // timeline, upstream primary stopped), the stream must continue on a node
        // that actually reports the current timeline. The open loop otherwise
        // picks the first configured node (often the base host standby) and opens
        // a stream that immediately goes idle again.
        PostgresConfig config = new PostgresConfig();
        config.setHost("postgres-master");
        config.setPort(6434);
        config.setDatabase("postgres");
        config.setUser("postgres");
        config.setPassword("postgres");
        config.setDeploymentMode("master-slave");
        ArrayList<LinkedHashMap<String, Object>> nodes = new ArrayList<>();
        nodes.add(node("postgres-slave2", 6433));
        config.setMasterSlaveAddress((ArrayList) nodes);
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);
        ReflectionTestUtils.setField(miner, "currentTimeline", 41);

        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), any(Properties.class))).thenAnswer(inv -> {
                String url = inv.getArgument(0);
                String host;
                int port;
                if (url.contains("postgres-slave2")) {
                    host = "postgres-slave2";
                    port = 6433;
                } else {
                    host = "postgres-master";
                    port = 6434;
                }
                Connection conn = mock(Connection.class);
                when(conn.prepareStatement(anyString())).thenAnswer(psInv -> {
                    String sql = psInv.getArgument(0);
                    PreparedStatement ps = mock(PreparedStatement.class);
                    when(ps.executeQuery()).thenAnswer(qInv -> {
                        ResultSet rs = mock(ResultSet.class);
                        when(rs.next()).thenReturn(true);
                        if (sql.contains("pg_stat_wal_receiver")) {
                            when(rs.getInt(1)).thenReturn("postgres-slave2".equals(host) ? 41 : 40);
                        } else if (sql.contains("pg_walfile_name")) {
                            when(rs.getString(1)).thenReturn("postgres-slave2".equals(host) ? "00000029" : "00000028");
                        } else if (sql.contains("pg_control_checkpoint()")) {
                            when(rs.getInt(1)).thenReturn("postgres-slave2".equals(host) ? 41 : 40);
                        } else if (sql.contains("pg_last_wal_replay_lsn()") || sql.contains("pg_current_wal_flush_lsn()")) {
                            when(rs.getString(1)).thenReturn("postgres-slave2".equals(host) ? "0/00002000" : "0/00001000");
                        } else if (sql.contains("pg_is_in_recovery()")) {
                            when(rs.getBoolean(1)).thenReturn("postgres-slave2".equals(host));
                        }
                        return rs;
                    });
                    return ps;
                });
                return conn;
            });

            List<?> sources = (List<?>) ReflectionTestUtils.invokeMethod(miner, "timelineSources");
            assertNotNull(sources);
            List<?> prioritized = (List<?>) ReflectionTestUtils.invokeMethod(miner, "prioritizeCurrentTimelineSources", sources);
            assertNotNull(prioritized);
            Object first = prioritized.get(0);
            assertEquals("postgres-slave2", ReflectionTestUtils.getField(first, "host"));
            assertEquals(6433, ReflectionTestUtils.getField(first, "port"));
        }
    }

    @Test
    public void testPrioritizeCurrentTimelineSourcesPrefersHigherReadableLsnAmongCurrentTimelineStandbys() {
        // Two standbys are on the same current timeline, but one is further
        // ahead on replay LSN. The retry loop should prefer that more reliable
        // standby first, even if it is later in the configured address order.
        PostgresConfig config = new PostgresConfig();
        config.setHost("postgres-master");
        config.setPort(6434);
        config.setDatabase("postgres");
        config.setUser("postgres");
        config.setPassword("postgres");
        config.setDeploymentMode("master-slave");
        ArrayList<LinkedHashMap<String, Object>> nodes = new ArrayList<>();
        nodes.add(node("postgres-slave2", 6433));
        nodes.add(node("postgres-slave3", 6435));
        config.setMasterSlaveAddress((ArrayList) nodes);
        PhysicalWalLogMiner miner = new PhysicalWalLogMiner(mock(PostgresJdbcContext.class), mock(Log.class));
        ReflectionTestUtils.setField(miner, "postgresConfig", config);
        ReflectionTestUtils.setField(miner, "currentTimeline", 41);

        try (MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            dm.when(() -> DriverManager.getConnection(anyString(), any(Properties.class))).thenAnswer(inv -> {
                String url = inv.getArgument(0);
                String host;
                int port;
                if (url.contains("postgres-slave3")) {
                    host = "postgres-slave3";
                    port = 6435;
                } else if (url.contains("postgres-slave2")) {
                    host = "postgres-slave2";
                    port = 6433;
                } else {
                    host = "postgres-master";
                    port = 6434;
                }
                Connection conn = mock(Connection.class);
                when(conn.prepareStatement(anyString())).thenAnswer(psInv -> {
                    String sql = psInv.getArgument(0);
                    PreparedStatement ps = mock(PreparedStatement.class);
                    when(ps.executeQuery()).thenAnswer(qInv -> {
                        ResultSet rs = mock(ResultSet.class);
                        when(rs.next()).thenReturn(true);
                        if (sql.contains("pg_stat_wal_receiver")) {
                            when(rs.getInt(1)).thenReturn("postgres-slave3".equals(host) ? 41 : 40);
                        } else if (sql.contains("pg_walfile_name")) {
                            when(rs.getString(1)).thenReturn("postgres-slave3".equals(host) ? "00000029" : "00000028");
                        } else if (sql.contains("pg_control_checkpoint()")) {
                            when(rs.getInt(1)).thenReturn("postgres-slave3".equals(host) ? 41 : 40);
                        } else if (sql.contains("pg_last_wal_replay_lsn()") || sql.contains("pg_current_wal_flush_lsn()")) {
                            when(rs.getString(1)).thenReturn("postgres-slave3".equals(host) ? "0/00002000" : "0/00001000");
                        } else if (sql.contains("pg_is_in_recovery()")) {
                            when(rs.getBoolean(1)).thenReturn(!"postgres-master".equals(host));
                        }
                        return rs;
                    });
                    return ps;
                });
                return conn;
            });

            List<?> sources = (List<?>) ReflectionTestUtils.invokeMethod(miner, "timelineSources");
            assertNotNull(sources);
            List<?> prioritized = (List<?>) ReflectionTestUtils.invokeMethod(miner, "prioritizeCurrentTimelineSources", sources);
            assertNotNull(prioritized);
            Object first = prioritized.get(0);
            assertEquals("postgres-slave3", ReflectionTestUtils.getField(first, "host"));
            assertEquals(6435, ReflectionTestUtils.getField(first, "port"));
        }
    }
}
