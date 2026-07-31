package io.tapdata.connector.postgres.cdc.physical;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
}
