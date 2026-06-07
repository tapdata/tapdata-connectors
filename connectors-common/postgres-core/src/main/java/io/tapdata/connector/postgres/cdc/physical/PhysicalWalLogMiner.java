package io.tapdata.connector.postgres.cdc.physical;

import com.google.common.collect.Lists;
import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.common.concurrent.exception.ConcurrentProcessorApplyException;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.cdc.AbstractWalLogMiner;
import io.tapdata.connector.postgres.cdc.NormalRedo;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.tapdata.connector.postgres.cdc.physical.WalConstants.*;

/**
 * Streams raw physical WAL from a physical replication slot and decodes it
 * entirely in-process (no logical-decoding plugin) into committed DML events.
 *
 * <p>The pipeline is {@code stream bytes -> WalPageDecoder -> XLogRecord ->
 * HeapRmgrDecoder}. Heap changes are buffered per transaction and only flushed
 * to the consumer when the matching {@code RM_XACT} COMMIT record is seen, so
 * aborted transactions are never emitted. Completeness of UPDATE before-images
 * and DELETE old values depends on {@code wal_level=logical} plus the table's
 * REPLICA IDENTITY; when the old tuple is not logged the after-image is still
 * emitted and the before-image is left null (graceful degradation).</p>
 *
 * @author Jarad
 */
public class PhysicalWalLogMiner extends AbstractWalLogMiner {

    private static final long PG_EPOCH_MICROS = 946684800L * 1_000_000L;
    private static final long HEARTBEAT_INTERVAL_MS = 1000L;
    private static final int DECODE_THREADS = 8;
    private static final int DECODE_QUEUE_SIZE = 32;
    /* Diagnostic guard: surface a warning (not an error) when uncommitted heap
     * changes pile up, which is the signature of transactions whose COMMIT we
     * never match (e.g. very long-running transactions) and a potential OOM. */
    private static final long PENDING_REDO_WARN_THRESHOLD = 1_000_000L;
    private static final long PENDING_WARN_INTERVAL_MS = 30_000L;
    private static final long[] EMPTY_SUBXACTS = new long[0];

    private String slotName;
    private String startLsn;
    private RelationCatalog catalog;
    private final Set<String> allowTables = new HashSet<>();
    private final Map<Long, List<NormalRedo>> pendingByXid = new HashMap<>();
    private long pendingRedoCount;
    private long lastPendingWarnMs;

    public PhysicalWalLogMiner(PostgresJdbcContext postgresJdbcContext, Log tapLogger) {
        super(postgresJdbcContext, tapLogger);
    }

    public PhysicalWalLogMiner useSlot(String slotName) {
        this.slotName = slotName;
        return this;
    }

    @Override
    public PhysicalWalLogMiner offset(Object offsetState) {
        if (offsetState instanceof String && EmptyKit.isNotBlank((String) offsetState)) {
            this.startLsn = ((String) offsetState).split(",")[0];
        }
        return this;
    }

    @Override
    public void startMiner(Supplier<Boolean> isAlive) throws Throwable {
        buildAllowSet();
        catalog = new RelationCatalog(postgresJdbcContext, tapLogger);
        long segSize = querySegmentSize();
        if (EmptyKit.isBlank(startLsn)) {
            startLsn = queryCurrentLsn();
        }
        long startLsnLong = LogSequenceNumber.valueOf(startLsn).asLong();
        tapLogger.info("Physical WAL miner starting from lsn {} on slot {}", startLsn, slotName);
        consumer.streamReadStarted();
        try (Connection conn = DriverManager.getConnection(postgresConfig.getDatabaseUrl(), replicationProps())) {
            PGConnection pg = conn.unwrap(PGConnection.class);
            PGReplicationStream stream = pg.getReplicationAPI()
                    .replicationStream()
                    .physical()
                    .withSlotName(slotName)
                    .withStartPosition(LogSequenceNumber.valueOf(startLsnLong))
                    .start();
            run(stream, startLsnLong, segSize, isAlive);
        } finally {
            consumer.streamReadEnded();
        }
    }

    /* Reader thread: serially frames the WAL byte stream (WalPageDecoder is
     * stateful) and offloads the heavy per-record decoding to a thread pool. A
     * single consumer thread drains the processor in submission order, so the
     * transaction buffer and emitted LSN sequence stay correct. */
    private void run(PGReplicationStream stream, long startLsnLong, long segSize, Supplier<Boolean> isAlive) throws Exception {
        WalPageDecoder decoder = new WalPageDecoder(startLsnLong, segSize);
        String initialOffset = lsnStr(startLsnLong);
        AtomicReference<String> lastReceiveLsn = new AtomicReference<>(initialOffset);
        try (ConcurrentProcessor<WalPageDecoder.RawRecord, Decoded> processor =
                     TapExecutors.createSimple(DECODE_THREADS, DECODE_QUEUE_SIZE, "physical-wal-miner")) {
            Thread consumerThread = new Thread(() -> consumeLoop(processor, isAlive, initialOffset, lastReceiveLsn));
            consumerThread.setName("physical-wal-miner-Consumer");
            consumerThread.start();
            try {
                while (isAlive.get() && threadException.get() == null) {
                    ByteBuffer buf = stream.readPending();
                    if (buf == null) {
                        LogSequenceNumber recv = stream.getLastReceiveLSN();
                        if (recv != null) {
                            lastReceiveLsn.set(recv.asString());
                        }
                        TimeUnit.MILLISECONDS.sleep(10);
                        continue;
                    }
                    int n = buf.remaining();
                    byte[] arr = new byte[n];
                    buf.get(arr);
                    decoder.feed(arr, 0, n);
                    WalPageDecoder.RawRecord raw;
                    while ((raw = decoder.nextRecord()) != null) {
                        if (!submit(processor, raw, isAlive)) {
                            break;
                        }
                    }
                    decoder.compact();
                    LogSequenceNumber recv = stream.getLastReceiveLSN();
                    if (recv != null) {
                        lastReceiveLsn.set(recv.asString());
                        stream.setFlushedLSN(recv);
                        stream.setAppliedLSN(recv);
                        stream.forceUpdateStatus();
                    }
                }
            } finally {
                ErrorKit.ignoreAnyError(stream::close);
            }
            ErrorKit.ignoreAnyError(consumerThread::join);
        }
        if (threadException.get() != null) {
            throw new RuntimeException(threadException.get());
        }
    }

    /* Offer a record for parallel decoding, blocking with backpressure until the
     * processor accepts it; returns false if shutdown or a decode error aborts. */
    private boolean submit(ConcurrentProcessor<WalPageDecoder.RawRecord, Decoded> processor,
                           WalPageDecoder.RawRecord raw, Supplier<Boolean> isAlive) {
        while (isAlive.get() && threadException.get() == null) {
            if (processor.runAsync(raw, this::decodeUnit, 1, TimeUnit.SECONDS)) {
                return true;
            }
        }
        return false;
    }

    /* Consumer thread: drains decoded units in submission order, buffers heap
     * changes per transaction and flushes them on COMMIT, preserving LSN order. */
    private void consumeLoop(ConcurrentProcessor<WalPageDecoder.RawRecord, Decoded> processor,
                            Supplier<Boolean> isAlive, String initialOffset, AtomicReference<String> lastReceiveLsn) {
        List<TapEvent> batch = new ArrayList<>();
        String lastOffset = initialOffset;
        long lastHeartbeat = System.currentTimeMillis();
        try {
            while (isAlive.get()) {
                Decoded d;
                try {
                    d = processor.get(1, TimeUnit.SECONDS);
                } catch (ConcurrentProcessorApplyException e) {
                    threadException.set(e.getCause() != null ? e.getCause() : e);
                    return;
                }
                if (d != null) {
                    lastOffset = d.offset;
                    applyDecoded(d, batch);
                    if (batch.size() >= recordSize) {
                        consumer.accept(batch, lastOffset);
                        batch = new ArrayList<>();
                    }
                } else if (!batch.isEmpty()) {
                    consumer.accept(batch, lastOffset);
                    batch = new ArrayList<>();
                    lastHeartbeat = System.currentTimeMillis();
                } else if (System.currentTimeMillis() - lastHeartbeat >= HEARTBEAT_INTERVAL_MS) {
                    String hb = lastReceiveLsn.get();
                    consumer.accept(Collections.singletonList(new HeartbeatEvent().init().referenceTime(System.currentTimeMillis())),
                            hb == null ? lastOffset : hb);
                    lastHeartbeat = System.currentTimeMillis();
                }
            }
        } catch (Exception e) {
            threadException.set(e);
        }
    }

    /* CPU-heavy, order-independent decode executed on the pool threads: parse the
     * record and (for heap changes) resolve the relation and deform tuples. */
    private Decoded decodeUnit(WalPageDecoder.RawRecord raw) {
        XLogRecord rec = XLogRecord.parse(raw);
        Decoded d = new Decoded();
        d.offset = lsnStr(raw.nextLsn);
        if (rec.rmid == RM_HEAP_ID || rec.rmid == RM_HEAP2_ID) {
            d.kind = Decoded.HEAP;
            // Buffer under the top-level xid so subtransaction (savepoint /
            // PL-pgSQL EXCEPTION) changes flush on the top-level COMMIT instead
            // of leaking forever under their subxid.
            d.xid = rec.topXid != 0 ? rec.topXid : rec.xid;
            d.redos = decodeHeap(rec);
        } else if (rec.rmid == RM_TRANSACTION_ID) {
            d.xid = rec.xid;
            int op = rec.info & XLOG_XACT_OPMASK;
            if (op == XLOG_XACT_COMMIT || op == XLOG_XACT_COMMIT_PREPARED) {
                d.kind = Decoded.COMMIT;
                d.commitMillis = readCommitMillis(rec.mainData);
                d.subxids = readSubxacts(rec.mainData, rec.info);
            } else if (op == XLOG_XACT_ABORT || op == XLOG_XACT_ABORT_PREPARED) {
                d.kind = Decoded.ABORT;
                d.subxids = readSubxacts(rec.mainData, rec.info);
            }
        }
        return d;
    }

    private List<NormalRedo> decodeHeap(XLogRecord rec) {
        XLogRecord.BlockRef b0 = rec.block(0);
        if (b0 == null) {
            return Collections.emptyList();
        }
        RelationInfo rel = catalog.lookup(b0.relNumber);
        if (rel == null || !allowed(rel)) {
            return Collections.emptyList();
        }
        List<NormalRedo> redos = HeapRmgrDecoder.decode(rec, rel);
        for (NormalRedo r : redos) {
            r.setCdcSequenceId(rec.lsn);   // source LSN, used to order a transaction's changes
        }
        return redos;
    }

    /* Parse the subtransaction xids carried in an xl_xact_commit / xl_xact_abort
     * body. Layout after the record header: xact_time(8) [+ xinfo(4) if HAS_INFO]
     * [+ dbinfo(8) if HAS_DBINFO] [+ nsubxacts(4) + subxids if HAS_SUBXACTS]. */
    static long[] readSubxacts(byte[] mainData, int info) {
        if (mainData == null || (info & XLOG_XACT_HAS_INFO) == 0) {
            return EMPTY_SUBXACTS;
        }
        try {
            WalByteReader r = new WalByteReader(mainData);
            r.skip(8);                       // xact_time (TimestampTz)
            long xinfo = r.readUInt32();     // xl_xact_xinfo
            if ((xinfo & XACT_XINFO_HAS_DBINFO) != 0) {
                r.skip(8);                   // xl_xact_dbinfo: dbId(4) + tsId(4)
            }
            if ((xinfo & XACT_XINFO_HAS_SUBXACTS) != 0) {
                int n = (int) r.readUInt32();
                if (n <= 0) {
                    return EMPTY_SUBXACTS;
                }
                long[] subs = new long[n];
                for (int i = 0; i < n; i++) {
                    subs[i] = r.readUInt32();
                }
                return subs;
            }
        } catch (RuntimeException ignore) {
            // malformed/short body: degrade gracefully to no subxacts
        }
        return EMPTY_SUBXACTS;
    }

    /* Single-threaded application of the ordered decode result to the per-xid
     * buffer; only this method touches {@code pendingByXid}. */
    private void applyDecoded(Decoded d, List<TapEvent> batch) {
        switch (d.kind) {
            case Decoded.HEAP:
                if (d.redos != null && !d.redos.isEmpty()) {
                    pendingByXid.computeIfAbsent(d.xid, k -> new ArrayList<>()).addAll(d.redos);
                    pendingRedoCount += d.redos.size();
                    checkPendingPressure();
                }
                break;
            case Decoded.COMMIT:
                List<NormalRedo> redos = drainTransaction(d.xid, d.subxids);
                if (!redos.isEmpty()) {
                    // Merge of top-level and subtransaction buckets; stable-sort by
                    // source LSN to restore the original intra-transaction order.
                    redos.sort(Comparator.comparingLong(PhysicalWalLogMiner::redoLsn));
                    for (NormalRedo r : redos) {
                        r.setTimestamp(d.commitMillis);
                        r.setCdcSequenceStr(d.offset);
                        TapEvent ev = toEvent(r);
                        if (ev != null) {
                            batch.add(ev);
                        }
                    }
                }
                break;
            case Decoded.ABORT:
                drainTransaction(d.xid, d.subxids);
                break;
            default:
                break;
        }
    }

    /* Remove and return the buffered changes for a top-level xid together with all
     * of its subtransaction xids, keeping {@code pendingRedoCount} in step. */
    private List<NormalRedo> drainTransaction(long topXid, long[] subxids) {
        List<NormalRedo> all = new ArrayList<>();
        List<NormalRedo> top = pendingByXid.remove(topXid);
        if (top != null) {
            all.addAll(top);
        }
        if (subxids != null) {
            for (long sub : subxids) {
                List<NormalRedo> bucket = pendingByXid.remove(sub);
                if (bucket != null) {
                    all.addAll(bucket);
                }
            }
        }
        pendingRedoCount -= all.size();
        if (pendingRedoCount < 0) {
            pendingRedoCount = 0;
        }
        return all;
    }

    private void checkPendingPressure() {
        if (pendingRedoCount < PENDING_REDO_WARN_THRESHOLD) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastPendingWarnMs >= PENDING_WARN_INTERVAL_MS) {
            lastPendingWarnMs = now;
            tapLogger.warn("Physical WAL miner has buffered {} uncommitted changes across {} open transactions; "
                    + "check for long-running transactions on the source to avoid excessive memory use",
                    pendingRedoCount, pendingByXid.size());
        }
    }

    private static long redoLsn(NormalRedo r) {
        Long id = r.getCdcSequenceId();
        return id == null ? 0L : id;
    }

    /* Carrier for one decoded WAL record passed from pool threads to the consumer. */
    private static final class Decoded {
        static final int OTHER = 0;
        static final int HEAP = 1;
        static final int COMMIT = 2;
        static final int ABORT = 3;
        int kind = OTHER;
        long xid;
        String offset;
        long commitMillis;
        long[] subxids;
        List<NormalRedo> redos;
    }

    private TapEvent toEvent(NormalRedo r) {
        TapRecordEvent ev;
        switch (NormalRedo.OperationEnum.valueOf(r.getOperation())) {
            case INSERT:
                ev = new TapInsertRecordEvent().init().after(r.getRedoRecord());
                break;
            case UPDATE:
                ev = new TapUpdateRecordEvent().init().after(r.getRedoRecord()).before(r.getUndoRecord());
                break;
            case DELETE:
                ev = new TapDeleteRecordEvent().init().before(r.getUndoRecord() != null ? r.getUndoRecord() : r.getRedoRecord());
                break;
            default:
                return null;
        }
        ev.setTableId(r.getTableName());
        ev.setReferenceTime(r.getTimestamp());
        if (withSchema) {
            ev.setNamespaces(Lists.newArrayList(r.getNameSpace(), r.getTableName()));
        }
        return ev;
    }

    private boolean allowed(RelationInfo rel) {
        if (withSchema) {
            return allowTables.contains(rel.schema + "." + rel.table);
        }
        return rel.schema.equals(postgresConfig.getSchema()) && allowTables.contains(rel.table);
    }

    private void buildAllowSet() {
        if (withSchema) {
            if (schemaTableMap != null) {
                schemaTableMap.forEach((schema, tables) -> tables.forEach(t -> allowTables.add(schema + "." + t)));
            }
        } else if (tableList != null) {
            allowTables.addAll(tableList);
        }
    }

    private Properties replicationProps() {
        Properties props = new Properties();
        PGProperty.USER.set(props, postgresConfig.getUser());
        PGProperty.PASSWORD.set(props, postgresConfig.getPassword());
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        return props;
    }

    private String queryCurrentLsn() {
        String[] lsn = {"0/0"};
        // recovery-aware: a standby cannot run pg_current_wal_lsn(), use the last replayed lsn there
        ErrorKit.ignoreAnyError(() -> postgresJdbcContext.queryWithNext(
                "SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END",
                rs -> lsn[0] = rs.getString(1)));
        return lsn[0];
    }

    private long querySegmentSize() {
        String[] raw = {null};
        ErrorKit.ignoreAnyError(() -> postgresJdbcContext.queryWithNext(
                "SHOW wal_segment_size", rs -> raw[0] = rs.getString(1)));
        long parsed = parseSize(raw[0]);
        return parsed > 0 ? parsed : DEFAULT_WAL_SEGMENT_SIZE;
    }

    static long parseSize(String text) {
        if (text == null) {
            return 0;
        }
        String t = text.trim().toLowerCase();
        long mult = 1;
        if (t.endsWith("gb")) {
            mult = 1024L * 1024 * 1024;
            t = t.substring(0, t.length() - 2);
        } else if (t.endsWith("mb")) {
            mult = 1024L * 1024;
            t = t.substring(0, t.length() - 2);
        } else if (t.endsWith("kb")) {
            mult = 1024L;
            t = t.substring(0, t.length() - 2);
        } else if (t.endsWith("b")) {
            t = t.substring(0, t.length() - 1);
        }
        try {
            return Long.parseLong(t.trim()) * mult;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    static long readCommitMillis(byte[] mainData) {
        if (mainData == null || mainData.length < 8) {
            return System.currentTimeMillis();
        }
        long micros = new WalByteReader(mainData).readInt64();
        return (micros + PG_EPOCH_MICROS) / 1000L;
    }

    static String lsnStr(long lsn) {
        return LogSequenceNumber.valueOf(lsn).asString();
    }
}
