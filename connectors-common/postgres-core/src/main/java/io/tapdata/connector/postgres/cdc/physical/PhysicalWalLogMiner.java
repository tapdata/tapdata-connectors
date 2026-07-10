package io.tapdata.connector.postgres.cdc.physical;

import com.google.common.collect.Lists;
import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.common.concurrent.exception.ConcurrentProcessorApplyException;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.cdc.AbstractWalLogMiner;
import io.tapdata.connector.postgres.cdc.NormalRedo;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldAttributesEvent;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.event.ddl.table.TapDropFieldEvent;
import io.tapdata.entity.event.ddl.table.TapNewFieldEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
    private static final long REPLICATION_STATUS_INTERVAL_MS = 5_000L;
    private static final int SPILL_STREAM_RESET_INTERVAL = 10_000;
    /* Per-xid in-memory limit before spilling to disk to survive multi-million-row
     * transactions without OOM. ObjectOutputStream writes NormalRedo sequentially
     * (NormalRedo is already Serializable); the files are read back via streaming
     * k-way merge on COMMIT and deleted. Set via TAPDATA_WAL_SPILL_THRESHOLD (default 500K). */
    private static final long PER_XID_SPILL_THRESHOLD = parseSpillThreshold();
    private static final long[] EMPTY_SUBXACTS = new long[0];
    /* How often to surface the running tally of UPDATE/DELETE whose before-image
     * could not be recovered (page-state cache miss under wal_level=replica). */
    private static final long NULL_IMAGE_WARN_INTERVAL_MS = 30_000L;

    private String slotName;
    private String startLsn;
    private Long filterStartTimeMs; // Time-based filtering: drop events before this timestamp
    private RelationCatalog catalog;
    private final Set<String> allowTables = new HashSet<>();
    /* DDL detection: maps each monitored relation's pg_class OID to its
     * schema/table name, the on-disk relfilenode of pg_catalog.pg_attribute,
     * and that catalog's column layout. A heap change on pg_attribute whose
     * attrelid is a monitored OID signals a DDL on the source. Built once at
     * startup; read only from the consumer thread thereafter. */
    private final Map<Long, MonitoredTable> monitoredOidToName = new HashMap<>();
    private long pgAttributeRelNode;
    private RelationInfo pgAttributeRel;
    private HeapRmgrDecoder.Ctx catalogDecodeCtx;
    /* DDL barrier: when a transaction containing DDL is detected, reader thread
     * pauses submission to decode queue until consumer completes DDL processing
     * and downstream acknowledges schema update. Guards against DDL/DML race where
     * DML referencing new schema columns arrives before DDL is applied downstream. */
    private final Object ddlBarrierLock = new Object();
    private volatile boolean ddlBarrierActive = false;
    private final Set<Long> ddlPendingXids = new HashSet<>();
    /* Page-state overlay for pg_attribute itself, so an FPI-less catalog UPDATE
     * (DROP/RENAME/ALTER COLUMN under wal_level=replica) can still reconstruct
     * its before/after tuple images from an earlier FPI on the same page.
     * Separate from the user-table pageCache. Consumer-thread only. */
    private PageStateCache catalogPageCache;
    /* Last-known column layout per monitored OID (attnum -> column). Seeded from
     * the live catalog at startup and advanced from each committed pg_attribute
     * tuple; used as the before-image fallback when a catalog UPDATE delta does
     * not carry the old tuple. Consumer-thread only. */
    private final Map<Long, Map<Integer, ColSnap>> schemaSnapshots = new HashMap<>();
    /* Resolved SQL type name cache keyed by "atttypid:atttypmod", filled lazily
     * via format_type so each distinct column type is resolved once. */
    private final Map<String, String> typeNameCache = new HashMap<>();
    /* Decoded pg_attribute changes for monitored tables within an open
     * transaction, buffered by top-level xid and turned into concrete field DDL
     * on COMMIT (or discarded on ABORT), so DDL is emitted only for committed
     * transactions and in stream order. Consumer-thread only. */
    private final Map<Long, List<NormalRedo>> ddlRedosByXid = new HashMap<>();
    private final Map<Long, List<NormalRedo>> pendingByXid = new HashMap<>();
    /* XLOG_XACT_ASSIGNMENT maps subtransaction xids to their top-level xid before
     * those subxids write heap/catalog records. Consumer-thread only; used as a
     * fallback when individual heap records do not carry XLR_BLOCK_ID_TOPLEVEL_XID. */
    private final Map<Long, Long> subxidToTopXid = new HashMap<>();
    /* Pending pg_attribute column changes per monitored table, keyed by
     * "schema.table". When a DDL (ADD/DROP/RENAME COLUMN) is decoded from WAL
     * before any DML has caused the affected table's RelationInfo to be loaded
     * into the catalog cache, the change is buffered here. On the next catalog
     * lookup for that table the pending changes are applied to the freshly-loaded
     * RelationInfo and removed. Consumer-thread only. */
    private final Map<String, List<RelationCatalog.PgAttributeChange>> pendingColumnChanges = new HashMap<>();
    /* Per-xid spill state: when a single transaction's in-memory NormalRedo list
     * exceeds PER_XID_SPILL_THRESHOLD the entire bucket is written to a sequential
     * spill file and the list is cleared, bounding per-xid heap to ~threshold *
     * sizeof(NormalRedo). On COMMIT/ABORT the files are deserialized back in write
     * order, merged with any in-memory remainder, and deleted. Consumer-thread only. */
    private final Map<Long, SpillState> spillStates = new HashMap<>();
    /* Spill directory root — created once per miner lifecycle, cleaned up on shutdown. */
    private Path spillDir;
    private long pendingRedoCount;
    private long lastPendingWarnMs;
    /* Page-state cache for in-memory mini-redo: enables UPDATE/DELETE before/after
     * recovery under wal_level=replica without REPLICA IDENTITY FULL. Touched
     * only from the single consumer thread; see applyDecoded(). */
    private PageStateCache pageCache;
    private boolean walLevelLogical;
    private HeapRmgrDecoder.Ctx decodeCtx;
    /* Running diagnostics for before-image coverage under wal_level=replica:
     * how many emitted UPDATE/DELETE carried a null before-image (cache miss),
     * throttled to one warning per interval. Consumer-thread only. */
    private long nullImageCount;
    private long lastNullImageWarnMs;
    /* First LSN whose committed changes are emitted downstream. WAL may be read
     * from an earlier point (the checkpoint redo) to warm the page cache; every
     * record before this LSN is decoded for its cache side effect only and never
     * emitted. Set once in startMiner and read from the reader/consumer threads. */
    private volatile long emitFromLsn;
    private final AtomicLong lastReceiveLsnForStatus = new AtomicLong();

    /* Verbose [WAL-DEBUG] logging is gated behind the TAPDATA_WAL_DEBUG env var
     * so a normal run stays quiet; set TAPDATA_WAL_DEBUG=true to surface the
     * per-record and per-checkpoint trace used for byte-level troubleshooting. */
    static final boolean WAL_DEBUG_ENABLED =
            "true".equalsIgnoreCase(System.getenv("TAPDATA_WAL_DEBUG"));

    /* Page-cache size. Under wal_level=replica a page's before-image is only
     * recoverable while its FPI-seeded overlay stays cached. Default is
     * unbounded (no eviction) so a page is never lost once seen within the read
     * window — matching walminer's full FPW-replay model — at the cost of heap
     * proportional to the checkpoint cycle's touched-page count. Set a positive
     * TAPDATA_WAL_PAGE_CACHE_CAPACITY to re-impose an LRU bound when heap is the
     * tighter constraint (trading before-image coverage for a fixed budget). */
    static final int PAGE_CACHE_CAPACITY =
            parseCapacity(System.getenv("TAPDATA_WAL_PAGE_CACHE_CAPACITY"), 0);

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

    /**
     * Configure time-based filtering for physical WAL mining.
     * Events with commit time before the specified timestamp will be silently dropped.
     *
     * @param filterStartTimeMs Minimum commit timestamp in milliseconds since epoch
     * @return this
     */
    public PhysicalWalLogMiner setFilterStartTime(Long filterStartTimeMs) {
        this.filterStartTimeMs = filterStartTimeMs;
        if (filterStartTimeMs != null) {
            tapLogger.info("Physical WAL miner configured with time filter: only events >= {} will be emitted",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(filterStartTimeMs)));
        }
        return this;
    }

    @Override
    public void startMiner(Supplier<Boolean> isAlive) throws Throwable {
        buildAllowSet();
        catalog = new RelationCatalog(postgresJdbcContext, tapLogger);
        long segSize = querySegmentSize();
        walLevelLogical = queryWalLevelLogical();
        pageCache = walLevelLogical ? null : new PageStateCache(PAGE_CACHE_CAPACITY);
        // Spill directory for large transactions — created once per miner lifecycle
        // so the consumer thread can spill and drain without IO coordination.
        String spillRoot = System.getenv().getOrDefault("TAPDATA_WAL_SPILL_DIR",
                System.getProperty("java.io.tmpdir"));
        spillDir = Paths.get(spillRoot, "physical-wal-spill-" + UUID.randomUUID());
        try {
            Files.createDirectories(spillDir);
            tapLogger.info("Physical WAL miner spill directory: {}", spillDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create spill directory " + spillDir, e);
        }
        decodeCtx = new HeapRmgrDecoder.Ctx(pageCache, walLevelLogical,
                WAL_DEBUG_ENABLED ? tapLogger::info : null);
        buildDdlWatch();
        // Seed strategy (standby-friendly). PageStateCache is seeded by each
        // page's first post-checkpoint FPI, which is what unlocks UPDATE/DELETE
        // before-image recovery under wal_level=replica. We PREFER a look-back
        // anchor: replay existing WAL from the redo pointer of the checkpoint
        // already recorded in the control file, which replays every page's
        // prior-cycle FPI into the cache without mutating the source and works on
        // a hot standby. Only when no usable look-back anchor exists do we fall
        // back to forcing a CHECKPOINT on a primary (so subsequent modifications
        // carry fresh FPIs) or, on a standby, warning. No-op when bypass is in
        // effect (logical already self-logs full tuples).
        long preSeedLsn = 0L;
        long priorRedoLsn = 0L;
        boolean seedCheckpointed = false;
        if (!walLevelLogical) {
            // Look-back probe only (no source mutation): the redo pointer of the
            // checkpoint already in the control file, plus the current WAL write
            // position kept as a last-resort lower bound if we later have to force.
            priorRedoLsn = queryCheckpointRedoLsn();
            preSeedLsn = lsnAsLong(queryCurrentLsn());
        }
        boolean freshStart = EmptyKit.isBlank(startLsn);
        if (freshStart) {
            startLsn = queryCurrentLsn();
        }
        emitFromLsn = LogSequenceNumber.valueOf(startLsn).asLong();
        // PageStateCache is cold on (re)start. To reconstruct the FPI-less
        // UPDATE/DELETE records we are about to emit we begin *reading* at a
        // checkpoint redo at/before emitFromLsn so every page's first
        // post-checkpoint FPI replays into the overlay first, while still
        // *emitting* from startLsn (the earlier records warm the cache and are
        // dropped at the commit gate in applyDecoded()).
        long readFromLsn = emitFromLsn;
        if (pageCache != null) {
            long anchor = 0L;
            String anchorSrc = null;
            // (1) Prefer the LOOK-BACK anchor: the redo of the checkpoint already
            // in the control file. Usable when it sits at/before the emit position
            // so reading never skips a record we must still emit. It costs the
            // source nothing, is the only option on a hot standby, and avoids the
            // checkpoint-redo boundary race entirely — being strictly in the past,
            // every hot page has been modified (and so written an FPI) after it.
            if (priorRedoLsn > 0 && priorRedoLsn <= emitFromLsn) {
                anchor = priorRedoLsn;
                anchorSrc = "existing checkpoint redo";
            }
            // (2) No usable look-back anchor (redo unreadable, or a checkpoint fired
            // while the task was down so the only redo sits past the emit position):
            // fall back to forcing a fresh checkpoint. forceSeedCheckpoint() issues
            // CHECKPOINT on a primary and only warns (returns false) on a standby,
            // so this degrades to "warn" there. On a fresh start advance the emit
            // position past the forced checkpoint so emitting begins right after its
            // redo with no cold FPI-less window ahead of it; on a resume the emit
            // position is fixed and the forced redo only helps the live tail. The
            // forced redo seeds via newRedo; preSeedLsn (the position captured before
            // forcing) is the last resort when pg_control_checkpoint() is not granted
            // so neither redo is readable.
            if (anchor <= 0) {
                seedCheckpointed = forceSeedCheckpoint();
                if (seedCheckpointed) {
                    if (freshStart) {
                        String afterLsn = queryCurrentLsn();
                        long afterLong = lsnAsLong(afterLsn);
                        if (afterLong > 0) {
                            startLsn = afterLsn;
                            emitFromLsn = afterLong;
                            readFromLsn = afterLong;
                        }
                    }
                    long newRedo = queryCheckpointRedoLsn();
                    if (newRedo > 0 && newRedo <= emitFromLsn) {
                        anchor = newRedo;
                        anchorSrc = "forced checkpoint redo";
                    } else if (preSeedLsn > 0 && preSeedLsn <= emitFromLsn) {
                        anchor = preSeedLsn;
                        anchorSrc = "pre-checkpoint position";
                    }
                }
            }
            // The chosen redo anchor can predate the WAL the slot still retains —
            // a freshly (re)created slot, or aggressive recycling after a bulk load,
            // leaves restart_lsn far ahead of the prior checkpoint redo. Reading from
            // there fails the stream with "requested WAL segment ... has already been
            // removed". Clamp to restart_lsn, the oldest LSN PostgreSQL guarantees
            // readable for this slot, so warming never requests recycled segments;
            // pages whose FPI predates restart_lsn simply seed once the stream crosses
            // their next FPI (same cold-page ceiling as wal_level=replica in general).
            long restartLsn = querySlotRestartLsn();
            if (anchor > 0 && restartLsn > 0 && anchor < restartLsn) {
                anchor = Math.min(restartLsn, emitFromLsn);
                anchorSrc = "slot restart_lsn (prior redo recycled)";
            }
            if (anchor > 0 && anchor < readFromLsn) {
                // Rewind reads to the cycle start so the cold cache warms with every
                // hot page's post-checkpoint FPI before we emit; the earlier records
                // are dropped at the commit gate in applyDecoded().
                readFromLsn = anchor;
                tapLogger.info("Physical WAL miner will warm the page cache from {} {} before emitting from {}",
                        anchorSrc, lsnStr(readFromLsn), startLsn);
            } else if (anchor > 0) {
                // emitFromLsn sits exactly on a checkpoint redo: nothing earlier to
                // replay, the cache warms in-line as the stream advances.
                tapLogger.info("Physical WAL miner emits from {} which sits on the {} {}; the page cache warms in-line.",
                        startLsn, anchorSrc, lsnStr(anchor));
            } else {
                // No checkpoint redo at or before the emit position is reachable: a
                // saved offset stranded behind a checkpoint that fired while the task
                // was down, a standby with no forceable checkpoint, or
                // pg_control_checkpoint() not granted. Hot pages whose FPI predates the
                // emit position stay un-seeded until the stream crosses the next
                // checkpoint's fresh FPI. Make the cause and remedies visible.
                tapLogger.warn("Physical WAL miner cannot pre-warm the page cache: no checkpoint redo at or before the "
                                + "emit position {} is reachable (saved offset stranded behind a later checkpoint, a standby, "
                                + "or pg_control_checkpoint() not granted). UPDATE/DELETE before-images on hot pages (e.g. "
                                + "bmsql_district) stay null until the stream crosses the next checkpoint's fresh FPI. Grant "
                                + "EXECUTE on pg_control_checkpoint() to the connection user (or add it to pg_monitor), set "
                                + "REPLICA IDENTITY FULL on the source tables, or use wal_level=logical for immediate coverage.",
                        startLsn);
            }
        }
        long startLsnLong = readFromLsn;
        tapLogger.info("Physical WAL miner starting from lsn {} on slot {}", startLsn, slotName);
        if (WAL_DEBUG_ENABLED) {
            tapLogger.info("[WAL-DEBUG] miner config: withSchema={} schema={} walLevelLogical={} pageCache={} allowTables({})={}",
                    withSchema, postgresConfig.getSchema(), walLevelLogical,
                    pageCache == null ? "off"
                            : "on(cap=" + (PAGE_CACHE_CAPACITY > 0 ? String.valueOf(PAGE_CACHE_CAPACITY) : "unbounded") + ")",
                    allowTables.size(), allowTables);
        }
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
            cleanupAllSpills();
        }
    }

    /* Reader thread: serially frames the WAL byte stream (WalPageDecoder is
     * stateful) and offloads the heavy per-record decoding to a thread pool. A
     * single consumer thread drains the processor in submission order, so the
     * transaction buffer and emitted LSN sequence stay correct. */
    private void run(PGReplicationStream stream, long startLsnLong, long segSize, Supplier<Boolean> isAlive) throws Exception {
        WalPageDecoder decoder = new WalPageDecoder(startLsnLong, segSize);
        // Frame from startLsnLong (possibly the checkpoint redo) but report offsets
        // only from emitFromLsn so the saved resume point never moves backwards
        // while the cache-warming prefix is being replayed.
        String initialOffset = lsnStr(emitFromLsn);
        long[] lastStatusUpdateMs = {0L};
        try (ConcurrentProcessor<WalPageDecoder.RawRecord, Decoded> processor =
                     TapExecutors.createSimple(DECODE_THREADS, DECODE_QUEUE_SIZE, "physical-wal-miner")) {
            Thread consumerThread = new Thread(() -> consumeLoop(processor, isAlive, initialOffset));
            consumerThread.setName("physical-wal-miner-Consumer");
            consumerThread.start();
            try {
                while (isAlive.get() && threadException.get() == null) {
                    ByteBuffer buf = stream.readPending();
                    if (buf == null) {
                        updateReplicationStatusIfDue(stream, lastStatusUpdateMs, false);
                        TimeUnit.MILLISECONDS.sleep(10);
                        continue;
                    }
                    LogSequenceNumber recv = stream.getLastReceiveLSN();
                    if (recv != null) {
                        lastReceiveLsnForStatus.set(recv.asLong());
                        updateReplicationStatusIfDue(stream, lastStatusUpdateMs, false);
                    }
                    int n = buf.remaining();
                    byte[] arr = new byte[n];
                    buf.get(arr);
                    decoder.feed(arr, 0, n);
                    WalPageDecoder.RawRecord raw;
                    while ((raw = decoder.nextRecord()) != null) {
                        if (!submit(processor, raw, isAlive, stream, lastStatusUpdateMs)) {
                            break;
                        }
                    }
                    decoder.compact();
                    recv = stream.getLastReceiveLSN();
                    if (recv != null) {
                        lastReceiveLsnForStatus.set(recv.asLong());
                        updateReplicationStatusIfDue(stream, lastStatusUpdateMs, false);
                    }
                }
            } finally {
                ErrorKit.ignoreAnyError(() -> updateReplicationStatusIfDue(stream, lastStatusUpdateMs, true));
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
                           WalPageDecoder.RawRecord raw, Supplier<Boolean> isAlive,
                           PGReplicationStream stream, long[] lastStatusUpdateMs) throws Exception {
        while (isAlive.get() && threadException.get() == null) {
            // Check if DDL barrier is active - if so, pause submission until
            // consumer thread clears it after downstream acknowledges schema update
            synchronized (ddlBarrierLock) {
                while (ddlBarrierActive && isAlive.get() && threadException.get() == null) {
                    tapLogger.info("Physical WAL miner reader thread paused - waiting for DDL barrier to clear");
                    try {
                        ddlBarrierLock.wait(1000);  // Wait max 1 second, then recheck conditions
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return false;
                    }
                    updateReplicationStatusIfDue(stream, lastStatusUpdateMs, false);
                }
            }
            if (processor.runAsync(raw, this::decodeUnit, 1, TimeUnit.SECONDS)) {
                return true;
            }
            updateReplicationStatusIfDue(stream, lastStatusUpdateMs, false);
        }
        return false;
    }

    /* Send a standby status update to prevent wal_sender_timeout disconnects.
     * IMPORTANT: this does NOT advance the replication slot. The slot's
     * confirmed_flush_lsn is only advanced on transaction COMMIT via the
     * connector's flushOffset path. Reporting the receive LSN as "flushed"
     * would allow WAL to be recycled before the consumer has actually
     * committed the data — a process crash at that point would lose the
     * uncommitted WAL segment. */
    private void updateReplicationStatusIfDue(PGReplicationStream stream, long[] lastStatusUpdateMs, boolean force) throws Exception {
        long now = System.currentTimeMillis();
        if (!force && now - lastStatusUpdateMs[0] < REPLICATION_STATUS_INTERVAL_MS) {
            return;
        }
        // Only send a status update if we have received at least some WAL;
        // the flushed/applied LSNs are left at their last committed values
        // so the slot is NOT advanced here — this is purely a keepalive.
        long recvLong = lastReceiveLsnForStatus.get();
        if (recvLong <= 0) {
            return;
        }
        try {
            stream.forceUpdateStatus();
            lastStatusUpdateMs[0] = now;
        } catch (Exception e) {
            if (force || !(e instanceof org.postgresql.util.PSQLException)) {
                throw e;
            }
            // Shutdown-time Broken Pipe is expected and swallowed
            tapLogger.warn("Physical WAL miner failed to update replication status: {}", e.getMessage());
        }
    }

    /* Consumer thread: drains decoded units in submission order, buffers heap
     * changes per transaction and flushes them on COMMIT, preserving LSN order. */
    private void consumeLoop(ConcurrentProcessor<WalPageDecoder.RawRecord, Decoded> processor,
                             Supplier<Boolean> isAlive, String initialOffset) {
        List<TapEvent> batch = new ArrayList<>();
        // The persisted resume offset must always sit on a transaction boundary.
        // Emitted batches only ever contain committed changes, so saving a
        // mid-transaction position (a buffered HEAP record, or the reader's receive
        // LSN that runs ahead of the consumer) would, on restart, make PG resend
        // only the records at/after that point — replaying an in-flight transaction
        // partially. That surfaces downstream as duplicate inserts / primary-key
        // conflicts (or silently drops the skipped half). Advance it on COMMIT/ABORT
        // only.
        String lastCommitOffset = initialOffset;
        // Furthest position the consumer has actually processed. Lets the resume
        // offset progress during idle, but only when no transaction is open so it
        // remains a safe boundary (anything the reader fetched beyond it is re-read
        // on restart).
        String lastProcessedOffset = initialOffset;
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
                    String safeOffsetBeforeApply = lastCommitOffset;
                    // Hold offsets back during the cache-warming prefix so a crash
                    // there cannot rewind the saved resume point below the emit
                    // position (which would re-emit snapshot/prior changes).
                    if (d.nextLsn >= emitFromLsn) {
                        lastProcessedOffset = d.offset;
                        if (d.kind == Decoded.COMMIT || d.kind == Decoded.ABORT) {
                            lastCommitOffset = d.offset;
                        }
                    }
                    batch = applyDecoded(d, batch, safeOffsetBeforeApply);
                    if (batch.size() >= recordSize) {
                        consumer.accept(batch, lastCommitOffset);
                        batch = new ArrayList<>();
                    }
                } else if (!batch.isEmpty()) {
                    consumer.accept(batch, lastCommitOffset);
                    batch = new ArrayList<>();
                    lastHeartbeat = System.currentTimeMillis();
                } else if (System.currentTimeMillis() - lastHeartbeat >= HEARTBEAT_INTERVAL_MS) {
                    // Idle with nothing buffered: advance to the caught-up position
                    // only when no transaction is open; otherwise hold at the last
                    // committed boundary so the open transaction's earlier changes
                    // are not skipped on restart.
                    String hb = pendingByXid.isEmpty() ? lastProcessedOffset : lastCommitOffset;
                    consumer.accept(Collections.singletonList(new HeartbeatEvent().init().referenceTime(System.currentTimeMillis())), hb);
                    lastHeartbeat = System.currentTimeMillis();
                }
            }
        } catch (Exception e) {
            threadException.set(e);
        }
    }

    /* CPU-light, order-independent slice executed on the pool threads: parse the
     * record (XLogRecord.parse is the only heavy part) and pre-resolve the
     * relation for heap changes. The actual HeapRmgrDecoder pass and any cache
     * mutation happen on the single consumer thread to keep page-state
     * mutations race-free; see PageStateCache.javadoc.
     *
     * Any unchecked exception from XLogRecord.parse (or the catalog lookup) is
     * swallowed here and the record is downgraded to OTHER. Letting it escape
     * would surface as a ConcurrentProcessorApplyException on get() and kill
     * the entire miner over a single malformed/unexpected record. */
    private Decoded decodeUnit(WalPageDecoder.RawRecord raw) {
        Decoded d = new Decoded();
        d.offset = lsnStr(raw.nextLsn);
        d.nextLsn = raw.nextLsn;
        try {
            XLogRecord rec = XLogRecord.parse(raw);
            d.rec = rec;
            if (rec.rmid == RM_HEAP_ID || rec.rmid == RM_HEAP2_ID) {
                d.kind = Decoded.HEAP;
                // Buffer under the top-level xid so subtransaction (savepoint /
                // PL-pgSQL EXCEPTION) changes flush on the top-level COMMIT instead
                // of leaking forever under their subxid.
                d.xid = rec.topXid != 0 ? rec.topXid : rec.xid;
                // Defer RelationInfo resolution to the consumer thread so that
                // catalog DDL changes already applied (via detectCatalogDdl →
                // applyPgAttributeChange) are visible to this record's DML decode.
                // Storing only the relNumber avoids pinning a stale RelationInfo
                // that was cached before a same-transaction DDL ran.
                XLogRecord.BlockRef b0 = rec.block(0);
                d.relNumber = b0 != null ? b0.relNumber : 0;
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
                } else if (op == XLOG_XACT_ASSIGNMENT) {
                    d.kind = Decoded.ASSIGNMENT;
                    XactAssignment assignment = readAssignment(rec.mainData);
                    d.xid = assignment.topXid != 0 ? assignment.topXid : rec.xid;
                    d.subxids = assignment.subxids;
                }
            } else if (rec.rmid == RM_XLOG_ID) {
                d.kind = Decoded.XLOG;
            }
        } catch (RuntimeException ex) {
            tapLogger.warn("skip WAL record at lsn={} due to parse error: {}",
                    lsnStr(raw.lsn), ex.getMessage());
            d.kind = Decoded.OTHER;
            d.rec = null;
        }
        return d;
    }

    /* Pool-side relation lookup: catalog has its own thread-safe cache. Returns
     * null when the relation is not in the allow set or cannot be resolved. */
    private RelationInfo resolveRel(XLogRecord rec) {
        XLogRecord.BlockRef b0 = rec.block(0);
        if (b0 == null) {
            return null;
        }
        RelationInfo rel = catalog.lookup(b0.relNumber);
        if (rel == null || !allowed(rel)) {
            return null;
        }
        // Apply any pending DDL column changes that were decoded from WAL before
        // this table's RelationInfo was loaded into the catalog cache. This
        // handles the edge case where ADD/DROP/RENAME COLUMN precedes the first
        // DML on the table within a transaction.
        String tableKey = rel.schema + "." + rel.table;
        List<RelationCatalog.PgAttributeChange> pending = pendingColumnChanges.remove(tableKey);
        if (pending != null) {
            rel = RelationCatalog.applyPendingChanges(rel, pending);
            catalog.cache(b0.relNumber, rel);
        }
        return rel;
    }

    /* Consumer-thread relation resolution. Same as resolveRel(XLogRecord) but
     * takes a raw relfilenode — called from applyDecoded (single consumer thread)
     * after any catalog DDL updates for earlier LSNs have already been applied,
     * guaranteeing the DML decode sees the correct column layout. */
    private RelationInfo resolveRelForConsumer(long relNumber) {
        if (relNumber <= 0) {
            return null;
        }
        RelationInfo rel = catalog.lookup(relNumber);
        if (rel == null || !allowed(rel)) {
            return null;
        }
        // Apply and consume any pending DDL column changes for this table.
        // Keyed by schema.table, not relfilenode, because DDL detection uses
        // pg_attribute.attrelid (pg_class OID) which may differ from relfilenode
        // after TRUNCATE / VACUUM FULL.
        String tableKey = rel.schema + "." + rel.table;
        List<RelationCatalog.PgAttributeChange> pending = pendingColumnChanges.remove(tableKey);
        if (pending != null) {
            if (WAL_DEBUG_ENABLED) {
                tapLogger.info("[WAL-DEBUG] applying {} pending pg_attribute change(s) to {}.{} before DML decode; beforeColumns={}",
                        pending.size(), rel.schema, rel.table, columnLayout(rel));
            }
            rel = RelationCatalog.applyPendingChanges(rel, pending);
            catalog.cache(relNumber, rel);
            if (WAL_DEBUG_ENABLED) {
                tapLogger.info("[WAL-DEBUG] applied pending pg_attribute changes to {}.{}; afterColumns={}",
                        rel.schema, rel.table, columnLayout(rel));
            }
        }
        return rel;
    }

    /* Consumer-side heap decode + page-state maintenance. Must run on the
     * single consumer thread because PageStateCache mutations and the
     * mini-redo applied by HeapRmgrDecoder are not thread-safe. A decode
     * failure on a single record is logged and the record is skipped — the
     * stream must keep advancing so the slot can be released. */
    private List<NormalRedo> decodeHeapOnConsumer(XLogRecord rec, RelationInfo rel) {
        List<NormalRedo> redos;
        try {
            redos = HeapRmgrDecoder.decode(rec, rel, decodeCtx);
        } catch (RuntimeException ex) {
            tapLogger.warn("skip heap record at lsn={} rel={}.{} due to decode error: {}",
                    lsnStr(rec.lsn), rel.schema, rel.table, ex.getMessage());
            return Collections.emptyList();
        }
        for (NormalRedo r : redos) {
            r.setCdcSequenceId(rec.lsn);   // source LSN, used to order a transaction's changes
            r.setSourceXid(rec.xid);
        }
        return expandKeyUpdates(redos, rel);
    }

    /* A heap UPDATE that changes a primary-key / replica-identity column value is
     * not idempotent under the at-least-once replay inherent to CDC bootstrap and
     * resume: re-applying "UPDATE ... SET pk=B WHERE pk=A" after the row has
     * already moved to B (the ABA case) makes the target miss the old row and
     * fall back to inserting B, which collides with the existing B and surfaces
     * downstream as a duplicate-key conflict. Rewrite such an UPDATE as
     * DELETE(before) + INSERT(after) so each half survives replay (delete-missing
     * is a no-op, insert is upserted). Requires a before-image carrying the key
     * columns; when it is unavailable (page-cache miss under wal_level=replica)
     * the original UPDATE is kept unchanged. */
    static List<NormalRedo> expandKeyUpdates(List<NormalRedo> redos, RelationInfo rel) {
        if (rel.keyColumns == null || rel.keyColumns.isEmpty()) {
            return redos;
        }
        List<NormalRedo> out = null;
        for (int i = 0; i < redos.size(); i++) {
            NormalRedo r = redos.get(i);
            if (isKeyChangingUpdate(r, rel.keyColumns)) {
                if (out == null) {
                    out = new ArrayList<>(redos.subList(0, i));
                }
                out.add(asDelete(r));
                out.add(asInsert(r));
            } else if (out != null) {
                out.add(r);
            }
        }
        return out == null ? redos : out;
    }

    static boolean isKeyChangingUpdate(NormalRedo r, List<String> keyColumns) {
        if (!NormalRedo.OperationEnum.UPDATE.name().equals(r.getOperation())) {
            return false;
        }
        Map<String, Object> before = r.getUndoRecord();
        Map<String, Object> after = r.getRedoRecord();
        if (before == null || after == null) {
            return false;
        }
        for (String k : keyColumns) {
            if (!before.containsKey(k)) {
                return false;   // key not recovered in the before-image: cannot tell, keep the UPDATE
            }
            if (!Objects.equals(before.get(k), after.get(k))) {
                return true;
            }
        }
        return false;
    }

    private static NormalRedo asDelete(NormalRedo u) {
        NormalRedo d = copyMeta(u);
        d.setOperation(NormalRedo.OperationEnum.DELETE.name());
        d.setUndoRecord(u.getUndoRecord());   // before-image identifies the old-key row to remove
        return d;
    }

    private static NormalRedo asInsert(NormalRedo u) {
        NormalRedo i = copyMeta(u);
        i.setOperation(NormalRedo.OperationEnum.INSERT.name());
        i.setRedoRecord(u.getRedoRecord());   // after-image is the new-key row to (up)insert
        return i;
    }

    /* Carry forward only the fields set at decode time; timestamp and
     * cdcSequenceStr are stamped later in the COMMIT drain loop. */
    private static NormalRedo copyMeta(NormalRedo u) {
        NormalRedo c = new NormalRedo();
        c.setNameSpace(u.getNameSpace());
        c.setTableName(u.getTableName());
        c.setTransactionId(u.getTransactionId());
        c.setCdcSequenceId(u.getCdcSequenceId());
        c.setSourceXid(u.getSourceXid());
        return c;
    }

    /* Drop cached pages only for a relation-wide rewrite (truncate). Per-page
     * maintenance (prune/vacuum/freeze/visibility) is deliberately a no-op: with
     * the logical overlay the visible tuple at every live offset is unchanged by
     * those records, and under wal_level=replica dropping a hot page would leave
     * it without an FPI to reseed from until the next checkpoint — exactly the
     * desync that stranded later UPDATE/DELETE with a null before-image. */
    private void invalidateForMaintenance(XLogRecord rec) {
        if (pageCache == null) {
            return;
        }
        if (rec.rmid == RM_HEAP_ID && rec.heapOp() == XLOG_HEAP_TRUNCATE) {
            // Payload lists relids but parsing them adds little value here;
            // truncate is rare in OLTP, so we drop every block ref carried by
            // the record (and accept the small risk of a stale entry for a
            // relation referenced only by the relid list).
            for (XLogRecord.BlockRef b : rec.blocks) {
                pageCache.invalidateRelation(b.relNumber);
            }
            if (WAL_DEBUG_ENABLED) {
                tapLogger.info("[WAL-DEBUG] TRUNCATE rmid={} lsn={} cacheSize={}", rec.rmid, lsnStr(rec.lsn), pageCache.size());
            }
            return;
        }
        if (WAL_DEBUG_ENABLED && rec.rmid == RM_HEAP2_ID) {
            int op = rec.heapOp();
            if (op == XLOG_HEAP2_PRUNE || op == XLOG_HEAP2_VACUUM
                    || op == XLOG_HEAP2_FREEZE_PAGE || op == XLOG_HEAP2_VISIBLE) {
                tapLogger.info("[WAL-DEBUG] HEAP2 maint (no-op) op=0x{} blocks={} lsn={}",
                        Integer.toHexString(op), rec.blocks.size(), lsnStr(rec.lsn));
            }
        }
    }

    /* Emit a one-line debug summary every time a checkpoint record floats by:
     * each checkpoint resets the per-page FPI-required flag, which is the
     * pivot for understanding why a given record does/doesn't carry an FPI. */
    private void logCheckpointIfAny(XLogRecord rec) {
        if (!WAL_DEBUG_ENABLED || rec.rmid != RM_XLOG_ID) {
            return;
        }
        int op = rec.info & XLR_RMGR_INFO_MASK;
        if (op == XLOG_CHECKPOINT_SHUTDOWN || op == XLOG_CHECKPOINT_ONLINE) {
            tapLogger.info("[WAL-DEBUG] CHECKPOINT kind={} lsn={} mainDataLen={} cacheSize={}",
                    op == XLOG_CHECKPOINT_SHUTDOWN ? "SHUTDOWN" : "ONLINE",
                    lsnStr(rec.lsn),
                    rec.mainData == null ? 0 : rec.mainData.length,
                    pageCache == null ? -1 : pageCache.size());
        } else if (op == XLOG_FPI || op == XLOG_FPI_FOR_HINT) {
            tapLogger.info("[WAL-DEBUG] FPI lsn={} blocks={} forHint={}",
                    lsnStr(rec.lsn), rec.blocks.size(), op == XLOG_FPI_FOR_HINT);
        }
    }

    /* Seed the page cache from *every* full-page image a record carries, no matter
     * which resource manager wrote it. PG takes an FPI on the first modification of
     * a page after a checkpoint (full_page_writes), and that image can ride on a
     * standalone XLOG_FPI / XLOG_FPI_FOR_HINT record or on a heap2 maintenance
     * record (prune/vacuum/freeze/visibility) — not only on the heap
     * INSERT/UPDATE/DELETE the decoder seeds from. Capturing the rest lets a later
     * FPI-less UPDATE/DELETE on a cold page still recover a before-image, mirroring
     * walminer's full-FPW replay. Index/system pages resolve to a null relation and
     * are skipped; RI-FULL and non-allowed relations don't use the cache. */
    private void seedFpisFromRecord(XLogRecord rec) {
        if ((pageCache == null && catalogPageCache == null) || rec.blocks.isEmpty()) {
            return;
        }
        for (XLogRecord.BlockRef b : rec.blocks) {
            if (!b.hasImage || b.image.length == 0) {
                continue;
            }
            // pg_attribute pages resolve to no user relation; seed them into the
            // dedicated catalog overlay so a later FPI-less catalog UPDATE can
            // reconstruct its before/after tuple from this page image.
            if (catalogPageCache != null && b.relNumber == pgAttributeRelNode) {
                byte[] catPage = PageImageExtractor.reconstructPage(b);
                if (catPage != null) {
                    catalogPageCache.getOrCreate(b.relNumber, b.blockNumber).seedFromImage(catPage);
                }
                continue;
            }
            if (pageCache == null) {
                continue;
            }
            RelationInfo rel = catalog.lookup(b.relNumber);
            if (rel == null || rel.replicaIdentityFull || !allowed(rel)) {
                continue;
            }
            byte[] page = PageImageExtractor.reconstructPage(b);
            if (page == null) {
                continue;
            }
            CachedPage cp = pageCache.getOrCreate(b.relNumber, b.blockNumber);
            boolean applied = cp.seedFromImage(page);
            if (WAL_DEBUG_ENABLED) {
                if (applied) {
                    tapLogger.info("[WAL-DEBUG] SEED rel={}.{} blk={} offsets={} (FPI rmid={} info=0x{})",
                            rel.schema, rel.table, b.blockNumber, cp.size(),
                            rec.rmid, Integer.toHexString(rec.info & 0xFF));
                } else {
                    tapLogger.info("[WAL-DEBUG] SEED-SKIP rel={}.{} blk={} (empty/will-init FPI; kept {} tuples rmid={} info=0x{})",
                            rel.schema, rel.table, b.blockNumber, cp.size(),
                            rec.rmid, Integer.toHexString(rec.info & 0xFF));
                }
            }
        }
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

    static XactAssignment readAssignment(byte[] mainData) {
        if (mainData == null || mainData.length < 8) {
            return XactAssignment.EMPTY;
        }
        try {
            WalByteReader r = new WalByteReader(mainData);
            long topXid = r.readUInt32();
            int n = (int) r.readUInt32();
            if (topXid == 0 || n <= 0) {
                return topXid == 0 ? XactAssignment.EMPTY : new XactAssignment(topXid, EMPTY_SUBXACTS);
            }
            long[] subs = new long[n];
            for (int i = 0; i < n; i++) {
                subs[i] = r.readUInt32();
            }
            return new XactAssignment(topXid, subs);
        } catch (RuntimeException ignore) {
            return XactAssignment.EMPTY;
        }
    }

    /* Single-threaded application of the ordered decode result to the per-xid
     * buffer; only this method touches {@code pendingByXid} and the page
     * cache. Heap records are decoded here (not on the pool) so the cache
     * mini-redo sees them in strict WAL order. */
    private List<TapEvent> applyDecoded(Decoded d, List<TapEvent> batch, String safeOffsetBeforeApply) {
        if (d.rec != null) {
            // Seed from FPIs carried by *any* record (standalone XLOG_FPI /
            // XLOG_FPI_FOR_HINT, heap2 prune/visible/freeze, etc.), not just the
            // heap INSERT/UPDATE/DELETE the decoder handles. Runs before heap
            // decode so primePage's own (identical) reseed leaves heap behaviour
            // unchanged while cold pages whose post-checkpoint FPI rode on a
            // non-heap record finally get seeded — matching walminer's FPW replay.
            seedFpisFromRecord(d.rec);
        }
        switch (d.kind) {
            case Decoded.HEAP:
                if (d.rec == null) {
                    return batch;
                }
                long xid = topXidFor(d.xid);
                // Resolve RelationInfo on the CONSUMER thread so that catalog DDL
                // changes already applied by detectCatalogDdl (same thread, earlier
                // LSN) are visible. Worker-thread resolution would pin a stale
                // column layout from before the DDL.
                d.rel = resolveRelForConsumer(d.relNumber);
                if (d.rel != null) {
                    List<NormalRedo> redos = decodeHeapOnConsumer(d.rec, d.rel);
                    if (!redos.isEmpty()) {
                        appendRedos(xid, redos);
                        pendingRedoCount += redos.size();
                        checkPendingPressure();
                    }
                } else {
                    detectCatalogDdl(d.rec, xid);
                }
                invalidateForMaintenance(d.rec);
                break;
            case Decoded.ASSIGNMENT:
                registerSubxids(d.xid, d.subxids);
                break;
            case Decoded.XLOG:
                if (d.rec != null) {
                    logCheckpointIfAny(d.rec);
                }
                break;
            case Decoded.COMMIT:
                long[] commitSubxids = subxidsForTop(d.xid, d.subxids);
                Set<Long> committedXids = committedXids(d.xid, d.subxids);
                // Debug: log the subxids we parsed from this COMMIT
                if (WAL_DEBUG_ENABLED && commitSubxids.length > 0) {
                    tapLogger.info("[WAL-DEBUG] COMMIT xid={} has {} subxids: {}",
                            d.xid, commitSubxids.length, java.util.Arrays.toString(commitSubxids));
                }
                // Drain DDL redos first (pg_attribute changes) - these are processed
                // separately from normal DML and used to synthesize DDL events.
                List<NormalRedo> ddlChanges = drainDdlRedos(d.xid, commitSubxids);
                ddlChanges.removeIf(r -> !isCommittedRedo(r, committedXids));
                ddlChanges.sort(Comparator.comparingLong(PhysicalWalLogMiner::redoLsn));

                // Drain normal DML via streaming k-way merge
                try (DrainResult dr = drainTransaction(d.xid, commitSubxids)) {
                    // Always drain (frees the per-xid buffers and keeps the page cache
                    // in step), but suppress commits that landed before the emit
                    // position: their records only served to warm the cache and were
                    // already captured by the snapshot / a prior offset.
                    if (d.rec != null && d.rec.lsn < emitFromLsn) {
                        // Early exit for warm-up transactions, but must still clean up
                        // DDL tracking state to prevent memory leaks
                        clearDdlPending(d.xid, commitSubxids);
                        clearSubxidAssignments(d.xid, commitSubxids);
                        clearPendingColumnChanges(d.xid, commitSubxids);
                        break;
                    }

                    // Time-based filtering: drop transactions committed before the filter time
                    if (filterStartTimeMs != null && d.commitMillis < filterStartTimeMs) {
                        if (WAL_DEBUG_ENABLED) {
                            tapLogger.info("[WAL-DEBUG] FILTER-DROP transaction xid={} with commit time {} < filter time {}",
                                    d.xid, d.commitMillis, filterStartTimeMs);
                        }
                        clearDdlPending(d.xid, commitSubxids);
                        clearSubxidAssignments(d.xid, commitSubxids);
                        clearPendingColumnChanges(d.xid, commitSubxids);
                        break;
                    }

                    int emitted = 0;
                    int emittedDdl = 0;
                    int ddlIndex = 0;
                    int totalRedos = dr.count;
                    int skipped = 0;
                    while (dr.iterator.hasNext()) {
                        NormalRedo r = dr.iterator.next();
                        long dmlLsn = redoLsn(r);
                        while (ddlIndex < ddlChanges.size()
                                && redoLsn(ddlChanges.get(ddlIndex)) <= dmlLsn) {
                            batch = emitDdlAndWait(ddlChanges.get(ddlIndex++), d.commitMillis, batch, safeOffsetBeforeApply, d.xid);
                            emittedDdl++;
                        }
                        if (!isCommittedRedo(r, committedXids)) {
                            skipped++;
                            if (WAL_DEBUG_ENABLED) {
                                tapLogger.info("[WAL-DEBUG] SKIP rolled-back xid={} op={} {}.{} lsn={}",
                                        r.getSourceXid(), r.getOperation(), r.getNameSpace(), r.getTableName(),
                                        lsnStr(redoLsn(r)));
                            }
                            continue;
                        }
                        r.setTimestamp(d.commitMillis);
                        r.setCdcSequenceStr(d.offset);
                        TapEvent ev = toEvent(r);
                        if (ev != null) {
                            batch.add(ev);
                            emitted++;
                            trackNullImage(r);
                            if (WAL_DEBUG_ENABLED) {
                                tapLogger.info("[WAL-DEBUG] EMIT op={} {}.{} lsn={} before={} after={}",
                                        r.getOperation(), r.getNameSpace(), r.getTableName(),
                                        lsnStr(redoLsn(r)), r.getUndoRecord(), r.getRedoRecord());
                            }
                            // Batch-flush mid-COMMIT so multi-million-row
                            // transactions don't pile all TapEvents in memory.
                            //
                            // IMPORTANT — duplicate-on-crash-replay trade-off:
                            // These intermediate batches use safeOffsetBeforeApply
                            // (the offset of the previous COMMIT), NOT the current
                            // transaction's offset. This is intentional:
                            //
                            // 1. If we used the current offset, a crash mid-COMMIT
                            //    would save a position inside the transaction, and
                            //    on restart the PRE-commit portion of this xid's
                            //    WAL would be skipped → data LOSS.
                            //
                            // 2. With safeOffsetBeforeApply, a crash mid-COMMIT
                            //    rewinds to the previous transaction boundary. PG
                            //    re-decodes this entire xid → already-emitted
                            //    internal batches are produced again → downstream
                            //    MUST be idempotent (upsert or dedup semantics).
                            //
                            // The final batch (after the while loop) uses the
                            // current COMMIT offset via consumeLoop's normal
                            // lastCommitOffset, so a successful full drain
                            // advances the resume point past this xid.
                            if (batch.size() >= recordSize && dr.iterator.hasNext()) {
                                consumer.accept(batch, safeOffsetBeforeApply);
                                batch = new ArrayList<>();
                            }
                        }
                    }
                    while (ddlIndex < ddlChanges.size()) {
                        batch = emitDdlAndWait(ddlChanges.get(ddlIndex++), d.commitMillis, batch, safeOffsetBeforeApply, d.xid);
                        emittedDdl++;
                    }
                    if (WAL_DEBUG_ENABLED) {
                        tapLogger.info("[WAL-DEBUG] COMMIT drain xid={} redos={} emitted={} skipped={} ddl={} subxids={} offset={}",
                                d.xid, totalRedos, emitted, skipped, emittedDdl, commitSubxids.length, d.offset);
                    }
                    clearDdlPending(d.xid, commitSubxids);
                    clearSubxidAssignments(d.xid, commitSubxids);
                    clearPendingColumnChanges(d.xid, commitSubxids);
                }
                break;
            case Decoded.ABORT:
                long[] abortSubxids = subxidsForTop(d.xid, d.subxids);
                boolean subtransactionAbort = subxidToTopXid.containsKey(d.xid);
                if (subtransactionAbort) {
                    discardTransaction(d.xid, d.subxids);
                } else {
                    discardTransaction(d.xid, abortSubxids);
                }
                List<NormalRedo> abortedDdls = drainDdlRedos(d.xid, abortSubxids);
                // Revert in-memory RelationCatalog changes from aborted DDL
                if (!abortedDdls.isEmpty()) {
                    rebuildCatalogOverlaysFromBufferedDdl();
                    if (WAL_DEBUG_ENABLED) {
                        tapLogger.info("[WAL-DEBUG] ABORT xid={} with {} DDL changes; catalog invalidated",
                                d.xid, abortedDdls.size());
                    }
                }
                // Clear DDL tracking for aborted transactions. If this transaction
                // had activated the barrier, clear it unconditionally to prevent deadlock.
                synchronized (ddlBarrierLock) {
                    boolean wasTracked = clearDdlPendingLocked(d.xid, abortSubxids);
                    if (wasTracked && ddlBarrierActive) {
                        // This aborted transaction might have activated the barrier,
                        // clear it to allow reader to resume
                        ddlBarrierActive = false;
                        ddlBarrierLock.notifyAll();
                        tapLogger.info("Physical WAL miner DDL barrier cleared after ABORT xid={}", d.xid);
                    }
                }
                clearSubxidAssignments(d.xid, abortSubxids);
                clearPendingColumnChanges(d.xid, abortSubxids);
                break;
            default:
                break;
        }
        return batch;
    }

    private long topXidFor(long xid) {
        Long top = subxidToTopXid.get(xid);
        return top == null ? xid : top;
    }

    private long[] subxidsForTop(long topXid, long[] walSubxids) {
        return mergeSubxids(topXid, walSubxids, subxidToTopXid);
    }

    static Set<Long> committedXids(long topXid, long[] walSubxids) {
        Set<Long> committed = new HashSet<>();
        if (topXid != 0) {
            committed.add(topXid);
        }
        if (walSubxids != null) {
            for (long sub : walSubxids) {
                if (sub != 0) {
                    committed.add(sub);
                }
            }
        }
        return committed;
    }

    static boolean isCommittedRedo(NormalRedo redo, Set<Long> committedXids) {
        Long sourceXid = redo.getSourceXid();
        return sourceXid == null || committedXids.contains(sourceXid);
    }

    static long[] mergeSubxids(long topXid, long[] walSubxids, Map<Long, Long> assignments) {
        if ((walSubxids == null || walSubxids.length == 0) && (assignments == null || assignments.isEmpty())) {
            return EMPTY_SUBXACTS;
        }
        LinkedHashSet<Long> merged = new LinkedHashSet<>();
        if (walSubxids != null) {
            for (long sub : walSubxids) {
                if (sub != 0 && sub != topXid) {
                    merged.add(sub);
                }
            }
        }
        if (assignments != null && topXid != 0) {
            for (Map.Entry<Long, Long> e : assignments.entrySet()) {
                Long parent = e.getValue();
                Long sub = e.getKey();
                if (Objects.equals(parent, topXid) && sub != null && sub != 0 && sub != topXid) {
                    merged.add(sub);
                }
            }
        }
        if (merged.isEmpty()) {
            return EMPTY_SUBXACTS;
        }
        long[] out = new long[merged.size()];
        int i = 0;
        for (Long sub : merged) {
            out[i++] = sub;
        }
        return out;
    }

    private void registerSubxids(long topXid, long[] subxids) {
        if (topXid == 0 || subxids == null || subxids.length == 0) {
            return;
        }
        for (long sub : subxids) {
            if (sub != 0 && sub != topXid) {
                subxidToTopXid.put(sub, topXid);
            }
        }
        if (WAL_DEBUG_ENABLED) {
            tapLogger.info("[WAL-DEBUG] XACT assignment topXid={} subxids={}",
                    topXid, Arrays.toString(subxids));
        }
    }

    private void clearSubxidAssignments(long topXid, long[] subxids) {
        subxidToTopXid.remove(topXid);
        if (subxids != null && subxids.length > 0) {
            for (long sub : subxids) {
                subxidToTopXid.remove(sub);
            }
        }
        subxidToTopXid.entrySet().removeIf(e -> Objects.equals(e.getValue(), topXid));
    }

    private void clearPendingColumnChanges(long topXid, long[] subxids) {
        // Pending column changes are keyed by OID, not XID. At COMMIT/ABORT time
        // we don't know which OIDs this transaction affected without scanning all
        // entries. Since pending changes only accumulate for the edge case where
        // DDL precedes first DML (rare), we keep them until the next resolveRel
        // applies them. The worst case is a slow leak if DDLs keep happening
        // without DML — but that's an unusual CDC pattern.
        //
        // For the common path, pendingColumnChanges entries are empty because
        // catalog.applyPgAttributeChange returned true (cache hit).
    }

    private void rebuildCatalogOverlaysFromBufferedDdl() {
        catalog.invalidate();
        pendingColumnChanges.clear();
        List<NormalRedo> remaining = new ArrayList<>();
        for (List<NormalRedo> redos : ddlRedosByXid.values()) {
            remaining.addAll(redos);
        }
        remaining.sort(Comparator.comparingLong(PhysicalWalLogMiner::redoLsn));
        for (NormalRedo r : remaining) {
            bufferPendingColumnChange(r);
        }
    }

    private void bufferPendingColumnChange(NormalRedo r) {
        Long attrelid = readAttrelid(r);
        if (attrelid == null) {
            return;
        }
        MonitoredTable mt = monitoredOidToName.get(attrelid);
        if (mt == null) {
            return;
        }
        Integer attnum = readInt(r.getRedoRecord(), "attnum");
        if (attnum == null) {
            attnum = readInt(r.getUndoRecord(), "attnum");
        }
        String attname = readStr(r.getRedoRecord(), "attname");
        if (attname == null) {
            attname = readStr(r.getUndoRecord(), "attname");
        }
        Long atttypid = readLong(r.getRedoRecord(), "atttypid");
        if (atttypid == null) {
            atttypid = readLong(r.getUndoRecord(), "atttypid");
        }
        Integer attlen = readInt(r.getRedoRecord(), "attlen");
        if (attlen == null) {
            attlen = readInt(r.getUndoRecord(), "attlen");
        }
        String attalignStr = readStr(r.getRedoRecord(), "attalign");
        if (attalignStr == null) {
            attalignStr = readStr(r.getUndoRecord(), "attalign");
        }
        Boolean attisdropped = readBool(r.getRedoRecord(), "attisdropped");
        if (attisdropped == null) {
            attisdropped = readBool(r.getUndoRecord(), "attisdropped");
        }
        if (attnum == null || attname == null || atttypid == null || attlen == null) {
            return;
        }
        char attalign = (attalignStr != null && !attalignStr.isEmpty()) ? attalignStr.charAt(0) : 'c';
        String tableKey = mt.schema + "." + mt.table;
        pendingColumnChanges.computeIfAbsent(tableKey, k -> new ArrayList<>())
                .add(new RelationCatalog.PgAttributeChange(
                        r.getOperation(), attnum, attname, atttypid, attlen, attalign,
                        Boolean.TRUE.equals(attisdropped)));
    }

    /** Build a streaming, LSN-ordered iterator over the buffered changes for a
     * top-level xid together with all of its subtransaction xids. Spill files are
     * opened as ObjectInputStreams and merged via a k-way PriorityQueue so that
     * COMMIT-time heap is bounded to O(num_streams), not O(total_rows). */
    private DrainResult drainTransaction(long topXid, long[] subxids) {
        List<RedoStream> streams = new ArrayList<>();
        List<Path> spillFiles = new ArrayList<>();
        int count = 0;

        try {
            // Close spill writers BEFORE reading to avoid file locking issues
            closeSpillWriterForXid(topXid);
            if (subxids != null) {
                for (long sub : subxids) {
                    closeSpillWriterForXid(sub);
                }
            }

            count += collectStreams(topXid, streams, spillFiles);
            if (subxids != null) {
                for (long sub : subxids) {
                    count += collectStreams(sub, streams, spillFiles);
                }
            }

            pendingRedoCount -= count;
            if (pendingRedoCount < 0) {
                pendingRedoCount = 0;
            }

            Iterator<NormalRedo> merged = kwayMerge(streams);
            return new DrainResult(merged, count, spillFiles);
        } catch (RuntimeException e) {
            closeStreams(streams);
            deleteFiles(spillFiles);
            throw e;
        }
    }

    /* Remove and return the buffered pg_attribute changes under a top-level xid
     * (and any of its subtransactions). Mirrors drainTransaction so catalog DDL
     * buffered under a savepoint flushes on the top-level COMMIT. */
    private List<NormalRedo> drainDdlRedos(long topXid, long[] subxids) {
        List<NormalRedo> all = new ArrayList<>();
        List<NormalRedo> top = ddlRedosByXid.remove(topXid);
        if (top != null) {
            all.addAll(top);
        }
        if (subxids != null) {
            for (long sub : subxids) {
                List<NormalRedo> bucket = ddlRedosByXid.remove(sub);
                if (bucket != null) {
                    all.addAll(bucket);
                }
            }
        }
        return all;
    }

    private List<TapEvent> emitDdlAndWait(NormalRedo ddlRedo, long commitMillis,
                                          List<TapEvent> batch, String offset, long xid) {
        int before = batch.size();
        emitDdlEvents(Collections.singletonList(ddlRedo), commitMillis, batch);
        if (batch.size() == before) {
            return batch;
        }
        synchronized (ddlBarrierLock) {
            ddlBarrierActive = true;
            tapLogger.info("Physical WAL miner DDL barrier activated for xid={} lsn={} - reader thread will pause",
                    xid, lsnStr(redoLsn(ddlRedo)));
        }
        try {
            tapLogger.info("Physical WAL miner flushing DDL batch with {} events for xid={}, offset={}",
                    batch.size(), xid, offset);
            consumer.accept(batch, offset);
            tapLogger.info("Physical WAL miner waiting for downstream DDL processing (xid={}, lsn={})",
                    xid, lsnStr(redoLsn(ddlRedo)));
            TimeUnit.MILLISECONDS.sleep(500);
            return new ArrayList<>();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("DDL wait interrupted", e);
        } finally {
            synchronized (ddlBarrierLock) {
                ddlBarrierActive = false;
                ddlBarrierLock.notifyAll();
                tapLogger.info("Physical WAL miner DDL barrier cleared for xid={} lsn={} - reader thread resuming",
                        xid, lsnStr(redoLsn(ddlRedo)));
            }
        }
    }

    private void clearDdlPending(long topXid, long[] subxids) {
        synchronized (ddlBarrierLock) {
            clearDdlPendingLocked(topXid, subxids);
        }
    }

    private boolean clearDdlPendingLocked(long topXid, long[] subxids) {
        boolean removed = ddlPendingXids.remove(topXid);
        if (subxids != null) {
            for (long sub : subxids) {
                removed |= ddlPendingXids.remove(sub);
            }
        }
        return removed;
    }

    private void checkPendingPressure() {
        if (pendingRedoCount < PENDING_REDO_WARN_THRESHOLD) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastPendingWarnMs >= PENDING_WARN_INTERVAL_MS) {
            lastPendingWarnMs = now;
            int spilled = spillStates.size();
            tapLogger.warn("Physical WAL miner has buffered {} uncommitted changes across {} open transactions"
                            + (spilled > 0 ? " ({} transaction(s) spilled to disk)" : "")
                            + "; check for long-running transactions on the source to avoid excessive memory use",
                    pendingRedoCount, pendingByXid.size(), spilled);
        }
    }

    private void appendRedos(long xid, List<NormalRedo> redos) {
        SpillState ss = spillStates.get(xid);
        if (ss != null && ss.writer != null) {
            spillRedos(xid, ss, redos);
            return;
        }
        List<NormalRedo> bucket = pendingByXid.computeIfAbsent(xid, k -> new ArrayList<>());
        bucket.addAll(redos);
        if (bucket.size() >= PER_XID_SPILL_THRESHOLD) {
            ss = spillStates.computeIfAbsent(xid, k -> new SpillState());
            spillRedos(xid, ss, bucket);
            bucket.clear();
        }
    }

    /* Once a transaction crosses the spill threshold, keep appending its later
     * NormalRedo objects directly to disk instead of repeatedly growing and
     * serialising a large ArrayList. Called on the consumer thread only. */
    private void spillRedos(long xid, SpillState ss, List<NormalRedo> redos) {
        if (redos.isEmpty()) {
            return;
        }
        if (ss.writer == null) {
            openSpillWriter(xid, ss);
        }
        try {
            for (NormalRedo r : redos) {
                ss.writer.writeObject(r);
                ss.spilledCount++;
                ss.objectsSinceReset++;
                if (ss.objectsSinceReset >= SPILL_STREAM_RESET_INTERVAL) {
                    ss.writer.reset();
                    ss.objectsSinceReset = 0;
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Physical WAL miner failed to append " + redos.size()
                    + " NormalRedo objects for xid=" + xid + " to spill file " + ss.currentFile
                    + "; aborting miner so WAL can be replayed", e);
        }
    }

    private void openSpillWriter(long xid, SpillState ss) {
        Path file = spillDir.resolve("spill_" + xid + "_" + ss.files.size() + ".dat");
        OutputStream fos = null;
        try {
            fos = Files.newOutputStream(file);
            BufferedOutputStream bos = new BufferedOutputStream(fos, 256 * 1024);
            ss.writer = new ObjectOutputStream(bos);
            fos = null; // owned by ObjectOutputStream now
            ss.currentFile = file;
            ss.files.add(file);
            tapLogger.info("Physical WAL miner opened spill file for xid={} at {}", xid, file);
        } catch (IOException e) {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException ignored) {
                }
            }
            throw new IllegalStateException("Physical WAL miner cannot open spill file " + file
                    + " for xid=" + xid + "; aborting miner so WAL can be replayed", e);
        }
    }

    private void closeSpillWriter(SpillState ss) {
        if (ss == null || ss.writer == null) {
            return;
        }
        try {
            ss.writer.close();
        } catch (IOException e) {
            throw new IllegalStateException("Physical WAL miner failed to close spill file "
                    + ss.currentFile + "; aborting miner so WAL can be replayed", e);
        } finally {
            ss.writer = null;
            ss.currentFile = null;
            ss.objectsSinceReset = 0;
        }
    }

    private void closeSpillWriterQuietly(SpillState ss) {
        if (ss == null || ss.writer == null) {
            return;
        }
        try {
            ss.writer.close();
        } catch (IOException ignored) {
        } finally {
            ss.writer = null;
            ss.currentFile = null;
            ss.objectsSinceReset = 0;
        }
    }

    /**
     * Close the spill writer for a specific xid if it exists. Called before
     * COMMIT/ABORT to ensure the file is properly flushed and closed before
     * reading begins, avoiding file locking issues on some platforms.
     */
    private void closeSpillWriterForXid(long xid) {
        SpillState ss = spillStates.get(xid);
        if (ss != null) {
            closeSpillWriter(ss);
        }
    }

    /* Per-xid spill state: ordered list of spill files written oldest-first, plus
     * a running count of objects that have been evicted to disk. */
    private static final class SpillState {
        final List<Path> files = new ArrayList<>();
        ObjectOutputStream writer;
        Path currentFile;
        int objectsSinceReset;
        int spilledCount;
    }

    /* Delete all remaining spill files and the spill directory. Called at miner
     * shutdown; any in-flight transaction's spilled rows are lost (the slot
     * rewinds to the last committed boundary on restart). */
    private void cleanupAllSpills() {
        for (SpillState ss : spillStates.values()) {
            closeSpillWriterQuietly(ss);
            for (Path file : ss.files) {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    tapLogger.warn("Physical WAL miner could not delete spill file {} during shutdown: {}",
                            file, e.getMessage());
                }
            }
        }
        spillStates.clear();
        if (spillDir != null) {
            try {
                // Walk depth-first so only empty dirs remain for deleteIfExists
                Files.walk(spillDir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                Files.deleteIfExists(p);
                            } catch (IOException ignored) {
                            }
                        });
            } catch (IOException e) {
                tapLogger.warn("Physical WAL miner could not clean spill directory {}: {}",
                        spillDir, e.getMessage());
            }
        }
    }

    // ── Streaming spill merge for COMMIT ────────────────────────────────
    //
    // Instead of reading all spill files back into a single List at COMMIT time
    // (which would defeat the purpose of spilling for multi-million-row
    // transactions), the k-way merge below streams records directly from spill
    // files via ObjectInputStream, merging across xids with a PriorityQueue
    // ordered by source LSN. This bounds COMMIT-time heap to O(num_streams)
    // rather than O(total_rows).

    /**
     * A closeable, sorted stream of NormalRedo objects from one source (in-memory
     * list or on-disk spill file).
     */
    private interface RedoStream extends AutoCloseable, Iterator<NormalRedo> {
        /**
         * LSN of the next record, or {@code Long.MAX_VALUE} if exhausted.
         */
        long peekLsn();
    }

    private static class InMemoryRedoStream implements RedoStream {
        private final Iterator<NormalRedo> iter;
        private NormalRedo next;

        InMemoryRedoStream(List<NormalRedo> list) {
            this.iter = list.iterator();
            advance();
        }

        private void advance() {
            next = iter.hasNext() ? iter.next() : null;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public NormalRedo next() {
            NormalRedo r = next;
            advance();
            return r;
        }

        @Override
        public long peekLsn() {
            Long id = next != null ? next.getCdcSequenceId() : null;
            return id == null ? Long.MAX_VALUE : id;
        }

        @Override
        public void close() {
        }
    }

    private static class SpillFileRedoStream implements RedoStream {
        final Path file;
        private final ObjectInputStream ois;
        private NormalRedo next;

        SpillFileRedoStream(Path file) throws IOException {
            this.file = file;
            this.ois = new ObjectInputStream(
                    new BufferedInputStream(Files.newInputStream(file), 256 * 1024));
            advance();
        }

        private void advance() {
            try {
                Object obj = ois.readObject();
                next = obj instanceof NormalRedo ? (NormalRedo) obj : null;
            } catch (EOFException e) {
                next = null;
            } catch (Exception e) {
                next = null;
                throw new IllegalStateException("Physical WAL miner failed to read from spill file "
                        + file + "; aborting commit so WAL can be replayed", e);
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public NormalRedo next() {
            NormalRedo r = next;
            advance();
            return r;
        }

        @Override
        public long peekLsn() {
            Long id = next != null ? next.getCdcSequenceId() : null;
            return id == null ? Long.MAX_VALUE : id;
        }

        @Override
        public void close() {
            try {
                ois.close();
            } catch (IOException ignored) {
            }
        }
    }

    private static class SequentialRedoStream implements RedoStream {
        private final List<Path> files;
        private int fileIndex;
        private SpillFileRedoStream current;

        SequentialRedoStream(List<Path> files) {
            this.files = files;
            openNextNonEmpty();
        }

        private void openNextNonEmpty() {
            closeCurrent();
            current = null;
            while (fileIndex < files.size()) {
                Path file = files.get(fileIndex++);
                try {
                    current = new SpillFileRedoStream(file);
                    if (current.hasNext()) {
                        return;
                    }
                    closeCurrent();
                    current = null;
                } catch (IOException e) {
                    throw new IllegalStateException("Physical WAL miner cannot open spill file "
                            + file + "; aborting commit so WAL can be replayed", e);
                }
            }
        }

        private void closeCurrent() {
            if (current != null) {
                current.close();
            }
        }

        @Override
        public boolean hasNext() {
            return current != null && current.hasNext();
        }

        @Override
        public NormalRedo next() {
            NormalRedo r = current.next();
            if (!current.hasNext()) {
                openNextNonEmpty();
            }
            return r;
        }

        @Override
        public long peekLsn() {
            return current == null ? Long.MAX_VALUE : current.peekLsn();
        }

        @Override
        public void close() {
            closeCurrent();
        }
    }

    /**
     * Result of draining a transaction for COMMIT: a merged LSN-ordered iterator
     * plus metadata for count tracking and spill file cleanup.
     */
    private static final class DrainResult implements AutoCloseable {
        final Iterator<NormalRedo> iterator;
        final int count;
        private final List<Path> spillFiles;

        DrainResult(Iterator<NormalRedo> iterator, int count, List<Path> spillFiles) {
            this.iterator = iterator;
            this.count = count;
            this.spillFiles = spillFiles;
        }

        @Override
        public void close() {
            if (iterator instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) iterator).close();
                } catch (Exception ignored) {
                }
            }
            deleteFiles(spillFiles);
        }
    }

    private static void closeStreams(List<RedoStream> streams) {
        for (RedoStream stream : streams) {
            try {
                stream.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static void deleteFiles(List<Path> files) {
        for (Path file : files) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Discard a transaction's buffered changes without reading them (ABORT path).
     * Removes from pendingByXid, deletes spill files, adjusts pendingRedoCount.
     */
    private void discardTransaction(long topXid, long[] subxids) {
        // Close spill writers first to ensure files are properly closed
        closeSpillWriterForXid(topXid);
        if (subxids != null) {
            for (long sub : subxids) {
                closeSpillWriterForXid(sub);
            }
        }

        int removed = discardBucket(topXid);
        if (subxids != null) {
            for (long sub : subxids) {
                removed += discardBucket(sub);
            }
        }
        pendingRedoCount -= removed;
        if (pendingRedoCount < 0) {
            pendingRedoCount = 0;
        }
    }

    private int discardBucket(long xid) {
        List<NormalRedo> inMem = pendingByXid.remove(xid);
        int memCount = inMem != null ? inMem.size() : 0;
        SpillState ss = spillStates.remove(xid);
        if (ss != null) {
            closeSpillWriterQuietly(ss);
            for (Path file : ss.files) {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException ignored) {
                }
            }
            return memCount + ss.spilledCount;
        }
        return memCount;
    }

    /**
     * K-way merge of sorted RedoStreams, producing LSN-ordered output.
     * The returned iterator implements {@link AutoCloseable} so that
     * {@link DrainResult#close()} can close every underlying stream
     * (and its {@link ObjectInputStream}) even when the merge wraps
     * multiple spill files.
     */
    private static Iterator<NormalRedo> kwayMerge(List<RedoStream> streams) {
        if (streams.isEmpty()) {
            return Collections.emptyIterator();
        }
        if (streams.size() == 1) {
            // RedoStream already extends AutoCloseable; the single-stream
            // case is detected by the instanceof check in DrainResult.close().
            return streams.get(0);
        }
        PriorityQueue<RedoStream> pq = new PriorityQueue<>(
                Comparator.comparingLong(RedoStream::peekLsn));
        for (RedoStream s : streams) {
            if (s.hasNext()) {
                pq.add(s);
            }
        }
        // Wrap in an AutoCloseable so DrainResult.close() can clean up
        // all underlying ObjectInputStreams.
        return new MergedRedoIterator(pq, streams);
    }

    /**
     * An {@link Iterator}&lt;{@link NormalRedo}&gt; that is also
     * {@link AutoCloseable} — closes every constituent {@link RedoStream}
     * when the merge is done or the transaction is discarded.
     */
    private static final class MergedRedoIterator implements Iterator<NormalRedo>, AutoCloseable {
        private final PriorityQueue<RedoStream> pq;
        private final List<RedoStream> streams;

        MergedRedoIterator(PriorityQueue<RedoStream> pq, List<RedoStream> streams) {
            this.pq = pq;
            this.streams = streams;
        }

        @Override
        public boolean hasNext() {
            return !pq.isEmpty();
        }

        @Override
        public NormalRedo next() {
            RedoStream s = pq.poll();
            NormalRedo r = s.next();
            if (s.hasNext()) {
                pq.add(s);
            }
            return r;
        }

        @Override
        public void close() {
            for (RedoStream s : streams) {
                try {
                    s.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    /**
     * Collect streams for one xid, removing it from pendingByXid and spillStates.
     * Spill files are represented as one lazy sequential stream, so each xid opens
     * at most one spill file at a time. Returns the total record count.
     */
    private int collectStreams(long xid, List<RedoStream> streams, List<Path> spillFiles) {
        List<NormalRedo> inMem = pendingByXid.remove(xid);
        int memCount = inMem != null ? inMem.size() : 0;
        SpillState ss = spillStates.remove(xid);
        int spilledCount = 0;

        if (ss != null) {
            spilledCount = ss.spilledCount;
            spillFiles.addAll(ss.files);
            // Writer is already closed by drainTransaction/discardTransaction before calling this
            if (!ss.files.isEmpty()) {
                streams.add(new SequentialRedoStream(ss.files));
            }
        }

        if (inMem != null && !inMem.isEmpty()) {
            streams.add(new InMemoryRedoStream(inMem));
        }

        return memCount + spilledCount;
    }

    /* Track UPDATE/DELETE emitted without a before-image — a page-state cache
     * miss under wal_level=replica. When the cache is bounded the page's
     * FPI-seeded overlay may have been evicted; when unbounded the page's
     * post-checkpoint FPI simply falls outside the read window (rows last written
     * before the warm-up anchor, typical of FIFO/queue tables). Warn periodically
     * with the applicable levers rather than per-record. */
    private void trackNullImage(NormalRedo r) {
        if (pageCache == null) {
            return;
        }
        String op = r.getOperation();
        boolean missing = ("UPDATE".equals(op) || "DELETE".equals(op)) && r.getUndoRecord() == null;
        if (!missing) {
            return;
        }
        nullImageCount++;
        long now = System.currentTimeMillis();
        if (now - lastNullImageWarnMs >= NULL_IMAGE_WARN_INTERVAL_MS) {
            lastNullImageWarnMs = now;
            if (PAGE_CACHE_CAPACITY > 0) {
                tapLogger.warn("Physical WAL miner has emitted {} UPDATE/DELETE without a before-image due to "
                                + "page-state cache misses under wal_level=replica. This affects large/cold tables whose "
                                + "working set exceeds the page cache ({} pages) within a checkpoint cycle. Raise "
                                + "TAPDATA_WAL_PAGE_CACHE_CAPACITY, or set REPLICA IDENTITY FULL on those tables, or use "
                                + "wal_level=logical, to restore full before-image coverage.",
                        nullImageCount, PAGE_CACHE_CAPACITY);
            } else {
                tapLogger.warn("Physical WAL miner has emitted {} UPDATE/DELETE without a before-image under "
                                + "wal_level=replica. The page cache is unbounded, so these are pages whose "
                                + "post-checkpoint FPI falls outside the read window (rows last written before the warm-up "
                                + "anchor, e.g. deletes on FIFO/queue tables). Set REPLICA IDENTITY FULL on those tables, "
                                + "or use wal_level=logical, to restore full before-image coverage.",
                        nullImageCount);
            }
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
        static final int XLOG = 4;
        static final int ASSIGNMENT = 5;
        int kind = OTHER;
        long xid;
        String offset;
        long nextLsn;          // LSN just past this record; basis for the emit gate
        long commitMillis;
        long[] subxids;
        XLogRecord rec;
        /* relfilenode from the WAL block reference — resolved to RelationInfo
         * on the consumer thread so DDL-updated catalog columns are visible. */
        long relNumber;
        /* Resolved on the consumer thread (not in decodeUnit on the pool) so
         * that catalog DDL changes already applied are visible to DML decode. */
        RelationInfo rel;
    }

    static final class XactAssignment {
        static final XactAssignment EMPTY = new XactAssignment(0, EMPTY_SUBXACTS);
        final long topXid;
        final long[] subxids;

        XactAssignment(long topXid, long[] subxids) {
            this.topXid = topXid;
            this.subxids = subxids == null ? EMPTY_SUBXACTS : subxids;
        }
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

    private static String columnLayout(RelationInfo rel) {
        if (rel == null || rel.columns == null) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < rel.columns.size(); i++) {
            ColumnInfo c = rel.columns.get(i);
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(c.attnum).append(':').append(c.name);
            if (c.dropped) {
                sb.append("(dropped)");
            }
        }
        return sb.append(']').toString();
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

    /* Prepare the data needed to attribute pg_attribute catalog changes back to a
     * monitored table: (1) the pg_class OID of every monitored relation, (2) the
     * on-disk relfilenode of pg_catalog.pg_attribute (a mapped catalog whose
     * pg_class.relfilenode is 0, so pg_relation_filenode() is used), and (3) that
     * catalog's column layout for tuple deformation. Any failure degrades to "DDL
     * detection disabled" (logged) rather than aborting the miner.
     *
     * IMPORTANT: Uses a REPEATABLE READ transaction to get a consistent catalog snapshot
     * and avoids connection leaks by NOT calling getConnection() directly. */
    private void buildDdlWatch() {
        // All catalog queries use JdbcContext.query() which properly manages connection
        // pooling via try-with-resources. We execute them all in sequence; while not
        // in a single transaction, the DDL barrier mechanism prevents concurrent DDL
        // from corrupting the WAL stream by pausing decode when DDL is detected.
        try {
            postgresJdbcContext.query(
                    "SELECT c.oid, n.nspname, c.relname FROM pg_class c "
                            + "JOIN pg_namespace n ON c.relnamespace = n.oid "
                            + "WHERE c.relkind IN ('r','m','p')",
                    rs -> {
                        while (rs.next()) {
                            String schema = rs.getString("nspname");
                            String table = rs.getString("relname");
                            if (isMonitored(schema, table)) {
                                monitoredOidToName.put(rs.getLong("oid"), new MonitoredTable(schema, table));
                            }
                        }
                    });
        } catch (Throwable e) {
            tapLogger.warn("Physical WAL miner could not map monitored tables to OIDs; DDL detection on pg_attribute is disabled: {}", e.getMessage());
            return;
        }
        try {
            long[] node = {0L};
            postgresJdbcContext.queryWithNext(
                    "SELECT pg_relation_filenode('pg_catalog.pg_attribute'::regclass) AS f",
                    rs -> node[0] = rs.getLong("f"));
            pgAttributeRelNode = node[0];
            List<ColumnInfo> columns = new ArrayList<>();
            postgresJdbcContext.query(
                    "SELECT attname, attnum, atttypid, attlen, attalign, attisdropped FROM pg_attribute "
                            + "WHERE attrelid = 'pg_catalog.pg_attribute'::regclass AND attnum > 0 ORDER BY attnum",
                    rs -> {
                        while (rs.next()) {
                            String align = rs.getString("attalign");
                            columns.add(new ColumnInfo(
                                    rs.getString("attname"),
                                    rs.getInt("attnum"),
                                    rs.getLong("atttypid"),
                                    rs.getInt("attlen"),
                                    align == null || align.isEmpty() ? 'c' : align.charAt(0),
                                    rs.getBoolean("attisdropped")));
                        }
                    });
            pgAttributeRel = new RelationInfo("pg_catalog", "pg_attribute", columns, Collections.emptyList(), false);
        } catch (Throwable e) {
            tapLogger.warn("Physical WAL miner could not resolve pg_attribute layout; DDL detection is disabled: {}", e.getMessage());
            pgAttributeRel = null;
            return;
        }
        // Give the catalog its own page overlay so FPI-less pg_attribute UPDATEs
        // (DROP/RENAME/ALTER COLUMN) reconstruct their before/after tuple images
        // from an earlier FPI on the same page, mirroring the user-table cache.
        catalogPageCache = walLevelLogical ? null : new PageStateCache(PAGE_CACHE_CAPACITY);
        catalogDecodeCtx = new HeapRmgrDecoder.Ctx(catalogPageCache, walLevelLogical,
                WAL_DEBUG_ENABLED ? tapLogger::info : null);
        // Baseline column layout for every monitored table; later pg_attribute
        // changes are diffed against this to derive the concrete field DDL.
        try {
            schemaSnapshots.putAll(loadSchemas(monitoredOidToName.keySet()));
        } catch (Throwable e) {
            tapLogger.warn("Physical WAL miner could not load initial column snapshots; the first DDL diff may be incomplete: {}", e.getMessage());
        }
        tapLogger.info("Physical WAL miner DDL watch enabled: pg_attribute relfilenode={}, monitoring {} table OID(s)",
                pgAttributeRelNode, monitoredOidToName.size());
    }

    private boolean isMonitored(String schema, String table) {
        if (withSchema) {
            return allowTables.contains(schema + "." + table);
        }
        return schema.equals(postgresConfig.getSchema()) && allowTables.contains(table);
    }

    /* Consumer-thread inspection of a heap record that did not resolve to a
     * monitored user table: when it targets pg_catalog.pg_attribute and its
     * attrelid is a monitored table's OID, the change is a catalog mutation that
     * accompanies a DDL (ADD/DROP/ALTER COLUMN, DROP TABLE) on the source. The
     * decoded pg_attribute tuple is buffered under the transaction's top-level
     * xid and turned into concrete field DDL on COMMIT (drainDdlRedos +
     * emitDdlEvents), so a DDL is emitted only once its transaction commits and
     * in stream order. The decode always runs (even during the cache-warming
     * prefix) so the catalog page overlay stays current; pre-emit commits drain
     * and discard the buffer at the commit gate. */
    private void detectCatalogDdl(XLogRecord rec, long xid) {
        if (pgAttributeRel == null || pgAttributeRelNode <= 0) {
            return;
        }
        XLogRecord.BlockRef b0 = rec.block(0);
        if (b0 == null || b0.relNumber != pgAttributeRelNode) {
            // A catalog heap record on some other table. Surface insert/multi-insert
            // misses only under WAL debug so a relfilenode mismatch (e.g. pg_attribute
            // rewritten) on an ADD COLUMN can be spotted without flooding the log.
            if (WAL_DEBUG_ENABLED && b0 != null
                    && (rec.heapOp() == XLOG_HEAP_INSERT || rec.heapOp() == XLOG_HEAP2_MULTI_INSERT)) {
                tapLogger.info("[WAL-DEBUG] catalog heap op=0x{} on relNode={} (pg_attribute relNode={}) lsn={}",
                        Integer.toHexString(rec.heapOp()), b0.relNumber, pgAttributeRelNode, lsnStr(rec.lsn));
            }
            return;
        }
        List<NormalRedo> redos;
        try {
            redos = HeapRmgrDecoder.decode(rec, pgAttributeRel, catalogDecodeCtx);
        } catch (RuntimeException ex) {
            tapLogger.warn("Physical WAL miner failed to decode a pg_attribute change at lsn={}: {}",
                    lsnStr(rec.lsn), ex.getMessage());
            return;
        }
        // pg_attribute is written only by DDL, so an always-on summary here is rare
        // and makes a missed ADD/DROP/ALTER COLUMN diagnosable from the log alone.
        tapLogger.info("Physical WAL miner pg_attribute change: rmid={} op=0x{} lsn={} decodedRows={}",
                rec.rmid, Integer.toHexString(rec.heapOp()), lsnStr(rec.lsn), redos.size());
        for (NormalRedo r : redos) {
            Long attrelid = readAttrelid(r);
            boolean monitored = attrelid != null && monitoredOidToName.containsKey(attrelid);
            Integer attnum = readInt(r.getRedoRecord(), "attnum");
            if (attnum == null) {
                attnum = readInt(r.getUndoRecord(), "attnum");
            }
            String attname = readStr(r.getRedoRecord(), "attname");
            if (attname == null) {
                attname = readStr(r.getUndoRecord(), "attname");
            }
            tapLogger.info("Physical WAL miner pg_attribute row: op={} attrelid={} attnum={} attname={} monitored={}",
                    r.getOperation(), attrelid, attnum, attname, monitored);
            if (monitored) {
                // DDL redos are stored ONLY in ddlRedosByXid, NOT in pendingByXid.
                // They should not be emitted as normal DML (pg_attribute changes are
                // internal to PostgreSQL). Instead, they are used to synthesize DDL events.
                ddlRedosByXid.computeIfAbsent(xid, k -> new ArrayList<>()).add(r);

                // Update the in-memory RelationCatalog so that subsequent DML in
                // this same transaction decodes with the correct column layout.
                // Without this, a DML after ADD COLUMN still uses the old column
                // count and produces wrong/missing column data.
                MonitoredTable mt = monitoredOidToName.get(attrelid);
                String op = r.getOperation();
                Long atttypid = readLong(r.getRedoRecord(), "atttypid");
                if (atttypid == null) atttypid = readLong(r.getUndoRecord(), "atttypid");
                Integer attlen = readInt(r.getRedoRecord(), "attlen");
                if (attlen == null) attlen = readInt(r.getUndoRecord(), "attlen");
                String attalignStr = readStr(r.getRedoRecord(), "attalign");
                if (attalignStr == null) attalignStr = readStr(r.getUndoRecord(), "attalign");
                char attalign = (attalignStr != null && !attalignStr.isEmpty()) ? attalignStr.charAt(0) : 'c';
                Boolean attisdropped = readBool(r.getRedoRecord(), "attisdropped");
                if (attisdropped == null) attisdropped = readBool(r.getUndoRecord(), "attisdropped");
                if (attisdropped == null) attisdropped = false;

                if (mt != null && atttypid != null && attlen != null && attnum != null && attnum > 0 && attname != null) {
                    boolean applied = catalog.applyPgAttributeChange(
                            mt.schema, mt.table, op, attnum, attname,
                            atttypid, attlen, attalign, attisdropped);
                    String tableKey = mt.schema + "." + mt.table;
                    if (applied) {
                        // Catalog cache was updated in-place — any stale pending
                        // entries for this table are now superseded.
                        pendingColumnChanges.remove(tableKey);
                    } else {
                        // RelationInfo not cached yet — buffer for when it is loaded.
                        // Keyed by schema.table (not attrelid/relfilenode) because
                        // DDL detection uses pg_attribute.attrelid (pg_class OID)
                        // while DML block refs carry relfilenode, and the two may
                        // differ after TRUNCATE/VACUUM FULL.
                        pendingColumnChanges
                                .computeIfAbsent(tableKey, k -> new ArrayList<>())
                                .add(new RelationCatalog.PgAttributeChange(
                                        op, attnum, attname, atttypid, attlen, attalign, attisdropped));
                    }
                    if (WAL_DEBUG_ENABLED) {
                        tapLogger.info("[WAL-DEBUG] catalog column update applied={} {}.{} op={} attnum={} attname={}",
                                applied, mt.schema, mt.table, op, attnum, attname);
                    }
                } else if (WAL_DEBUG_ENABLED && mt != null && attnum != null && attnum <= 0) {
                    tapLogger.info("[WAL-DEBUG] catalog column update ignored system column {}.{} op={} attnum={} attname={}",
                            mt.schema, mt.table, r.getOperation(), attnum, attname);
                }

                // Mark this transaction as containing DDL - when its COMMIT is seen,
                // reader thread will be blocked until downstream acknowledges schema update
                synchronized (ddlBarrierLock) {
                    ddlPendingXids.add(xid);
                    tapLogger.info("Physical WAL miner marked xid={} as DDL transaction", xid);
                }
            }
        }
    }

    /* attrelid is pg_attribute's first column (the owning relation's OID), decoded
     * by PgTypeDecoder as a Long. Prefer the after-image; fall back to the
     * before-image for deletes. */
    private static Long readAttrelid(NormalRedo r) {
        Long v = readLong(r.getRedoRecord(), "attrelid");
        return v != null ? v : readLong(r.getUndoRecord(), "attrelid");
    }

    /* Turn each buffered pg_attribute change of a committed transaction into a
     * concrete field DDL, in stream order. The operation drives the kind of DDL:
     * INSERT is an added column; UPDATE is a drop (attisdropped), rename, type or
     * nullability change inferred by diffing the tuple's before/after images
     * (with the snapshot as the before-image fallback); DELETE is the per-column
     * removal of a dropped table. The snapshot is advanced after each change so
     * sequential edits to the same column within the transaction classify. */
    private void emitDdlEvents(List<NormalRedo> changes, long commitMillis, List<TapEvent> batch) {
        changes.sort(Comparator.comparingLong(PhysicalWalLogMiner::redoLsn));
        for (NormalRedo r : changes) {
            Long oid = readAttrelid(r);
            if (oid == null) {
                continue;
            }
            MonitoredTable t = monitoredOidToName.get(oid);
            if (t == null) {
                continue;
            }
            Map<Integer, ColSnap> snap = schemaSnapshots.computeIfAbsent(oid, k -> new LinkedHashMap<>());
            String op = r.getOperation();
            if (NormalRedo.OperationEnum.INSERT.name().equals(op)) {
                emitInsert(r.getRedoRecord(), snap, t, commitMillis, batch);
            } else if (NormalRedo.OperationEnum.UPDATE.name().equals(op)) {
                emitUpdate(r.getUndoRecord(), r.getRedoRecord(), snap, t, commitMillis, batch);
            } else if (NormalRedo.OperationEnum.DELETE.name().equals(op)) {
                Integer attnum = readInt(r.getUndoRecord(), "attnum");
                if (attnum != null) {
                    snap.remove(attnum);   // DROP TABLE removes every pg_attribute row
                }
            }
        }
    }

    /* INSERT on pg_attribute == ADD COLUMN: read the new column's metadata
     * straight from the inserted tuple, emit TapNewFieldEvent and record it in
     * the snapshot. System columns (attnum <= 0) are skipped. */
    private void emitInsert(Map<String, Object> after, Map<Integer, ColSnap> snap,
                            MonitoredTable t, long commitMillis, List<TapEvent> batch) {
        Integer attnum = readInt(after, "attnum");
        if (attnum == null || attnum <= 0) {
            return;
        }
        String name = readStr(after, "attname");
        if (name == null) {
            return;
        }
        boolean dropped = Boolean.TRUE.equals(readBool(after, "attisdropped"));
        boolean notNull = Boolean.TRUE.equals(readBool(after, "attnotnull"));
        String type = resolveType(readLong(after, "atttypid"), readInt(after, "atttypmod"));
        if (!dropped) {
            TapTable table = withSchema ? tableMap.get(t.schema + "." + t.table) : tableMap.get(t.table);
            batch.add(stamp(new TapNewFieldEvent().field(new TapField(name, type).nullable(!notNull).pos(table.getMaxPos() + 1)), t, commitMillis));
            tapLogger.info("Physical WAL miner synthesized ADD COLUMN {} {} on {}.{} from a pg_attribute insert.",
                    name, type, t.schema, t.table);
        }
        snap.put(attnum, new ColSnap(name, type, notNull, dropped));
    }

    /* UPDATE on pg_attribute: classify the change by diffing the after-image
     * against the before-image (the WAL old tuple when present, otherwise the
     * snapshot). attisdropped flipping true is a DROP COLUMN; attname change is a
     * rename; atttypid/atttypmod or attnotnull change is an attribute change. An
     * absent after-image (unrecoverable replica delta) is logged and skipped. */
    private void emitUpdate(Map<String, Object> before, Map<String, Object> after,
                            Map<Integer, ColSnap> snap, MonitoredTable t, long commitMillis, List<TapEvent> batch) {
        Integer attnum = readInt(after, "attnum");
        if (attnum == null) {
            attnum = readInt(before, "attnum");
        }
        if (attnum == null || attnum <= 0) {
            return;
        }
        if (after == null || after.get("attname") == null) {
            tapLogger.warn("Physical WAL miner could not decode a pg_attribute update for {}.{} (attnum={}); "
                    + "the after-image is unavailable under wal_level=replica. Set REPLICA IDENTITY FULL or use "
                    + "wal_level=logical for reliable ALTER/DROP/RENAME COLUMN capture.", t.schema, t.table, attnum);
            return;
        }
        ColSnap old = snap.get(attnum);
        String afterName = readStr(after, "attname");
        String afterType = resolveType(readLong(after, "atttypid"), readInt(after, "atttypmod"));
        boolean afterNotNull = Boolean.TRUE.equals(readBool(after, "attnotnull"));
        boolean afterDropped = Boolean.TRUE.equals(readBool(after, "attisdropped"));
        // Before-image: prefer the WAL old tuple, fall back to the snapshot.
        String beforeName = before != null && before.get("attname") != null ? readStr(before, "attname")
                : (old != null ? old.name : null);
        String beforeType = before != null && before.get("atttypid") != null
                ? resolveType(readLong(before, "atttypid"), readInt(before, "atttypmod"))
                : (old != null ? old.dataType : null);
        Boolean beforeNotNull = before != null && before.get("attnotnull") != null ? readBool(before, "attnotnull")
                : (old != null ? old.notNull : null);
        boolean beforeDropped = before != null && before.get("attisdropped") != null
                ? Boolean.TRUE.equals(readBool(before, "attisdropped"))
                : (old != null && old.dropped);

        if (afterDropped && !beforeDropped) {
            String fname = beforeName != null ? beforeName : afterName;
            batch.add(stamp(new TapDropFieldEvent().fieldName(fname), t, commitMillis));
            tapLogger.info("Physical WAL miner synthesized DROP COLUMN {} on {}.{} from a pg_attribute update.",
                    fname, t.schema, t.table);
        } else if (!afterDropped) {
            if (beforeName != null && !beforeName.equals(afterName)) {
                batch.add(stamp(new TapAlterFieldNameEvent().nameChange(ValueChange.create(beforeName, afterName)), t, commitMillis));
                tapLogger.info("Physical WAL miner synthesized RENAME COLUMN {} -> {} on {}.{} from a pg_attribute update.",
                        beforeName, afterName, t.schema, t.table);
            }
            boolean typeChanged = afterType != null && beforeType != null && !beforeType.equals(afterType);
            boolean nullChanged = beforeNotNull != null && beforeNotNull != afterNotNull;
            if (typeChanged || nullChanged) {
                TapAlterFieldAttributesEvent ev = new TapAlterFieldAttributesEvent().fieldName(afterName);
                if (typeChanged) {
                    ev.dataType(ValueChange.create(beforeType, afterType));
                }
                if (nullChanged) {
                    ev.nullable(ValueChange.create(!beforeNotNull, !afterNotNull));
                }
                batch.add(stamp(ev, t, commitMillis));
                tapLogger.info("Physical WAL miner synthesized ALTER COLUMN {} on {}.{} (typeChanged={}, nullChanged={}) from a pg_attribute update.",
                        afterName, t.schema, t.table, typeChanged, nullChanged);
            }
        }
        snap.put(attnum, new ColSnap(afterName, afterType != null ? afterType : beforeType, afterNotNull, afterDropped));
    }

    /* Resolve atttypid + atttypmod to the SQL type name format_type reports
     * (e.g. "character varying(50)"), caching each (typid, typmod) so the lookup
     * runs once. Returns null when the type cannot be resolved. */
    private String resolveType(Long typid, Integer typmod) {
        if (typid == null) {
            return null;
        }
        int mod = typmod == null ? -1 : typmod;
        String key = typid + ":" + mod;
        String cached = typeNameCache.get(key);
        if (cached != null) {
            return cached;
        }
        String[] out = {null};
        try {
            postgresJdbcContext.queryWithNext(
                    "SELECT format_type(" + typid + ", " + mod + ") AS t",
                    rs -> out[0] = rs.getString("t"));
        } catch (Throwable e) {
            return null;
        }
        if (out[0] != null) {
            typeNameCache.put(key, out[0]);
        }
        return out[0];
    }

    private static Integer readInt(Map<String, Object> m, String key) {
        Object v = m == null ? null : m.get(key);
        if (v instanceof Number) {
            return ((Number) v).intValue();
        }
        if (v == null) {
            return null;
        }
        try {
            return Integer.parseInt(v.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long readLong(Map<String, Object> m, String key) {
        Object v = m == null ? null : m.get(key);
        if (v instanceof Number) {
            return ((Number) v).longValue();
        }
        if (v == null) {
            return null;
        }
        try {
            return Long.parseLong(v.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Boolean readBool(Map<String, Object> m, String key) {
        Object v = m == null ? null : m.get(key);
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        if (v instanceof Number) {
            return ((Number) v).intValue() != 0;
        }
        if (v == null) {
            return null;
        }
        return Boolean.parseBoolean(v.toString());
    }

    private static String readStr(Map<String, Object> m, String key) {
        Object v = m == null ? null : m.get(key);
        return v == null ? null : v.toString();
    }

    /* Stamp a freshly built field DDL event with the target table identity and the
     * source commit time, mirroring how toEvent() tags DML so DDL and DML for the
     * same table line up downstream. */
    private <T extends TapBaseEvent> T stamp(T ev, MonitoredTable t, long commitMillis) {
        ev.setTableId(t.table);
        ev.setTime(System.currentTimeMillis());
        ev.setReferenceTime(commitMillis);
        if (withSchema) {
            ev.setNamespaces(Lists.newArrayList(t.schema, t.table));
        }
        return ev;
    }

    /* Load the live column layout (attnum -> ColSnap, in physical order, including
     * dropped tombstones) for a set of relation OIDs in a single pg_attribute
     * query. format_type renders the SQL type with its modifier so type changes
     * diff on the same string the catalog reports. */
    private Map<Long, Map<Integer, ColSnap>> loadSchemas(Collection<Long> oids) throws java.sql.SQLException {
        Map<Long, Map<Integer, ColSnap>> result = new HashMap<>();
        if (oids == null || oids.isEmpty()) {
            return result;
        }
        StringBuilder in = new StringBuilder();
        for (Long oid : oids) {
            if (in.length() > 0) {
                in.append(',');
            }
            in.append(oid);
        }
        postgresJdbcContext.query(
                "SELECT attrelid, attnum, attname, attnotnull, attisdropped, "
                        + "format_type(atttypid, atttypmod) AS data_type FROM pg_attribute "
                        + "WHERE attnum > 0 AND attrelid IN (" + in + ") ORDER BY attrelid, attnum",
                rs -> {
                    while (rs.next()) {
                        long oid = rs.getLong("attrelid");
                        result.computeIfAbsent(oid, k -> new LinkedHashMap<>())
                                .put(rs.getInt("attnum"), new ColSnap(
                                        rs.getString("attname"),
                                        rs.getString("data_type"),
                                        rs.getBoolean("attnotnull"),
                                        rs.getBoolean("attisdropped")));
                    }
                });
        return result;
    }

    /* Schema/table name a monitored OID resolves to, used to tag synthesized DDL. */
    private static final class MonitoredTable {
        final String schema;
        final String table;

        MonitoredTable(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }

        @Override
        public String toString() {
            return schema + "." + table;
        }
    }

    /* Minimal column attributes needed to diff two layouts into field DDL: the
     * name, the format_type-rendered SQL type, the NOT NULL flag, and whether the
     * column is a dropped tombstone (attisdropped). */
    private static final class ColSnap {
        final String name;
        final String dataType;
        final boolean notNull;
        final boolean dropped;

        ColSnap(String name, String dataType, boolean notNull, boolean dropped) {
            this.name = name;
            this.dataType = dataType;
            this.notNull = notNull;
            this.dropped = dropped;
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
        // recovery-aware: a standby cannot run pg_current_wal_flush_lsn(), use the last replayed lsn there.
        // Use flush (not insert) position — same reason as timestampToStreamOffset:
        // pg_current_wal_lsn() is the insert pointer and can be ahead of the flush pointer by
        // up to one WAL page, causing physical START_REPLICATION to fail with "requested starting
        // point is ahead of the WAL flush position".
        ErrorKit.ignoreAnyError(() -> postgresJdbcContext.queryWithNext(
                "SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_flush_lsn() END",
                rs -> lsn[0] = rs.getString(1)));
        return lsn[0];
    }

    /* Best-effort LSN string -> long; 0 on blank/parse failure so callers can
     * treat it as "unknown" without throwing during startup. */
    private static long lsnAsLong(String lsn) {
        try {
            return EmptyKit.isBlank(lsn) ? 0L : LogSequenceNumber.valueOf(lsn).asLong();
        } catch (RuntimeException e) {
            return 0L;
        }
    }

    /* Redo pointer of the latest checkpoint (or, on a standby, the latest
     * restartpoint) from the control file. Replaying WAL from here guarantees we
     * observe every page's first post-checkpoint FPI, which is what seeds the
     * page-state cache for FPI-less UPDATE/DELETE recovery under wal_level=replica.
     * Needs read access to pg_control_checkpoint(); on any failure we return 0 and
     * the caller falls back to starting at the emit position (before-image coverage
     * then improves only after the next natural checkpoint). */
    private long queryCheckpointRedoLsn() {
        String[] lsn = {null};
        ErrorKit.ignoreAnyError(() -> postgresJdbcContext.queryWithNext(
                "SELECT redo_lsn FROM pg_control_checkpoint()", rs -> lsn[0] = rs.getString(1)));
        if (EmptyKit.isBlank(lsn[0])) {
            return 0L;
        }
        try {
            return LogSequenceNumber.valueOf(lsn[0]).asLong();
        } catch (RuntimeException e) {
            return 0L;
        }
    }

    /* Oldest LSN this slot still guarantees readable: PostgreSQL never recycles
     * WAL at or after a slot's restart_lsn. Reading before it fails the stream
     * with "requested WAL segment ... has already been removed". We use it to
     * clamp the warm-up anchor so rewinding to a prior checkpoint redo never
     * requests recycled segments. Returns 0 when the slot has no restart_lsn yet
     * (never streamed) or on any error, so the caller leaves the anchor unclamped. */
    private long querySlotRestartLsn() {
        if (EmptyKit.isBlank(slotName)) {
            return 0L;
        }
        String[] lsn = {null};
        ErrorKit.ignoreAnyError(() -> postgresJdbcContext.query(
                "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = '" + slotName + "'",
                rs -> {
                    if (rs.next()) {
                        lsn[0] = rs.getString(1);
                    }
                }));
        if (EmptyKit.isBlank(lsn[0])) {
            return 0L;
        }
        try {
            return LogSequenceNumber.valueOf(lsn[0]).asLong();
        } catch (RuntimeException e) {
            return 0L;
        }
    }

    /* SHOW wal_level → 'logical' means PostgreSQL already logs full tuples
     * for UPDATE/DELETE without us needing the page cache; we then disable
     * the cache so heap decoding stays a pure function with no extra memory
     * footprint. Any error degrades to "assume not logical" (cache on),
     * which is the safe choice. */
    private boolean queryWalLevelLogical() {
        String[] level = {null};
        ErrorKit.ignoreAnyError(() -> postgresJdbcContext.queryWithNext(
                "SHOW wal_level", rs -> level[0] = rs.getString(1)));
        return level[0] != null && "logical".equalsIgnoreCase(level[0].trim());
    }

    private boolean queryIsInRecovery() {
        boolean[] inRecovery = {false};
        ErrorKit.ignoreAnyError(() -> postgresJdbcContext.queryWithNext(
                "SELECT pg_is_in_recovery()", rs -> inRecovery[0] = rs.getBoolean(1)));
        return inRecovery[0];
    }

    /* Issue a CHECKPOINT against the source so subsequent modifications carry
     * fresh FPIs and the page-state cache can seed itself on every page it
     * encounters. Three failure modes are handled in-place:
     *
     *  - hot standby: CHECKPOINT becomes a restartpoint that writes no WAL,
     *    so we skip and surface a clear warning telling the operator to run
     *    CHECKPOINT on the primary;
     *  - insufficient role (CHECKPOINT requires superuser or
     *    pg_checkpoint role): we log a warning and continue with reduced
     *    before-image coverage until the next natural checkpoint;
     *  - any other SQLException: best-effort, do not abort startup.
     */
    private boolean forceSeedCheckpoint() {
        if (queryIsInRecovery()) {
            tapLogger.warn("Physical WAL miner is connected to a standby (hot recovery); CHECKPOINT here would not "
                    + "emit FPIs into the WAL stream. Run CHECKPOINT on the primary before starting the task to "
                    + "ensure full UPDATE/DELETE before-image recovery, or set REPLICA IDENTITY FULL on the source tables.");
            return false;
        }
        try {
            long t0 = System.currentTimeMillis();
            postgresJdbcContext.execute("CHECKPOINT");
            tapLogger.info("Physical WAL miner: seed CHECKPOINT issued (took {} ms) — subsequent page modifications will carry FPIs",
                    System.currentTimeMillis() - t0);
            return true;
        } catch (Throwable e) {
            tapLogger.warn("Physical WAL miner could not issue seed CHECKPOINT ({}); before-image coverage will "
                    + "improve only after the next natural checkpoint. Grant pg_checkpoint role to the user, or "
                    + "set REPLICA IDENTITY FULL on the source tables to avoid this.", e.getMessage());
            return false;
        }
    }

    private long querySegmentSize() {
        String[] raw = {null};
        ErrorKit.ignoreAnyError(() -> postgresJdbcContext.queryWithNext(
                "SHOW wal_segment_size", rs -> raw[0] = rs.getString(1)));
        long parsed = parseSize(raw[0]);
        return parsed > 0 ? parsed : DEFAULT_WAL_SEGMENT_SIZE;
    }

    static long parseSpillThreshold() {
        String val = System.getenv("TAPDATA_WAL_SPILL_THRESHOLD");
        if (val == null || val.trim().isEmpty()) {
            return 500_000L;
        }
        try {
            long parsed = Long.parseLong(val.trim());
            return parsed > 0 ? parsed : 500_000L;
        } catch (NumberFormatException e) {
            System.err.println("[PhysicalWalLogMiner] Invalid TAPDATA_WAL_SPILL_THRESHOLD='" + val
                    + "', using default 500000: " + e.getMessage());
            return 500_000L;
        }
    }

    static int parseCapacity(String text, int fallback) {
        if (text == null || text.trim().isEmpty()) {
            return fallback;
        }
        try {
            int v = Integer.parseInt(text.trim());
            return v > 0 ? v : fallback;
        } catch (NumberFormatException e) {
            return fallback;
        }
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
