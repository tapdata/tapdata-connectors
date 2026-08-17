package io.tapdata.connector.paimon.commit;

import io.tapdata.connector.paimon.util.PaimonSpillDirCleaner;
import io.tapdata.entity.event.TapCallbackOffset;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Coordinates per-table micro-batch state and per-source offset barriers without performing I/O.
 *
 * <p>All compound transitions are protected by {@link #lock}. Paimon calls and external offset
 * callbacks are deliberately left to {@link PaimonService}, which can execute them after releasing
 * both its table lock and this coordinator lock.
 */
public final class PaimonMicroBatchCoordinator {

    private final Object lock = new Object();
    private final int batchAccumulationSize;
    private final long commitIntervalMs;
    private final Map<String, MutableTableState> tables = new LinkedHashMap<>();
    private final Map<String, LaneOffsetState> lanes = new LinkedHashMap<>();
    private long nextCommitTargetId;
    private long nextCallbackToken;

    public PaimonMicroBatchCoordinator(int batchAccumulationSize, long commitIntervalMs) {
        this.batchAccumulationSize = batchAccumulationSize;
        this.commitIntervalMs = commitIntervalMs;
    }

    public void acceptInitial(String tableKey, int recordCount) {
        requireNonBlank(tableKey, "Table key");
        requireRecordCount(recordCount);
        synchronized (lock) {
            table(tableKey).bufferedRecordCount += recordCount;
        }
    }

    public BatchDecision acceptCdc(
            String tableKey, int recordCount, Set<String> sourceLanes, long nowMs) {
        requireNonBlank(tableKey, "Table key");
        requireRecordCount(recordCount);
        if (recordCount == 0) {
            return new BatchDecision(0L, 0L, false, false);
        }
        if (sourceLanes == null || sourceLanes.isEmpty()) {
            throw new IllegalArgumentException("CDC batch must contain at least one source lane");
        }
        for (String sourceLane : sourceLanes) {
            requireNonBlank(sourceLane, "Source lane");
        }

        synchronized (lock) {
            MutableTableState state = table(tableKey);
            state.bufferedRecordCount += recordCount;
            state.accumulatedRecordCount += recordCount;
            state.acceptedGeneration++;
            state.cdcEligible = true;
            if (state.commitIntervalBaseTimeMs == null) {
                state.commitIntervalBaseTimeMs = nowMs;
            }
            for (String sourceLane : sourceLanes) {
                state.lastAcceptedGenerationBySource.put(
                        sourceLane, state.acceptedGeneration);
            }

            boolean bySize =
                    batchAccumulationSize <= 0
                            || state.accumulatedRecordCount >= batchAccumulationSize;
            boolean byTime =
                    commitIntervalMs > 0
                            && nowMs - state.commitIntervalBaseTimeMs >= commitIntervalMs;
            return new BatchDecision(
                    state.acceptedGeneration,
                    state.accumulatedRecordCount,
                    bySize,
                    byTime);
        }
    }

    public CommitTarget captureCommitTarget(String tableKey) {
        requireNonBlank(tableKey, "Table key");
        synchronized (lock) {
            MutableTableState state = table(tableKey);
            return new CommitTarget(
                    ++nextCommitTargetId,
                    tableKey,
                    state.acceptedGeneration,
                    state.bufferedRecordCount,
                    state.accumulatedRecordCount);
        }
    }

    public void markPendingCommit(CommitTarget target) {
        requireCommitTarget(target);
        synchronized (lock) {
            MutableTableState state = table(target.tableKey);
            if (state.pendingCommitTarget != null
                    && state.pendingCommitTarget.id != target.id) {
                throw new IllegalStateException(
                        "A different pending commit target already exists for " + target.tableKey);
            }
            state.pendingCommitTarget = target;
        }
    }

    public CommitTarget pendingCommitTarget(String tableKey) {
        requireNonBlank(tableKey, "Table key");
        synchronized (lock) {
            MutableTableState state = tables.get(tableKey);
            return state == null ? null : state.pendingCommitTarget;
        }
    }

    public List<CallbackReservation> publishCommit(CommitTarget target, long completedAtMs) {
        requireCommitTarget(target);
        synchronized (lock) {
            MutableTableState state = table(target.tableKey);
            state.committedGeneration =
                    Math.max(state.committedGeneration, target.acceptedGeneration);
            state.bufferedRecordCount =
                    Math.max(0L, state.bufferedRecordCount - target.bufferedRecordCount);
            state.accumulatedRecordCount =
                    Math.max(0L, state.accumulatedRecordCount - target.accumulatedRecordCount);
            state.commitIntervalBaseTimeMs = completedAtMs;
            if (state.pendingCommitTarget != null
                    && state.pendingCommitTarget.id == target.id) {
                state.pendingCommitTarget = null;
            }
            return reserveAllReadyLocked();
        }
    }

    public TableSnapshot tableSnapshot(String tableKey) {
        requireNonBlank(tableKey, "Table key");
        synchronized (lock) {
            MutableTableState state = table(tableKey);
            return new TableSnapshot(state);
        }
    }

    public OptionalLong nextDeadlineMs() {
        return nextDeadlineMs(Collections.emptySet());
    }

    public OptionalLong nextDeadlineMs(Set<String> excludedTables) {
        synchronized (lock) {
            if (commitIntervalMs <= 0) {
                return OptionalLong.empty();
            }
            Set<String> excluded =
                    excludedTables == null ? Collections.emptySet() : excludedTables;
            long nearest = Long.MAX_VALUE;
            boolean found = false;
            for (Map.Entry<String, MutableTableState> entry : tables.entrySet()) {
                if (excluded.contains(entry.getKey())) {
                    continue;
                }
                MutableTableState state = entry.getValue();
                if (!schedulerEligible(state) || state.commitIntervalBaseTimeMs == null) {
                    continue;
                }
                found = true;
                nearest = Math.min(nearest, deadlineMsLocked(state));
            }
            return found ? OptionalLong.of(nearest) : OptionalLong.empty();
        }
    }

    public List<String> dueTables(long nowMs) {
        return dueTables(nowMs, Collections.emptySet(), Integer.MAX_VALUE);
    }

    public List<String> dueTables(
            long nowMs, Set<String> excludedTables, int limit) {
        synchronized (lock) {
            if (commitIntervalMs <= 0 || limit <= 0) {
                return Collections.emptyList();
            }
            Set<String> excluded =
                    excludedTables == null ? Collections.emptySet() : excludedTables;
            List<ScheduledTable> due = new ArrayList<>();
            for (Map.Entry<String, MutableTableState> entry : tables.entrySet()) {
                if (excluded.contains(entry.getKey())) {
                    continue;
                }
                MutableTableState state = entry.getValue();
                if (isDueLocked(state, nowMs)) {
                    due.add(new ScheduledTable(entry.getKey(), deadlineMsLocked(state)));
                }
            }
            due.sort(
                    Comparator.comparingLong((ScheduledTable table) -> table.deadlineMs)
                            .thenComparing(table -> table.tableKey));
            List<String> selected = new ArrayList<>(Math.min(limit, due.size()));
            for (int index = 0; index < due.size() && index < limit; index++) {
                selected.add(due.get(index).tableKey);
            }
            return selected;
        }
    }

    public boolean isDue(String tableKey, long nowMs) {
        requireNonBlank(tableKey, "Table key");
        synchronized (lock) {
            MutableTableState state = tables.get(tableKey);
            return state != null && isDueLocked(state, nowMs);
        }
    }

    public void clearWriterDerivedStateAfterDdl(String tableKey) {
        requireNonBlank(tableKey, "Table key");
        synchronized (lock) {
            MutableTableState state = table(tableKey);
            if (state.bufferedRecordCount != 0 || state.pendingCommitTarget != null) {
                throw new IllegalStateException(
                        "Cannot clear writer state before draining table " + tableKey);
            }
            state.accumulatedRecordCount = 0L;
            state.commitIntervalBaseTimeMs = null;
            state.cdcEligible = false;
        }
    }

    public CallbackReservation registerHeartbeat(String sourceLane, TapCallbackOffset payload) {
        requireNonBlank(sourceLane, "Source lane");
        if (payload == null) {
            throw new IllegalArgumentException("Heartbeat payload must not be null");
        }
        synchronized (lock) {
            LaneOffsetState lane = lanes.computeIfAbsent(sourceLane, ignored -> new LaneOffsetState());
            Map<String, Long> required = new LinkedHashMap<>();
            for (Map.Entry<String, MutableTableState> entry : tables.entrySet()) {
                Long generation =
                        entry.getValue().lastAcceptedGenerationBySource.get(sourceLane);
                if (generation != null) {
                    required.put(entry.getKey(), generation);
                }
            }
            lane.pending =
                    new HeartbeatState(
                            sourceLane,
                            ++lane.lastVersion,
                            copyOffset(payload),
                            required);
            return reserveIfReadyLocked(lane);
        }
    }

    public CallbackReservation completeCallback(CallbackReservation reservation) {
        if (reservation == null) {
            throw new IllegalArgumentException("Callback reservation must not be null");
        }
        synchronized (lock) {
            LaneOffsetState lane = lanes.get(reservation.sourceLane);
            if (!matches(lane, reservation)) {
                return null;
            }
            lane.inFlight = null;
            return reserveIfReadyLocked(lane);
        }
    }

    public void failCallback(CallbackReservation reservation) {
        if (reservation == null) {
            throw new IllegalArgumentException("Callback reservation must not be null");
        }
        synchronized (lock) {
            LaneOffsetState lane = lanes.get(reservation.sourceLane);
            if (!matches(lane, reservation)) {
                throw new IllegalStateException("Callback reservation is no longer in flight");
            }
            // Deliberately retain both inFlight and the newest pending heartbeat for diagnosis.
        }
    }

    public boolean markConsumerStarted(CallbackReservation reservation) {
        if (reservation == null) {
            return false;
        }
        synchronized (lock) {
            LaneOffsetState lane = lanes.get(reservation.sourceLane);
            if (!matches(lane, reservation)) {
                return false;
            }
            lane.inFlight.consumerStarted = true;
            return true;
        }
    }

    public boolean consumerStarted(CallbackReservation reservation) {
        if (reservation == null) {
            return false;
        }
        synchronized (lock) {
            LaneOffsetState lane = lanes.get(reservation.sourceLane);
            return matches(lane, reservation) && lane.inFlight.consumerStarted;
        }
    }

    public boolean hasInFlight(String sourceLane) {
        synchronized (lock) {
            LaneOffsetState lane = lanes.get(sourceLane);
            return lane != null && lane.inFlight != null;
        }
    }

    public boolean hasPending(String sourceLane) {
        synchronized (lock) {
            LaneOffsetState lane = lanes.get(sourceLane);
            return lane != null && lane.pending != null;
        }
    }

    public List<CallbackReservation> reserveReadyCallbacks() {
        synchronized (lock) {
            return reserveAllReadyLocked();
        }
    }

    public List<CallbackReservation> reservedButNotStartedCallbacks() {
        synchronized (lock) {
            List<CallbackReservation> reservations = new ArrayList<>();
            for (LaneOffsetState lane : lanes.values()) {
                if (lane.inFlight != null && !lane.inFlight.consumerStarted) {
                    reservations.add(new CallbackReservation(lane.inFlight));
                }
            }
            return reservations;
        }
    }

    public void clear() {
        synchronized (lock) {
            tables.clear();
            lanes.clear();
        }
    }

    private List<CallbackReservation> reserveAllReadyLocked() {
        List<CallbackReservation> ready = new ArrayList<>();
        for (LaneOffsetState lane : lanes.values()) {
            CallbackReservation reservation = reserveIfReadyLocked(lane);
            if (reservation != null) {
                ready.add(reservation);
            }
        }
        return ready;
    }

    private CallbackReservation reserveIfReadyLocked(LaneOffsetState lane) {
        if (lane.inFlight != null || lane.pending == null || !isReadyLocked(lane.pending)) {
            return null;
        }
        lane.inFlight =
                new InFlightHeartbeat(lane.pending, ++nextCallbackToken);
        lane.pending = null;
        return new CallbackReservation(lane.inFlight);
    }

    private boolean isReadyLocked(HeartbeatState heartbeat) {
        for (Map.Entry<String, Long> dependency :
                heartbeat.requiredGenerationByTable.entrySet()) {
            MutableTableState table = tables.get(dependency.getKey());
            if (table == null || table.committedGeneration < dependency.getValue()) {
                return false;
            }
        }
        return true;
    }

    private boolean schedulerEligible(MutableTableState state) {
        return state.cdcEligible
                && (state.accumulatedRecordCount > 0
                        || (state.pendingCommitTarget != null
                                && state.pendingCommitTarget.accumulatedRecordCount > 0));
    }

    private boolean isDueLocked(MutableTableState state, long nowMs) {
        return commitIntervalMs > 0
                && schedulerEligible(state)
                && state.commitIntervalBaseTimeMs != null
                && nowMs >= deadlineMsLocked(state);
    }

    private long deadlineMsLocked(MutableTableState state) {
        return PaimonSpillDirCleaner.saturatedAdd(
                state.commitIntervalBaseTimeMs, commitIntervalMs);
    }

    private MutableTableState table(String tableKey) {
        return tables.computeIfAbsent(tableKey, ignored -> new MutableTableState());
    }

    private static final class ScheduledTable {
        private final String tableKey;
        private final long deadlineMs;

        private ScheduledTable(String tableKey, long deadlineMs) {
            this.tableKey = tableKey;
            this.deadlineMs = deadlineMs;
        }
    }

    private static boolean matches(
            LaneOffsetState lane, CallbackReservation reservation) {
        return lane != null
                && lane.inFlight != null
                && lane.inFlight.token == reservation.token
                && lane.inFlight.heartbeat.version == reservation.version;
    }

    private static void requireNonBlank(String value, String label) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(label + " must not be blank");
        }
    }

    private static void requireRecordCount(int recordCount) {
        if (recordCount < 0) {
            throw new IllegalArgumentException("Record count must not be negative");
        }
    }

    private static void requireCommitTarget(CommitTarget target) {
        if (target == null) {
            throw new IllegalArgumentException("Commit target must not be null");
        }
    }

    private static TapCallbackOffset copyOffset(TapCallbackOffset source) {
        TapCallbackOffset copy = new TapCallbackOffset();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            copy.put(entry.getKey(), copyValue(entry.getValue()));
        }
        return copy;
    }

    private static Object copyValue(Object value) {
        if (value instanceof Map) {
            Map<Object, Object> copy = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                copy.put(copyValue(entry.getKey()), copyValue(entry.getValue()));
            }
            return copy;
        }
        if (value instanceof List) {
            List<Object> copy = new ArrayList<>(((List<?>) value).size());
            for (Object item : (List<?>) value) {
                copy.add(copyValue(item));
            }
            return copy;
        }
        if (value instanceof Set) {
            Set<Object> copy = new LinkedHashSet<>();
            for (Object item : (Set<?>) value) {
                copy.add(copyValue(item));
            }
            return copy;
        }
        if (value instanceof Collection) {
            List<Object> copy = new ArrayList<>();
            for (Object item : (Collection<?>) value) {
                copy.add(copyValue(item));
            }
            return copy;
        }
        if (value != null && value.getClass().isArray()) {
            int length = Array.getLength(value);
            Object copy = Array.newInstance(value.getClass().getComponentType(), length);
            for (int index = 0; index < length; index++) {
                Array.set(copy, index, copyValue(Array.get(value, index)));
            }
            return copy;
        }
        return value;
    }

    public static final class BatchDecision {
        private final long acceptedGeneration;
        private final long accumulatedRecordCount;
        private final boolean shouldCommitBySize;
        private final boolean shouldCommitByTime;

        private BatchDecision(
                long acceptedGeneration,
                long accumulatedRecordCount,
                boolean shouldCommitBySize,
                boolean shouldCommitByTime) {
            this.acceptedGeneration = acceptedGeneration;
            this.accumulatedRecordCount = accumulatedRecordCount;
            this.shouldCommitBySize = shouldCommitBySize;
            this.shouldCommitByTime = shouldCommitByTime;
        }

        public long acceptedGeneration() {
            return acceptedGeneration;
        }

        public long accumulatedRecordCount() {
            return accumulatedRecordCount;
        }

        public boolean shouldCommitBySize() {
            return shouldCommitBySize;
        }

        public boolean shouldCommitByTime() {
            return shouldCommitByTime;
        }

        public boolean shouldCommit() {
            return shouldCommitBySize || shouldCommitByTime;
        }
    }

    public static final class CommitTarget {
        private final long id;
        private final String tableKey;
        private final long acceptedGeneration;
        private final long bufferedRecordCount;
        private final long accumulatedRecordCount;

        private CommitTarget(
                long id,
                String tableKey,
                long acceptedGeneration,
                long bufferedRecordCount,
                long accumulatedRecordCount) {
            this.id = id;
            this.tableKey = tableKey;
            this.acceptedGeneration = acceptedGeneration;
            this.bufferedRecordCount = bufferedRecordCount;
            this.accumulatedRecordCount = accumulatedRecordCount;
        }

        public String tableKey() {
            return tableKey;
        }

        public long acceptedGeneration() {
            return acceptedGeneration;
        }

        public long bufferedRecordCount() {
            return bufferedRecordCount;
        }

        public long accumulatedRecordCount() {
            return accumulatedRecordCount;
        }
    }

    public static final class TableSnapshot {
        private final long bufferedRecordCount;
        private final long accumulatedRecordCount;
        private final Long commitIntervalBaseTimeMs;
        private final long acceptedGeneration;
        private final long committedGeneration;
        private final boolean cdcEligible;
        private final boolean pendingCommit;
        private final Map<String, Long> lastAcceptedGenerationBySource;

        private TableSnapshot(MutableTableState state) {
            this.bufferedRecordCount = state.bufferedRecordCount;
            this.accumulatedRecordCount = state.accumulatedRecordCount;
            this.commitIntervalBaseTimeMs = state.commitIntervalBaseTimeMs;
            this.acceptedGeneration = state.acceptedGeneration;
            this.committedGeneration = state.committedGeneration;
            this.cdcEligible = state.cdcEligible;
            this.pendingCommit = state.pendingCommitTarget != null;
            this.lastAcceptedGenerationBySource =
                    Collections.unmodifiableMap(
                            new LinkedHashMap<>(state.lastAcceptedGenerationBySource));
        }

        public long bufferedRecordCount() {
            return bufferedRecordCount;
        }

        public long accumulatedRecordCount() {
            return accumulatedRecordCount;
        }

        public Long commitIntervalBaseTimeMs() {
            return commitIntervalBaseTimeMs;
        }

        public long acceptedGeneration() {
            return acceptedGeneration;
        }

        public long committedGeneration() {
            return committedGeneration;
        }

        public boolean cdcEligible() {
            return cdcEligible;
        }

        public boolean hasPendingCommit() {
            return pendingCommit;
        }

        public long lastAcceptedGeneration(String sourceLane) {
            Long value = lastAcceptedGenerationBySource.get(sourceLane);
            return value == null ? 0L : value;
        }
    }

    public static final class CallbackReservation {
        private final String sourceLane;
        private final long version;
        private final long token;
        private final TapCallbackOffset payload;
        private final Map<String, Long> requiredGenerationByTable;

        private CallbackReservation(InFlightHeartbeat inFlight) {
            this.sourceLane = inFlight.heartbeat.sourceLane;
            this.version = inFlight.heartbeat.version;
            this.token = inFlight.token;
            this.payload = copyOffset(inFlight.heartbeat.payload);
            this.requiredGenerationByTable =
                    Collections.unmodifiableMap(
                            new LinkedHashMap<>(
                                    inFlight.heartbeat.requiredGenerationByTable));
        }

        public String sourceLane() {
            return sourceLane;
        }

        public long version() {
            return version;
        }

        public long token() {
            return token;
        }

        public TapCallbackOffset payload() {
            return copyOffset(payload);
        }

        public Map<String, Long> requiredGenerationByTable() {
            return requiredGenerationByTable;
        }
    }

    private static final class MutableTableState {
        private long bufferedRecordCount;
        private long accumulatedRecordCount;
        private Long commitIntervalBaseTimeMs;
        private long acceptedGeneration;
        private long committedGeneration;
        private CommitTarget pendingCommitTarget;
        private boolean cdcEligible;
        private final Map<String, Long> lastAcceptedGenerationBySource =
                new LinkedHashMap<>();
    }

    private static final class HeartbeatState {
        private final String sourceLane;
        private final long version;
        private final TapCallbackOffset payload;
        private final Map<String, Long> requiredGenerationByTable;

        private HeartbeatState(
                String sourceLane,
                long version,
                TapCallbackOffset payload,
                Map<String, Long> requiredGenerationByTable) {
            this.sourceLane = sourceLane;
            this.version = version;
            this.payload = payload;
            this.requiredGenerationByTable =
                    Collections.unmodifiableMap(new LinkedHashMap<>(requiredGenerationByTable));
        }
    }

    private static final class InFlightHeartbeat {
        private final HeartbeatState heartbeat;
        private final long token;
        private boolean consumerStarted;

        private InFlightHeartbeat(HeartbeatState heartbeat, long token) {
            this.heartbeat = heartbeat;
            this.token = token;
        }
    }

    private static final class LaneOffsetState {
        private long lastVersion;
        private HeartbeatState pending;
        private InFlightHeartbeat inFlight;
    }
}
