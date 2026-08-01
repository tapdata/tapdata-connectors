package io.tapdata.connector.paimon.service;

import io.tapdata.entity.event.TapCallbackOffset;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
final class PaimonMicroBatchCoordinator {

    private final Object lock = new Object();
    private final int batchAccumulationSize;
    private final long commitIntervalMs;
    private final Map<String, MutableTableState> tables = new LinkedHashMap<>();
    private final Map<String, LaneOffsetState> lanes = new LinkedHashMap<>();
    private long nextCommitTargetId;
    private long nextCallbackToken;

    PaimonMicroBatchCoordinator(int batchAccumulationSize, long commitIntervalMs) {
        this.batchAccumulationSize = batchAccumulationSize;
        this.commitIntervalMs = commitIntervalMs;
    }

    void acceptInitial(String tableKey, int recordCount) {
        requireTableKey(tableKey);
        requireRecordCount(recordCount);
        synchronized (lock) {
            table(tableKey).bufferedRecordCount += recordCount;
        }
    }

    BatchDecision acceptCdc(
            String tableKey, int recordCount, Set<String> sourceLanes, long nowMs) {
        requireTableKey(tableKey);
        requireRecordCount(recordCount);
        if (recordCount == 0) {
            return new BatchDecision(0L, 0L, false, false);
        }
        if (sourceLanes == null || sourceLanes.isEmpty()) {
            throw new IllegalArgumentException("CDC batch must contain at least one source lane");
        }
        for (String sourceLane : sourceLanes) {
            requireSourceLane(sourceLane);
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

    CommitTarget captureCommitTarget(String tableKey) {
        requireTableKey(tableKey);
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

    void markPendingCommit(CommitTarget target) {
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

    CommitTarget pendingCommitTarget(String tableKey) {
        requireTableKey(tableKey);
        synchronized (lock) {
            MutableTableState state = tables.get(tableKey);
            return state == null ? null : state.pendingCommitTarget;
        }
    }

    List<CallbackReservation> publishCommit(CommitTarget target, long completedAtMs) {
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

    TableSnapshot tableSnapshot(String tableKey) {
        requireTableKey(tableKey);
        synchronized (lock) {
            MutableTableState state = table(tableKey);
            return new TableSnapshot(state);
        }
    }

    OptionalLong nextDeadlineMs() {
        synchronized (lock) {
            if (commitIntervalMs <= 0) {
                return OptionalLong.empty();
            }
            long nearest = Long.MAX_VALUE;
            for (MutableTableState state : tables.values()) {
                if (!schedulerEligible(state) || state.commitIntervalBaseTimeMs == null) {
                    continue;
                }
                nearest =
                        Math.min(
                                nearest,
                                saturatedAdd(state.commitIntervalBaseTimeMs, commitIntervalMs));
            }
            return nearest == Long.MAX_VALUE
                    ? OptionalLong.empty()
                    : OptionalLong.of(nearest);
        }
    }

    List<String> dueTables(long nowMs) {
        synchronized (lock) {
            if (commitIntervalMs <= 0) {
                return Collections.emptyList();
            }
            List<String> due = new ArrayList<>();
            for (Map.Entry<String, MutableTableState> entry : tables.entrySet()) {
                if (isDueLocked(entry.getValue(), nowMs)) {
                    due.add(entry.getKey());
                }
            }
            return due;
        }
    }

    boolean isDue(String tableKey, long nowMs) {
        requireTableKey(tableKey);
        synchronized (lock) {
            MutableTableState state = tables.get(tableKey);
            return state != null && isDueLocked(state, nowMs);
        }
    }

    boolean hasSchedulerWork() {
        return nextDeadlineMs().isPresent();
    }

    void clearWriterDerivedStateAfterDdl(String tableKey) {
        requireTableKey(tableKey);
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

    CallbackReservation registerHeartbeat(String sourceLane, TapCallbackOffset payload) {
        requireSourceLane(sourceLane);
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

    CallbackReservation completeCallback(CallbackReservation reservation) {
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

    void failCallback(CallbackReservation reservation) {
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

    boolean markConsumerStarted(CallbackReservation reservation) {
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

    boolean consumerStarted(CallbackReservation reservation) {
        if (reservation == null) {
            return false;
        }
        synchronized (lock) {
            LaneOffsetState lane = lanes.get(reservation.sourceLane);
            return matches(lane, reservation) && lane.inFlight.consumerStarted;
        }
    }

    boolean hasInFlight(String sourceLane) {
        synchronized (lock) {
            LaneOffsetState lane = lanes.get(sourceLane);
            return lane != null && lane.inFlight != null;
        }
    }

    boolean hasPending(String sourceLane) {
        synchronized (lock) {
            LaneOffsetState lane = lanes.get(sourceLane);
            return lane != null && lane.pending != null;
        }
    }

    List<CallbackReservation> reserveReadyCallbacks() {
        synchronized (lock) {
            return reserveAllReadyLocked();
        }
    }

    List<CallbackReservation> reservedButNotStartedCallbacks() {
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

    void clear() {
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
                && nowMs >= saturatedAdd(state.commitIntervalBaseTimeMs, commitIntervalMs);
    }

    private MutableTableState table(String tableKey) {
        return tables.computeIfAbsent(tableKey, ignored -> new MutableTableState());
    }

    private static boolean matches(
            LaneOffsetState lane, CallbackReservation reservation) {
        return lane != null
                && lane.inFlight != null
                && lane.inFlight.token == reservation.token
                && lane.inFlight.heartbeat.version == reservation.version;
    }

    private static long saturatedAdd(long left, long right) {
        if (right > 0 && left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private static void requireTableKey(String tableKey) {
        if (tableKey == null || tableKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Table key must not be blank");
        }
    }

    private static void requireSourceLane(String sourceLane) {
        if (sourceLane == null || sourceLane.trim().isEmpty()) {
            throw new IllegalArgumentException("Source lane must not be blank");
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

    static final class BatchDecision {
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

        long acceptedGeneration() {
            return acceptedGeneration;
        }

        long accumulatedRecordCount() {
            return accumulatedRecordCount;
        }

        boolean shouldCommitBySize() {
            return shouldCommitBySize;
        }

        boolean shouldCommitByTime() {
            return shouldCommitByTime;
        }

        boolean shouldCommit() {
            return shouldCommitBySize || shouldCommitByTime;
        }
    }

    static final class CommitTarget {
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

        String tableKey() {
            return tableKey;
        }

        long acceptedGeneration() {
            return acceptedGeneration;
        }

        long bufferedRecordCount() {
            return bufferedRecordCount;
        }

        long accumulatedRecordCount() {
            return accumulatedRecordCount;
        }
    }

    static final class TableSnapshot {
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

        long bufferedRecordCount() {
            return bufferedRecordCount;
        }

        long accumulatedRecordCount() {
            return accumulatedRecordCount;
        }

        Long commitIntervalBaseTimeMs() {
            return commitIntervalBaseTimeMs;
        }

        long acceptedGeneration() {
            return acceptedGeneration;
        }

        long committedGeneration() {
            return committedGeneration;
        }

        boolean cdcEligible() {
            return cdcEligible;
        }

        boolean hasPendingCommit() {
            return pendingCommit;
        }

        long lastAcceptedGeneration(String sourceLane) {
            Long value = lastAcceptedGenerationBySource.get(sourceLane);
            return value == null ? 0L : value;
        }
    }

    static final class CallbackReservation {
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

        String sourceLane() {
            return sourceLane;
        }

        long version() {
            return version;
        }

        long token() {
            return token;
        }

        TapCallbackOffset payload() {
            return copyOffset(payload);
        }

        Map<String, Long> requiredGenerationByTable() {
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
