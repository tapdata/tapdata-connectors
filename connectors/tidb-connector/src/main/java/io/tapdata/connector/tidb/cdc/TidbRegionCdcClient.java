package io.tapdata.connector.tidb.cdc;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.FastByteComparisons;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.ChangeDataGrpc;
import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.common.collect.ImmutableSet;
import org.tikv.shade.io.grpc.ManagedChannel;
import org.tikv.shade.io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TidbRegionCdcClient implements AutoCloseable, StreamObserver<Cdcpb.ChangeDataEvent>{
    private static final Logger LOGGER = LoggerFactory.getLogger(TidbRegionCdcClient.class);
    private static final AtomicLong REQ_ID_COUNTER = new AtomicLong(0);
    private static final Set<Cdcpb.Event.LogType> ALLOWED_LOGTYPE =
            ImmutableSet.of(Cdcpb.Event.LogType.PREWRITE, Cdcpb.Event.LogType.COMMIT, Cdcpb.Event.LogType.COMMITTED, Cdcpb.Event.LogType.ROLLBACK);

    private TiRegion region;
    private final Coprocessor.KeyRange keyRange;
    private final Coprocessor.KeyRange regionKeyRange;
    private final ChangeDataGrpc.ChangeDataStub asyncStub;
    private final Consumer<TidbCdcEvent> eventConsumer;
    private final CDCConfig config;
    private final Predicate<Cdcpb.Event.Row> rowFilter;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final boolean started = false;

    private long resolvedTs = 0;

    public TidbRegionCdcClient(
            final TiRegion region,
            final Coprocessor.KeyRange keyRange,
            final ManagedChannel channel,
            final Consumer<TidbCdcEvent> eventConsumer,
            final CDCConfig config) {
        this.region = region;
        this.keyRange = keyRange;
        this.asyncStub = ChangeDataGrpc.newStub(channel);
        this.eventConsumer = eventConsumer;
        this.config = config;
        this.regionKeyRange =
                Coprocessor.KeyRange.newBuilder()
                        .setStart(region.getStartKey())
                        .setEnd(region.getEndKey())
                        .build();

        this.rowFilter =
                regionEnclosed()
                        ? ((row) -> true)
                        : new Predicate<Cdcpb.Event.Row>() {
                    final byte[] buffer = new byte[config.getMaxRowKeySize()];

                    final byte[] start = keyRange.getStart().toByteArray();
                    final byte[] end = keyRange.getEnd().toByteArray();

                    @Override
                    public boolean test(final Cdcpb.Event.Row row) {
                        final int len = row.getKey().size();
                        row.getKey().copyTo(buffer, 0);
                        return (FastByteComparisons.compareTo(
                                buffer, 0, len, start, 0, start.length)
                                >= 0)
                                && (FastByteComparisons.compareTo(
                                buffer, 0, len, end, 0, end.length)
                                < 0);
                    }
                };
    }

    public synchronized void start(final long startTs) {
        Preconditions.checkState(!started, "RegionCDCClient has already started");
        resolvedTs = startTs;
        running.set(true);
        LOGGER.info("start streaming region: {}, running: {}", region.getId(), running.get());
        final Cdcpb.ChangeDataRequest request =
                Cdcpb.ChangeDataRequest.newBuilder()
                        .setRequestId(REQ_ID_COUNTER.incrementAndGet())
                        .setHeader(Cdcpb.Header.newBuilder().setTicdcVersion("5.0.0").build())
                        .setRegionId(region.getId())
                        .setCheckpointTs(startTs)
                        .setStartKey(keyRange.getStart())
                        .setEndKey(keyRange.getEnd())
                        .setRegionEpoch(region.getRegionEpoch())
                        .setExtraOp(config.getExtraOp())
                        .build();
        final StreamObserver<Cdcpb.ChangeDataRequest> requestObserver = asyncStub.eventFeed(this);
        HashMap<String, Object> params = new HashMap<>();
        params.put("requestId", request.getRequestId());
        params.put("header", request.getHeader());
        params.put("regionId", request.getRegionId());
        params.put("checkpointTs", request.getCheckpointTs());
        params.put("startKey", request.getStartKey().toString());
        params.put("endKey", request.getEndKey().toString());
        params.put("regionEpoch", request.getRegionEpoch());
        params.put("extraOp", request.getExtraOp());
        requestObserver.onNext(request);
    }

    public TiRegion getRegion() {
        return region;
    }

    public void setRegion(TiRegion region) {
        this.region = region;
    }

    public Coprocessor.KeyRange getKeyRange() {
        return keyRange;
    }

    public Coprocessor.KeyRange getRegionKeyRange() {
        return regionKeyRange;
    }

    public boolean regionEnclosed() {
        return KeyRangeUtils.makeRange(keyRange.getStart(), keyRange.getEnd())
                .encloses(
                        KeyRangeUtils.makeRange(
                                regionKeyRange.getStart(), regionKeyRange.getEnd()));
    }

    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void close() throws Exception {
        LOGGER.info("close (region: {})", region.getId());
        running.set(false);
        LOGGER.info("terminated (region: {})", region.getId());
    }

    @Override
    public void onCompleted() {
        // should never been called
        onError(new IllegalStateException("RegionCDCClient should never complete"));
    }

    @Override
    public void onError(final Throwable error) {
        onError(error, this.resolvedTs);
    }

    private void onError(final Throwable error, long resolvedTs) {
        LOGGER.error(
                "region CDC error: region: {}, resolvedTs:{}, error: {}",
                region.getId(),
                resolvedTs,
                error);
        running.set(false);
        eventConsumer.accept(TidbCdcEvent.errorResolvedTs(region.getId(), error, resolvedTs));
    }

    @Override
    public void onNext(final Cdcpb.ChangeDataEvent event) {
        try {
            if (running.get()) {
                // fix: miss to process error event
                onErrorEventHandle(event);
                event.getEventsList().stream()
                        .flatMap(ev -> ev.getEntries().getEntriesList().stream())
                        .filter(row -> ALLOWED_LOGTYPE.contains(row.getType()))
                        .filter(this.rowFilter)
                        .map(row -> TidbCdcEvent.rowEvent(region.getId(), row))
                        .forEach(this::submitEvent);

                if (event.hasResolvedTs()) {
                    final Cdcpb.ResolvedTs eventResolvedTs = event.getResolvedTs();
                    this.resolvedTs = eventResolvedTs.getTs();
                    if (eventResolvedTs.getRegionsList().indexOf(region.getId()) >= 0) {
                        submitEvent(TidbCdcEvent.resolvedTsEvent(region.getId(), eventResolvedTs.getTs()));
                    }
                }
            }
        } catch (final Exception e) {
            onError(e, resolvedTs);
        }
    }

    // error event handle
    private void onErrorEventHandle(final Cdcpb.ChangeDataEvent event) {
        List<Cdcpb.Event> errorEvents =
                event.getEventsList().stream()
                        .filter(errEvent -> errEvent.hasError())
                        .collect(Collectors.toList());
        if (errorEvents != null && errorEvents.size() > 0) {
            onError(
                    new RuntimeException(
                            "regionCDC error:" + errorEvents.get(0).getError().toString()),
                    this.resolvedTs);
        }
    }

    private void submitEvent(final TidbCdcEvent event) {
        LOGGER.debug("submit event: {}", event);
        eventConsumer.accept(event);
    }
}
