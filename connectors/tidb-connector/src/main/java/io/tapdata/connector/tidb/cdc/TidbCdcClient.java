package io.tapdata.connector.tidb.cdc;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCClient;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.RangeSplitter;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.common.base.Preconditions;
import org.tikv.shade.com.google.common.collect.Range;
import org.tikv.shade.com.google.common.collect.TreeMultiset;
import org.tikv.shade.io.grpc.ManagedChannel;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class TidbCdcClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(CDCClient.class);

    private final TiSession session;
    private final Coprocessor.KeyRange keyRange;
    private final CDCConfig config;

    private final BlockingQueue<TidbCdcEvent> eventsBuffer;
    private final ConcurrentHashMap<Long, TidbRegionCdcClient> regionClients =
            new ConcurrentHashMap<>();
    private final Map<Long, Long> regionToResolvedTs = new HashMap<>();
    private final TreeMultiset<Long> resolvedTsSet = TreeMultiset.create();

    private boolean started = false;

    private Consumer<TidbCdcEvent> eventConsumer;

    private static final int TRY_TIMES = 3;

    public TidbCdcClient(final TiSession session, final Coprocessor.KeyRange keyRange) {
        this(session, keyRange, new CDCConfig());
    }

    public TidbCdcClient(final TiSession session, final Coprocessor.KeyRange keyRange, final CDCConfig config) {
        Preconditions.checkState(
                session.getConf().getIsolationLevel().equals(Kvrpcpb.IsolationLevel.SI),
                "Unsupported Isolation Level");
        this.session = session;
        this.keyRange = keyRange;
        this.config = config;
        eventsBuffer = new LinkedBlockingQueue<>(config.getEventBufferSize());
        eventConsumer =
                (event) -> {
                    for (int i = 0; i <TRY_TIMES; i++) {
                        if (eventsBuffer.offer(event)) {
                            return;
                        }
                    }
                    try {
                        eventsBuffer.put(event);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        e.printStackTrace();
                    }
                };
    }

    public synchronized void start(final long startTs) {
        Preconditions.checkState(!started, "Client is already started");
        applyKeyRange(keyRange, startTs);
        started = true;
    }

    public synchronized Cdcpb.Event.Row get() throws InterruptedException {
        final TidbCdcEvent event = eventsBuffer.poll();
        if (event != null) {
            switch (event.eventType) {
                case ROW:
                    return event.row;
                case RESOLVED_TS:
                    handleResolvedTs(event.regionId, event.resolvedTs);
                    break;
                case ERROR:
                    handleErrorEvent(event.regionId, event.error, event.resolvedTs);
                    break;
            }
        }
        return null;
    }

    public synchronized long getMinResolvedTs() {
        return resolvedTsSet.firstEntry().getElement();
    }

    public synchronized long getMaxResolvedTs() {
        return resolvedTsSet.lastEntry().getElement();
    }

    public synchronized void close() {
        removeRegions(regionClients.keySet());
    }

    private synchronized void applyKeyRange(final Coprocessor.KeyRange keyRange, final long timestamp) {
        final RangeSplitter splitter = RangeSplitter.newSplitter(session.getRegionManager());

        final Iterator<TiRegion> newRegionsIterator =
                splitter.splitRangeByRegion(Arrays.asList(keyRange)).stream()
                        .map(RangeSplitter.RegionTask::getRegion)
                        .sorted((a, b) -> Long.compare(a.getId(), b.getId()))
                        .iterator();
        final Iterator<TidbRegionCdcClient> oldRegionsIterator = regionClients.values().iterator();

        final ArrayList<TiRegion> regionsToAdd = new ArrayList<>();
        final ArrayList<Long> regionsToRemove = new ArrayList<>();

        TiRegion newRegion = newRegionsIterator.hasNext() ? newRegionsIterator.next() : null;
        TidbRegionCdcClient oldRegionClient =oldRegionsIterator.hasNext() ? oldRegionsIterator.next() : null;

        while (newRegion != null && oldRegionClient != null) {
            if (newRegion.getId() == oldRegionClient.getRegion().getId()) {
                // check if should refresh region
                if (!oldRegionClient.isRunning()) {
                    regionsToRemove.add(newRegion.getId());
                    regionsToAdd.add(newRegion);
                }

                newRegion = newRegionsIterator.hasNext() ? newRegionsIterator.next() : null;
                oldRegionClient = oldRegionsIterator.hasNext() ? oldRegionsIterator.next() : null;
            } else if (newRegion.getId() < oldRegionClient.getRegion().getId()) {
                regionsToAdd.add(newRegion);
                newRegion = newRegionsIterator.hasNext() ? newRegionsIterator.next() : null;
            } else {
                regionsToRemove.add(oldRegionClient.getRegion().getId());
                oldRegionClient = oldRegionsIterator.hasNext() ? oldRegionsIterator.next() : null;
            }
        }

        while (newRegion != null) {
            regionsToAdd.add(newRegion);
            newRegion = newRegionsIterator.hasNext() ? newRegionsIterator.next() : null;
        }

        while (oldRegionClient != null) {
            regionsToRemove.add(oldRegionClient.getRegion().getId());
            oldRegionClient = oldRegionsIterator.hasNext() ? oldRegionsIterator.next() : null;
        }

        removeRegions(regionsToRemove);
        addRegions(regionsToAdd, timestamp);
        LOGGER.info("keyRange applied");
    }

    private synchronized void addRegions(final Iterable<TiRegion> regions, final long timestamp) {
        LOGGER.info("add regions: {}, timestamp: {}", regions, timestamp);
        for (final TiRegion region : regions) {
            if (overlapWithRegion(region)) {
                final String address =
                        session.getRegionManager()
                                .getStoreById(region.getLeader().getStoreId())
                                .getStore()
                                .getAddress();
                final ManagedChannel channel =
                        session.getChannelFactory()
                                .getChannel(address, session.getPDClient().getHostMapping());
                try {
                    final TidbRegionCdcClient client = new TidbRegionCdcClient(region, keyRange, channel, eventConsumer, config);
                    regionClients.put(region.getId(), client);
                    regionToResolvedTs.put(region.getId(), timestamp);
                    resolvedTsSet.add(timestamp);
                    client.start(timestamp);
                } catch (final Exception e) {
                    LOGGER.error(
                            "failed to add region(regionId: {}, reason: {})", region.getId(), e);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private synchronized void removeRegions(final Iterable<Long> regionIds) {
        LOGGER.info("remove regions: {}", regionIds);
        for (final long regionId : regionIds) {
            final TidbRegionCdcClient regionClient = regionClients.remove(regionId);
            if (regionClient != null) {
                try {
                    regionClient.close();
                } catch (final Exception e) {
                    LOGGER.error( "failed to close region client, region id: {}, error: {}", regionId, e);
                } finally {
                    resolvedTsSet.remove(regionToResolvedTs.remove(regionId));
                    regionToResolvedTs.remove(regionId);
                }
            }
        }
    }

    private boolean overlapWithRegion(final TiRegion region) {
        final Range<Key> regionRange =
                Range.closedOpen(
                        Key.toRawKey(region.getStartKey()), Key.toRawKey(region.getEndKey()));
        final Range<Key> clientRange =
                Range.closedOpen(
                        Key.toRawKey(keyRange.getStart()), Key.toRawKey(keyRange.getEnd()));
        final Range<Key> intersection = regionRange.intersection(clientRange);
        return !intersection.isEmpty();
    }

    private void handleResolvedTs(final long regionId, final long resolvedTs) {
        LOGGER.info("handle resolvedTs: {}, regionId: {}", resolvedTs, regionId);
        resolvedTsSet.remove(regionToResolvedTs.replace(regionId, resolvedTs));
        resolvedTsSet.add(resolvedTs);
    }

    public void handleErrorEvent(final long regionId, final Throwable error, long resolvedTs) {
        LOGGER.info("handle error: {}, regionId: {}", error, regionId);
        final TiRegion region = regionClients.get(regionId).getRegion();
        session.getRegionManager()
                .onRequestFail(region); // invalidate cache for corresponding region

        removeRegions(Arrays.asList(regionId));
        applyKeyRange(keyRange, resolvedTs); // reapply the whole keyRange
    }
}
