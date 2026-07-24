package io.tapdata.connector.postgres.cdc.physical;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Page-state cache of {@link CachedPage} logical overlays keyed by
 * (relNumber, blockNumber). Acts as the physical miner's "in-memory redo"
 * buffer: a full-page image from any WAL record reseeds the page overlay, and
 * subsequent records mutate individual offsets so that later records on the same
 * page can recover their before-image even when {@code wal_level=replica} omits
 * the old tuple.
 *
 * <p>The cache is <em>unbounded by default</em> ({@code capacity <= 0}): once a
 * page's FPI-seeded state is observed within the read window it is never lost,
 * mirroring walminer's full FPW-replay model. Pass a positive {@code capacity}
 * to bound memory with approximate FIFO eviction, trading before-image coverage
 * for a fixed heap budget. Pages are also removed by relation-wide drop
 * (TRUNCATE/DROP); maintenance records (prune/vacuum/freeze/visibility) are
 * deliberately <em>not</em> allowed to evict, because under
 * {@code wal_level=replica} a dropped hot page would have no FPI to reseed it
 * until the next checkpoint.</p>
 *
 * <p>Thread-safe: backed by {@link ConcurrentHashMap}, CachedPage methods are
 * synchronized. Pool threads can seed/read/mutate pages concurrently.</p>
 *
 * @author Jarad
 */
public class PageStateCache {

    /** No-arg default: unbounded (no eviction). */
    public static final int DEFAULT_CAPACITY = 0;

    private final int capacity;
    private final ConcurrentHashMap<Key, CachedPage> pages = new ConcurrentHashMap<>();
    private final Object evictionLock = new Object();
    private final LinkedHashSet<Key> insertionOrder = new LinkedHashSet<>();

    public PageStateCache() {
        this(DEFAULT_CAPACITY);
    }

    public PageStateCache(int capacity) {
        this.capacity = Math.max(0, capacity);
    }

    /** Concurrent overlay for the page, or null when it has not been seen yet. */
    public CachedPage get(long relNumber, long blockNumber) {
        return pages.get(new Key(relNumber, blockNumber));
    }

    /** Overlay for the page, creating an empty one atomically if absent. */
    public CachedPage getOrCreate(long relNumber, long blockNumber) {
        Key key = new Key(relNumber, blockNumber);
        CachedPage existing = pages.get(key);
        if (existing != null) {
            return existing;
        }
        CachedPage created = new CachedPage();
        CachedPage previous = pages.putIfAbsent(key, created);
        if (previous != null) {
            return previous;
        }
        if (capacity > 0) {
            trackCreated(key);
        }
        return created;
    }

    public void invalidate(long relNumber, long blockNumber) {
        Key key = new Key(relNumber, blockNumber);
        pages.remove(key);
        if (capacity > 0) {
            synchronized (evictionLock) {
                insertionOrder.remove(key);
            }
        }
    }

    /** Drop every cached page belonging to a relation (TRUNCATE/DROP). */
    public void invalidateRelation(long relNumber) {
        pages.keySet().removeIf(k -> k.relNumber == relNumber);
        if (capacity > 0) {
            synchronized (evictionLock) {
                insertionOrder.removeIf(k -> k.relNumber == relNumber);
            }
        }
    }

    public int size() {
        return pages.size();
    }

    private void trackCreated(Key key) {
        synchronized (evictionLock) {
            insertionOrder.add(key);
            while (pages.size() > capacity && !insertionOrder.isEmpty()) {
                Iterator<Key> it = insertionOrder.iterator();
                Key eldest = it.next();
                it.remove();
                pages.remove(eldest);
            }
        }
    }

    private static final class Key {
        final long relNumber;
        final long blockNumber;

        Key(long relNumber, long blockNumber) {
            this.relNumber = relNumber;
            this.blockNumber = blockNumber;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key)) return false;
            Key k = (Key) o;
            return relNumber == k.relNumber && blockNumber == k.blockNumber;
        }

        @Override
        public int hashCode() {
            long h = relNumber * 1315423911L ^ blockNumber;
            return (int) (h ^ (h >>> 32));
        }
    }
}
