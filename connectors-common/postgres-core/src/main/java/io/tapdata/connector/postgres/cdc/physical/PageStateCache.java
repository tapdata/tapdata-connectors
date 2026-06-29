package io.tapdata.connector.postgres.cdc.physical;

import java.util.LinkedHashMap;
import java.util.Map;

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
 * mirroring walminer's full FPW-replay model so that no later FPI-less
 * UPDATE/DELETE on that page strands on an evicted overlay. Pass a positive
 * {@code capacity} to re-impose an LRU bound (trading before-image coverage for
 * a fixed heap budget). Either way pages are removed only by capacity eviction
 * or by a relation-wide drop (TRUNCATE/DROP); maintenance records
 * (prune/vacuum/freeze/visibility) are deliberately <em>not</em> allowed to
 * evict, because under {@code wal_level=replica} a dropped hot page would have no
 * FPI to reseed it until the next checkpoint, stranding every intervening
 * UPDATE/DELETE with a null before-image. The overlay makes those records safe
 * no-ops.</p>
 *
 * <p>All accesses happen on the single consumer thread (heap decoding was moved
 * off the parallel decode pool to make state mutation race-free), so the
 * underlying {@link LinkedHashMap} does not need external synchronization.</p>
 *
 * @author Jarad
 */
public class PageStateCache {

    /** No-arg default: unbounded (no eviction). */
    public static final int DEFAULT_CAPACITY = 0;

    /** Initial bucket count when unbounded; just an allocation hint. */
    private static final int UNBOUNDED_INITIAL = 1 << 14;

    private final int capacity;
    private final LinkedHashMap<Key, CachedPage> pages;

    public PageStateCache() {
        this(DEFAULT_CAPACITY);
    }

    public PageStateCache(int capacity) {
        this.capacity = capacity;
        boolean bounded = capacity > 0;
        // Access-order only matters for LRU eviction; keep insertion order when
        // unbounded to avoid reordering on every read.
        int initial = bounded ? capacity * 2 : UNBOUNDED_INITIAL;
        this.pages = new LinkedHashMap<Key, CachedPage>(initial, 0.75f, bounded) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Key, CachedPage> eldest) {
                return PageStateCache.this.capacity > 0 && size() > PageStateCache.this.capacity;
            }
        };
    }

    /** Current overlay for the page, or null when it has not been seen yet. */
    public CachedPage get(long relNumber, long blockNumber) {
        return pages.get(new Key(relNumber, blockNumber));
    }

    /** Overlay for the page, creating (and LRU-inserting) an empty one if absent. */
    public CachedPage getOrCreate(long relNumber, long blockNumber) {
        Key key = new Key(relNumber, blockNumber);
        CachedPage page = pages.get(key);
        if (page == null) {
            page = new CachedPage();
            pages.put(key, page);
        }
        return page;
    }

    public void invalidate(long relNumber, long blockNumber) {
        pages.remove(new Key(relNumber, blockNumber));
    }

    /** Drop every cached page belonging to a relation (TRUNCATE/DROP). */
    public void invalidateRelation(long relNumber) {
        pages.keySet().removeIf(k -> k.relNumber == relNumber);
    }

    public int size() {
        return pages.size();
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
