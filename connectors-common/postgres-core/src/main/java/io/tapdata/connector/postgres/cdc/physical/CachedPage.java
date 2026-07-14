package io.tapdata.connector.postgres.cdc.physical;

import java.util.HashMap;
import java.util.Map;

/**
 * Logical overlay of one heap page used by the physical miner as an in-memory
 * redo buffer. Instead of mirroring the byte-exact 8KB page (which forces us to
 * track free space and drop the cache on every prune/vacuum/freeze), the page is
 * kept as a sparse {@code offnum -> tuple} map in the WAL wire format expected by
 * {@link HeapTupleDecoder}.
 *
 * <p>This makes maintenance records harmless no-ops: pruning a dead tuple or
 * defragmenting free space does not change the visible tuple at any live offset,
 * so a hot page (e.g. {@code bmsql_district} block 0) keeps a usable
 * before-image across the constant HOT-prune traffic that previously evicted it
 * and broke {@code wal_level=replica} recovery.</p>
 *
 * <p>A full-page image reseeds the whole overlay authoritatively
 * ({@link #seedFromImage}); subsequent FPI-less records mutate individual slots.
 * Writing back the same tuple an FPI already holds is idempotent, so callers
 * need no "already applied" guard.</p>
 *
 * <p>Access is now concurrent (pool threads decode in parallel), so mutation
 * methods are synchronized. The overlay is bounded by the number
 * of line pointers a page can hold (~292 for 8KB), so it never grows without
 * limit even when prune is a no-op — reused offsets simply overwrite.</p>
 *
 * @author Jarad
 */
public final class CachedPage {

    private final Map<Integer, byte[]> tuples = new HashMap<>();

    /**
     * Replace the overlay with every LP_NORMAL tuple on a reconstructed
     * full-page image. The image is the authoritative post-modification state,
     * so any prior drift is discarded. Dead / redirected / unused line pointers
     * yield null from the extractor and are skipped.
     *
     * <p>One exception: a WILL_INIT / freshly-pruned FPI reconstructs to zero
     * live tuples because the page is about to be rebuilt from the record's own
     * data delta (which the seed path does not apply). Replacing a populated
     * overlay with that empty image would strand every later prefix/suffix
     * UPDATE on the page — the old tuple it deltas against would be gone. In
     * that case keep the existing overlay so the in-flight before-image still
     * resolves (post-decode slot maintenance walks the offsets forward) and
     * return {@code false} to let callers log the skip.</p>
     *
     * @return {@code true} when the overlay was reseeded from the image,
     *         {@code false} when an empty image was ignored to preserve a
     *         populated overlay.
     */
    public synchronized boolean seedFromImage(byte[] page) {
        Map<Integer, byte[]> seeded = new HashMap<>();
        int maxOff = PageImageExtractor.maxOffset(page);
        for (int off = 1; off <= maxOff; off++) {
            byte[] onDisk = PageImageExtractor.extractOnDiskTuple(page, off);
            if (onDisk == null) {
                continue;
            }
            byte[] wal = PageImageExtractor.toWalTupleFormat(onDisk);
            if (wal != null) {
                seeded.put(off, wal);
            }
        }
        if (seeded.isEmpty() && !tuples.isEmpty()) {
            return false;
        }
        tuples.clear();
        tuples.putAll(seeded);
        return true;
    }

    /** WAL-format tuple currently at the 1-based offset, or null if unknown. */
    public synchronized byte[] get(int offnum) {
        return tuples.get(offnum);
    }

    /** Install / overwrite the tuple at a 1-based offset (INSERT, UPDATE new). */
    public synchronized void put(int offnum, byte[] walTuple) {
        if (offnum >= 1 && walTuple != null) {
            tuples.put(offnum, walTuple);
        }
    }

    /** Drop the tuple at a 1-based offset (DELETE, UPDATE old version). */
    public synchronized void remove(int offnum) {
        tuples.remove(offnum);
    }

    /** Forget every tuple, e.g. when a record re-initialises the page in place. */
    public synchronized void clear() {
        tuples.clear();
    }

    public synchronized int size() {
        return tuples.size();
    }
}
