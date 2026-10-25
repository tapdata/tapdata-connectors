package io.tapdata.perftest.core.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Fixed-bucket histogram for latency percentiles (p50/p90/p95/p99/p999/max).
 *
 * <p>Buckets are piecewise-uniform so that sub-100ms latencies are not collapsed into
 * ~10ms buckets (the previous single-range histogram reported 9.77ms resolution, which is
 * coarser than the p50 of fast write paths):</p>
 * <ul>
 *   <li>0 – 100 ms      : 1000 buckets, 0.1 ms resolution</li>
 *   <li>100 – 1000 ms   :  900 buckets, 1   ms resolution</li>
 *   <li>1000 – 20000 ms : 1900 buckets, 10  ms resolution</li>
 * </ul>
 *
 * <p>Every reported percentile is the <em>upper bound</em> of the bucket that contains the
 * requested quantile, i.e. the value is rounded up to the resolution of its range. This is a
 * conservative (never-optimistic) estimate and is what {@link #resolutionMs(double)} reports
 * for a given measurement.</p>
 */
public class LatencyTracker {

    private static final double SMALL_MAX_MS   = 100.0;
    private static final double MEDIUM_MAX_MS  = 1000.0;
    private static final double MAX_MS         = 20_000.0;

    private static final int SMALL_BUCKETS     = 1000;   // 0.1 ms each
    private static final int MEDIUM_BUCKETS    = 900;    // 1 ms each
    private static final int LARGE_BUCKETS     = 1900;   // 10 ms each
    private static final int BUCKETS           = SMALL_BUCKETS + MEDIUM_BUCKETS + LARGE_BUCKETS;

    private final AtomicLong[] buckets      = new AtomicLong[BUCKETS];
    private final AtomicLong   totalSamples = new AtomicLong(0);
    private final AtomicLong   totalMicros  = new AtomicLong(0);
    private volatile double    maxMs        = 0;

    public LatencyTracker() {
        for (int i = 0; i < BUCKETS; i++) buckets[i] = new AtomicLong(0);
    }

    private static int indexOf(double ms) {
        if (ms <= 0) return 0;
        if (ms < SMALL_MAX_MS)  return (int) (ms * 10);                                   // 0 .. 999
        if (ms < MEDIUM_MAX_MS) return SMALL_BUCKETS + (int) (ms - SMALL_MAX_MS);          // 1000 .. 1899
        if (ms < MAX_MS)        return SMALL_BUCKETS + MEDIUM_BUCKETS
                                        + (int) ((ms - MEDIUM_MAX_MS) / 10.0);             // 1900 .. 3799
        return BUCKETS - 1;
    }

    private static double upperBoundOf(int idx) {
        if (idx < SMALL_BUCKETS)  return (idx + 1) * 0.1;
        if (idx < SMALL_BUCKETS + MEDIUM_BUCKETS) {
            return SMALL_MAX_MS + (idx - SMALL_BUCKETS + 1);
        }
        return MEDIUM_MAX_MS + (idx - SMALL_BUCKETS - MEDIUM_BUCKETS + 1) * 10.0;
    }

    /** Bucket width (in ms) used for the bucket a value of {@code ms} falls into. */
    public static double resolutionMs(double ms) {
        int idx = indexOf(ms);
        if (idx < SMALL_BUCKETS) return 0.1;
        if (idx < SMALL_BUCKETS + MEDIUM_BUCKETS) return 1.0;
        return 10.0;
    }

    public void record(double ms) {
        buckets[indexOf(ms)].incrementAndGet();
        totalSamples.incrementAndGet();
        totalMicros.addAndGet((long) (ms * 1000));
        if (ms > maxMs) {
            synchronized (this) {
                if (ms > maxMs) maxMs = ms;
            }
        }
    }

    public long count() {
        return totalSamples.get();
    }

    public double avg() {
        long s = totalSamples.get();
        return s > 0 ? totalMicros.get() / 1000.0 / s : 0;
    }

    public double p50()  { return percentile(0.50); }
    public double p90()  { return percentile(0.90); }
    public double p95()  { return percentile(0.95); }
    public double p99()  { return percentile(0.99); }
    public double p999() { return percentile(0.999); }
    public double max()  { return maxMs; }

    public double percentile(double p) {
        long samples = totalSamples.get();
        if (samples == 0) return 0;
        long target = (long) Math.ceil(samples * p);
        if (target < 1) target = 1;
        long cumul = 0;
        for (int i = 0; i < BUCKETS; i++) {
            cumul += buckets[i].get();
            if (cumul >= target) {
                double upper = upperBoundOf(i);
                double observedMax = maxMs;
                // The bucket that contains the maximum sample would otherwise report an upper
                // bound slightly above the true maximum; clamp so no percentile exceeds max().
                return observedMax > 0 && upper > observedMax ? observedMax : upper;
            }
        }
        return maxMs;
    }
}
