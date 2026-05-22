package io.tapdata.perftest.core.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Fixed-bucket histogram for latency percentiles (p50/p99/p999/max).
 * 2048 buckets covering 0-20000ms (~10ms resolution).
 */
public class LatencyTracker {

    private static final int    BUCKETS = 2048;
    private static final double MAX_MS  = 20_000.0;

    private final AtomicLong[] buckets      = new AtomicLong[BUCKETS];
    private final AtomicLong   totalSamples = new AtomicLong(0);
    private final AtomicLong   totalMicros  = new AtomicLong(0);
    private volatile double    maxMs        = 0;

    public LatencyTracker() {
        for (int i = 0; i < BUCKETS; i++) buckets[i] = new AtomicLong(0);
    }

    public void record(double ms) {
        int idx = Math.min((int)(ms / MAX_MS * BUCKETS), BUCKETS - 1);
        buckets[idx].incrementAndGet();
        totalSamples.incrementAndGet();
        totalMicros.addAndGet((long)(ms * 1000));
        if (ms > maxMs) {
            synchronized (this) {
                if (ms > maxMs) maxMs = ms;
            }
        }
    }

    public double avg() {
        long s = totalSamples.get();
        return s > 0 ? totalMicros.get() / 1000.0 / s : 0;
    }

    public double p50()  { return percentile(0.50); }
    public double p99()  { return percentile(0.99); }
    public double p999() { return percentile(0.999); }
    public double max()  { return maxMs; }

    private double percentile(double p) {
        long target = (long)(totalSamples.get() * p);
        long cumul  = 0;
        for (int i = 0; i < BUCKETS; i++) {
            cumul += buckets[i].get();
            if (cumul >= target) return (i + 1) * MAX_MS / BUCKETS;
        }
        return maxMs;
    }
}
