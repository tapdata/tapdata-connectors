package io.tapdata.perftest.core.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class ThroughputTracker {

    private final AtomicLong totalOps = new AtomicLong(0);
    private final long startMs = System.currentTimeMillis();

    public void record(long ops) {
        totalOps.addAndGet(ops);
    }

    public double overallQps() {
        long elapsed = System.currentTimeMillis() - startMs;
        return elapsed > 0 ? totalOps.get() * 1000.0 / elapsed : 0;
    }

    public long getTotalOps() {
        return totalOps.get();
    }
}
