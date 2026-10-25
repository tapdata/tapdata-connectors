package io.tapdata.perftest.core.metrics;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that reported latency percentiles are monotonic, conservative (bucket upper bound),
 * and accurate within the documented resolution of their range.
 */
class LatencyTrackerTest {

    @Test
    @DisplayName("percentiles are 0 on an empty tracker")
    void emptyTracker() {
        LatencyTracker t = new LatencyTracker();
        assertEquals(0.0, t.p50());
        assertEquals(0.0, t.p95());
        assertEquals(0.0, t.p99());
        assertEquals(0L, t.count());
    }

    @Test
    @DisplayName("sub-100ms latencies resolve to 0.1ms buckets")
    void fineResolutionBelow100ms() {
        LatencyTracker t = new LatencyTracker();
        for (int i = 0; i < 1000; i++) t.record(5.0);

        // 5.0ms must not be rounded into a ~10ms bucket any more.
        assertEquals(5.0, t.p50(), 0.11);
        assertEquals(5.0, t.p99(), 0.11);
        assertEquals(0.1, LatencyTracker.resolutionMs(5.0), 1e-9);
    }

    @Test
    @DisplayName("p50/p90/p95/p99 follow a known distribution")
    void percentilesOfKnownDistribution() {
        LatencyTracker t = new LatencyTracker();
        // 1000 samples: 900 x 10ms, 50 x 200ms, 40 x 500ms, 10 x 1500ms
        for (int i = 0; i < 900; i++) t.record(10.0);
        for (int i = 0; i < 50; i++)  t.record(200.0);
        for (int i = 0; i < 40; i++)  t.record(500.0);
        for (int i = 0; i < 10; i++)  t.record(1500.0);

        assertEquals(1000, t.count());
        assertEquals(10.0,   t.p50(), 0.11);   // 500th sample -> 10ms bucket
        assertEquals(10.0,   t.p90(), 1.01);   // 900th sample -> last 10ms sample
        assertEquals(200.0,  t.p95(), 1.01);   // 950th sample -> 200ms bucket
        assertEquals(500.0,  t.p99(), 10.01);  // 990th sample -> 500ms bucket
        assertTrue(t.max() >= 1500.0);
    }

    @Test
    @DisplayName("percentiles are monotonically non-decreasing")
    void percentilesAreMonotonic() {
        LatencyTracker t = new LatencyTracker();
        for (int i = 1; i <= 5000; i++) t.record(i % 777);
        assertTrue(t.p50() <= t.p90());
        assertTrue(t.p90() <= t.p95());
        assertTrue(t.p95() <= t.p99());
        assertTrue(t.p99() <= t.p999());
        assertTrue(t.p999() <= t.max());
    }

    @Test
    @DisplayName("resolution widens with the latency range")
    void resolutionScalesWithRange() {
        assertEquals(0.1,  LatencyTracker.resolutionMs(99.0), 1e-9);
        assertEquals(1.0,  LatencyTracker.resolutionMs(500.0), 1e-9);
        assertEquals(10.0, LatencyTracker.resolutionMs(5000.0), 1e-9);
    }

    @Test
    @DisplayName("values above the histogram ceiling land in the last bucket")
    void overflowIsClamped() {
        LatencyTracker t = new LatencyTracker();
        for (int i = 0; i < 100; i++) t.record(60_000.0);
        assertEquals(100, t.count());
        assertTrue(t.p99() > 0);
        assertEquals(60_000.0, t.max(), 1e-6);
    }
}
