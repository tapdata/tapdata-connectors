package io.tapdata.perftest.core.client;

public class ReadResult {
    private final long rowCount;
    private final long elapsedMs;

    public ReadResult(long rowCount, long elapsedMs) {
        this.rowCount  = rowCount;
        this.elapsedMs = elapsedMs;
    }

    public long getRowCount()  { return rowCount; }
    public long getElapsedMs() { return elapsedMs; }

    public double throughputPerSec() {
        return elapsedMs > 0 ? rowCount * 1000.0 / elapsedMs : 0;
    }

    @Override
    public String toString() {
        return String.format("ReadResult{rows=%d, elapsedMs=%d, throughput=%.0f/s}",
            rowCount, elapsedMs, throughputPerSec());
    }
}
