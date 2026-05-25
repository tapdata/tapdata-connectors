package io.tapdata.perftest.core.scenario;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.perftest.core.client.DataSourceClient;
import io.tapdata.perftest.core.client.ReadResult;
import io.tapdata.perftest.core.metrics.LatencyTracker;

public class ReadScenario implements BenchmarkScenario {

    private final String   name;
    private final TapTable tapTable;
    private final int      batchSize;
    private final int      rounds;

    public ReadScenario(String name, TapTable tapTable, int batchSize, int rounds) {
        this.name      = name;
        this.tapTable  = tapTable;
        this.batchSize = batchSize;
        this.rounds    = rounds;
    }

    @Override
    public String getName() { return name; }

    @Override
    public ScenarioResult execute(DataSourceClient client) throws Exception {
        LatencyTracker latTracker  = new LatencyTracker();
        long           totalRows   = 0;
        long           totalErrors = 0;
        long           startMs     = System.currentTimeMillis();

        for (int i = 0; i < rounds; i++) {
            try {
                long t0 = System.nanoTime();
                ReadResult rr = client.batchRead(tapTable, batchSize);
                double latencyMs = (System.nanoTime() - t0) / 1_000_000.0;
                latTracker.record(rr.getRowCount() > 0 ? latencyMs / rr.getRowCount() : latencyMs);
                totalRows += rr.getRowCount();
            } catch (UnsupportedOperationException e) {
                return new ScenarioResult(name, client.getDataSourceType())
                    .setErrorCount(1)
                    .setErrorMessage("batchRead not supported: " + e.getMessage());
            } catch (Exception e) {
                totalErrors++;
            }
        }

        long elapsedMs = System.currentTimeMillis() - startMs;

        return new ScenarioResult(name, client.getDataSourceType())
            .setTotalRecords(totalRows)
            .setElapsedMs(elapsedMs)
            .setThroughputPerSec(elapsedMs > 0 ? totalRows * 1000.0 / elapsedMs : 0)
            .setAvgLatencyMs(latTracker.avg())
            .setP50LatencyMs(latTracker.p50())
            .setP99LatencyMs(latTracker.p99())
            .setP999LatencyMs(latTracker.p999())
            .setMaxLatencyMs(latTracker.max())
            .setErrorCount(totalErrors);
    }
}
