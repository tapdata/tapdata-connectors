package io.tapdata.perftest.core.scenario;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.perftest.core.client.DataSourceClient;
import io.tapdata.perftest.core.client.WriteResult;
import io.tapdata.perftest.core.config.DataGenerationConfig;
import io.tapdata.perftest.core.data.RecordGenerator;
import io.tapdata.perftest.core.metrics.LatencyTracker;
import io.tapdata.perftest.core.metrics.ThroughputTracker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Time-bounded stability test. Runs until {@code duration} elapses; resets
 * the generator on exhaustion so data flows continuously.
 */
public class DrillTestScenario implements BenchmarkScenario {

    private final String               name;
    private final TapTable             tapTable;
    private final DataGenerationConfig genConfig;
    private final RecordGenerator      generator;
    private final Duration             duration;

    public DrillTestScenario(String name, TapTable tapTable,
                             DataGenerationConfig genConfig, RecordGenerator generator,
                             Duration duration) {
        this.name      = name;
        this.tapTable  = tapTable;
        this.genConfig = genConfig;
        this.generator = generator;
        this.duration  = duration;
    }

    @Override
    public String getName() { return name; }

    @Override
    public ScenarioResult execute(DataSourceClient client) throws Exception {
        client.createTable(tapTable);

        ThroughputTracker tpTracker  = new ThroughputTracker();
        LatencyTracker    latTracker = new LatencyTracker();
        AtomicLong        written    = new AtomicLong(0);
        AtomicLong        errors     = new AtomicLong(0);

        long deadline = System.currentTimeMillis() + duration.toMillis();
        long startMs  = System.currentTimeMillis();

        generator.reset();
        while (System.currentTimeMillis() < deadline) {
            if (!generator.hasMore()) generator.reset();

            int batchSz = genConfig.getBatchSize();
            List<TapRecordEvent> batch = new ArrayList<>(batchSz);
            for (int i = 0; i < batchSz && generator.hasMore(); i++) {
                TapInsertRecordEvent evt = new TapInsertRecordEvent();
                evt.setAfter(generator.nextRecord());
                evt.setTableId(tapTable.getId());
                evt.setReferenceTime(System.currentTimeMillis());
                batch.add(evt);
            }
            if (batch.isEmpty()) continue;

            try {
                long t0 = System.nanoTime();
                WriteResult wr = client.write(batch);
                double latencyMs = (System.nanoTime() - t0) / 1_000_000.0;
                latTracker.record(latencyMs / batch.size());
                tpTracker.record(batch.size());
                written.addAndGet(wr.getTotalWritten());
                errors.addAndGet(wr.getErrorCount());
            } catch (Exception e) {
                errors.incrementAndGet();
            }
        }

        long elapsedMs = System.currentTimeMillis() - startMs;

        return new ScenarioResult(name, client.getDataSourceType())
            .setTotalRecords(written.get())
            .setElapsedMs(elapsedMs)
            .setThroughputPerSec(elapsedMs > 0 ? written.get() * 1000.0 / elapsedMs : 0)
            .setAvgLatencyMs(latTracker.avg())
            .setP50LatencyMs(latTracker.p50())
            .setP99LatencyMs(latTracker.p99())
            .setP999LatencyMs(latTracker.p999())
            .setMaxLatencyMs(latTracker.max())
            .setErrorCount(errors.get());
    }
}
