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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class WriteScenario implements BenchmarkScenario {

    private final String               name;
    private final TapTable             tapTable;
    private final DataGenerationConfig genConfig;
    private final RecordGenerator      generator;

    public WriteScenario(String name, TapTable tapTable,
                         DataGenerationConfig genConfig, RecordGenerator generator) {
        this.name      = name;
        this.tapTable  = tapTable;
        this.genConfig = genConfig;
        this.generator = generator;
    }

    @Override
    public String getName() { return name; }

    @Override
    public ScenarioResult execute(DataSourceClient client) throws Exception {
        client.createTable(tapTable);

        ThroughputTracker tpTracker    = new ThroughputTracker();
        LatencyTracker    latTracker   = new LatencyTracker();
        AtomicLong        totalWritten = new AtomicLong(0);
        AtomicLong        totalErrors  = new AtomicLong(0);

        int threads   = genConfig.getWriteThreads();
        long total    = genConfig.getTotalRecords();
        long perThread = total / threads;

        ExecutorService pool    = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>(threads);

        long startMs = System.currentTimeMillis();

        for (int t = 0; t < threads; t++) {
            final long threadTotal = (t == threads - 1) ? total - perThread * (threads - 1) : perThread;
            futures.add(pool.submit(() -> {
                long remain = threadTotal;
                while (remain > 0) {
                    int batchSz = (int) Math.min(genConfig.getBatchSize(), remain);
                    List<TapRecordEvent> batch = new ArrayList<>(batchSz);
                    for (int i = 0; i < batchSz; i++) {
                        TapInsertRecordEvent evt = new TapInsertRecordEvent();
                        evt.setAfter(generator.nextRecord());
                        evt.setTableId(tapTable.getId());
                        evt.setReferenceTime(System.currentTimeMillis());
                        batch.add(evt);
                    }
                    long t0 = System.nanoTime();
                    WriteResult wr = client.write(batch);
                    double batchLatencyMs = (System.nanoTime() - t0) / 1_000_000.0;
                    latTracker.record(batchLatencyMs / batchSz);
                    tpTracker.record(batchSz);
                    totalWritten.addAndGet(wr.getTotalWritten());
                    totalErrors.addAndGet(wr.getErrorCount());
                    remain -= batchSz;
                }
                return null;
            }));
        }

        for (Future<?> f : futures) f.get();
        pool.shutdown();

        long elapsedMs = System.currentTimeMillis() - startMs;

        return new ScenarioResult(name, client.getDataSourceType())
            .setTotalRecords(totalWritten.get())
            .setElapsedMs(elapsedMs)
            .setThroughputPerSec(elapsedMs > 0 ? totalWritten.get() * 1000.0 / elapsedMs : 0)
            .setAvgLatencyMs(latTracker.avg())
            .setP50LatencyMs(latTracker.p50())
            .setP99LatencyMs(latTracker.p99())
            .setP999LatencyMs(latTracker.p999())
            .setMaxLatencyMs(latTracker.max())
            .setErrorCount(totalErrors.get());
    }
}
