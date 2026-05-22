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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrent write + read scenario. Write threads run for the full duration;
 * read threads interleave batchRead calls. Reports write-side metrics only —
 * read errors are counted but not treated as fatal.
 */
public class MixedScenario implements BenchmarkScenario {

    private final String               name;
    private final TapTable             tapTable;
    private final DataGenerationConfig genConfig;
    private final RecordGenerator      generator;
    private final int                  readThreads;

    public MixedScenario(String name, TapTable tapTable,
                         DataGenerationConfig genConfig, RecordGenerator generator,
                         int readThreads) {
        this.name        = name;
        this.tapTable    = tapTable;
        this.genConfig   = genConfig;
        this.generator   = generator;
        this.readThreads = readThreads;
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

        int   writeThreads = genConfig.getWriteThreads();
        long  total        = genConfig.getTotalRecords();
        long  perThread    = total / writeThreads;

        int   totalThreads = writeThreads + readThreads;
        ExecutorService    pool    = Executors.newFixedThreadPool(totalThreads);
        List<Future<?>>    futures = new ArrayList<>(totalThreads);

        long startMs = System.currentTimeMillis();

        for (int t = 0; t < writeThreads; t++) {
            final long threadTotal = (t == writeThreads - 1) ? total - perThread * (writeThreads - 1) : perThread;
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
                    latTracker.record((System.nanoTime() - t0) / 1_000_000.0 / batchSz);
                    tpTracker.record(batchSz);
                    written.addAndGet(wr.getTotalWritten());
                    errors.addAndGet(wr.getErrorCount());
                    remain -= batchSz;
                }
                return null;
            }));
        }

        // Read threads run until writes finish
        CountDownLatch writeDone = new CountDownLatch(1);
        for (int r = 0; r < readThreads; r++) {
            futures.add(pool.submit(() -> {
                while (!writeDone.await(100, TimeUnit.MILLISECONDS)) {
                    try { client.batchRead(tapTable, genConfig.getBatchSize()); }
                    catch (Exception ignored) { errors.incrementAndGet(); }
                }
                return null;
            }));
        }

        for (int i = 0; i < writeThreads; i++) futures.get(i).get();
        writeDone.countDown();
        for (int i = writeThreads; i < futures.size(); i++) futures.get(i).get();
        pool.shutdown();

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
