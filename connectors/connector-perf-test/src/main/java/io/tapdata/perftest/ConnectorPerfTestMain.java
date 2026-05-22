package io.tapdata.perftest;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDateTime;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.schema.type.TapString;
import io.tapdata.perftest.connector.paimon.PaimonDataSourceConfig;
import io.tapdata.perftest.core.config.BenchmarkConfig;
import io.tapdata.perftest.core.config.DataGenerationConfig;
import io.tapdata.perftest.core.data.DefaultRecordGenerator;
import io.tapdata.perftest.core.engine.BenchmarkEngine;
import io.tapdata.perftest.core.report.BenchmarkReport;
import io.tapdata.perftest.core.scenario.WriteScenario;

/**
 * New CLI entry point for the generic connector benchmark framework.
 * The legacy io.tapdata.connector.paimon.perf.PerformanceTestRunner remains usable unchanged.
 *
 * Usage:
 *   mvn exec:java -Pnew -Dexec.args="paimon-write-basic"
 */
public class ConnectorPerfTestMain {

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "paimon-write-basic";
        System.out.println("[ConnectorPerfTestMain] mode=" + mode);

        // ── 1. Test table schema ────────────────────────────────────────────
        TapTable table = new TapTable("perf_test_table");
        table.add(new TapField("id",    "string").tapType(new TapString()).primaryKeyPos(1));
        table.add(new TapField("name",  "string").tapType(new TapString()));
        table.add(new TapField("value", "double").tapType(new TapNumber().scale(2)));
        table.add(new TapField("ts",    "datetime").tapType(new TapDateTime()));

        // ── 2. Data source config ───────────────────────────────────────────
        PaimonDataSourceConfig dsConfig = new PaimonDataSourceConfig()
            .warehouse("/tmp/connector-perf-test/paimon")
            .storageType("local")
            .writeBufferSizeMb(256)
            .batchAccumulationSize(100_000)
            .enableAutoCompaction(false);
        dsConfig.setDatabase("default");
        dsConfig.setTapTable(table);

        // ── 3. Data generation config ───────────────────────────────────────
        DataGenerationConfig genConfig = new DataGenerationConfig()
            .totalRecords(1_000_000)
            .batchSize(10_000)
            .writeThreads(2);

        // ── 4. Scenario ─────────────────────────────────────────────────────
        WriteScenario writeScenario = new WriteScenario(
            "Paimon Write Basic",
            table,
            genConfig,
            new DefaultRecordGenerator(genConfig.getTotalRecords(), genConfig.getPrimaryKeyDuplicateRate())
        );

        // ── 5. Run ──────────────────────────────────────────────────────────
        BenchmarkConfig benchConfig = new BenchmarkConfig()
            .addScenario(writeScenario)
            .reportOutputDir("/tmp/connector-perf-test/reports")
            .reportTitle("Paimon Write Benchmark");

        BenchmarkEngine engine = new BenchmarkEngine(benchConfig, dsConfig);
        BenchmarkReport report = engine.run();

        System.out.println("\n=== Benchmark Complete ===");
        report.getResults().forEach(r ->
            System.out.printf("%-30s | %,.0f rec/s | p99=%.1fms | errors=%d%n",
                r.getScenarioName(), r.getThroughputPerSec(), r.getP99LatencyMs(), r.getErrorCount()));
    }
}
