package io.tapdata.perftest.core.engine;

import io.tapdata.perftest.core.client.DataSourceClient;
import io.tapdata.perftest.core.config.BenchmarkConfig;
import io.tapdata.perftest.core.config.DataSourceConfig;
import io.tapdata.perftest.core.report.BenchmarkReport;
import io.tapdata.perftest.core.report.MarkdownReportGenerator;
import io.tapdata.perftest.core.scenario.BenchmarkScenario;
import io.tapdata.perftest.core.scenario.ScenarioResult;

import java.util.ArrayList;
import java.util.List;

public class BenchmarkEngine {

    private final BenchmarkConfig  benchmarkConfig;
    private final DataSourceConfig dataSourceConfig;

    public BenchmarkEngine(BenchmarkConfig benchmarkConfig, DataSourceConfig dataSourceConfig) {
        this.benchmarkConfig  = benchmarkConfig;
        this.dataSourceConfig = dataSourceConfig;
    }

    public BenchmarkReport run() throws Exception {
        List<ScenarioResult> results = new ArrayList<>();

        try (DataSourceClient client = dataSourceConfig.createClient()) {
            client.init();
            System.out.println("[BenchmarkEngine] Connected to: " + client.getDataSourceType());

            for (BenchmarkScenario scenario : benchmarkConfig.getScenarios()) {
                System.out.println("[BenchmarkEngine] Running scenario: " + scenario.getName());
                long t0 = System.currentTimeMillis();
                ScenarioResult result = scenario.execute(client);
                System.out.printf("[BenchmarkEngine] Done: %s | %,.0f rec/s | elapsed=%,dms%n",
                    scenario.getName(), result.getThroughputPerSec(),
                    System.currentTimeMillis() - t0);
                results.add(result);
            }
        }

        BenchmarkReport report = new BenchmarkReport(benchmarkConfig.getReportTitle(), results);
        new MarkdownReportGenerator(benchmarkConfig.getReportOutputDir()).generate(report);
        return report;
    }
}
