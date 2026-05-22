package io.tapdata.perftest.core.report;

import io.tapdata.perftest.core.scenario.ScenarioResult;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.format.DateTimeFormatter;

public class MarkdownReportGenerator implements ReportGenerator {

    private final String outputDir;

    public MarkdownReportGenerator(String outputDir) {
        this.outputDir = outputDir;
    }

    @Override
    public void generate(BenchmarkReport report) throws IOException {
        new File(outputDir).mkdirs();
        String filePath = outputDir + "/benchmark-report.md";
        try (PrintWriter w = new PrintWriter(new FileWriter(filePath))) {
            w.println("# " + report.getTitle());
            w.println("Generated: " + report.getGeneratedAt().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            w.println();
            w.println("## Summary");
            w.println();
            w.println("| Scenario | DataSource | Total Records | Throughput (rec/s) | Avg Latency (ms) | P99 Latency (ms) | P999 Latency (ms) | Max Latency (ms) | Errors |");
            w.println("|---|---|---:|---:|---:|---:|---:|---:|---:|");
            for (ScenarioResult r : report.getResults()) {
                w.printf("| %s | %s | %,d | %,.0f | %.2f | %.2f | %.2f | %.2f | %d |%n",
                    r.getScenarioName(), r.getDataSourceType(), r.getTotalRecords(),
                    r.getThroughputPerSec(), r.getAvgLatencyMs(),
                    r.getP99LatencyMs(), r.getP999LatencyMs(), r.getMaxLatencyMs(),
                    r.getErrorCount());
            }
            w.println();
            w.println("## Detail");
            w.println();
            for (ScenarioResult r : report.getResults()) {
                w.println("### " + r.getScenarioName());
                w.printf("- **Data source**: %s%n", r.getDataSourceType());
                w.printf("- **Total records**: %,d%n", r.getTotalRecords());
                w.printf("- **Elapsed**: %,d ms%n", r.getElapsedMs());
                w.printf("- **Throughput**: %,.2f records/sec%n", r.getThroughputPerSec());
                w.printf("- **Avg latency**: %.2f ms%n", r.getAvgLatencyMs());
                w.printf("- **P50 latency**: %.2f ms%n", r.getP50LatencyMs());
                w.printf("- **P99 latency**: %.2f ms%n", r.getP99LatencyMs());
                w.printf("- **P999 latency**: %.2f ms%n", r.getP999LatencyMs());
                w.printf("- **Max latency**: %.2f ms%n", r.getMaxLatencyMs());
                w.printf("- **Errors**: %d%n", r.getErrorCount());
                if (r.getErrorMessage() != null) {
                    w.printf("- **Error message**: %s%n", r.getErrorMessage());
                }
                w.println();
            }
        }
        System.out.println("[Report] Written to: " + filePath);
    }
}
