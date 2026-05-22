package io.tapdata.perftest.core.config;

import io.tapdata.perftest.core.scenario.BenchmarkScenario;

import java.util.ArrayList;
import java.util.List;

/**
 * 一次 Benchmark 运行的顶层配置。
 * 支持多个场景串行（默认）或并行执行。
 */
public class BenchmarkConfig {

    private List<BenchmarkScenario> scenarios  = new ArrayList<>();
    private boolean parallelScenarios          = false;
    private String  reportOutputDir            = "/tmp/connector-perf-test/reports";
    private String  reportTitle                = "Connector Benchmark Report";

    // ── Fluent API ──────────────────────────────────────────────────────────
    public BenchmarkConfig addScenario(BenchmarkScenario s) { scenarios.add(s); return this; }
    public BenchmarkConfig parallelScenarios(boolean v)     { this.parallelScenarios = v; return this; }
    public BenchmarkConfig reportOutputDir(String d)        { this.reportOutputDir = d; return this; }
    public BenchmarkConfig reportTitle(String t)            { this.reportTitle = t; return this; }

    // ── Getters ─────────────────────────────────────────────────────────────
    public List<BenchmarkScenario> getScenarios()  { return scenarios; }
    public boolean isParallelScenarios()           { return parallelScenarios; }
    public String  getReportOutputDir()            { return reportOutputDir; }
    public String  getReportTitle()                { return reportTitle; }
}
