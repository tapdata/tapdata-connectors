package io.tapdata.perftest.core.scenario;

/**
 * 单次场景（Scenario）的执行结果快照。不可变 DTO，供报告生成器消费。
 */
public class ScenarioResult {

    private final String scenarioName;
    private final String dataSourceType;

    private long   totalRecords;
    private long   elapsedMs;
    private double throughputPerSec;
    private double avgLatencyMs;
    private double p50LatencyMs;
    private double p90LatencyMs;
    private double p95LatencyMs;
    private double p99LatencyMs;
    private double p999LatencyMs;
    /** Batch-level latency (per write call), 0 when the scenario does not measure it. */
    private double batchP50LatencyMs;
    private double batchP95LatencyMs;
    private double batchP99LatencyMs;
    private double maxLatencyMs;
    private long   errorCount;
    private String errorMessage;

    public ScenarioResult(String scenarioName, String dataSourceType) {
        this.scenarioName   = scenarioName;
        this.dataSourceType = dataSourceType;
    }

    // ── Fluent setters ──────────────────────────────────────────────────────
    public ScenarioResult setTotalRecords(long v)    { this.totalRecords = v;    return this; }
    public ScenarioResult setElapsedMs(long v)       { this.elapsedMs = v;       return this; }
    public ScenarioResult setThroughputPerSec(double v){ this.throughputPerSec = v; return this; }
    public ScenarioResult setAvgLatencyMs(double v)  { this.avgLatencyMs = v;    return this; }
    public ScenarioResult setP50LatencyMs(double v)  { this.p50LatencyMs = v;    return this; }
    public ScenarioResult setP90LatencyMs(double v)  { this.p90LatencyMs = v;    return this; }
    public ScenarioResult setP95LatencyMs(double v)  { this.p95LatencyMs = v;    return this; }
    public ScenarioResult setP99LatencyMs(double v)  { this.p99LatencyMs = v;    return this; }
    public ScenarioResult setP999LatencyMs(double v) { this.p999LatencyMs = v;   return this; }
    public ScenarioResult setMaxLatencyMs(double v)  { this.maxLatencyMs = v;    return this; }
    public ScenarioResult setBatchP50LatencyMs(double v) { this.batchP50LatencyMs = v; return this; }
    public ScenarioResult setBatchP95LatencyMs(double v) { this.batchP95LatencyMs = v; return this; }
    public ScenarioResult setBatchP99LatencyMs(double v) { this.batchP99LatencyMs = v; return this; }
    public ScenarioResult setErrorCount(long v)      { this.errorCount = v;      return this; }
    public ScenarioResult setErrorMessage(String v)  { this.errorMessage = v;    return this; }

    // ── Getters ─────────────────────────────────────────────────────────────
    public String getScenarioName()    { return scenarioName; }
    public String getDataSourceType()  { return dataSourceType; }
    public long   getTotalRecords()    { return totalRecords; }
    public long   getElapsedMs()       { return elapsedMs; }
    public double getThroughputPerSec(){ return throughputPerSec; }
    public double getAvgLatencyMs()    { return avgLatencyMs; }
    public double getP50LatencyMs()    { return p50LatencyMs; }
    public double getP90LatencyMs()    { return p90LatencyMs; }
    public double getP95LatencyMs()    { return p95LatencyMs; }
    public double getP99LatencyMs()    { return p99LatencyMs; }
    public double getP999LatencyMs()   { return p999LatencyMs; }
    public double getMaxLatencyMs()    { return maxLatencyMs; }
    public double getBatchP50LatencyMs() { return batchP50LatencyMs; }
    public double getBatchP95LatencyMs() { return batchP95LatencyMs; }
    public double getBatchP99LatencyMs() { return batchP99LatencyMs; }
    public long   getErrorCount()      { return errorCount; }
    public String getErrorMessage()    { return errorMessage; }

    @Override
    public String toString() {
        return String.format("[%s/%s] total=%,d | %.0f rec/s | avg=%.1fms | p50=%.1fms | p95=%.1fms | p99=%.1fms | errors=%d",
            scenarioName, dataSourceType, totalRecords, throughputPerSec,
            avgLatencyMs, p50LatencyMs, p95LatencyMs, p99LatencyMs, errorCount);
    }
}
