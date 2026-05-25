package io.tapdata.perftest.core.config;

/**
 * 数据生成参数配置。
 * 与数据源无关，由测试场景（Scenario）消费。
 */
public class DataGenerationConfig {

    private long totalRecords           = 1_000_000;
    private int  batchSize              = 10_000;
    private int  primaryKeyDuplicateRate = 0;   // 0-100 %，控制 UPDATE 比例
    private int  writeThreads           = 1;
    private int  maxQps                 = 0;    // 0 = 不限速

    // ── Fluent API ──────────────────────────────────────────────────────────
    public DataGenerationConfig totalRecords(long v)       { this.totalRecords = v;            return this; }
    public DataGenerationConfig batchSize(int v)           { this.batchSize = v;               return this; }
    public DataGenerationConfig pkDuplicateRate(int v)     { this.primaryKeyDuplicateRate = v; return this; }
    public DataGenerationConfig writeThreads(int v)        { this.writeThreads = v;            return this; }
    public DataGenerationConfig maxQps(int v)              { this.maxQps = v;                  return this; }

    // ── Getters ─────────────────────────────────────────────────────────────
    public long getTotalRecords()          { return totalRecords; }
    public int  getBatchSize()             { return batchSize; }
    public int  getPrimaryKeyDuplicateRate(){ return primaryKeyDuplicateRate; }
    public int  getWriteThreads()          { return writeThreads; }
    public int  getMaxQps()               { return maxQps; }
}
