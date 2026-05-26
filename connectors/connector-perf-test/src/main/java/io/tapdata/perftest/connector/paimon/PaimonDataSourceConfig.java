package io.tapdata.perftest.connector.paimon;

import io.tapdata.perftest.core.client.DataSourceClient;
import io.tapdata.perftest.core.config.DataSourceConfig;

public class PaimonDataSourceConfig extends DataSourceConfig {

    private String  warehouse             = "/tmp/perf-test-warehouse";
    private String  storageType           = "local";
    private String  s3Endpoint, s3AccessKey, s3SecretKey, s3Region;
    private String  hdfsHost;
    private Integer hdfsPort              = 9000;
    private Integer writeBufferSize       = 256;
    private Integer batchAccumulationSize = 100_000;
    private Integer commitIntervalMs      = 30_000;
    private Boolean enableAsyncCommit     = true;
    private Integer writeThreads          = 4;
    private Boolean enableAutoCompaction  = false;
    private Integer targetFileSize        = 128;
    private String  bucketMode            = "dynamic";
    private Integer bucketCount           = 4;

    @Override
    public DataSourceClient createClient() {
        return new PaimonDataSourceClient(this);
    }

    // ── Fluent setters ──────────────────────────────────────────────────────
    public PaimonDataSourceConfig warehouse(String v)               { this.warehouse = v; return this; }
    public PaimonDataSourceConfig storageType(String v)             { this.storageType = v; return this; }
    public PaimonDataSourceConfig s3(String ep, String ak, String sk, String region) {
        this.s3Endpoint = ep; this.s3AccessKey = ak; this.s3SecretKey = sk; this.s3Region = region; return this;
    }
    public PaimonDataSourceConfig hdfs(String host, int port)       { this.hdfsHost = host; this.hdfsPort = port; return this; }
    public PaimonDataSourceConfig writeBufferSizeMb(int v)          { this.writeBufferSize = v; return this; }
    public PaimonDataSourceConfig batchAccumulationSize(int v)      { this.batchAccumulationSize = v; return this; }
    public PaimonDataSourceConfig commitIntervalMs(int v)           { this.commitIntervalMs = v; return this; }
    public PaimonDataSourceConfig enableAsyncCommit(boolean v)      { this.enableAsyncCommit = v; return this; }
    public PaimonDataSourceConfig writeThreads(int v)               { this.writeThreads = v; return this; }
    public PaimonDataSourceConfig enableAutoCompaction(boolean v)   { this.enableAutoCompaction = v; return this; }
    public PaimonDataSourceConfig targetFileSizeMb(int v)           { this.targetFileSize = v; return this; }
    public PaimonDataSourceConfig bucketMode(String v)              { this.bucketMode = v; return this; }
    public PaimonDataSourceConfig bucketCount(int v)                { this.bucketCount = v; return this; }

    // ── Getters ─────────────────────────────────────────────────────────────
    public String  getWarehouse()            { return warehouse; }
    public String  getStorageType()          { return storageType; }
    public String  getS3Endpoint()           { return s3Endpoint; }
    public String  getS3AccessKey()          { return s3AccessKey; }
    public String  getS3SecretKey()          { return s3SecretKey; }
    public String  getS3Region()             { return s3Region; }
    public String  getHdfsHost()             { return hdfsHost; }
    public Integer getHdfsPort()             { return hdfsPort; }
    public Integer getWriteBufferSize()      { return writeBufferSize; }
    public Integer getBatchAccumulationSize(){ return batchAccumulationSize; }
    public Integer getCommitIntervalMs()     { return commitIntervalMs; }
    public Boolean getEnableAsyncCommit()    { return enableAsyncCommit; }
    public Integer getPaimonWriteThreads()   { return writeThreads; }
    public Boolean getEnableAutoCompaction() { return enableAutoCompaction; }
    public Integer getTargetFileSize()       { return targetFileSize; }
    public String  getBucketMode()           { return bucketMode; }
    public Integer getBucketCount()          { return bucketCount; }
}
