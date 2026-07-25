package io.tapdata.connector.paimon.config;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.kit.EmptyKit;
import org.apache.paimon.table.BucketMode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Configuration for Paimon connector
 *
 * @author Tapdata
 */
public class PaimonConfig extends CommonDbConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // Warehouse path
    private String warehouse;
    
    // Storage type: s3, hdfs, oss, local
    private String storageType = "local";
    private List<LinkedHashMap<String, String>> s3Properties = new ArrayList<>();
    
    // S3 configuration
    private String s3Endpoint;
    private String s3AccessKey;
    private String s3SecretKey;
    private String s3Region;
    
    // HDFS configuration
    private String hdfsHost;
    private Integer hdfsPort = 9000;
    private String hdfsUser = "hadoop";
    
    // OSS configuration
    private String ossEndpoint;
    private String ossAccessKey;
    private String ossSecretKey;
    
    // Database name (Paimon database)
    private String database = "default";

    private Boolean hashKey = false;
    private List<String> partitionKey;

    /*
     * Connector-level bucket choice. All modes use one StreamTableWrite per physical table.
     * "dynamic" writes bucket=-1, "postpone" writes bucket=-2, and "fixed" requires a positive
     * bucket count. Paimon materializes the final BucketMode from the schema and table shape.
     *
     * Sources:
     * https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L100-L112
     * https://github.com/apache/paimon/blob/release-1.3.1/paimon-common/src/main/java/org/apache/paimon/table/BucketMode.java#L63-L73
     * https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/KeyValueFileStore.java#L99-L109
     * https://github.com/apache/paimon/blob/release-1.3.1/paimon-core/src/main/java/org/apache/paimon/AppendOnlyFileStore.java#L72-L75
     */
    private String bucketMode = "dynamic";

    // Bucket count for fixed bucket mode (only used when bucketMode = "fixed")
    // Must be > 0 when using fixed mode
    private Integer bucketCount = 4;

    private String fileFormat = "";

    private String compression = "";

    private List<LinkedHashMap<String, String>> tableProperties = new ArrayList<>();

    // ===== Performance Optimization Settings =====

    // Write buffer size in MB (default: 256MB)
    // Larger buffer = better performance but more memory usage
    private Integer writeBufferSize = 256;

    private Boolean diskOverflowWrite = false;

    private Integer diskMaxSize = 1;

    private String diskTmpDir = "/tmp";

    // Batch accumulation size before commit (default: 10000 records)
    // 0 = commit immediately (no batching)
    private Integer batchAccumulationSize = 100000;

    // Initial-sync buffer timing knob. CDC currently commits synchronously before every callback
    // return because PDK 2.0.8 exposes no verified durable asynchronous-offset contract.
    private Integer commitIntervalMs = 30000;

    // Reserved compatibility knob: PaimonService intentionally disables asynchronous CDC commit
    // until source-offset acknowledgement and ordering can be proven durable.
    private Boolean enableAsyncCommit = true;

    // Currently copied to the Flink-only "sink.parallelism" table option. This direct Paimon Core
    // writer does not create connector write threads from the value.
    private Integer writeThreads = 4;

    // Enable auto compaction (default: true)
    // Compaction merges small files for better query performance
    private Boolean enableAutoCompaction = true;

    // Full Compaction interval in minutes (default: 60 minutes)
    private Integer compactionIntervalMinutes = 60;

    // Target file size in MB (default: 128MB)
    // Paimon will try to create files of this size
    private Integer targetFileSize = 128;

    // Enable primary key update detection (default: false)
    // When enabled, automatically detects primary key changes and converts update operations to delete+insert
    // Requires source database to provide before-update data
    private Boolean enablePrimaryKeyUpdate = false;

    public String getWarehouse() {
        return warehouse;
    }

    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    public String getStorageType() {
        return storageType;
    }

    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    public List<LinkedHashMap<String, String>> getS3Properties() {
        return s3Properties;
    }

    public void setS3Properties(List<LinkedHashMap<String, String>> s3Properties) {
        this.s3Properties = s3Properties;
    }

    public String getS3Endpoint() {
        return s3Endpoint;
    }

    public void setS3Endpoint(String s3Endpoint) {
        this.s3Endpoint = s3Endpoint;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public void setS3AccessKey(String s3AccessKey) {
        this.s3AccessKey = s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }

    public void setS3SecretKey(String s3SecretKey) {
        this.s3SecretKey = s3SecretKey;
    }

    public String getS3Region() {
        return s3Region;
    }

    public void setS3Region(String s3Region) {
        this.s3Region = s3Region;
    }

    public String getHdfsHost() {
        return hdfsHost;
    }

    public void setHdfsHost(String hdfsHost) {
        this.hdfsHost = hdfsHost;
    }

    public Integer getHdfsPort() {
        return hdfsPort;
    }

    public void setHdfsPort(Integer hdfsPort) {
        this.hdfsPort = hdfsPort;
    }

    public String getHdfsUser() {
        return hdfsUser;
    }

    public void setHdfsUser(String hdfsUser) {
        this.hdfsUser = hdfsUser;
    }

    public String getOssEndpoint() {
        return ossEndpoint;
    }

    public void setOssEndpoint(String ossEndpoint) {
        this.ossEndpoint = ossEndpoint;
    }

    public String getOssAccessKey() {
        return ossAccessKey;
    }

    public void setOssAccessKey(String ossAccessKey) {
        this.ossAccessKey = ossAccessKey;
    }

    public String getOssSecretKey() {
        return ossSecretKey;
    }

    public void setOssSecretKey(String ossSecretKey) {
        this.ossSecretKey = ossSecretKey;
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public void setDatabase(String database) {
        this.database = database;
    }

    public Boolean getHashKey() {
        return hashKey;
    }

    public Boolean getHashKey(String key) {
        return getTableConfigValue(key, "hashKey", hashKey);
    }

    public void setHashKey(Boolean hashKey) {
        this.hashKey = hashKey;
    }

    public List<String> getPartitionKey() {
        return partitionKey;
    }

    public List<String> getPartitionKey(String key) {
        return getTableConfigValue(key, "partitionKey", partitionKey);
    }

    public void setPartitionKey(List<String> partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getBucketMode() {
        return bucketMode;
    }

    public String getBucketMode(String key) {
        return getTableConfigValue(key, "bucketMode", bucketMode);
    }

    public void setBucketMode(String bucketMode) {
        this.bucketMode = bucketMode;
    }

    public Integer getBucketCount() {
        return bucketCount;
    }

    public Integer getBucketCount(String key) {
        return getTableConfigValue(key, "bucketCount", bucketCount);
    }

    public void setBucketCount(Integer bucketCount) {
        this.bucketCount = bucketCount;
    }

    /**
     * Resolve the first-class bucket configuration for one logical table to Paimon's native
     * {@code bucket} value.
     *
     * <p>Paimon 1.3.1 defines -1 as dynamic, -2 as postpone, and positive values as fixed bucket
     * mode. A non-positive fixed count is never reinterpreted as another mode.
     *
     * <p>Sources:
     * https://github.com/apache/paimon/blob/release-1.3.1/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L100-L112
     * https://github.com/apache/paimon/blob/release-1.3.1/paimon-common/src/main/java/org/apache/paimon/table/BucketMode.java#L63-L73
     *
     * @param tableName logical table name used for per-table configuration lookup
     * @return Paimon's native bucket value
     */
    public int resolveBucket(String tableName) {
        return resolveBucket(getBucketMode(tableName), getBucketCount(tableName));
    }

    private static int resolveBucket(String mode, Integer fixedBucketCount) {
        if (mode == null || mode.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket mode is required");
        }

        switch (mode.trim().toLowerCase(Locale.ROOT)) {
            case "dynamic":
                return -1;
            case "postpone":
                return BucketMode.POSTPONE_BUCKET;
            case "fixed":
                if (fixedBucketCount == null || fixedBucketCount <= 0) {
                    throw new IllegalArgumentException(
                            "Bucket count must be greater than 0 when using fixed bucket mode");
                }
                return fixedBucketCount;
            default:
                throw new IllegalArgumentException(
                        "Bucket mode must be one of 'dynamic', 'postpone', or 'fixed'");
        }
    }

    /**
     * Check if using dynamic bucket mode
     *
     * @return true if using dynamic bucket mode
     */
    public boolean isDynamicBucketMode() {
        return "dynamic".equalsIgnoreCase(bucketMode);
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public String getFileFormat(String key) {
        return getTableConfigValue(key, "fileFormat", fileFormat);
    }

    public void setFileFormat(String fileFormat) {
        this.fileFormat = fileFormat;
    }

    public String getCompression() {
        return compression;
    }

    public String getCompression(String key) {
        return getTableConfigValue(key, "compression", compression);
    }

    public void setCompression(String compression) {
        this.compression = compression;
    }

    public List<LinkedHashMap<String, String>> getTableProperties() {
        return tableProperties;
    }

    public List<LinkedHashMap<String, String>> getTableProperties(String key) {
        return getTableConfigValue(key, "tableProperties", tableProperties);
    }

    public void setTableProperties(List<LinkedHashMap<String, String>> tableProperties) {
        this.tableProperties = tableProperties;
    }

    public Integer getWriteBufferSize() {
        return writeBufferSize;
    }

    public void setWriteBufferSize(Integer writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
    }

    public Boolean getDiskOverflowWrite() {
        return diskOverflowWrite;
    }

    public void setDiskOverflowWrite(Boolean diskOverflowWrite) {
        this.diskOverflowWrite = diskOverflowWrite;
    }

    public Integer getDiskMaxSize() {
        return diskMaxSize;
    }

    public void setDiskMaxSize(Integer diskMaxSize) {
        this.diskMaxSize = diskMaxSize;
    }

    public String getDiskTmpDir() {
        return diskTmpDir;
    }

    public String getDiskTmpDir(String key) {
        return getTableConfigValue(key, "diskTmpDir", diskTmpDir);
    }

    public void setDiskTmpDir(String diskTmpDir) {
        this.diskTmpDir = diskTmpDir;
    }

    public Integer getBatchAccumulationSize() {
        return batchAccumulationSize;
    }

    public void setBatchAccumulationSize(Integer batchAccumulationSize) {
        this.batchAccumulationSize = batchAccumulationSize;
    }

    public Integer getCommitIntervalMs() {
        return commitIntervalMs;
    }

    public void setCommitIntervalMs(Integer commitIntervalMs) {
        this.commitIntervalMs = commitIntervalMs;
    }

    public Boolean getEnableAsyncCommit() {
        return enableAsyncCommit;
    }

    public void setEnableAsyncCommit(Boolean enableAsyncCommit) {
        this.enableAsyncCommit = enableAsyncCommit;
    }

    public Integer getWriteThreads() {
        return writeThreads;
    }

    public void setWriteThreads(Integer writeThreads) {
        this.writeThreads = writeThreads;
    }

    public Boolean getEnableAutoCompaction() {
        return enableAutoCompaction;
    }

    public Boolean getEnableAutoCompaction(String key) {
        return getTableConfigValue(key, "enableAutoCompaction", enableAutoCompaction);
    }

    public void setEnableAutoCompaction(Boolean enableAutoCompaction) {
        this.enableAutoCompaction = enableAutoCompaction;
    }

    public Integer getCompactionIntervalMinutes() {
        return compactionIntervalMinutes;
    }

    public Integer getCompactionIntervalMinutes(String key) {
        return getTableConfigValue(key, "compactionIntervalMinutes", compactionIntervalMinutes);
    }

    public void setCompactionIntervalMinutes(Integer compactionIntervalMinutes) {
        this.compactionIntervalMinutes = compactionIntervalMinutes;
    }

    public Integer getTargetFileSize() {
        return targetFileSize;
    }

    public Integer getTargetFileSize(String key) {
        return getTableConfigValue(key, "targetFileSize", targetFileSize);
    }

    public void setTargetFileSize(Integer targetFileSize) {
        this.targetFileSize = targetFileSize;
    }

    public Boolean getEnablePrimaryKeyUpdate() {
        return enablePrimaryKeyUpdate;
    }

    public Boolean getEnablePrimaryKeyUpdate(String key) {
        return getTableConfigValue(key, "enablePrimaryKeyUpdate", enablePrimaryKeyUpdate);
    }

    public void setEnablePrimaryKeyUpdate(Boolean enablePrimaryKeyUpdate) {
        this.enablePrimaryKeyUpdate = enablePrimaryKeyUpdate;
    }

    /**
     * Override load method to return PaimonConfig type
     *
     * @param map configuration map
     * @return PaimonConfig instance
     */
    @Override
    public PaimonConfig load(Map<String, Object> map) {
        return (PaimonConfig) super.load(map);
    }

    /**
     * Check if Paimon S3 FileIO is available on the classpath
     *
     * @return true if paimon-s3 is available, false otherwise
     */
    private static boolean isPaimonS3Available() {
        /*
         * REVIEW: paimon-s3 1.3.1 packages S3FileIO below the nested paimon-plugin-s3 directory and
         * exposes the root-classpath S3Loader as the FileIOLoader. Class.forName(S3FileIO) therefore
         * returns false with the connector's current artifact, selecting s3a:// even though the
         * native loader is packaged. Capability detection should use FileIO loader discovery.
         * Source:
         * https://github.com/apache/paimon/blob/release-1.3.1/paimon-filesystems/paimon-s3/src/main/java/org/apache/paimon/s3/S3Loader.java#L35-L83
         */
        try {
            Class.forName("org.apache.paimon.s3.S3FileIO");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Get the full warehouse path based on storage type
     *
     * Normalization rules:
     * - If a scheme is already present (e.g. s3a://, s3://, hdfs://), respect user's choice:
     *   - For s3://, check if paimon-s3 is available; if not, fall back to s3a://
     *   - For other schemes, keep as-is
     * - If no scheme and storage type is S3:
     *   - Prefer s3:// if paimon-s3 is available
     *   - Otherwise use s3a:// (Hadoop S3A)
     * - If storage type is not S3 and no scheme present, apply the corresponding default
     *
     * @return full warehouse path
     */
    public String getFullWarehousePath() {
        if (warehouse == null || warehouse.trim().isEmpty()) {
            throw new IllegalArgumentException("Warehouse path cannot be empty");
        }

        String w = warehouse.trim();
        String st = storageType == null ? "" : storageType.trim().toLowerCase();

        // Normalize explicit scheme when present
        int schemeIdx = w.indexOf("://");
        if (schemeIdx > 0) {
            // If user explicitly provided s3://, check if paimon-s3 is available
            if (w.startsWith("s3://")) {
                if (isPaimonS3Available()) {
                    // paimon-s3 is available, use native s3:// support
                    return w;
                } else {
                    // Fall back to Hadoop S3A
                    return "s3a://" + w.substring("s3://".length());
                }
            }
            // For other schemes (s3a://, s3n://, hdfs://, oss://, file://), keep as-is
            return w;
        }

        // Add protocol based on storage type
        switch (st) {
            case "s3":
                // Prefer native s3:// if paimon-s3 is available, otherwise use Hadoop S3A
                if (isPaimonS3Available()) {
                    return "s3://" + w;
                } else {
                    return "s3a://" + w;
                }
            case "hdfs":
                return "hdfs://" + hdfsHost + ":" + hdfsPort + w;
            case "oss":
                return "oss://" + w;
            case "local":
            default:
                return "file://" + w;
        }
    }


    public String getConnectionString() {
        String st = storageType == null ? "" : storageType.trim().toLowerCase();
        switch (st) {
            case "s3":
                // Prefer native s3:// if paimon-s3 is available, otherwise use Hadoop S3A
                if (isPaimonS3Available()) {
                    return "s3://" + removeProtocol(s3Endpoint) + "/" + removeProtocol(warehouse.trim());
                } else {
                    return "s3a://" + removeProtocol(s3Endpoint) + "/" + removeProtocol(warehouse.trim());
                }
            case "hdfs":
                return "hdfs://" + hdfsHost + ":" + hdfsPort + "/" + removeProtocol(warehouse.trim());
            case "oss":
                return "oss://" + removeProtocol(ossEndpoint) + "/" + removeProtocol(warehouse.trim());
            case "local":
            default:
                return "file://" + warehouse.trim();
        }
    }

    private String removeProtocol(String str) {
        if(EmptyKit.isEmpty(str)) {
            return "";
        }
        return str.substring(str.indexOf("://") + 3);
    }
    /**
     * Validate configuration
     *
     * @throws IllegalArgumentException if configuration is invalid
     */
    public void validate() {
        if (warehouse == null || warehouse.trim().isEmpty()) {
            throw new IllegalArgumentException("Warehouse path is required");
        }

        if (storageType == null || storageType.trim().isEmpty()) {
            throw new IllegalArgumentException("Storage type is required");
        }

        resolveBucket(bucketMode, bucketCount);
        
        switch (storageType.toLowerCase()) {
            case "s3":
                if (s3Endpoint == null || s3Endpoint.trim().isEmpty()) {
                    throw new IllegalArgumentException("S3 endpoint is required for S3 storage");
                }
                if (s3AccessKey == null || s3AccessKey.trim().isEmpty()) {
                    throw new IllegalArgumentException("S3 access key is required for S3 storage");
                }
                if (s3SecretKey == null || s3SecretKey.trim().isEmpty()) {
                    throw new IllegalArgumentException("S3 secret key is required for S3 storage");
                }
                break;
            case "hdfs":
                if (hdfsHost == null || hdfsHost.trim().isEmpty()) {
                    throw new IllegalArgumentException("HDFS host is required for HDFS storage");
                }
                break;
            case "oss":
                if (ossEndpoint == null || ossEndpoint.trim().isEmpty()) {
                    throw new IllegalArgumentException("OSS endpoint is required for OSS storage");
                }
                if (ossAccessKey == null || ossAccessKey.trim().isEmpty()) {
                    throw new IllegalArgumentException("OSS access key is required for OSS storage");
                }
                if (ossSecretKey == null || ossSecretKey.trim().isEmpty()) {
                    throw new IllegalArgumentException("OSS secret key is required for OSS storage");
                }
                break;
            case "local":
                // No additional validation needed for local storage
                break;
            default:
                throw new IllegalArgumentException("Unsupported storage type: " + storageType);
        }
    }
}
