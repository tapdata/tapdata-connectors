package io.tapdata.connector.paimon.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;
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

    // Bucket mode: "dynamic" or "fixed"
    // Dynamic mode: better for general use, uses StreamTableWrite
    // Fixed mode: better performance, uses BatchTableWrite
    private String bucketMode = "dynamic";

    // Bucket count for fixed bucket mode (only used when bucketMode = "fixed")
    // Must be > 0 when using fixed mode
    private Integer bucketCount = 4;

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

    public String getBucketMode() {
        return bucketMode;
    }

    public void setBucketMode(String bucketMode) {
        this.bucketMode = bucketMode;
    }

    public Integer getBucketCount() {
        return bucketCount;
    }

    public void setBucketCount(Integer bucketCount) {
        this.bucketCount = bucketCount;
    }

    /**
     * Check if using dynamic bucket mode
     *
     * @return true if using dynamic bucket mode
     */
    public boolean isDynamicBucketMode() {
        return "dynamic".equalsIgnoreCase(bucketMode);
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

        if (bucketMode == null || bucketMode.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket mode is required");
        }

        if (!"dynamic".equalsIgnoreCase(bucketMode) && !"fixed".equalsIgnoreCase(bucketMode)) {
            throw new IllegalArgumentException("Bucket mode must be either 'dynamic' or 'fixed'");
        }

        // Validate bucket count only for fixed mode
        if ("fixed".equalsIgnoreCase(bucketMode)) {
            if (bucketCount == null || bucketCount <= 0) {
                throw new IllegalArgumentException("Bucket count must be greater than 0 when using fixed bucket mode");
            }
        }
        
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

