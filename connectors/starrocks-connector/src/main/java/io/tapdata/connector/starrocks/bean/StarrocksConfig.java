package io.tapdata.connector.starrocks.bean;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.kit.EmptyKit;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jarad
 * @date 7/14/22
 */
public class StarrocksConfig extends CommonDbConfig {

    private String starrocksHttp;
    private List<String> duplicateKey;
    private List<String> distributedKey;
    private Integer writeByteBufferCapacity = 10240;
    private String writeFormat = "json";
    private String uniqueKeyType = "Unique";
    private int bucket = 2;
    private List<LinkedHashMap<String, String>> tableProperties = new ArrayList<>();
    private Boolean jdbcCompletion = false;

    private Boolean useHTTPS =false;

    private Integer backendNum;

    //customize
    public StarrocksConfig() {
        setDbType("starrocks");
        setEscapeChar('`');
        setJdbcDriver("com.mysql.cj.jdbc.Driver");
    }

    public StarrocksConfig load(Map<String, Object> map) {
        StarrocksConfig config = (StarrocksConfig) super.load(map);
        config.setSchema(config.getDatabase());
        if(Boolean.TRUE.equals(useHTTPS)){
            config.setStarrocksHttp(getStarrocksHttp().replace("https://", ""));
        }else{
            config.setStarrocksHttp(getStarrocksHttp().replace("http://", ""));
        }
        return config;
    }

    public String getDatabaseUrlPattern() {
        return "jdbc:mysql://%s:%s/%s?rewriteBatchedStatements=true%s";
    }

    @Override
    public String getConnectionString() {
        return getHost() + ":" + getPort() + "/" + getDatabase();
    }

    public String getDatabaseUrl() {
        if (EmptyKit.isNull(this.getExtParams())) {
            this.setExtParams("");
        }
        if (EmptyKit.isNotEmpty(this.getExtParams()) && (this.getExtParams().startsWith("?") || this.getExtParams().startsWith(":"))) {
            this.setExtParams(this.getExtParams().substring(1));
        }
        return String.format(this.getDatabaseUrlPattern(), this.getHost(), this.getPort(), this.getDatabase(), this.getExtParams());
    }

    public String getStarrocksHttp() {
        return starrocksHttp;
    }

    public void setStarrocksHttp(String starrocksHttp) {
        this.starrocksHttp = starrocksHttp;
    }

    public List<String> getDuplicateKey() {
        return duplicateKey;
    }

    public void setDuplicateKey(List<String> duplicateKey) {
        this.duplicateKey = duplicateKey;
    }

    public List<String> getDistributedKey() {
        return distributedKey;
    }

    public void setDistributedKey(List<String> distributedKey) {
        this.distributedKey = distributedKey;
    }

    public Integer getWriteByteBufferCapacity() {
        return writeByteBufferCapacity;
    }

    public void setWriteByteBufferCapacity(Integer writeByteBufferCapacity) {
        this.writeByteBufferCapacity = writeByteBufferCapacity;
    }

    public String getWriteFormat() {
        return writeFormat;
    }

    public void setWriteFormat(String writeFormat) {
        this.writeFormat = writeFormat;
    }

    public String getUniqueKeyType() {
        return uniqueKeyType;
    }

    public void setUniqueKeyType(String uniqueKeyType) {
        this.uniqueKeyType = uniqueKeyType;
    }

    public int getBucket() {
        return bucket;
    }

    public Integer getBackendNum() {
        return backendNum;
    }

    public void setBackendNum(Integer backendNum) {
        this.backendNum = backendNum;
    }

    public Boolean getUseHTTPS() {
        return useHTTPS;
    }

    public void setUseHTTPS(Boolean useHTTPS) {
        this.useHTTPS = useHTTPS;
    }

    public void setBucket(int bucket) {
        this.bucket = bucket;
    }

    public List<LinkedHashMap<String, String>> getTableProperties() {
        return tableProperties;
    }

    public void setTableProperties(List<LinkedHashMap<String, String>> tableProperties) {
        this.tableProperties = tableProperties;
    }

    public Boolean getJdbcCompletion() {
        return jdbcCompletion;
    }

    public void setJdbcCompletion(Boolean jdbcCompletion) {
        this.jdbcCompletion = jdbcCompletion;
    }

    public enum WriteFormat {
        json,
        csv,
    }

    public WriteFormat getWriteFormatEnum() {
        return WriteFormat.valueOf(writeFormat);
    }
}
