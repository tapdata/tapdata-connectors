package io.tapdata.connector.clickhouse.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;
import java.util.*;

public class ClickhouseConfig extends CommonDbConfig implements Serializable {

    private Integer mergeMinutes = 60;
    private Boolean mixFastWrite = false;
    private Boolean supportPk = true;
    private String engineExpr;
    private String partitionExpr;
    private String orderExpr;
    private List<LinkedHashMap<String, String>> tableProperties = new ArrayList<>();;

    public ClickhouseConfig() {
        setDbType("clickhouse");
        setEscapeChar('`');
        setJdbcDriver("ru.yandex.clickhouse.ClickHouseDriver");
    }

    @Override
    public ClickhouseConfig load(Map<String, Object> map) {
        ClickhouseConfig config = (ClickhouseConfig) super.load(map);
        setSchema(getDatabase());
        Properties properties = new Properties();
        properties.put("max_query_size", "102400000");
        setProperties(properties);
        return config;
    }

    public Integer getMergeMinutes() {
        return mergeMinutes;
    }

    public void setMergeMinutes(Integer mergeMinutes) {
        this.mergeMinutes = mergeMinutes;
    }

    public Boolean getMixFastWrite() {
        return mixFastWrite;
    }

    public void setMixFastWrite(Boolean mixFastWrite) {
        this.mixFastWrite = mixFastWrite;
    }

    public Boolean getSupportPk() {
        return supportPk;
    }

    public void setSupportPk(Boolean supportPk) {
        this.supportPk = supportPk;
    }

    public String getEngineExpr() {
        return engineExpr;
    }

    public void setEngineExpr(String engineExpr) {
        this.engineExpr = engineExpr;
    }

    public String getPartitionExpr() {
        return partitionExpr;
    }

    public void setPartitionExpr(String partitionExpr) {
        this.partitionExpr = partitionExpr;
    }

    public String getOrderExpr() {
        return orderExpr;
    }

    public void setOrderExpr(String orderExpr) {
        this.orderExpr = orderExpr;
    }

    public List<LinkedHashMap<String, String>> getTableProperties() {
        return tableProperties;
    }

    public void setTableProperties(List<LinkedHashMap<String, String>> tableProperties) {
        this.tableProperties = tableProperties;
    }
}
