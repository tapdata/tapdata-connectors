package io.tapdata.connector.tdengine.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;
import java.util.List;

/**
 * TDengine database config
 */
public class TDengineConfig extends CommonDbConfig implements Serializable {

    private Boolean supportWebSocket = false;
    private int originPort = 6030;
    private Boolean supportSuperTable = false;
    private List<String> superTableTags;
    private String subTableNameType = "AutoHash";
    private String subTableSuffix = "";
    private List<String> loadTableOptions;

    //customize
    public TDengineConfig() {
        setEscapeChar('`');
        setDbType("TAOS-RS");
        setJdbcDriver("com.taosdata.jdbc.rs.RestfulDriver");
    }

    public Boolean getSupportWebSocket() {
        return supportWebSocket;
    }

    public void setSupportWebSocket(Boolean supportWebSocket) {
        this.supportWebSocket = supportWebSocket;
    }

    public int getOriginPort() {
        return originPort;
    }

    public void setOriginPort(int originPort) {
        this.originPort = originPort;
    }

    public List<String> getSuperTableTags() {
        return superTableTags;
    }

    public void setSuperTableTags(List<String> superTableTags) {
        this.superTableTags = superTableTags;
    }

    public Boolean getSupportSuperTable() {
        return supportSuperTable;
    }

    public void setSupportSuperTable(Boolean supportSuperTable) {
        this.supportSuperTable = supportSuperTable;
    }

    public String getSubTableNameType() {
        return subTableNameType;
    }

    public void setSubTableNameType(String subTableNameType) {
        this.subTableNameType = subTableNameType;
    }

    public String getSubTableSuffix() {
        return subTableSuffix;
    }

    public void setSubTableSuffix(String subTableSuffix) {
        this.subTableSuffix = subTableSuffix;
    }

    public List<String> getLoadTableOptions() {
        return loadTableOptions;
    }

    public void setLoadTableOptions(List<String> loadTableOptions) {
        this.loadTableOptions = loadTableOptions;
    }
}
