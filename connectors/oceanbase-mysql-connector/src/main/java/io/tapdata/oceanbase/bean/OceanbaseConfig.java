package io.tapdata.oceanbase.bean;

import io.tapdata.connector.mysql.config.MysqlConfig;

import java.io.Serializable;
import java.util.Map;

public class OceanbaseConfig extends MysqlConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String tenant;
    private String rootServerList;
    private String logProxyHost;
    private int logProxyPort;

    //customize
    public OceanbaseConfig() {
        setEscapeChar('`');
        setDbType("oceanbase");
        setJdbcDriver("com.oceanbase.jdbc.Driver");
    }

    public OceanbaseConfig load(Map<String, Object> map) {
        OceanbaseConfig config = (OceanbaseConfig) super.load(map);
        properties.put("rewriteBatchedStatements", "true");
        setSchema(getDatabase());
        return config;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getRootServerList() {
        return rootServerList;
    }

    public void setRootServerList(String rootServerList) {
        this.rootServerList = rootServerList;
    }

    public String getLogProxyHost() {
        return logProxyHost;
    }

    public void setLogProxyHost(String logProxyHost) {
        this.logProxyHost = logProxyHost;
    }

    public int getLogProxyPort() {
        return logProxyPort;
    }

    public void setLogProxyPort(int logProxyPort) {
        this.logProxyPort = logProxyPort;
    }
}
