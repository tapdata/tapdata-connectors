package io.tapdata.oceanbase.bean;

import io.tapdata.connector.mysql.config.MysqlConfig;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Map;

public class OceanbaseConfig extends MysqlConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String tenant;
    private int rpcPort;
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

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public int getLogProxyPort() {
        return logProxyPort;
    }

    public void setLogProxyPort(int logProxyPort) {
        this.logProxyPort = logProxyPort;
    }
}
