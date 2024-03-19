package io.tapdata.oceanbase.bean;

import io.tapdata.common.CommonDbConfig;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Map;

public class OceanbaseConfig extends CommonDbConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private String databaseUrlPattern = "jdbc:mysql://%s:%s/%s?rewriteBatchedStatements=true&useSSL=false";

    private String tenant;
    private int rpcPort;
    private int logProxyPort;

    //customize
    public OceanbaseConfig() {
        setEscapeChar('`');
        setDbType("oceanbase");
        setJdbcDriver("com.mysql.jdbc.Driver");
    }

    public OceanbaseConfig load(Map<String, Object> map) {
        OceanbaseConfig config = (OceanbaseConfig) super.load(map);
        setSchema(getDatabase());
        return config;
    }

    public String getDatabaseUrl() {
        String url = this.databaseUrlPattern;
        if (StringUtils.isNotBlank(getExtParams())) {
            url += "?" + getExtParams();
        }
        return String.format(url, this.getHost(), this.getPort(), this.getDatabase());
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
