package io.tapdata.oceanbase.bean;

import io.tapdata.connector.mysql.config.MysqlConfig;

import java.io.Serializable;
import java.util.Map;

public class OceanbaseConfig extends MysqlConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String tenant;
    private String rootServerList;
    private String cdcUser;
    private String cdcPassword;
    private String rawLogServerHost;
    private int rawLogServerPort;
    private Boolean useNativeCdc = false;
    /** libobcdc memory_limit passed through to the native CDC helper (e.g. "2G", "4G"). */
    private String cdcMemoryLimit = "2G";

    //customize
    public OceanbaseConfig() {
        setEscapeChar('`');
        setDbType("oceanbase");
        setJdbcDriver("com.oceanbase.jdbc.Driver");
    }

    public OceanbaseConfig load(Map<String, Object> map) {
        OceanbaseConfig config = (OceanbaseConfig) super.load(map);
        properties.put("rewriteBatchedStatements", "true");
        properties.put("tinyInt1isBit", Boolean.FALSE.toString());
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

    public String getCdcUser() {
        return cdcUser;
    }

    public void setCdcUser(String cdcUser) {
        this.cdcUser = cdcUser;
    }

    public String getCdcPassword() {
        return cdcPassword;
    }

    public void setCdcPassword(String cdcPassword) {
        this.cdcPassword = cdcPassword;
    }

    public String getRawLogServerHost() {
        return rawLogServerHost;
    }

    public void setRawLogServerHost(String rawLogServerHost) {
        this.rawLogServerHost = rawLogServerHost;
    }

    public int getRawLogServerPort() {
        return rawLogServerPort;
    }

    public void setRawLogServerPort(int rawLogServerPort) {
        this.rawLogServerPort = rawLogServerPort;
    }

    public Boolean getUseNativeCdc() {
        return useNativeCdc;
    }

    public void setUseNativeCdc(Boolean useNativeCdc) {
        this.useNativeCdc = useNativeCdc;
    }




    /** Per-task libobcdc ceiling, independent of the engine JVM heap. */
    public String getCdcMemoryLimit() {
        String value = cdcMemoryLimit == null ? "" : cdcMemoryLimit.trim();
        if (value.isEmpty()) return "2G";
        value = value.toUpperCase(java.util.Locale.ROOT);
        if (!value.matches("[1-9]\\d*[MG]")) {
            throw new IllegalArgumentException("cdcMemoryLimit must be a positive integer followed by M or G (e.g. 2G)");
        }
        return value;
    }

    public void setCdcMemoryLimit(String cdcMemoryLimit) {
        this.cdcMemoryLimit = cdcMemoryLimit;
    }
}
