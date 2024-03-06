package io.tapdata.connector.gauss.core;

import io.tapdata.connector.postgres.config.PostgresConfig;

public class GaussDBConfig extends PostgresConfig {
    protected static final String dbType = "opengauss";
    protected static final String jdbcDriver = "com.huawei.opengauss.jdbc.Driver";
    private int haPort;
    private String haHost;

    public GaussDBConfig() {
        setDbType(dbType);
        setJdbcDriver(jdbcDriver);
    }

    public int getHaPort() {
        return haPort;
    }

    public void setHaPort(int haPort) {
        this.haPort = haPort;
    }

    public String getHaHost() {
        return haHost;
    }

    public void setHaHost(String haHost) {
        this.haHost = haHost;
    }

    public static GaussDBConfig instance() {
        return new GaussDBConfig();
    }
}
