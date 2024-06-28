package io.tapdata.connector.gauss.core;

import io.tapdata.connector.postgres.config.PostgresConfig;

public class GaussDBConfig extends PostgresConfig {
    protected static final String DB_TYPE = "opengauss";
    protected static final String JDBC_DRIVER = "com.huawei.opengauss.jdbc.Driver";
    private int haPort;
    private String haHost;

    public GaussDBConfig() {
        setDbType(DB_TYPE);
        setJdbcDriver(JDBC_DRIVER);
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
