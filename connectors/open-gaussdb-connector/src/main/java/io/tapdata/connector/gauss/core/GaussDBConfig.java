package io.tapdata.connector.gauss.core;

import io.tapdata.connector.postgres.config.PostgresConfig;

public class GaussDBConfig extends PostgresConfig {
    private int haPort;
    private String haHost;
    public GaussDBConfig() {
        setDbType("opengauss");
        setJdbcDriver("com.huawei.opengauss.jdbc.Driver");
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
}
