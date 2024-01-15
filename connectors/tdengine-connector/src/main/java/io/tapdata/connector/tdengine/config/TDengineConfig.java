package io.tapdata.connector.tdengine.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

/**
 * TDengine database config
 */
public class TDengineConfig extends CommonDbConfig implements Serializable {

    private Boolean supportWebSocket = false;
    private int originPort = 6030;

    //customize
    public TDengineConfig() {
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
}
