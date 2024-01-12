package io.tapdata.connector.hive.config;

import io.tapdata.common.CommonDbConfig;

public class HiveConfig extends CommonDbConfig {
    private String hdfsAddr;
    public HiveConfig() {
        setDbType("hive2");
        setJdbcDriver("org.apache.hive.jdbc.HiveDriver");
        setEscapeChar('`');
    }
    public String getHdfsAddr() {
        return hdfsAddr;
    }

    public void setHdfsAddr(String hdfsAddr) {
        this.hdfsAddr = hdfsAddr;
    }
}
