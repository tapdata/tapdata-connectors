package io.tapdata.connector.aws.clickhouse.config;

import io.tapdata.connector.clickhouse.config.ClickhouseConfig;


public class AwsClickhouseConfig extends ClickhouseConfig {


    public AwsClickhouseConfig() {
        setDbType("clickhouse");
        setEscapeChar('`');
        setJdbcDriver("com.clickhouse.jdbc.ClickHouseDriver");
    }

}
