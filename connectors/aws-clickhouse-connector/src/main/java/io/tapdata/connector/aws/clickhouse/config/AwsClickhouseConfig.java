package io.tapdata.connector.aws.clickhouse.config;

import io.tapdata.connector.clickhouse.config.ClickhouseConfig;

import java.util.Map;
import java.util.Properties;


public class AwsClickhouseConfig extends ClickhouseConfig {


    public AwsClickhouseConfig() {
        setDbType("clickhouse");
        setEscapeChar('`');
        setJdbcDriver("com.clickhouse.jdbc.ClickHouseDriver");
    }

    @Override
    public ClickhouseConfig load(Map<String, Object> map) {
        AwsClickhouseConfig config = (AwsClickhouseConfig) super.load(map);
        setSchema(getDatabase());
        Properties properties = new Properties();
        properties.put("max_query_size", "102400000");
        setProperties(properties);
        return config;
    }

}
