package io.tapdata.connector.aws.clickhouse;

import io.tapdata.connector.clickhouse.ClickhouseTest;
import io.tapdata.connector.clickhouse.config.ClickhouseConfig;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;

public class AwsClickhouseTest extends ClickhouseTest {
    public AwsClickhouseTest(ClickhouseConfig clickhouseConfig, Consumer<TestItem> consumer, ConnectionOptions connectionOptions) {
        super(clickhouseConfig, consumer,connectionOptions);
    }


    @Override
    protected Boolean testHostPort() {
        return true;
    }
}
