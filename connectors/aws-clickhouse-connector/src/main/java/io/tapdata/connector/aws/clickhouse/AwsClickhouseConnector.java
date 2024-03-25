package io.tapdata.connector.aws.clickhouse;

import io.tapdata.connector.aws.clickhouse.config.AwsClickhouseConfig;
import io.tapdata.connector.clickhouse.ClickhouseConnector;
import io.tapdata.connector.clickhouse.config.ClickhouseConfig;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.sql.SQLException;
import java.util.function.Consumer;

@TapConnectorClass("aws_clickhouse.json")
public class AwsClickhouseConnector extends ClickhouseConnector {


    @Override
    protected void initConnection(TapConnectionContext connectionContext) throws SQLException {
        super.initConnection(connectionContext);
        clickhouseConfig = new AwsClickhouseConfig().load(connectionContext.getConnectionConfig());
    }
    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        clickhouseConfig = new ClickhouseConfig().load(connectionContext.getConnectionConfig());
        try (
                AwsClickhouseTest clickhouseTest = new AwsClickhouseTest(clickhouseConfig, consumer)
        ) {
            clickhouseTest.testOneByOne();
            return connectionOptions;
        }
    }
}
