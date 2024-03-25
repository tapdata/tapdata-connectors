package io.tapdata.connector.aws.clickhouse;

import io.tapdata.connector.aws.clickhouse.config.AwsClickhouseConfig;
import io.tapdata.connector.clickhouse.ClickhouseConnector;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;

import java.sql.SQLException;
@TapConnectorClass("aws_clickhouse.json")
public class AwsClickhouseConnector extends ClickhouseConnector {


    @Override
    protected void initConnection(TapConnectionContext connectionContext) throws SQLException {
        super.initConnection(connectionContext);
        clickhouseConfig = new AwsClickhouseConfig().load(connectionContext.getConnectionConfig());
    }
}
