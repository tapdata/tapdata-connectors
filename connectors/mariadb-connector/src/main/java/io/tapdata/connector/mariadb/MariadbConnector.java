package io.tapdata.connector.mariadb;

import io.tapdata.connector.mysql.MysqlConnector;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.function.Consumer;


@TapConnectorClass("spec_mariadb.json")
public class MariadbConnector extends MysqlConnector {

    @Override
    public void onStart(TapConnectionContext tapConnectionContext) throws Throwable {
        super.onStart(tapConnectionContext);
        MysqlJdbcContextV2 fromSuper = mysqlJdbcContext;
        mysqlJdbcContext = new MariadbJdbcContextV2(commonDbConfig);
        jdbcContext = mysqlJdbcContext;
        if (fromSuper != null) {
            try {
                fromSuper.close();
            } catch (Exception e) {
                tapLogger.error("Release connector failed, error: " + e.getMessage() + "\n" + getStackString(e));
            }
        }
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) {
        mysqlConfig = new MysqlConfig().load(connectionContext.getConnectionConfig());
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        connectionOptions.connectionString(mysqlConfig.getConnectionString());
        try (
                MariadbTest mariadbTest = new MariadbTest(mysqlConfig, consumer,connectionOptions)
        ) {
            mariadbTest.testOneByOne();
        }
        return connectionOptions;
    }
}

