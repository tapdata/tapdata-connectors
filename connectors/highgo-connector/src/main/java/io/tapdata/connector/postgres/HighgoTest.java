package io.tapdata.connector.postgres;

import com.highgo.jdbc.Driver;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.TapTestStreamReadEx;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class HighgoTest extends PostgresTest {

    public HighgoTest(HighgoConfig highgoConfig, Consumer<TestItem> consumer, ConnectionOptions connectionOptions) {
        super(highgoConfig, consumer, connectionOptions);
    }

    @Override
    public HighgoTest initContext() {
        jdbcContext = new PostgresJdbcContext((HighgoConfig) commonDbConfig);
        return this;
    }

    public Boolean testStreamRead() {
        if ("walminer".equals(((PostgresConfig) commonDbConfig).getLogPluginName())) {
            return testWalMiner();
        }
        Properties properties = new Properties();
        properties.put("user", commonDbConfig.getUser());
        properties.put("password", commonDbConfig.getPassword());
        properties.put("replication", "database");
        properties.put("assumeMinServerVersion", "9.4");
        try {
            Connection connection = new Driver().connect(commonDbConfig.getDatabaseUrl(), properties);
            assert connection != null;
            connection.close();
            List<String> testSqls = TapSimplify.list();
            String testSlotName = "test_tapdata_" + UUID.randomUUID().toString().replaceAll("-", "_");
            testSqls.add(String.format(PG_LOG_PLUGIN_CREATE_TEST, testSlotName, ((PostgresConfig) commonDbConfig).getLogPluginName()));
            testSqls.add(PG_LOG_PLUGIN_DROP_TEST);
            jdbcContext.batchExecute(testSqls);
            consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY, "Cdc can work normally"));
            return true;
        } catch (Throwable e) {
            consumer.accept(new TestItem(TestItem.ITEM_READ_LOG, new TapTestStreamReadEx(e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            return null;
        }
    }

}
