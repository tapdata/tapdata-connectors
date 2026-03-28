package io.tapdata.connector.snowflake;

import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.snowflake.config.SnowflakeConfig;
import io.tapdata.constant.DbTestItem;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.TapTestHostPortEx;
import io.tapdata.util.NetUtil;

import java.io.IOException;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

/**
 * Snowflake Connection Test
 *
 * @author Jarad
 * @date 2026/03/24
 */
public class SnowflakeTest extends CommonDbTest {

    public SnowflakeTest(SnowflakeConfig config, Consumer<TestItem> consumer) {
        super(config, consumer);
        jdbcContext = new SnowflakeJdbcContext(config);
    }

    protected Boolean testHostPort() {
        try {
            NetUtil.validateHostPortWithSocket(((SnowflakeConfig) commonDbConfig).getAccount() + ".snowflakecomputing.com", 443);
            consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY,
                    String.format(TEST_HOST_PORT_MESSAGE, ((SnowflakeConfig) commonDbConfig).getAccount() + ".snowflakecomputing.com", 443)));
            return true;
        } catch (IOException e) {
            consumer.accept(new TestItem(DbTestItem.HOST_PORT.getContent(), new TapTestHostPortEx(e, ((SnowflakeConfig) commonDbConfig).getAccount() + ".snowflakecomputing.com", "443"), TestItem.RESULT_FAILED));
            return false;
        }
    }

    private void testWarehouse() {
//        TestItem testItem = new TestItem("Test Warehouse", TestItem.RESULT_SUCCESSFULLY, null);
//        if (connection == null) {
//            testItem.setResult(TestItem.RESULT_FAILED);
//            testItem.setInformation("Connection not established");
//            consumer.accept(testItem);
//            return;
//        }
//
//        try (Statement stmt = connection.createStatement()) {
//            String sql = "SHOW WAREHOUSES LIKE '" + config.getWarehouse() + "'";
//            ResultSet rs = stmt.executeQuery(sql);
//            if (rs.next()) {
//                testItem.setResult(TestItem.RESULT_SUCCESSFULLY);
//            } else {
//                testItem.setResult(TestItem.RESULT_FAILED);
//                testItem.setInformation("Warehouse '" + config.getWarehouse() + "' not found");
//            }
//        } catch (Exception e) {
//            testItem.setResult(TestItem.RESULT_FAILED);
//            testItem.setInformation("Failed to access warehouse: " + e.getMessage());
//        }
//        consumer.accept(testItem);
    }
}

