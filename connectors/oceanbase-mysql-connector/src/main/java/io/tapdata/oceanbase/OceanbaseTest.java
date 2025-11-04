package io.tapdata.oceanbase;

import io.tapdata.connector.mysql.MysqlConnectionTest;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.oceanbase.bean.OceanbaseConfig;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.TapTestVersionEx;
import io.tapdata.util.NetUtil;

import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class OceanbaseTest extends MysqlConnectionTest implements AutoCloseable {

    public OceanbaseTest(OceanbaseConfig oceanbaseConfig, Consumer<TestItem> consumer, ConnectionOptions connectionOptions) {
        super(oceanbaseConfig, consumer, connectionOptions);
        if (!ConnectionTypeEnum.TARGET.getType().equals(commonDbConfig.get__connectionType())) {
            testFunctionMap.put("testObLogProxy", this::testObLogProxy);
        }
        jdbcContext = new MysqlJdbcContextV2(oceanbaseConfig);
    }

    protected Boolean testVersion() {
        try {
            jdbcContext.queryWithNext("select version()", rs -> consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, rs.getString(1))));
        } catch (Exception e) {
            consumer.accept(new TestItem(TestItem.ITEM_VERSION, new TapTestVersionEx(e), TestItem.RESULT_FAILED));
        }
        return true;
    }

    public boolean testWriteOrReadPrivilegeV2(String grantSql, List<String> tableList, String databaseName, String mark) {
        return super.testWriteOrReadPrivilege(grantSql, tableList, databaseName, mark);
    }

    public Boolean testObLogProxy() {
        try {
            NetUtil.validateHostPortWithSocket(((OceanbaseConfig) commonDbConfig).getRawLogServerHost(), ((OceanbaseConfig) commonDbConfig).getRawLogServerPort());
            consumer.accept(testItem(ITEM_OB_LOG_PROXY, TestItem.RESULT_SUCCESSFULLY, "ObLogProxy is available"));
        } catch (Exception e) {
            consumer.accept(testItem(ITEM_OB_LOG_PROXY, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "ObLogProxy is not available"));
        }
        return true;
    }

    @Override
    protected Boolean testDatasourceInstanceInfo() {
        buildDatasourceInstanceInfo(connectionOptions);
        return true;
    }

    private static final String ITEM_OB_LOG_PROXY = "ObLogProxy";
}


