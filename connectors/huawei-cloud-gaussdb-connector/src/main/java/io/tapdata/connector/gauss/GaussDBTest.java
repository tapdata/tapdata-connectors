package io.tapdata.connector.gauss;

import com.google.common.collect.Lists;
import io.tapdata.common.CommonDbTest;
import io.tapdata.common.JdbcContext;
import io.tapdata.connector.gauss.core.GaussDBConfig;
import io.tapdata.connector.gauss.entity.TestAccept;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.TapTestReadPrivilegeEx;
import io.tapdata.pdk.apis.exception.testItem.TapTestWritePrivilegeEx;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.tapdata.base.ConnectorBase.testItem;

public class GaussDBTest extends CommonDbTest {
    public static final String PG_TABLE_NUM = "SELECT COUNT(*) FROM pg_tables WHERE schemaname='%s'";
    public static final String PG_TABLE_SELECT_NUM = "SELECT count(*) FROM information_schema.table_privileges " +
            "WHERE grantee='%s' AND table_catalog='%s' AND table_schema='%s' AND privilege_type='SELECT'";


    public GaussDBTest() {
        super();
    }

    public static void instance(GaussDBConfig gaussDBConfig, Consumer<TestItem> consumer, TestAccept accept) throws Throwable {
        try (GaussDBTest test = new GaussDBTest(gaussDBConfig, consumer).init().initContext()) {
            accept.accept(test);
        }
    }

    protected GaussDBTest init() {
        testFunctionMap().put("testConnectorVersion", this::testConnectorVersion);
        return this;
    }

    protected GaussDBTest(GaussDBConfig gaussDBConfig, Consumer<TestItem> consumer) {
        super(gaussDBConfig, consumer);
    }

    protected Map<String, Supplier<Boolean>> testFunctionMap() {
        return testFunctionMap;
    }

    protected GaussDBConfig getCommonDbConfig() {
        return (GaussDBConfig)commonDbConfig;
    }

    protected JdbcContext getJdbcContext() {
        return jdbcContext;
    }

    protected Consumer<TestItem> getConsumer() {
        return consumer;
    }

    public GaussDBTest initContext() {
        jdbcContext = new PostgresJdbcContext(getCommonDbConfig()).withPostgresVersion("92000");
        return this;
    }

    public Boolean testConnectorVersion() {
        getConsumer().accept(testItem("Connector Version", TestItem.RESULT_SUCCESSFULLY, "v1.1.0"));
        return true;
    }

    @Override
    protected List<String> supportVersions() {
        return Lists.newArrayList("9.2", "9.4", "9.5", "9.6", "1*");
    }

    //Test number of tables and privileges
    public Boolean testReadPrivilege() {
        AtomicInteger tableSelectPrivileges = new AtomicInteger();
        GaussDBConfig gaussDBConfig = getCommonDbConfig();
        try {
            getJdbcContext().queryWithNext(
                String.format(PG_TABLE_SELECT_NUM, gaussDBConfig.getUser(), gaussDBConfig.getDatabase(), gaussDBConfig.getSchema()),
                    resultSet -> tableSelectPrivileges.set(resultSet.getInt(1))
            );
            if (tableSelectPrivileges.get() >= tableCount()) {
                getConsumer().accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY, "All tables can be selected"));
            } else {
                getConsumer().accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user may have no read privilege for some tables, Check it"));
            }
            return true;
        } catch (SQLException e) {
            getConsumer().accept(new TestItem(TestItem.ITEM_READ, new TapTestReadPrivilegeEx(e), TestItem.RESULT_FAILED));
            return false;
        }
    }

    protected int tableCount() throws SQLException {
        AtomicInteger tableCount = new AtomicInteger();
        getJdbcContext().queryWithNext(PG_TABLE_NUM, resultSet -> tableCount.set(resultSet.getInt(1)));
        return tableCount.get();
    }

    @Override
    protected Boolean testWritePrivilege() {
        JdbcContext jdbcContext = getJdbcContext();
        GaussDBConfig gaussDBConfig = getCommonDbConfig();
        String schema = gaussDBConfig.getSchema();
        List<String> sqlArray = new ArrayList<>();
        try {
            String schemaPrefix = EmptyKit.isNotEmpty(schema) ? ("\"" + schema + "\".") : "";
            if (!jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE, TEST_WRITE_TABLE.toUpperCase())).isEmpty()) {
                sqlArray.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            }
            //create
            sqlArray.add(String.format(TEST_CREATE_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            //insert
            sqlArray.add(String.format(TEST_WRITE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //update
            sqlArray.add(String.format(TEST_UPDATE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //delete
            sqlArray.add(String.format(TEST_DELETE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //drop
            sqlArray.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            jdbcContext.batchExecute(sqlArray);
            getConsumer().accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            getConsumer().accept(new TestItem(TestItem.ITEM_WRITE, new TapTestWritePrivilegeEx(e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
        }
        return true;
    }

    @Override
    public Boolean testStreamRead() {
        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }
}
