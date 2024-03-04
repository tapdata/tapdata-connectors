package io.tapdata.connector.gauss;

import com.google.common.collect.Lists;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.CommonDbTest;
import io.tapdata.common.JdbcContext;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.gauss.core.GaussDBConfig;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.entity.TestItem;
import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.tapdata.base.ConnectorBase.testItem;

public class GaussDBTest extends CommonDbTest {
    protected final static String PG_TABLE_NUM = "SELECT COUNT(*) FROM pg_tables WHERE schemaname='%s'";
    protected final static String PG_TABLE_SELECT_NUM = "SELECT count(*) FROM information_schema.table_privileges " +
            "WHERE grantee='%s' AND table_catalog='%s' AND table_schema='%s' AND privilege_type='SELECT'";

    public GaussDBTest() {
        super();
    }

    public GaussDBTest(GaussDBConfig gaussDBConfig, Consumer<TestItem> consumer) {
        super(gaussDBConfig, consumer);
        testFunctionMap().remove("testStreamRead");
    }

    protected Map<String, Supplier<Boolean>> testFunctionMap() {
        return testFunctionMap;
    }

    protected GaussDBConfig commonDbConfig() {
        return (GaussDBConfig)commonDbConfig;
    }

    protected JdbcContext jdbcContext() {
        return jdbcContext;
    }

    protected Consumer<TestItem> consumer() {
        return consumer;
    }

    public GaussDBTest initContext() {
        jdbcContext = new PostgresJdbcContext(commonDbConfig());
        return this;
    }



    @Override
    protected List<String> supportVersions() {
        return Lists.newArrayList("9.2", "9.4", "9.5", "9.6", "1*");
    }

    //Test number of tables and privileges
    public Boolean testReadPrivilege() {
        AtomicInteger tableSelectPrivileges = new AtomicInteger();
        GaussDBConfig gaussDBConfig = commonDbConfig();
        try {
            jdbcContext().queryWithNext(
                String.format(PG_TABLE_SELECT_NUM, gaussDBConfig.getUser(), gaussDBConfig.getDatabase(), gaussDBConfig.getSchema()),
                resultSet -> tableSelectPrivileges.set(resultSet.getInt(1))
            );
            if (tableSelectPrivileges.get() >= tableCount()) {
                consumer().accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY, "All tables can be selected"));
            } else {
                consumer().accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user may have no read privilege for some tables, Check it"));
            }
            return true;
        } catch (Throwable e) {
            consumer().accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    protected int tableCount() throws SQLException {
        AtomicInteger tableCount = new AtomicInteger();
        jdbcContext().queryWithNext(PG_TABLE_NUM, resultSet -> tableCount.set(resultSet.getInt(1)));
        return tableCount.get();
    }

    @Override
    protected Boolean testWritePrivilege() {
        JdbcContext jdbcContext = jdbcContext();
        GaussDBConfig gaussDBConfig = commonDbConfig();
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
            consumer().accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            consumer().accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, e.getMessage()));
        }
        return true;
    }
}
