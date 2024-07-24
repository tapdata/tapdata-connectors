package io.tapdata.connector.clickhouse;

import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.clickhouse.config.ClickhouseConfig;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.getStackString;
import static io.tapdata.base.ConnectorBase.testItem;


public class ClickhouseTest extends CommonDbTest {
    protected ConnectionOptions connectionOptions;

    public ClickhouseTest(ClickhouseConfig clickhouseConfig, Consumer<TestItem> consumer, ConnectionOptions connectionOptions) {
        super(clickhouseConfig, consumer);
        jdbcContext = new ClickhouseJdbcContext(clickhouseConfig);
        this.connectionOptions = connectionOptions;
    }

    protected Boolean testWritePrivilege() {
        try {
            List<String> sqls = new ArrayList<>();
            if (jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE, TEST_WRITE_TABLE.toUpperCase())).size() > 0) {
                sqls.add(String.format(TEST_DROP_TABLE, TEST_WRITE_TABLE));
            }
            //create
            sqls.add(String.format(TEST_CK_CREATE_TABLE, TEST_WRITE_TABLE));
            //insert
            sqls.add(String.format(TEST_CK_WRITE_RECORD, TEST_WRITE_TABLE));
            //update
            sqls.add(String.format(TEST_CK_UPDATE_RECORD, TEST_WRITE_TABLE));
            //delete
            sqls.add(String.format(TEST_CK_DELETE_RECORD, TEST_WRITE_TABLE));
            //drop
            sqls.add(String.format(TEST_DROP_TABLE, TEST_WRITE_TABLE));
            jdbcContext.batchExecute(sqls);
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, e.getMessage()));
        }
        return true;
    }

    @Override
    public Boolean testTimeDifference(){
        try {
            long nowTime = jdbcContext.queryTimestamp();
            connectionOptions.setTimeDifference(getTimeDifference(nowTime));
        } catch (SQLException e) {
            consumer.accept(testItem(TestItem.ITEM_TIME_DETECTION, TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                    "Failed to obtain current database time: " + e.getMessage() + "\n" + getStackString(e)));
        }
        return true;
    }

    private static final String TEST_CK_CREATE_TABLE = "create table %s(col1 int,col2 int) ENGINE = MergeTree primary key(col1) ORDER BY col1";
    private static final String TEST_CK_WRITE_RECORD = "insert into %s values(0,0)";
    private static final String TEST_CK_UPDATE_RECORD = "alter table %s update col2=1 where 1=1";
    private static final String TEST_CK_DELETE_RECORD = "alter table %s delete where 1=1";
}
