package io.tapdata.connector.starrocks;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpUtil;
import io.tapdata.common.CommonDbTest;

import java.util.*;

import io.tapdata.connector.starrocks.bean.StarrocksConfig;
import io.tapdata.connector.starrocks.streamload.StarrocksStreamLoader;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.TapTestConnectionEx;
import io.tapdata.pdk.apis.exception.testItem.TapTestUnknownEx;
import io.tapdata.pdk.apis.exception.testItem.TapTestVersionEx;
import io.tapdata.pdk.apis.exception.testItem.TapTestWritePrivilegeEx;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class StarrocksTest extends CommonDbTest {

    public StarrocksTest(StarrocksConfig StarrocksConfig, Consumer<TestItem> consumer) {
        super(StarrocksConfig, consumer);
        testFunctionMap.put("testStreamLoadPrivilege", this::testStreamLoadPrivilege);
        jdbcContext = new StarrocksJdbcContext(StarrocksConfig);
    }

    @Override
    protected Boolean testConnect() {
        try (
                Connection connection = jdbcContext.getConnection()
        ) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, TEST_CONNECTION_LOGIN));
            return true;
        } catch (Exception e) {
            if (e instanceof SQLException && ((SQLException) e).getErrorCode() == 1045) {
                consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Incorrect username or password"));
            } else {
                consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, new TapTestConnectionEx(e), TestItem.RESULT_FAILED));
            }
            return false;
        }
    }

    @Override
    protected Boolean testVersion() {
        try {
            String StarrocksVersion = jdbcContext.queryVersion();
            consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, StarrocksVersion));
        } catch (Throwable throwable) {
            consumer.accept(new TestItem(TestItem.ITEM_VERSION, new TapTestVersionEx(throwable), TestItem.RESULT_FAILED));
        }
        return true;
    }

    @Override
    protected Boolean testWritePrivilege() {
        try {
            AtomicInteger beCount = new AtomicInteger(0);
            try {
                jdbcContext.normalQuery("show backends", resultSet -> {
                    while (resultSet.next()) {
                        beCount.incrementAndGet();
                    }
                });
            } catch (SQLSyntaxErrorException e) {
                if ("42000".equals(e.getSQLState())) {
                    Integer backendNum = ((StarrocksConfig) commonDbConfig).getBackendNum();
                    if (null != backendNum) {
                        beCount.set(backendNum);
                    } else {
                        beCount.set(1);
                    }
                }
            }

            List<String> sqls = new ArrayList<>();
            String schemaPrefix = EmptyKit.isNotEmpty(commonDbConfig.getSchema()) ? ("`" + commonDbConfig.getSchema() + "`.") : "";
            if (jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE, TEST_WRITE_TABLE.toUpperCase())).size() > 0) {
                sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            }
            //create
            sqls.add(String.format(TEST_Starrocks_CREATE_TABLE, schemaPrefix + TEST_WRITE_TABLE, Math.min(beCount.get(), 3)));
            //insert
            sqls.add(String.format(TEST_Starrocks_WRITE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //update
            sqls.add(String.format(TEST_Starrocks_UPDATE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //delete
            sqls.add(String.format(TEST_Starrocks_DELETE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //drop
            sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            jdbcContext.batchExecute(sqls);
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, new TapTestWritePrivilegeEx(e), TestItem.RESULT_FAILED));
        }
        return true;
    }

    protected static final String TEST_Starrocks_CREATE_TABLE = "create table %s(col1 int not null, col2 int) PRIMARY key(col1) distributed by hash(col1) buckets 2 PROPERTIES(\"replication_num\"=\"%s\")";
    protected static final String TEST_Starrocks_WRITE_RECORD = "insert into %s values(0,0)";
    protected static final String TEST_Starrocks_UPDATE_RECORD = "update %s set col2=1 where col1=0";
    protected static final String TEST_Starrocks_DELETE_RECORD = "delete from %s where col1=0";

    public Boolean testStreamLoadPrivilege() {
        try {
            boolean testResult = false;
            String authStr = ((StarrocksConfig) commonDbConfig).getUser() + ":";
            if (((StarrocksConfig) commonDbConfig).getPassword() != null) {
                authStr += ((StarrocksConfig) commonDbConfig).getPassword();
            }
            String encodedAuth = Base64.getEncoder().encodeToString(authStr.getBytes());
            String authHeader = "Basic " + encodedAuth;
            if (((StarrocksConfig) commonDbConfig).getUseHTTPS()) {
                CloseableHttpClient client = io.tapdata.connector.starrocks.streamload.HttpUtil.generationHttpClient();
                HttpGet httpGet = new HttpGet();
                httpGet.setURI(new URI("https://" + ((StarrocksConfig) commonDbConfig).getStarrocksHttp()));
                httpGet.setHeader("Authorization", authHeader);
                testResult = EntityUtils.toString(client.execute(httpGet).getEntity()).contains("starrocks");
            } else {
                testResult = HttpRequest.get("http://" + ((StarrocksConfig) commonDbConfig).getStarrocksHttp()).header("Authorization", authHeader).execute().body().contains("starrocks");
            }

            if (testResult) {
                consumer.accept(testItem(STREAM_WRITE, TestItem.RESULT_SUCCESSFULLY, "StreamLoad Service is available"));
            } else {
                consumer.accept(testItem(STREAM_WRITE, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "port of StreamLoad Service is not right"));
            }
        } catch (Exception e) {
            consumer.accept(new TestItem(STREAM_WRITE, new TapTestUnknownEx(e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
        }
        return true;
    }

    private static final String STREAM_WRITE = "Stream Write";
}
