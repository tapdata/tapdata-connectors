package io.tapdata.connector.doris;

import cn.hutool.http.HttpUtil;
import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.*;
import io.tapdata.util.NetUtil;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class DorisTest extends CommonDbTest {

    public DorisTest(DorisConfig dorisConfig, Consumer<TestItem> consumer) {
        super(dorisConfig, consumer);
        testFunctionMap.put("testStreamLoadPrivilege", this::testStreamLoadPrivilege);
        jdbcContext = new DorisJdbcContext(dorisConfig);
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
            String dorisVersion = jdbcContext.queryVersion();
            consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, dorisVersion));
        } catch (Throwable throwable) {
            consumer.accept(new TestItem(TestItem.ITEM_VERSION, new TapTestVersionEx(throwable), TestItem.RESULT_FAILED));
        }
        return true;
    }

    @Override
    protected Boolean testWritePrivilege() {
        try {
            AtomicInteger beCount = new AtomicInteger(0);
            AtomicReference<String> invalidBeHost = new AtomicReference<>();
            AtomicReference<Integer> invalidHttpPort = new AtomicReference<>();
            AtomicReference<IOException> throwable = new AtomicReference<>();
            try {
                jdbcContext.normalQuery("show backends", resultSet -> {
                    boolean hasValidNode = false;
                    while (resultSet.next()) {
                        beCount.incrementAndGet();
                        String beHost = (resultSet.getString("Host"));
                        Integer httpPort = (resultSet.getInt("HttpPort"));
                        if (null == beHost || null == httpPort) continue;
                        try {
                            if (!hasValidNode) {
                                NetUtil.validateHostPortWithSocket(beHost, httpPort);
                                hasValidNode = true;
                            }
                        } catch (IOException e) {
                            invalidBeHost.set(beHost);
                            invalidHttpPort.set(httpPort);
                            throwable.set(e);
                        }
                    }
                    if (!hasValidNode && null != throwable.get()) {
                        throw new TapTestHostPortEx(throwable.get(), invalidBeHost.get(), String.valueOf(invalidHttpPort.get()));
                    }
                });
            } catch (SQLSyntaxErrorException e) {
                if ("42000".equals(e.getSQLState())) {
                    Integer backendNum = ((DorisConfig) commonDbConfig).getBackendNum();
                    if (null != backendNum) {
                        beCount.set(backendNum);
                    } else {
                        beCount.set(1);
                    }
                }
            } catch (TapTestHostPortEx e) {
                consumer.accept(new TestItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, "Validate BE nodes failed: " + e.getMessage()));
                return false;
            }

            List<String> sqls = new ArrayList<>();
            String schemaPrefix = EmptyKit.isNotEmpty(commonDbConfig.getSchema()) ? ("`" + commonDbConfig.getSchema() + "`.") : "";
            if (jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE, TEST_WRITE_TABLE.toUpperCase())).size() > 0) {
                sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            }
            //create
            sqls.add(String.format(TEST_DORIS_CREATE_TABLE, schemaPrefix + TEST_WRITE_TABLE, Math.min(beCount.get(), 3)));
            //insert
            sqls.add(String.format(TEST_DORIS_WRITE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //update
            sqls.add(String.format(TEST_DORIS_UPDATE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //delete
            sqls.add(String.format(TEST_DORIS_DELETE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //drop
            sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            jdbcContext.batchExecute(sqls);
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, new TapTestWritePrivilegeEx(e), TestItem.RESULT_FAILED));
        }
        return true;
    }

    protected static final String TEST_DORIS_CREATE_TABLE = "create table %s(col1 int not null, col2 int) unique key(col1) distributed by hash(col1) buckets 2 PROPERTIES(\"replication_num\"=\"%s\")";
    protected static final String TEST_DORIS_WRITE_RECORD = "insert into %s values(0,0)";
    protected static final String TEST_DORIS_UPDATE_RECORD = "update %s set col2=1 where col1=0";
    protected static final String TEST_DORIS_DELETE_RECORD = "delete from %s where col1=0";

    public Boolean testStreamLoadPrivilege() {
        try {
            boolean testResult = false;
            if (((DorisConfig) commonDbConfig).getUseHTTPS()) {
                CloseableHttpClient client = io.tapdata.connector.doris.streamload.HttpUtil.generationHttpClient();
                HttpGet httpGet = new HttpGet();
                httpGet.setURI(new URI("https://" + ((DorisConfig) commonDbConfig).getDorisHttp()));
                testResult=EntityUtils.toString(client.execute(httpGet).getEntity()).contains("Doris</title>");
            } else {
                testResult = HttpUtil.get("http://" + ((DorisConfig) commonDbConfig).getDorisHttp()).contains("Doris</title>");
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
