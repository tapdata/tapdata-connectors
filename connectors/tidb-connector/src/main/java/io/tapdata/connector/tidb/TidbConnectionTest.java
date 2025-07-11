package io.tapdata.connector.tidb;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.common.CommonDbTest;
import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.connector.mysql.constant.MysqlTestItem;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.connector.tidb.stage.NetworkUtils;
import io.tapdata.connector.tidb.stage.VersionControl;
import io.tapdata.constant.ConnectionTypeEnum;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.*;
import io.tapdata.util.NetUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.*;

/**
 * @author lemon
 */
public class TidbConnectionTest extends CommonDbTest {
    private final TidbConfig tidbConfig;
    private static final String TI_DB_GC_LIFE_TIME = "TiCDC GC Life Time";
    private final static String PB_SERVER_SUCCESS = "Check PDServer host port is valid";
    private final static String IC_CONFIGURATION_ENABLED = " Check  Incremental is enable";
    protected static final String CHECK_DATABASE_PRIVILEGES_SQL = "SHOW GRANTS FOR CURRENT_USER";
    protected static final String CHECK_DATABASE_BINLOG_STATUS_SQL = "SHOW GLOBAL VARIABLES where variable_name = 'log_bin' OR variable_name = 'binlog_format'";
    protected static final String CHECK_DATABASE_BINLOG_ROW_IMAGE_SQL = "SHOW VARIABLES LIKE '%binlog_row_image%'";
    protected static final String CHECK_CREATE_TABLE_PRIVILEGES_SQL = "SELECT count(1)\n" +
            "FROM INFORMATION_SCHEMA.USER_PRIVILEGES\n" +
            "WHERE GRANTEE LIKE '%%%s%%' and PRIVILEGE_TYPE = 'CREATE'";
    protected static final String CHECK_LOW_CREATE_TABLE_PRIVILEGES_SQL = "SELECT count(1)\n" +
            "FROM INFORMATION_SCHEMA.USER_PRIVILEGES\n" +
            "WHERE GRANTEE LIKE '%%%s%%' and PRIVILEGE_TYPE = 'Create'";
    protected static String CHECK_TIDB_VERSION = "SELECT VERSION()";
    private boolean cdcCapability;
    private final ConnectionOptions connectionOptions;
    private String[] array;

    public TidbConnectionTest(TidbConfig tidbConfig, Consumer<TestItem> consumer, ConnectionOptions connectionOptions) {
        super(tidbConfig, consumer);
        if (!ConnectionTypeEnum.TARGET.getType().equals(commonDbConfig.get__connectionType())) {
            testFunctionMap.put("testPbserver", this::testPbserver);
            testFunctionMap.put("testGcLifeTime", this::testGcLifeTime);
        }
        testFunctionMap.put("testVersion", this::testVersion);
        this.tidbConfig = tidbConfig;
        this.connectionOptions = connectionOptions;
        jdbcContext = new TidbJdbcContext(tidbConfig);
    }


    @Override
    public Boolean testOneByOne() {
        return super.testOneByOne();
    }

    /**
     * check Pbserver
     *
     * @return
     */
    public Boolean testPbserver() {
        try {
            String pdServer = tidbConfig.getPdServer().startsWith("http") ?
                    tidbConfig.getPdServer()
                    : "http://" + tidbConfig.getPdServer();
            URI uri = URI.create(pdServer);
            NetUtil.validateHostPortWithSocket(uri.getHost(), uri.getPort());
            try {
                NetworkUtils.check(uri.getHost(), uri.getPort(), tidbConfig.getTiKvPort());
                consumer.accept(testItem(PB_SERVER_SUCCESS, TestItem.RESULT_SUCCESSFULLY, "Smooth connection between PDServer and TiKV"));
            } catch (Exception e) {
                consumer.accept(testItem(PB_SERVER_SUCCESS, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, String.format("Unable to connect to network %s, %s", tidbConfig.getPdServer(), e.getMessage())));
            }
            return true;
        } catch (Exception e) {
            consumer.accept(new TestItem(PB_SERVER_SUCCESS, new TapTestUnknownEx(e), TestItem.RESULT_FAILED));
            return false;
        }
    }

    @Override
    public Boolean testConnect() {
        try (
                Connection connection = jdbcContext.getConnection();
        ) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY));
            return true;
        } catch (Exception e) {
            if (e instanceof SQLException) {
                String errMsg = e.getMessage();
                if (errMsg.contains("using password")) {
                    String password = tidbConfig.getPassword();
                    if (StringUtils.isNotEmpty(password)) {
                        errMsg = "password or username is error ,please check";
                    } else {
                        errMsg = "password is empty,please enter password";
                    }
                    consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, new TapTestAuthEx(e), TestItem.RESULT_FAILED));


                }
            }
            consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, new TapTestConnectionEx(e), TestItem.RESULT_FAILED));
            return false;
        }
    }


    @Override
    protected Boolean testWritePrivilege() {
        try {
            List<String> sqls = new ArrayList<>();
            String schemaPrefix = EmptyKit.isNotEmpty(commonDbConfig.getSchema()) ? ("`" + commonDbConfig.getSchema() + "`.") : "";
            if (jdbcContext.queryAllTables(Arrays.asList(TEST_WRITE_TABLE, TEST_WRITE_TABLE.toUpperCase())).size() > 0) {
                sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            }
            //create
            sqls.add(String.format(TEST_CREATE_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            //insert
            sqls.add(String.format(TEST_WRITE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //update
            sqls.add(String.format(TEST_UPDATE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //delete
            sqls.add(String.format(TEST_DELETE_RECORD, schemaPrefix + TEST_WRITE_TABLE));
            //drop
            sqls.add(String.format(TEST_DROP_TABLE, schemaPrefix + TEST_WRITE_TABLE));
            jdbcContext.batchExecute(sqls);
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, TEST_WRITE_SUCCESS));
        } catch (Exception e) {
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, new TapTestWritePrivilegeEx(e), TestItem.RESULT_FAILED));
        }
        return true;
    }

    private boolean WriteOrReadPrivilege(String mark) {
        String databaseName = tidbConfig.getDatabase();
        List<String> tableList = new ArrayList<>();
        AtomicReference<Boolean> globalWrite = new AtomicReference<>();
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        String itemMark = TestItem.ITEM_READ;
        if ("write".equals(mark)) {
            itemMark = TestItem.ITEM_WRITE;
        }
        try {
            String finalItemMark = itemMark;
            jdbcContext.query(CHECK_DATABASE_PRIVILEGES_SQL, resultSet -> {
                while (resultSet.next()) {
                    String grantSql = resultSet.getString(1);
                    if (testWriteOrReadPrivilege(grantSql, tableList, databaseName, mark)) {
                        testItem.set(testItem(finalItemMark, TestItem.RESULT_SUCCESSFULLY));
                        globalWrite.set(true);
                        return;
                    }
                }

            });
        } catch (Throwable e) {
            consumer.accept(new TestItem(itemMark, new TapTestReadPrivilegeEx(e), TestItem.RESULT_FAILED));
            return false;
        }
        if (globalWrite.get() != null) {
            consumer.accept(testItem.get());
            return true;
        }
        if (CollectionUtils.isNotEmpty(tableList)) {
            consumer.accept(testItem(itemMark, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, JSONObject.toJSONString(tableList)));
            return true;
        }
        consumer.accept(testItem(itemMark, TestItem.RESULT_FAILED, "Without table can " + mark));
        return false;
    }

    public boolean testWriteOrReadPrivilege(String grantSql, List<String> tableList, String databaseName, String mark) {
        boolean privilege;
        privilege = grantSql.contains("INSERT") && grantSql.contains("UPDATE") && grantSql.contains("DELETE")
                || grantSql.contains("ALL PRIVILEGES");
        if ("read".equals(mark)) {
            privilege = grantSql.contains("SELECT") || grantSql.contains("ALL PRIVILEGES");
        }
        if (grantSql.contains("*.* TO")) {
            if (privilege) {
                return true;
            }

        } else if (grantSql.contains(databaseName + ".* TO")) {
            if (privilege) {
                return true;
            }
        }else if (grantSql.contains(databaseName + ".")) {
            String grantPrefixDatabase = databaseName + ".";
            String table = grantSql.substring(grantSql.indexOf(grantPrefixDatabase) + databaseName.length()+1, grantSql.indexOf("TO")).trim();
            if (privilege) {
                tableList.add(table);
            }
        } else if (databaseName.contains(
                (grantSql.substring(grantSql.indexOf("`")+1, grantSql.indexOf("`" + ".")).trim().replaceAll("%", "")))) {
            if (grantSql.contains("`" + ".* TO")) {
                if (privilege) {
                    return true;
                }
            } else {
                String table = grantSql.substring(grantSql.indexOf("`" + "."), grantSql.indexOf("TO")).trim();
                tableList.add(table);
            }
        }
        return false;
    }

    protected Boolean testGcLifeTime() {
        try {
            String gcLifeTime = ((TidbJdbcContext) jdbcContext).queryGcLifeTime();
            if (spilt(gcLifeTime)) {
                consumer.accept(testItem(TI_DB_GC_LIFE_TIME, TestItem.RESULT_SUCCESSFULLY, gcLifeTime));
            } else {
                consumer.accept(testItem(TI_DB_GC_LIFE_TIME, TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        String.format("The current value is %s, the recommended value is 24h, which can be set through the command: SET GLOBAL tidb_gc_life_time = '24h'",
                                gcLifeTime)
                ));
            }
            return true;
        } catch (Exception e) {
            consumer.accept(new TestItem(TI_DB_GC_LIFE_TIME, new TapTestUnknownEx(e), TestItem.RESULT_FAILED));
            return false;
        }
    }

    protected boolean spilt(String gcLifeTime) {
        String[] split = gcLifeTime.split("[a-z|A-Z]");
        if (split.length <= 2) {
            //3m0s
            return false;
        } else if (split.length > 3) {
            //1d0h3m0s
            return true;
        }
        //2h3m0s
        String h = split[split.length - 3];
        try {
            //2h3m0s 32h3m0s
            return Integer.parseInt(h) >= 24;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Boolean testReadPrivilege() {
        return WriteOrReadPrivilege("read");
    }

    //    @Override
//    public Boolean testVersion() {
//        consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY));
//        return true;
//    }
    @Override
    public Boolean testVersion() {
        AtomicReference<String> version = new AtomicReference<>();
        try {
            jdbcContext.query(CHECK_TIDB_VERSION, resultSet -> {
                while (resultSet.next()) {
                    String versionMsg = resultSet.getString(1);
                    version.set(versionMsg);
                }
            });
            array = String.valueOf(version).split("-");
            try {
                VersionControl.redirect(array[2]);
                consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, array[1] + "-" + array[2]));
            } catch (Exception e) {
                consumer.accept(new TestItem(TestItem.ITEM_VERSION, new TapTestVersionEx(e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            }
        } catch (Throwable e) {
            consumer.accept(new TestItem(TestItem.ITEM_VERSION, new TapTestVersionEx(e), TestItem.RESULT_FAILED));
        }
        return true;
    }

    @Override
    public Boolean testStreamRead() {
        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }

    public Boolean testCDCPrivileges() {
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        try {
            StringBuilder missPri = new StringBuilder();
            List<TidbConnectionTest.CdcPrivilege> cdcPrivileges = new ArrayList<>(Arrays.asList(TidbConnectionTest.CdcPrivilege.values()));
            jdbcContext.query(CHECK_DATABASE_PRIVILEGES_SQL, resultSet -> {
                while (resultSet.next()) {
                    String grantSql = resultSet.getString(1);
                    Iterator<TidbConnectionTest.CdcPrivilege> iterator = cdcPrivileges.iterator();
                    while (iterator.hasNext()) {
                        boolean match = false;
                        TidbConnectionTest.CdcPrivilege cdcPrivilege = iterator.next();
                        String privileges = cdcPrivilege.getPrivileges();
                        String[] split = privileges.split("\\|");
                        for (String privilege : split) {
                            match = grantSql.contains(privilege);
                            if (match) {
                                if (cdcPrivilege.onlyNeed) {
                                    testItem.set(testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY));
                                    return;
                                }
                                break;
                            }
                        }
                        if (match) {
                            iterator.remove();
                        }
                    }
                }
            });
            if (null == testItem.get()) {
                if (CollectionUtils.isNotEmpty(cdcPrivileges) && cdcPrivileges.size() > 1) {
                    for (TidbConnectionTest.CdcPrivilege cdcPrivilege : cdcPrivileges) {
                        String[] split = cdcPrivilege.privileges.split("\\|");
                        if (cdcPrivilege.onlyNeed) {
                            continue;
                        }
                        for (String s : split) {
                            missPri.append(s).append("|");
                        }
                        missPri.replace(missPri.lastIndexOf("|"), missPri.length(), "").append(" ,");
                    }

                    missPri.replace(missPri.length() - 2, missPri.length(), "");
                    cdcCapability = false;
                    testItem.set(testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                            "User does not have privileges [" + missPri + "], will not be able to use the incremental sync feature."));
                }
            }
            if (null == testItem.get()) {
                testItem.set(testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY));
            }
        } catch (SQLException e) {
            int errorCode = e.getErrorCode();
            String sqlState = e.getSQLState();
            String message = e.getMessage();

            // 如果源库是关闭密码认证时，默认权限校验通过
            if (errorCode == 1290 && "HY000".equals(sqlState) && StringUtils.isNotBlank(message) && message.contains("--skip-grant-tables")) {
                consumer.accept(testItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), TestItem.RESULT_SUCCESSFULLY));
            } else {
                cdcCapability = false;
                consumer.accept(new TestItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(), new TapTestCDCPrivilegeEx(e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            }

        } catch (Throwable e) {
            consumer.accept(new TestItem(MysqlTestItem.CHECK_CDC_PRIVILEGES.getContent(),new TapTestCDCPrivilegeEx(e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            cdcCapability = false;
            return true;
        }
        consumer.accept(testItem.get());
        return true;
    }

    public Boolean testBinlogMode() {
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        try {
            jdbcContext.query(CHECK_DATABASE_BINLOG_STATUS_SQL, resultSet -> {
                String mode = null;
                String logbin = null;
                while (resultSet.next()) {
                    if ("binlog_format".equals(resultSet.getString(1))) {
                        mode = resultSet.getString(2);
                    } else {
                        logbin = resultSet.getString(2);
                    }
                }

                if (!"ROW".equalsIgnoreCase(mode) || !"ON".equalsIgnoreCase(logbin)) {
                    cdcCapability = false;
                    testItem.set(testItem(MysqlTestItem.CHECK_BINLOG_MODE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                            "TidbServer dose not open row level binlog mode, will not be able to use the incremental sync feature"));
                } else {
                    testItem.set(testItem(MysqlTestItem.CHECK_BINLOG_MODE.getContent(), TestItem.RESULT_SUCCESSFULLY));
                }
            });
        } catch (SQLException e) {
            cdcCapability = false;
            consumer.accept(new TestItem(MysqlTestItem.CHECK_BINLOG_MODE.getContent(), new TapTestUnknownEx("Check binlog mode failed.",e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));

        } catch (Throwable e) {
            cdcCapability = false;
            consumer.accept(new TestItem(MysqlTestItem.CHECK_BINLOG_MODE.getContent(), new TapTestUnknownEx("Check binlog mode failed.",e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
        }
        consumer.accept(testItem.get());
        return true;
    }

    public Boolean testBinlogRowImage() {
        AtomicReference<TestItem> testItem = new AtomicReference<>();
        try {
            jdbcContext.query(CHECK_DATABASE_BINLOG_ROW_IMAGE_SQL, resultSet -> {
                while (resultSet.next()) {
                    String value = resultSet.getString(2);
                    if (!StringUtils.equalsAnyIgnoreCase("FULL", value)) {
                        testItem.set(testItem(MysqlTestItem.CHECK_BINLOG_ROW_IMAGE.getContent(), TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                                "binlog row image is [" + value + "]"));
                        cdcCapability = false;
                    }
                }
            });
            if (null == testItem.get()) {
                testItem.set(testItem(MysqlTestItem.CHECK_BINLOG_ROW_IMAGE.getContent(), TestItem.RESULT_SUCCESSFULLY));
            }
        } catch (Throwable e) {
            consumer.accept(new TestItem(MysqlTestItem.CHECK_BINLOG_ROW_IMAGE.getContent(), new TapTestUnknownEx("Check binlog row image failed.",e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            cdcCapability = false;
        }
        consumer.accept(testItem.get());
        return true;
    }

    public Boolean setCdcCapabilitie() {
        if (cdcCapability) {
            List<Capability> ddlCapabilities = DDLFactory.getCapabilities(DDLParserType.MYSQL_CCJ_SQL_PARSER);
            ddlCapabilities.forEach(connectionOptions::capability);
        }
        return true;
    }

    protected enum CdcPrivilege {
        ALL_PRIVILEGES("ALL PRIVILEGES ON *.*", true),
        REPLICATION_CLIENT("REPLICATION CLIENT|Super", false),
        REPLICATION_SLAVE("REPLICATION SLAVE", false);

        private final String privileges;
        private final boolean onlyNeed;

        CdcPrivilege(String privileges, boolean onlyNeed) {
            this.privileges = privileges;
            this.onlyNeed = onlyNeed;
        }

        public String getPrivileges() {
            return privileges;
        }

        public boolean isOnlyNeed() {
            return onlyNeed;
        }
    }



}
