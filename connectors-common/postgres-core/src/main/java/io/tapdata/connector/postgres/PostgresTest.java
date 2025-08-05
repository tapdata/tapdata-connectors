package io.tapdata.connector.postgres;

import com.google.common.collect.Lists;
import io.tapdata.common.CommonDbTest;
import io.tapdata.common.util.FileUtil;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.testItem.TapTestCurrentTimeConsistentEx;
import io.tapdata.pdk.apis.exception.testItem.TapTestReadPrivilegeEx;
import io.tapdata.pdk.apis.exception.testItem.TapTestStreamReadEx;
import io.tapdata.pdk.apis.exception.testItem.TapTestWritePrivilegeEx;
import io.tapdata.util.NetUtil;
import org.apache.commons.io.IOUtils;
import org.postgresql.Driver;

import java.io.*;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class PostgresTest extends CommonDbTest {

    protected ConnectionOptions connectionOptions;
    protected boolean masterConnected = true;

    public PostgresTest() {
        super();
    }

    public PostgresTest(PostgresConfig postgresConfig, Consumer<TestItem> consumer, ConnectionOptions connectionOptions) {
        super(postgresConfig, consumer);
        testFunctionMap.put("testMaster", this::testMaster);
        this.connectionOptions = connectionOptions;
    }

    public PostgresTest initContext() {
        if ("master-slave".equals(((PostgresConfig) commonDbConfig).getDeploymentMode())) {
            testHostPortForMasterSlave();
        }
        jdbcContext = new PostgresJdbcContext((PostgresConfig) commonDbConfig);
        return this;
    }

    private void testHostPortForMasterSlave() {
        AtomicBoolean isMaster = new AtomicBoolean();
        String availableHost = null;
        int availablePort = 0;
        for (LinkedHashMap<String, Integer> hostPort : ((PostgresConfig) commonDbConfig).getMasterSlaveAddress()) {
            commonDbConfig.setHost(String.valueOf(hostPort.get("host")));
            commonDbConfig.setPort(hostPort.get("port"));
            try (PostgresJdbcContext jdbcContext = new PostgresJdbcContext((PostgresConfig) commonDbConfig)) {
                jdbcContext.queryWithNext("SELECT pg_is_in_recovery()", resultSet -> {
                    isMaster.set(!resultSet.getBoolean(1));
                });
                if (isMaster.get()) {
                    return;
                } else {
                    availableHost = commonDbConfig.getHost();
                    availablePort = commonDbConfig.getPort();
                    masterConnected = false;
                }
            } catch (Throwable ignore) {
            }
        }
        if (EmptyKit.isNotNull(availableHost)) {
            commonDbConfig.setHost(availableHost);
            commonDbConfig.setPort(availablePort);
        }
    }

    public PostgresTest withPostgresVersion(String version) {
        ((PostgresJdbcContext) jdbcContext).withPostgresVersion(version);
        return this;
    }

    public PostgresTest withPostgresVersion() throws SQLException {
        ((PostgresJdbcContext) jdbcContext).withPostgresVersion(jdbcContext.queryVersion());
        return this;
    }

    @Override
    protected List<String> supportVersions() {
        return Lists.newArrayList("9.4", "9.5", "9.6", "1*");
    }

    protected boolean testMaster() {
        if (!masterConnected) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "Current node is not master, please check the connection address"));
        }
        return true;
    }

    //Test number of tables and privileges
    public Boolean testReadPrivilege() {
        try {
            AtomicInteger tableSelectPrivileges = new AtomicInteger();
            jdbcContext.queryWithNext(String.format(PG_TABLE_SELECT_NUM, StringKit.escape(commonDbConfig.getUser(), "'"),
                    StringKit.escape(commonDbConfig.getDatabase(), "'"), StringKit.escape(commonDbConfig.getSchema(), "'")), resultSet -> tableSelectPrivileges.set(resultSet.getInt(1)));
            if (tableSelectPrivileges.get() >= tableCount()) {
                consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY, "All tables can be selected"));
            } else {
                consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY_WITH_WARN,
                        "Current user may have no read privilege for some tables, Check it"));
            }
            return true;
        } catch (Throwable e) {
            consumer.accept(new TestItem(TestItem.ITEM_READ, new TapTestReadPrivilegeEx(e), TestItem.RESULT_FAILED));
            return false;
        }
    }

    public Boolean testStreamRead() {
        if ("walminer".equals(((PostgresConfig) commonDbConfig).getLogPluginName())) {
            if (EmptyKit.isBlank(((PostgresConfig) commonDbConfig).getPgtoHost())) {
                return testWalMiner();
            } else {
                return testWalMinerPgto();
            }
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

    public Boolean testWalMiner() {
        try {
            jdbcContext.query("select walminer_stop()", resultSet -> {
                if (resultSet.next()) {
                    consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY, "Cdc can work normally"));
                }
            });
            return true;
        } catch (Throwable e) {
            consumer.accept(new TestItem(TestItem.ITEM_READ_LOG, new TapTestStreamReadEx(e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            return null;
        }
    }

    public Boolean testWalMinerPgto() {
        try {
            NetUtil.validateHostPortWithSocket(((PostgresConfig) commonDbConfig).getPgtoHost(), ((PostgresConfig) commonDbConfig).getPgtoPort());
            consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY, "Cdc can work normally"));
            return true;
        } catch (Throwable e) {
            if ("127.0.0.1,localhost".contains(((PostgresConfig) commonDbConfig).getPgtoHost()) && deployPgto()) {
                consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY, "pgto server deployed successfully"));
                return true;
            } else {
                consumer.accept(new TestItem(TestItem.ITEM_READ_LOG, new TapTestStreamReadEx(e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
                return null;
            }
        }
    }

    private static final String WALMINER_PACKAGE_NAME = "walminer_x86_64_v4.11.0";

    private boolean deployPgto() {
        String toolPath = FileUtil.paths("run-resources", "pg-db", "walminer");
        File toolDir = new File(toolPath);
        if ((!toolDir.exists() || !toolDir.isDirectory()) && !toolDir.mkdirs()) {
            return false;
        }

        try {
            // 从资源中提取 tar.gz 文件
            URL gzUrl = this.getClass().getClassLoader().getResource("walminer/" + WALMINER_PACKAGE_NAME + ".tar.gz");
            if (gzUrl == null) {
                System.err.println("Cannot find resource: walminer/" + WALMINER_PACKAGE_NAME + ".tar.gz");
                return false;
            }

            // 使用更安全的方式获取输入流
            try (InputStream jarInputStream = gzUrl.openStream()) {
                File tempFile = new File(toolDir, WALMINER_PACKAGE_NAME + ".tar.gz");
                try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile)) {
                    IOUtils.copy(jarInputStream, fileOutputStream);
                }
            }

        } catch (Exception e) {
            return false;
        }
        Runtime runtime = Runtime.getRuntime();
        try {
            Process process = runtime.exec(new String[]{"/bin/sh", "-c", "hostid"});
            process.waitFor(10, TimeUnit.SECONDS);
            String hostId;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                hostId = reader.readLine();
            }
            String ctlDir = "walminer_" + commonDbConfig.getHost() + "_" + commonDbConfig.getPort();
            String dicName = "walminer_" + commonDbConfig.getHost() + "_" + commonDbConfig.getPort() + ".dic";
            String slotName = "_tap_walminer_slot_" + hostId;
            if (!new File(FileUtil.paths(toolPath, ctlDir)).exists()) {
                try {
                    jdbcContext.query(String.format("select * from pg_drop_replication_slot('%s')", slotName), resultSet -> {
                    });
                } catch (Exception ignore) {
                }
            }
            runtime.exec(new String[]{"/bin/sh", "-c", "tar -xvf " + toolDir.getAbsolutePath() + "/" + WALMINER_PACKAGE_NAME + ".tar.gz -C " + toolDir.getAbsolutePath()}).waitFor(10, TimeUnit.SECONDS);
            String appendBinPath = toolDir.getAbsolutePath() + "/" + WALMINER_PACKAGE_NAME + "/bin";
            String appendLdPath = toolDir.getAbsolutePath() + "/" + WALMINER_PACKAGE_NAME + "/lib";
            ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", String.format("walminer builtdic -d %s -h %s -p %s -u %s -D %s -W %s -f", commonDbConfig.getDatabase(), commonDbConfig.getHost(), commonDbConfig.getPort(), commonDbConfig.getUser(), toolDir.getAbsolutePath() + "/" + dicName, commonDbConfig.getPassword()));
            Map<String, String> env = processBuilder.environment();
            String currentBinPath = env.get("PATH");
            String newBinPath = (currentBinPath == null || currentBinPath.isEmpty()) ? appendBinPath : (currentBinPath + ":" + appendBinPath);
            env.put("PATH", newBinPath);
            String currentLdPath = env.get("LD_LIBRARY_PATH");
            String newLdPath = (currentLdPath == null || currentLdPath.isEmpty()) ? appendLdPath : (currentLdPath + ":" + appendLdPath);
            env.put("LD_LIBRARY_PATH", newLdPath);
            process = processBuilder.start();
            process.waitFor(60, TimeUnit.SECONDS);
            new File(FileUtil.paths(toolPath, ctlDir)).mkdir();
            processBuilder.command("/bin/sh", "-c", String.format("walminer pgto -i -c %s -s '%s' -e %s -t 4 --source-connstr1='host=%s port=%s username=%s dbanme=%s password=%s'", toolDir.getAbsolutePath() + "/" + ctlDir, slotName, ((PostgresConfig) commonDbConfig).getPgtoPort(), commonDbConfig.getHost(), commonDbConfig.getPort(), commonDbConfig.getUser(), commonDbConfig.getDatabase(), commonDbConfig.getPassword()));
            process = processBuilder.start();
            process.waitFor(60, TimeUnit.SECONDS);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }
            int exitCode = process.waitFor();
            System.out.println("exit code: " + exitCode);
            processBuilder.command("/bin/sh", "-c", String.format("walminer pgto -m -c %s", toolDir.getAbsolutePath() + "/" + ctlDir));
            processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            process = processBuilder.start();
            process.waitFor(60, TimeUnit.SECONDS);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }
            exitCode = process.waitFor();
            System.out.println("exit code: " + exitCode);
        } catch (IOException | InterruptedException ignored) {
            return false;
        }
        return true;

    }

    protected int tableCount() throws Throwable {
        AtomicInteger tableCount = new AtomicInteger();
        jdbcContext.queryWithNext(String.format(PG_TABLE_NUM, StringKit.escape(commonDbConfig.getSchema(), "'")), resultSet -> tableCount.set(resultSet.getInt(1)));
        return tableCount.get();
    }

    private final static String PG_TABLE_NUM = "SELECT COUNT(*) FROM pg_tables WHERE schemaname='%s'";
    private final static String PG_TABLE_SELECT_NUM = "SELECT count(*) FROM information_schema.table_privileges " +
            "WHERE grantee='%s' AND table_catalog='%s' AND table_schema='%s' AND privilege_type='SELECT'";
    protected final static String PG_LOG_PLUGIN_CREATE_TEST = "SELECT pg_create_logical_replication_slot('%s','%s')";
    protected final static String PG_LOG_PLUGIN_DROP_TEST = "select pg_drop_replication_slot(a.slot_name) " +
            "from (select * from pg_replication_slots where slot_name like 'test_tapdata_%') a;";

    @Override
    protected Boolean testWritePrivilege() {
        try {
            List<String> sqls = new ArrayList<>();
            String schemaPrefix = EmptyKit.isNotEmpty(commonDbConfig.getSchema()) ? ("\"" + StringKit.escape(commonDbConfig.getSchema(), "\"") + "\".") : "";
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
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, new TapTestWritePrivilegeEx(e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
        }
        return true;
    }

    @Override
    public Boolean testTimeDifference() {
        try {
            long nowTime = jdbcContext.queryTimestamp();
            connectionOptions.setTimeDifference(getTimeDifference(nowTime));
        } catch (SQLException e) {
            consumer.accept(new TestItem(TestItem.ITEM_TIME_DETECTION, new TapTestCurrentTimeConsistentEx(e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
        }
        return true;
    }

    @Override
    protected Boolean testDatasourceInstanceInfo() {
        buildDatasourceInstanceInfo(connectionOptions);
        return true;
    }
}
