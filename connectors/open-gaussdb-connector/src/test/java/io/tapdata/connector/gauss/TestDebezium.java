package io.tapdata.connector.gauss;

import io.tapdata.connector.gauss.cdc.GaussDBRunner;
import io.tapdata.connector.gauss.core.GaussDBConfig;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.cdc.PostgresCdcRunner;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapdata.base.ConnectorBase.toJson;

///1.
//设置节点机器pg配置文件：wal_level = logical
//一般在/var/chroot/var/lib/engine/data1/data目录下找到各dn_xxx目录，分别修改相应目录下的postgresql.conf

public class TestDebezium {
    static String sourceURL = "jdbc:opengauss://121.37.171.158:8000/postgres";
    String logPluginName = "pgoutput";
    private Object slotName;

    private String username;
    private String pwd;

    protected GaussDBRunner cdcRunner;
    protected PostgresJdbcContext postgresJdbcContext;
    List<String> tableList = new ArrayList<>();
    Object offsetState = new PostgresOffset();
    int recordSize = 100;
    StreamReadConsumer consumer;
    GaussDBConfig postgresConfig;
    TapConnectorContext connectionContext;
    DataMap connectionConfig = new DataMap();
    io.tapdata.entity.utils.DataMap nodeConfig = new DataMap();
    TapNodeSpecification specification = new TapNodeSpecification();
    Log log = new TapLog();

    class TestConsumer extends StreamReadConsumer {
        @Override
        public void accept(List<TapEvent> events, Object offset) {
            System.out.println("===============================");
            System.out.println(events.size());
            System.out.println(toJson(events));
            System.out.println("===============================");
        }
    }

    @BeforeEach
    void init() throws ClassNotFoundException {
        pwd = "Gotapd8!";
        username = "root";
        Class.forName("com.huawei.opengauss.jdbc.Driver");
        tableList.add("root.mysql_table");
        connectionConfig.put("user", username);
        connectionConfig.put("password", pwd);
        connectionConfig.put("host", "1.94.122.172");
        connectionConfig.put("port", 8001);
        connectionConfig.put("haPort", 8001);
        connectionConfig.put("database", "postgres");
        connectionConfig.put("schema", "public");
        connectionConfig.put("logPluginName", logPluginName);
        connectionConfig.put("timezone", "+8:00");
        connectionConfig.put("extParams", "");

        nodeConfig.put("closeNotNull", false);

        connectionContext = new TapConnectorContext(specification, connectionConfig, nodeConfig, log);
        postgresConfig = (GaussDBConfig) new GaussDBConfig().load(connectionConfig);
        //postgresJdbcContext = new PostgresJdbcContext(postgresConfig);
        consumer = new TestConsumer();
    }

    @Test
    public void testConnection() throws Throwable {
        //getConnect(username, pwd);
        //getConnectUseProp(username, pwd);
        cdcRunner = new GaussDBRunner(postgresConfig, new TapLog());
        //testReplicateIdentity(nodeContext.getTableMap());
        //buildSlot(connectionContext, true);
        slotName = "slot1";
        cdcRunner.useSlot(slotName.toString())
                .watch(tableList)
                .offset(offsetState)
                .registerConsumer(consumer, recordSize);
        cdcRunner.startCdcRunner();
        if (EmptyKit.isNotNull(cdcRunner) && EmptyKit.isNotNull(cdcRunner.getThrowable().get())) {
            throw ErrorKit.getLastCause(cdcRunner.getThrowable().get());
        }
    }

    private void buildSlot(TapConnectorContext connectorContext, Boolean needCheck) throws Throwable {
        if (EmptyKit.isNull(slotName)) {
            slotName = "tapdata_cdc_" + UUID.randomUUID().toString().replaceAll("-", "_");
            postgresJdbcContext.execute("SELECT pg_create_logical_replication_slot('" + slotName + "','" + logPluginName + "')");
            System.out.println("new logical replication slot created, slotName: " + slotName);
            //connectorContext.getStateMap().put("tapdata_pg_slot", slotName);
        } else if (needCheck) {
            AtomicBoolean existSlot = new AtomicBoolean(true);
            postgresJdbcContext.queryWithNext("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name='" + slotName + "'", resultSet -> {
                if (resultSet.getInt(1) <= 0) {
                    existSlot.set(false);
                }
            });
            if (existSlot.get()) {
                System.out.println("Using an existing logical replication slot, slotName: " + slotName);
            } else {
                System.out.println("The previous logical replication slot no longer exists. Although it has been rebuilt, there is a possibility of data loss. Please check");
            }
        }
    }

    public static Connection getConnect(String username, String passwd) {
        String driver = "com.huawei.opengauss.jdbc.Driver";

        Connection conn = null;
        try {
            Class.forName(driver);
        } catch( Exception e ) {
            e.printStackTrace();
            return null;
        }
        try {
            //创建连接。
            conn = DriverManager.getConnection(sourceURL, username, passwd);
            System.out.println("Connection succeed!");
        } catch(Exception e) {
            e.printStackTrace();
            return null;
        }
        return conn;
    }
    // 以下代码将使用Properties对象作为参数建立连接
    public static Connection getConnectUseProp(String username, String passwd) {
        String driver = "com.huawei.opengauss.jdbc.Driver";
        String sURL = sourceURL + "?autoBalance=true";
        Connection conn = null;
        Properties info = new Properties();
        try {
            Class.forName(driver);
        } catch( Exception e ) {
            e.printStackTrace();
            return null;
        }
        try {
            info.setProperty("user", username);
            info.setProperty("password", passwd);
            conn = DriverManager.getConnection(sURL, info);
            System.out.println("Connection succeed!");
        } catch(Exception e) {
            e.printStackTrace();
            return null;
        }
        return conn;
    }

}

