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
    static String sourceURL = "jdbc:opengauss://*.*.*.*:8000/postgres";
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
            System.out.println(toJson(events));
            System.out.println("===============================");
        }
    }

    //@BeforeEach
    void init() throws ClassNotFoundException {
        pwd = "***";
        username = "****";
        Class.forName("com.huawei.opengauss.jdbc.Driver");
        tableList.add("public.ddl_table");
        connectionConfig.put("user", username);
        connectionConfig.put("password", pwd);
        connectionConfig.put("host", "*.*.*.*");
        connectionConfig.put("port", 8000);
        connectionConfig.put("haHost", "*.*.*.*");
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

    //@Test
    public void testConnection() throws Throwable {
        //getConnect(username, pwd);
        //getConnectUseProp(username, pwd);
        cdcRunner = new GaussDBRunner().init(postgresConfig, new TapLog());
        //testReplicateIdentity(nodeContext.getTableMap());
        //buildSlot(connectionContext, true);
        slotName = "slot1";//"tapdata_cdc_9d1b7907_e5e7_462a_9a8a_7e5485840466";//"slot1";
        cdcRunner.useSlot(slotName.toString())
                .watch(tableList)
                .offset(offsetState)
                .supplierIsAlive(()-> true)
                .waitTime(10)
                .registerConsumer(consumer, recordSize);
        cdcRunner.startCdcRunner();
        if (EmptyKit.isNotNull(cdcRunner) && EmptyKit.isNotNull(cdcRunner.getThrowable().get())) {
            throw ErrorKit.getLastCause(cdcRunner.getThrowable().get());
        }
    }
}

