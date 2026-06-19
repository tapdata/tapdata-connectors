package io.tapdata.connector.hbase;

import io.tapdata.pdk.apis.entity.TestItem;

import static io.tapdata.base.ConnectorBase.testItem;

public class HbaseTest {

    private final HbaseConfig hbaseConfig;
    private HbaseContext hbaseContext;

    public HbaseTest(HbaseConfig hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
    }

    public TestItem testHostPort() {
        try {
            String quorum = hbaseConfig.getZookeeperQuorum();
            if (quorum == null || quorum.trim().isEmpty()) {
                return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "ZooKeeper Quorum is empty");
            }
            String[] hosts = quorum.split(",");
            for (String hostPort : hosts) {
                String[] parts = hostPort.trim().split(":");
                String host = parts[0].trim();
                int port = 2181;
                if (parts.length > 1) {
                    try {
                        port = Integer.parseInt(parts[1].trim());
                    } catch (NumberFormatException e) {
                        return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED,
                                "Invalid port in ZooKeeper quorum: " + parts[1]);
                    }
                }
                java.net.Socket socket = new java.net.Socket();
                try {
                    socket.connect(new java.net.InetSocketAddress(host, port), 5000);
                } finally {
                    socket.close();
                }
            }
        } catch (Exception e) {
            return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage());
        }
        return testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY);
    }

    public TestItem testConnect() {
        try {
            hbaseContext = new HbaseContext(hbaseConfig);
            hbaseContext.getAdmin().listTableNames();
            return testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_SUCCESSFULLY);
        } catch (Exception e) {
            return testItem(TestItem.ITEM_LOGIN, TestItem.RESULT_FAILED, e.getMessage());
        }
    }

    public HbaseContext getHbaseContext() {
        return hbaseContext;
    }

    public void close() {
        if (hbaseContext != null) {
            hbaseContext.close();
        }
    }
}
