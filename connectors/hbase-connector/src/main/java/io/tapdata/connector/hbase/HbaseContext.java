package io.tapdata.connector.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseContext implements AutoCloseable {

    private final HbaseConfig hbaseConfig;
    private Connection connection;
    private Admin admin;

    public HbaseContext(HbaseConfig hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
        initConnection();
    }

    private void initConnection() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", hbaseConfig.getZookeeperQuorum());
        if (hbaseConfig.getZookeeperParent() != null) {
            conf.set("zookeeper.znode.parent", hbaseConfig.getZookeeperParent());
        }
        conf.setInt("hbase.client.retries.number", 3);
        conf.setInt("zookeeper.recovery.retry", 3);
        try {
            this.connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create HBase connection: " + e.getMessage(), e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public Admin getAdmin() throws IOException {
        if (admin == null) {
            admin = connection.getAdmin();
        }
        return admin;
    }

    @Override
    public void close() {
        try {
            if (admin != null) {
                admin.close();
            }
        } catch (Exception ignored) {
        }
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception ignored) {
        }
    }
}
