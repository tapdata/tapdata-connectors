package io.tapdata.connector.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class HbaseContext implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(HbaseContext.class);

    private final HbaseConfig hbaseConfig;
    private Connection connection;
    private UserGroupInformation ugi;

    public HbaseContext(HbaseConfig hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
        initConnection();
    }

    private void initConnection() {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", hbaseConfig.getZookeeperQuorum());
            if (hbaseConfig.getZookeeperParent() != null) {
                conf.set("zookeeper.znode.parent", hbaseConfig.getZookeeperParent());
            }
            conf.setInt("hbase.client.retries.number", 3);
            conf.setInt("zookeeper.recovery.retry", 3);

            String user = hbaseConfig.getUser();
            if (user != null && !user.trim().isEmpty()) {
                conf.set("hbase.security.authentication", "simple");
                conf.set("hadoop.security.authentication", "simple");
                this.ugi = UserGroupInformation.createRemoteUser(user.trim());
                this.connection = ugi.doAs(
                        (PrivilegedExceptionAction<Connection>) () -> ConnectionFactory.createConnection(conf));
            } else {
                this.connection = ConnectionFactory.createConnection(conf);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create HBase connection: " + e.getMessage(), e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    /**
     * Create a new Admin instance. Callers must close it with try-with-resources.
     * Admin is not thread-safe, so each call creates a fresh instance.
     */
    public Admin getAdmin() throws IOException {
        return connection.getAdmin();
    }

    public HbaseConfig getHbaseConfig() {
        return hbaseConfig;
    }

    @Override
    public void close() {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            logger.warn("Failed to close HBase connection: {}", e.getMessage());
        }
    }
}
