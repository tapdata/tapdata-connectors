package io.tapdata.connector.hudi.write;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.connector.hudi.util.SiteXMLUtil;
import io.tapdata.entity.error.CoreException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.concurrent.atomic.AtomicReference;

public class ClientHandler {
    HudiConfig config;
    HiveJdbcContext hiveJdbcContext;

    Configuration hadoopConf;
    private static final String DESCRIBE_EXTENDED_SQL = "describe Extended `%s`.`%s`";

    public ClientHandler(HudiConfig config, HiveJdbcContext hiveJdbcContext) {
        this.config = config;
        this.hiveJdbcContext = hiveJdbcContext;
        initHadoopConfig();
    }

    public String getTablePath(String tableId) {
        final String sql = String.format(DESCRIBE_EXTENDED_SQL, config.getDatabase(), tableId);
        AtomicReference<String> tablePath = new AtomicReference<>();
        try {
            hiveJdbcContext.normalQuery(sql, resultSet -> {
                while (resultSet.next()) {
                    if ("Location".equals(resultSet.getString("col_name"))) {
                        tablePath.set(resultSet.getString("data_type"));
                        break;
                    }
                }
            });
        } catch (Exception e) {
            throw new CoreException("Fail to get table path from hdfs system, select sql: {}, error message: {}", sql, e.getMessage());
        }
        if (null == tablePath.get()) {
            throw new CoreException("Can not get table path from hdfs system, select sql: {}", sql);
        }
        return tablePath.get();
    }

    private void initHadoopConfig() {
        hadoopConf = new Configuration();
        hadoopConf.clear();
        hadoopConf.set("dfs.namenode.kerberos.principal", config.getPrincipal());
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hadoopConf.set("fs.hdfs.impl.disable.cache", "true");
        hadoopConf.set("mapreduce.app-submission.cross-platform", "true");
        hadoopConf.set("dfs.client.socket-timeout", "600000");
        hadoopConf.set("dfs.image.transfer.timeout", "600000");
        hadoopConf.set("dfs.client.failover.proxy.provider.hacluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hadoopConf.setInt("ipc.maximum.response.length", 352518912);
        hadoopConf.addResource(new Path(config.authFilePath(SiteXMLUtil.CORE_SITE_NAME)));
        hadoopConf.addResource(new Path(config.authFilePath(SiteXMLUtil.HDFS_SITE_NAME)));
        hadoopConf.addResource(new Path(config.authFilePath(SiteXMLUtil.HIVE_SITE_NAME)));
    }

    public Configuration getHadoopConf() {
        return hadoopConf;
    }
}
