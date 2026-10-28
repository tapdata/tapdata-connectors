package io.tapdata.connector.hudi.write;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.connector.hudi.util.SiteXMLUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.types.StructType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;

public class ClientHandler {
    private final HudiConfig config;
    private final HiveJdbcContext hiveJdbcContext;

    private Configuration hadoopConf;
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

    public Schema getJDBCSchema(String tableId, boolean nullable, Log log) {
        JdbcDialect dialect = JdbcDialects.get(config.getDatabaseUrl());
        try (Connection connection = hiveJdbcContext.getConnection();
             final Statement confStatement = connection.createStatement();
             PreparedStatement schemaQueryStatement = connection.prepareStatement(dialect.getSchemaQuery(tableId))) {
            final String settingSql = "set hive.resultset.use.unique.column.names = false";
            try {
                confStatement.execute(settingSql);
            } catch (SQLException e) {
                log.warn("Set config error before get JDBC schema, table id: {}, setting sql: {}, message: {}", tableId, settingSql, e.getMessage());
            }

            try (ResultSet rs = schemaQueryStatement.executeQuery()) {
                StructType structType = JdbcUtils.getSchema(rs, dialect, nullable);
                final String structName = "hoodie_" + tableId + "_record";
                final String recordNamespace = "hoodie." + tableId;
                return SchemaConverters.toAvroType(structType, false, structName, recordNamespace);
            }
        } catch (Exception e) {
            log.debug("Can not get schema by jdbc, will get schema from hdfs file system next, table id: {}, message: {}", tableId, e.getMessage());
        }
        return null;
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
