package io.tapdata.connector.hudi.write;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Date;

public class ClientEntity implements AutoCloseable {
    public static final long CACHE_TIME = 300000; //5min
    FileSystem fs;
    String database;
    String tableId;
    String tablePath;

    JavaSparkContext jsc;
    SparkRDDWriteClient<HoodieAvroPayload> client;

    String primaryKeys;
    String partitionKeys;

    Log log;

    long timestamp;

    Schema schema;

    public ClientEntity(FileSystem fs, String database, String tableId, String tablePath, String primaryKeys, String partitionKeys, JavaSparkContext jsc, Log log) {
        this.fs = fs;
        this.database = database;
        this.tableId = tableId;
        this.tablePath = tablePath;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.jsc = jsc;
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
                .withPath(tablePath)
                .withSchema(HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA)
                .withParallelism(2, 2)
                .withDeleteParallelism(2).forTable(tableId)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build())
                .build();
        this.client = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), cfg);
        saveArvoSchema();
        this.log = log;
        updateTimestamp();
    }

    public void updateTimestamp() {
        this.timestamp = new Date().getTime();
    }

    public FileSystem getFs() {
        updateTimestamp();
        return fs;
    }

    public void setFs(FileSystem fs) {
        updateTimestamp();
        this.fs = fs;
    }

    public String getDatabase() {
        updateTimestamp();
        return database;
    }

    public void setDatabase(String database) {
        updateTimestamp();
        this.database = database;
    }

    public String getTableId() {
        updateTimestamp();
        return tableId;
    }

    public void setTableId(String tableId) {
        updateTimestamp();
        this.tableId = tableId;
    }

    public String getTablePath() {
        updateTimestamp();
        return tablePath;
    }

    public void setTablePath(String tablePath) {
        updateTimestamp();
        this.tablePath = tablePath;
    }

    public SparkRDDWriteClient<HoodieAvroPayload> getClient() {
        updateTimestamp();
        return client;
    }

    public void setClient(SparkRDDWriteClient<HoodieAvroPayload> client) {
        updateTimestamp();
        this.client = client;
    }

    public JavaSparkContext getJsc() {
        updateTimestamp();
        return jsc;
    }

    public void setJsc(JavaSparkContext jsc) {
        updateTimestamp();
        this.jsc = jsc;
    }

    private void saveArvoSchema() {
        updateTimestamp();
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(jsc.hadoopConfiguration())
                .setBasePath(tablePath)
                .setLoadActiveTimelineOnLoad(true)
                .build();
        TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
        try {
            this.schema = schemaResolver.convertParquetSchemaToAvro(schemaResolver.getTableParquetSchema());
        } catch (Exception e) {
            throw new CoreException("Can not find Schema from HuDi, table id: {}, error message: {}", tableId, e.getMessage());
        }
        if (null == schema) {
            throw new CoreException("Can not find Schema from HuDi, table id: {}", tableId);
        }
    }

    public Schema getSchema() {
        updateTimestamp();
        return schema;
    }

    @Override
    public void close() {
        updateTimestamp();
    }

    public void doClose() {
        log.debug("Table [{}] client info do close", tableId);
        closeFileSystem();
        closeClient();
        closeJavaSparkContext();
        log.debug("Table [{}] client info do closed", tableId);
    }

    private void closeFileSystem() {
        try {
            fs.close();
            log.debug("File System has closed, table id: {}", tableId);
        } catch (Exception e) {
            log.warn("Fail to close File System, table id: [{}], error message: {}", tableId, e.getMessage());
        }
    }

    private void closeClient() {
        try {
            client.close();
            log.debug("SparkRDDWriteClient has closed, table id: {}", tableId);
        } catch (Exception e) {
            log.warn("Fail to close SparkRDDWriteClient, table id: [{}], error message: {}", tableId, e.getMessage());
        }
    }

    private void closeJavaSparkContext() {
        try {
            jsc.close();
            log.debug("JavaSparkContext has closed, table id: {}", tableId);
        } catch (Exception e) {
            log.warn("Fail to close JavaSparkContext, table id: [{}], error message: {}", tableId, e.getMessage());
        }
    }

    public String getPrimaryKeys() {
        updateTimestamp();
        return primaryKeys;
    }

    public void setPrimaryKeys(String primaryKeys) {
        updateTimestamp();
        this.primaryKeys = primaryKeys;
    }

    public String getPartitionKeys() {
        updateTimestamp();
        return partitionKeys;
    }

    public void setPartitionKeys(String partitionKeys) {
        updateTimestamp();
        this.partitionKeys = partitionKeys;
    }

    public synchronized long getTimestamp() {
        updateTimestamp();
        return timestamp;
    }
}
