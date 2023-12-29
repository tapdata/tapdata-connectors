package io.tapdata.connector.hudi.write;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.config.*;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.index.HoodieIndex;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieIndexConfig.BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES;

public class ClientEntity implements AutoCloseable {
    public static final long CACHE_TIME = 300000; //5min
    FileSystem fs;
    String database;
    String tableId;
    String tablePath;
    Path path;

    HoodieJavaWriteClient<HoodieRecordPayload> client;

    Set<String> primaryKeys;
    Set<String> preCombineFields;
    Set<String> orderingFields;
    Set<String> partitionKeys;

    Log log;

    long timestamp;

    Schema schema;

    TapTable tapTable;
    Configuration hadoopConf;

    WriteOperationType operationType;

    static class Param {
        Configuration hadoopConf;
        String database;
        String tableId;
        String tablePath;
        TapTable tapTable;
        WriteOperationType operationType;
        Log log;

        public static Param witStart() {
            return new Param();
        }
        public Param withHadoopConf(Configuration hadoopConf) {
            this.hadoopConf = hadoopConf;
            return this;
        }

        public Param withDatabase(String database) {
            this.database = database;
            return this;
        }

        public Param withTableId(String tableId) {
            this.tableId = tableId;
            return this;
        }

        public Param withTablePath(String tablePath) {
            this.tablePath = tablePath;
            return this;
        }

        public Param withTapTable(TapTable tapTable) {
            this.tapTable = tapTable;
            return this;
        }

        public Param withOperationType(WriteOperationType operationType) {
            this.operationType = operationType;
            return this;
        }

        public Param withLog(Log log) {
            this.log = log;
            return this;
        }

        public Configuration getHadoopConf() {
            return hadoopConf;
        }

        public String getDatabase() {
            return database;
        }

        public String getTableId() {
            return tableId;
        }

        public String getTablePath() {
            return tablePath;
        }

        public TapTable getTapTable() {
            return tapTable;
        }

        public WriteOperationType getOperationType() {
            return operationType;
        }

        public Log getLog() {
            return log;
        }
    }
    public ClientEntity(Param param) {
        //@TODO
        this.operationType = param.getOperationType();

        this.hadoopConf = param.getHadoopConf();
        this.tableId = param.getTableId();
        this.tablePath = param.getTablePath();
        try {
            initFs();
        } catch (IOException e) {
            throw new CoreException("Can not init FileSystem, table path: {}", tablePath, e);
        }
        this.database = param.getDatabase();
        this.tapTable = param.getTapTable();
        this.primaryKeys = getAllPrimaryKeys();
        this.partitionKeys = getAllPartitionKeys();
        saveArvoSchema();
        try {
            initClient(hadoopConf);
        } catch (IOException e) {
            throw new CoreException("Can not init Hadoop client, table path: {}", tablePath, e);
        }
        this.log = param.getLog();
        updateTimestamp();
    }

    //@TODO
    String tableType = HoodieTableType.COPY_ON_WRITE.name();
    private void initFs() throws IOException {
        this.fs = FSUtils.getFs(tablePath, hadoopConf);
        path = new Path(tablePath);
        if (!fs.exists(path)) {
            HoodieTableMetaClient.withPropertyBuilder()
                    .setTableType(tableType)
                    .setTableName(tableId)
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .initTable(hadoopConf, tablePath);
        }
    }

    //@TODO preCombineField, orderingField
    String preCombineField = "id";
    String orderingField = "ts";

    private Long smallFileLimit = 25 * 1024 * 1024L;
    private Long maxFileSize = 32 * 1024 * 1024L;

    private static Integer recordSizeEstimate = 64;
    void initClient(Configuration hadoopConf) throws IOException {
        List<String> writeFiledNames = schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
        final boolean shouldCombine = WriteOperationType.UPSERT.equals(operationType) && writeFiledNames.contains(preCombineField);
        final boolean shouldOrdering = WriteOperationType.UPSERT.equals(operationType) && writeFiledNames.contains(orderingField);
        final String payloadClassName = shouldOrdering ? DefaultHoodieRecordPayload.class.getName() :
                shouldCombine ? OverwriteWithLatestAvroPayload.class.getName() : HoodieAvroPayload.class.getName();
        final Path hoodiePath = new Path(tablePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME);
        if (!(fs.exists(path) && fs.exists(hoodiePath))) {
            if (Arrays.asList(WriteOperationType.INSERT, WriteOperationType.UPSERT).contains(operationType)) {
                HoodieTableMetaClient.withPropertyBuilder()
                        .setTableType(tableType)
                        .setTableName(tableId)
                        .setPayloadClassName(payloadClassName)
                        .setRecordKeyFields(StringUtils.join(primaryKeys, ","))
                        .setPreCombineField(preCombineField)
                        .setPartitionFields(StringUtils.join(partitionKeys, ","))
                        .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                        .initTable(hadoopConf, tablePath);
            } else if (WriteOperationType.DELETE.equals(operationType)) {
                throw new TableNotFoundException(tablePath);
            }
        }
        final Properties indexProperties = new Properties();
        indexProperties.put(BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES.key(), 150000); // 1000万总体时间提升1分钟
        final HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
                .withSchema(schema.toString())
                .withParallelism(2, 2).withDeleteParallelism(2)
                .forTable(tableId)
                .withWritePayLoad(payloadClassName)
                .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadOrderingField(orderingField).build())
                .withIndexConfig(HoodieIndexConfig.newBuilder()
                        .withIndexType(HoodieIndex.IndexType.BLOOM)
//                            .bloomIndexPruneByRanges(false) // 1000万总体时间提升1分钟
                        .bloomFilterFPP(0.000001)   // 1000万总体时间提升3分钟
                        .fromProperties(indexProperties)
                        .build())
                .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                        .compactionSmallFileSize(smallFileLimit)
                        .approxRecordSize(recordSizeEstimate).build())
                .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(150, 200).build())
                .withCleanConfig(HoodieCleanConfig.newBuilder().retainCommits(100).build())
                .withStorageConfig(HoodieStorageConfig.newBuilder()
                        .parquetMaxFileSize(maxFileSize).build())
                .build();
        final HoodieJavaEngineContext engineContext = new HoodieJavaEngineContext(hadoopConf);
        client = new HoodieJavaWriteClient<>(engineContext, cfg);
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

    public HoodieJavaWriteClient<HoodieRecordPayload> getClient() {
        updateTimestamp();
        return client;
    }

    public void setClient(HoodieJavaWriteClient<HoodieRecordPayload> client) {
        updateTimestamp();
        this.client = client;
    }

    private void saveArvoSchema() {
        updateTimestamp();
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(hadoopConf)
                .setBasePath(tablePath)
                .setLoadActiveTimelineOnLoad(true)
                .build();
        TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
        try {
            this.schema = schemaResolver.getTableAvroSchema();
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
        log.debug("Table [{}] client info do closed", tableId);
    }

    private void closeFileSystem() {
        try {
            if (null != fs) {
                fs.close();
                fs = null;
                log.debug("File System has closed, table id: {}", tableId);
            }
        } catch (Exception e) {
            log.warn("Fail to close File System, table id: [{}], error message: {}", tableId, e.getMessage());
        }
    }

    private void closeClient() {
        try {
            if (null != client) {
                client.close();
                client = null;
                log.debug("SparkRDDWriteClient has closed, table id: {}", tableId);
            }
        } catch (Exception e) {
            log.warn("Fail to close SparkRDDWriteClient, table id: [{}], error message: {}", tableId, e.getMessage());
        }
    }

    public Set<String> getPrimaryKeys() {
        updateTimestamp();
        return primaryKeys;
    }

    public void setPrimaryKeys(Set<String> primaryKeys) {
        updateTimestamp();
        this.primaryKeys = primaryKeys;
    }

    public Set<String> getPartitionKeys() {
        updateTimestamp();
        return partitionKeys;
    }

    public void setPartitionKeys(Set<String> partitionKeys) {
        updateTimestamp();
        this.partitionKeys = partitionKeys;
    }

    public synchronized long getTimestamp() {
        updateTimestamp();
        return timestamp;
    }

    private Set<String> getAllPrimaryKeys(){
        Set<String> pks = new HashSet<>();
        Collection<String> keys = tapTable.primaryKeys(true);
        if (null != keys && !keys.isEmpty()) {
            pks.addAll(keys);
        } else {
            pks.add("uuid");
        }
        return pks;
    }

    private Set<String> getAllPartitionKeys(){
        //@TODO do not support partition table now,
        return new HashSet<>();
    }
}
