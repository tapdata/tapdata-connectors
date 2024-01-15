package io.tapdata.connector.hudi.write;

import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.index.HoodieIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.hudi.config.HoodieIndexConfig.BLOOM_INDEX_FILTER_DYNAMIC_MAX_ENTRIES;

public class ClientPerformer implements AutoCloseable {
    private FileSystem fs;
    private String database;
    private String tableId;
    private String tablePath;
    private Path path;
    private HoodieJavaWriteClient<HoodieRecordPayload> client;
    private Set<String> primaryKeys;
    private Set<String> preCombineFields;
    private Set<String> orderingFields;
    private Set<String> partitionKeys;
    private Log log;
    private long timestamp;
    private boolean usage;
    private Schema schema;
    private TapTable tapTable;
    private Configuration hadoopConf;
    private WriteOperationType operationType;
    private HudiConfig config;
    private String tableType;

    //@TODO preCombineField, orderingField
    private String preCombineField = "id";
    private String orderingField = "ts";
    private final Long smallFileLimit = 25 * 1024 * 1024L;
    private final Long maxFileSize = 32 * 1024 * 1024L;
    private final static Integer recordSizeEstimate = 64;

    public ClientPerformer(Param param) {
        updateTimestamp();
        this.tableType = Optional.ofNullable(param.getTableType()).orElse(HoodieTableType.COPY_ON_WRITE.name());
        this.config = param.getConfig();
        this.operationType = param.getOperationType();
        this.hadoopConf = param.getHadoopConf();
        this.tableId = param.getTableId();
        this.tablePath = param.getTablePath();
        try {
            initFs();
        } catch (IOException e) {
            throw new CoreException("Can not init FileSystem, table path: {}, message: {}", tablePath, e.getMessage(), e);
        }
        this.database = param.getDatabase();
        this.tapTable = param.getTapTable();
        this.primaryKeys = getAllPrimaryKeys();
        this.partitionKeys = getAllPartitionKeys();
        this.schema = param.getSchema();
        if (null == schema) {
            saveArvoSchema();
        }
        try {
            initClient(hadoopConf);
        } catch (IOException e) {
            throw new CoreException("Can not init Hadoop client, table path: {}", tablePath, e);
        }
        this.log = param.getLog();
    }

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

    private void initClient(Configuration hadoopConf) throws IOException {
        List<String> writeFiledNames = new ArrayList<>(tapTable.getNameFieldMap().keySet());//schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
        final boolean shouldCombine = WriteOperationType.UPSERT.equals(operationType) && writeFiledNames.contains(preCombineField);
        final boolean shouldOrdering = WriteOperationType.UPSERT.equals(operationType) && writeFiledNames.contains(orderingField);
        final String payloadClassName = shouldOrdering ? DefaultHoodieRecordPayload.class.getName() :
                shouldCombine ? OverwriteWithLatestAvroPayload.class.getName() : HoodieAvroPayload.class.getName();

        final Path hoodiePath = new Path(tablePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME);
        if (!(fs.exists(path) && fs.exists(hoodiePath))) {
            if (Arrays.asList(WriteOperationType.INSERT, WriteOperationType.UPSERT).contains(operationType)) {
                HoodieTableMetaClient.withPropertyBuilder()
                        .setTableType(tableType)                        .setTableName(tableId)
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
                .withAutoCommit(true)
                .withWriteBufferLimitBytes(100 * 1024 * 1024)
                .withParallelism(2, 2).withDeleteParallelism(2)
                .forTable(tableId)
                .withWritePayLoad(payloadClassName)
                .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadOrderingField(orderingField).build())
                .withIndexConfig(HoodieIndexConfig.newBuilder()
                        .withIndexType(HoodieIndex.IndexType.BLOOM)
                        //.bloomIndexPruneByRanges(false) // 1000万总体时间提升1分钟
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

    public void releaseUsage() {
        this.usage = false;
    }

    public HoodieJavaWriteClient<HoodieRecordPayload> getClient() {
        updateTimestamp();
        return client;
    }

    private void saveArvoSchema() {
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                .setConf(hadoopConf)
                .setBasePath(tablePath)
                .setLoadActiveTimelineOnLoad(true)
                .build();
        TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
        try {
            this.schema = schemaResolver.getTableAvroSchemaFromDataFile();
        } catch (Exception e) {
            throw new CoreException("Can not find Schema from hdfs file system, table id: {}, message: {}", tableId, e.getMessage(), e);
        }
        if (null == schema) {
            throw new CoreException("Can not find Schema from hdfs file system, table id: {}", tableId);
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

    public Set<String> getPartitionKeys() {
        updateTimestamp();
        return partitionKeys;
    }

    private Set<String> getAllPrimaryKeys(){
        Set<String> pks = new HashSet<>();
        Collection<String> keys = tapTable.primaryKeys(true);
        if (null != keys && !keys.isEmpty()) {
            pks.addAll(keys);
        } else {
            //无主键表， 仅追加写， 取第一个字段模拟主键用来生成hooidkey
            List<String> fields = new ArrayList<>(tapTable.getNameFieldMap().keySet());
            if (!fields.isEmpty()) {
                pks.add(fields.get(0));
            }
        }
        return pks;
    }

    private Set<String> getAllPartitionKeys(){
        //@TODO do not support partition table now,
        return new HashSet<>();
    }

    public Log getLog() {
        return log;
    }

    public TapTable getTapTable() {
        return tapTable;
    }
    static class Param {
        Configuration hadoopConf;
        String database;
        String tableId;
        String tablePath;
        TapTable tapTable;
        Schema schema;
        WriteOperationType operationType;
        Log log;
        String tableType;
        HudiConfig config;

        public Param withTableType(String tableType) {
            this.tableType = tableType;
            return this;
        }

        public String getTableType() {
            return tableType;
        }

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
        public Param withConfig(HudiConfig config) {
            this.config = config;
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
        public HudiConfig getConfig() {
            return config;
        }

        public Param withSchema(Schema jdbcSchema) {
            this.schema = jdbcSchema;
            return this;
        }

        public Schema getSchema() {
            return schema;
        }
    }
}
