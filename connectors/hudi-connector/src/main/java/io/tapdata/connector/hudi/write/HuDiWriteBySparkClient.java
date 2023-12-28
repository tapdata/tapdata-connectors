package io.tapdata.connector.hudi.write;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.SparkConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HuDiWriteBySparkClient extends HudiWrite {
    private final ScheduledExecutorService cleanService = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> cleanFuture;
    private static final String DESCRIBE_EXTENDED_SQL = "describe Extended `%s`.`%s`";
    private static String tableType = HoodieTableType.COPY_ON_WRITE.name();
    private static String localKeytab = "user.keytab";
    private static String principal = "tapdata_test@HADOOP.COM";
    private static String krb5 = "krb5.conf";
    private static String coreSitePath = "core-site.xml";
    private static String hdfsSitePath = "hdfs-site.xml";

    private static String hiveSitePath = "hive-site.xml";
    Configuration hadoopConf;

    String insertDmlPolicy;
    String updateDmlPolicy;

    Log log;

    /**
     * @apiNote Use function getClientEntity(TableId) to get ClientEntity
     * */
    private ConcurrentHashMap<String, ClientEntity> tablePathMap;

    private ClientEntity getClientEntity(TapTable tapTable) {
        final String tableId = tapTable.getId();
        ClientEntity clientEntity = null;
        synchronized (clientEntityLock) {
            if (tablePathMap.containsKey(tableId)
                    && null != tablePathMap.get(tableId)
                    && null != (clientEntity = tablePathMap.get(tableId))) {
                clientEntity.updateTimestamp();
                return clientEntity;
            }
        }
        String tablePath = getTablePath(tableId);
        /**
         * @see org.apache.hudi.keygen.constant.KeyGeneratorOptions
         * **/
        // add parimary keys : hoodie.datasource.write.recordkey.field
        // add partition keys : hoodie.datasource.write.partitionpath.field
        SparkConf sparkConf = HoodieExampleSparkUtils.buildSparkConf("hoodie-client-example", hadoopConf.getValByRegex(".*"));
        sparkConf.setMaster("local");
        sparkConf.set("spark.driver.maxResultSize", "8G");
        sparkConf.set("ipc.maximum.data.length", "8G");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        clientEntity = new ClientEntity(getFileSystem(tableId, tablePath, sparkConf), config.getDatabase(), tableId, tablePath, tapTable, jsc, log);
        synchronized (clientEntityLock) {
            tablePathMap.put(tableId, clientEntity);
        }
        return clientEntity;
    }

    HudiConfig config;
    FileSystem fs;

    final Object clientEntityLock = new Object();
    public HuDiWriteBySparkClient(HiveJdbcContext hiveJdbcContext, HudiConfig hudiConfig) {
        super(hiveJdbcContext, hudiConfig);
        init();
        this.config = hudiConfig;
        this.cleanFuture = this.cleanService.scheduleWithFixedDelay(() -> {
            ConcurrentHashMap.KeySetView<String, ClientEntity> tableIds = tablePathMap.keySet();
            long timestamp = new Date().getTime();
            for (String tableId : tableIds) {
                if (!isAlive()) {
                    break;
                }
                ClientEntity clientEntity = tablePathMap.get(tableId);
                if (null == clientEntity || timestamp - ClientEntity.CACHE_TIME >= clientEntity.getTimestamp()) {
                    synchronized (clientEntityLock) {
                        if (null != clientEntity) {
                            clientEntity.doClose();
                        }
                        tablePathMap.remove(tableId);
                    }
                }
            }
        }, 5, 5, TimeUnit.MINUTES);
    }

    public HuDiWriteBySparkClient log(Log log) {
        this.log = log;
        return this;
    }

    protected void init() {
        tablePathMap = new ConcurrentHashMap<>();
        hadoopConf = new Configuration();

        hadoopConf.clear();
        hadoopConf.set("dfs.namenode.kerberos.principal", "hdfs/hadoop.hadoop.com@HADOOP.COM");
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        hadoopConf.set("fs.hdfs.impl.disable.cache", "true");
        hadoopConf.set("mapreduce.app-submission.cross-platform", "true");
        hadoopConf.set("dfs.client.failover.proxy.provider.hacluster", "org.apache.hadoop.hdfs.server.namenode.ha.AdaptiveFailoverProxyProvider");
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        hadoopConf.addResource(new Path(file(coreSitePath)));
        hadoopConf.addResource(new Path(file(hdfsSitePath)));
        hadoopConf.addResource(new Path(file(hiveSitePath)));
    }

    private void cleanTableClientMap() {
        tablePathMap.values().stream().filter(Objects::nonNull).forEach(ClientEntity::doClose);
        tablePathMap.clear();
    }
    @Override
    public void onDestroy() {
        super.onDestroy();
        cleanTableClientMap();
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

    private FileSystem getFileSystem(String tableId, String tablePath, SparkConf sparkConf) {
        FileSystem fileSystem;
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            Path path = new Path(tablePath);
            fileSystem = FSUtils.getFs(path, jsc.hadoopConfiguration());
            if (!fs.exists(path)) {
                HoodieTableMetaClient.withPropertyBuilder()
                        .setTableType(tableType)
                        .setTableName(tableId)
                        .setPayloadClass(HoodieAvroPayload.class)
                        .initTable(jsc.hadoopConfiguration(), tablePath);
            }
        } catch (IOException e) {
            throw new CoreException("Can not get table path from hdfs system, table id: {}, table path: {}, error message: {}", tableId, tablePath, e.getMessage());
        }
        if (null == fileSystem) {
            throw new CoreException("Can not get table path from hdfs system, table id: {}, table path: {}", tableId, tablePath);
        }
        return fileSystem;
    }

    private WriteListResult<TapRecordEvent> groupRecordsByEventType(TapTable tapTable, List<TapRecordEvent> events, WriteListResult<TapRecordEvent> writeListResult) {
        TapRecordEvent errorRecord = null;
        List<HoodieRecord<HoodieAvroPayload>> recordsOneBatch = new ArrayList<>();
        List<HoodieKey> deleteEventsKeys = new ArrayList<>();
        TapRecordEvent batchFirstRecord = null;
        int tag = -1;
        //String tableId = null;
        try {
            for (TapRecordEvent e : events) {
                if (!isAlive()) break;
                if (-1 == tag) {
                    batchFirstRecord = e;
                }
                HoodieRecord<HoodieAvroPayload> hoodieRecord = null;
                int tempTag = tag;
                //String tempTableId = tableId;
                try {
                    if (e instanceof TapInsertRecordEvent) {
                        tag = 1;
                        TapInsertRecordEvent insertRecord = (TapInsertRecordEvent) e;
                        //tableId = insertRecord.getTableId();
                        hoodieRecord = genericRecord(tapTable, insertRecord.getAfter());
                    } else if (e instanceof TapUpdateRecordEvent) {
                        tag = 1;
                        TapUpdateRecordEvent updateRecord = (TapUpdateRecordEvent) e;
                        //tableId = updateRecord.getTableId();
                        hoodieRecord = genericRecord(tapTable, updateRecord.getAfter());
                    } else if (e instanceof TapDeleteRecordEvent) {
                        tag = 3;
                        TapDeleteRecordEvent deleteRecord = (TapDeleteRecordEvent) e;
                        //tableId = deleteRecord.getTableId();
                        deleteEventsKeys.add(genericDeleteRecord(tapTable, deleteRecord.getBefore()));
                    }

                    if ((-1 != tempTag && tempTag != tag)) { //|| (null != tempTableId && !tempTableId.equals(tableId))) {
                        commitButch(tempTag, tapTable, recordsOneBatch, deleteEventsKeys);
                        batchFirstRecord = e;
                    }
                    if (tag > 0 && null != hoodieRecord) {
                        recordsOneBatch.add(hoodieRecord);
                    }
                } catch (Exception fail) {
                    log.error("target database process message failed", "table name:{},error msg:{}", tapTable.getId(), fail.getMessage(), fail);
                    errorRecord = batchFirstRecord;
                    throw fail;
                }
            }
            if (!recordsOneBatch.isEmpty() || !deleteEventsKeys.isEmpty()) {
                try {
                    commitButch(tag, tapTable, recordsOneBatch, deleteEventsKeys);
                } catch (Exception fail) {
                    log.error("target database process message failed", "table name:{},error msg:{}", tapTable.getId(), fail.getMessage(), fail);
                    errorRecord = batchFirstRecord;
                    throw fail;
                }
            }
        } catch (Throwable e) {
            if (null != errorRecord) writeListResult.addError(errorRecord, e);
            throw e;
        }
        return writeListResult;
    }

    private void commitButch(int batchType, TapTable tapTable, List<HoodieRecord<HoodieAvroPayload>> batch, List<HoodieKey> deleteEventsKeys) {
        ClientEntity clientEntity = getClientEntity(tapTable);
        SparkRDDWriteClient<HoodieAvroPayload> client = clientEntity.getClient();
        if (null == client) {
            throw new CoreException("Fail to write recodes, message: SparkRDDWriteClient can not be empty for table {}", clientEntity.tableId);
        }
        JavaSparkContext jsc = clientEntity.getJsc();
        if (null == jsc) {
            throw new CoreException("Fail to write recodes, message: JavaSparkContext can not be empty for table {}", clientEntity.tableId);
        }
        switch (batchType) {
            case 1 :
            case 2 :
                String startCommit = startCommitAndGetCommitTime(clientEntity);
                client.upsert(jsc.parallelize(batch, 1), startCommit);
                batch.clear();
                break;
            case 3 :
                String startDeleteCommit = startCommitAndGetCommitTime(clientEntity);
                client.delete(jsc.parallelize(deleteEventsKeys, 1), startDeleteCommit);
                deleteEventsKeys.clear();
                break;

        }
    }

    private HoodieRecord<HoodieAvroPayload> genericRecord(TapTable tapTable, Map<String, Object> record) {
        ClientEntity clientEntity = getClientEntity(tapTable);
        GenericRecord genericRecord = getGenericRecordOfRecord(tapTable, record);
        HoodieKey hoodieKey =  genericHoodieKey(genericRecord, clientEntity.getPrimaryKeys(), clientEntity.getPartitionKeys());
        HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(genericRecord));
        return new HoodieAvroRecord<>(hoodieKey, payload);
    }

    private GenericRecord getGenericRecordOfRecord(TapTable tapTable, Map<String, Object> record) {
        ClientEntity clientEntity = getClientEntity(tapTable);
        Schema schema = clientEntity.getSchema();
        GenericRecord genericRecord = new GenericData.Record(schema);
        record.forEach(genericRecord::put);
        return genericRecord;
    }
    private HoodieKey genericDeleteRecord(TapTable tapTable, Map<String, Object> record) {
        ClientEntity clientEntity = getClientEntity(tapTable);
        GenericRecord genericRecord = getGenericRecordOfRecord(tapTable, record);
        return genericHoodieKey(genericRecord, clientEntity.getPrimaryKeys(), clientEntity.getPartitionKeys());
    }

    private HoodieKey genericHoodieKey(GenericRecord genericRecord, Set<String> keyNames, Set<String> partitionKeys){
        TypedProperties props = new TypedProperties();
        props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), StringUtils.join(keyNames, ","));
        props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), StringUtils.join(partitionKeys, ","));
        ComplexKeyGenerator keyGenerator = new ComplexKeyGenerator(props);
        return keyGenerator.getKey(genericRecord);
    }

//    public void process() {
//        String tableId = "";
//        ClientEntity clientEntity = getClientEntity(tableId);
//
//        SparkRDDWriteClient<HoodieAvroPayload> client = clientEntity.getClient();
//        JavaSparkContext jsc = clientEntity.getJsc();
//
//
//
//        // inserts
//        String newCommitTime = client.startCommit();
//        //LOG.info("Starting commit " + newCommitTime);
//
//        List<HoodieRecord<HoodieAvroPayload>> records = dataGen.generateInserts(newCommitTime, 10);
//        List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>(records);
//        JavaRDD<HoodieRecord<HoodieAvroPayload>> writeRecords = jsc.parallelize(records, 1);
//        client.upsert(writeRecords, newCommitTime);
//
//        // updates
//        newCommitTime = client.startCommit();
//        //LOG.info("Starting commit " + newCommitTime);
//        List<HoodieRecord<HoodieAvroPayload>> toBeUpdated = dataGen.generateUpdates(newCommitTime, 2);
//        records.addAll(toBeUpdated);
//        recordsSoFar.addAll(toBeUpdated);
//        writeRecords = jsc.parallelize(records, 1);
//        client.upsert(writeRecords, newCommitTime);
//
//        // Delete
//        newCommitTime = client.startCommit();
//        //LOG.info("Starting commit " + newCommitTime);
//        // just delete half of the records
//        int numToDelete = recordsSoFar.size() / 2;
//        List<HoodieKey> toBeDeleted = recordsSoFar.stream().map(HoodieRecord::getKey).limit(numToDelete).collect(Collectors.toList());
//        JavaRDD<HoodieKey> deleteRecords = jsc.parallelize(toBeDeleted, 1);
//        client.delete(deleteRecords, newCommitTime);
//
//        // compaction
//        if (HoodieTableType.valueOf(tableType) == HoodieTableType.MERGE_ON_READ) {
//            Option<String> instant = client.scheduleCompaction(Option.empty());
//            HoodieWriteMetadata<JavaRDD<WriteStatus>> writeStatues = client.compact(instant.get());
//            client.commitCompaction(instant.get(), writeStatues.getCommitMetadata().get(), Option.empty());
//        }
//
//    }

    public WriteListResult<TapRecordEvent> writeByClient(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        this.insertDmlPolicy = dmlInsertPolicy(tapConnectorContext);
        this.updateDmlPolicy = dmlUpdatePolicy(tapConnectorContext);
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        //if (ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertDmlPolicy)) {
            return groupRecordsByEventType(tapTable, tapRecordEvents, writeListResult);
       // } else {
           // return null;
       // }
    }

    public WriteListResult<TapRecordEvent> writeRecord(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        return this.writeByClient(tapConnectorContext, tapTable, tapRecordEvents);
    }

    @Override
    public WriteListResult<TapRecordEvent> writeJdbcRecord(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        throw new UnsupportedOperationException("UnSupport JDBC operator");
    }

    public WriteListResult<TapRecordEvent> notExistsInsert(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEventList) throws Throwable {
        throw new UnsupportedOperationException("UnSupport JDBC operator");
    }

    @Override
    public WriteListResult<TapRecordEvent> writeOne(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        throw new UnsupportedOperationException("UnSupport JDBC operator");
    }

    @Override
    protected int doInsertOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        throw new UnsupportedOperationException("UnSupport JDBC operator");
    }

    @Override
    protected int doUpdateOne(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        throw new UnsupportedOperationException("UnSupport JDBC operator");
    }

    private String startCommitAndGetCommitTime(ClientEntity clientEntity) {
        return clientEntity.getClient().startCommit();
    }
}
