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

    Predicate<Void> isAlive;
    Log log;

    /**
     * @apiNote Use function getClientEntity(TableId) to get ClientEntity
     * */
    private ConcurrentHashMap<String, ClientEntity> tablePathMap;

    private ClientEntity getClientEntity(String tableId) {
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
        String allPrimaryKeys = getAllPrimaryKeys();
        sparkConf.set("hoodie.datasource.write.recordkey.field", allPrimaryKeys);
        String allPartitionKeys = getAllPartitionKeys();
        if (null != allPartitionKeys) {
            sparkConf.set("hoodie.datasource.write.partitionpath.field", allPartitionKeys);
        }
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        clientEntity = new ClientEntity(getFileSystem(tableId, tablePath, sparkConf), config.getDatabase(), tableId, tablePath, allPrimaryKeys, allPartitionKeys, jsc, log);
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

    public HuDiWriteBySparkClient judgedAlive(Predicate<Void> isAlive) {
        this.isAlive = isAlive;
        return this;
    }

    protected boolean isAlive() {
        return null != isAlive && isAlive.test(null);
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

    private String getAllPrimaryKeys(){

        return "uuid";
    }

    private String getAllPartitionKeys(){

        return null;
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

    private Map<Integer, List<HoodieRecord<HoodieAvroPayload>>> groupRecordsByEventType(List<TapRecordEvent> events) {
        Map<Integer, List<HoodieRecord<HoodieAvroPayload>>> recordsGroup = new HashMap<>();
        List<HoodieRecord<HoodieAvroPayload>> recordsOneBatch = new ArrayList<>();
        List<List<HoodieKey>> deleteEventsKeys = new ArrayList<>();
        int tag = -1;
        String tableId = null;
        for (TapRecordEvent e : events) {
            HoodieRecord<HoodieAvroPayload> hoodieRecord = null;
            int tempTag = tag;
            String tempTableId = tableId;
            if (e instanceof TapInsertRecordEvent) {
                tag = 1;
                TapInsertRecordEvent insertRecord = (TapInsertRecordEvent)e;
                tableId = insertRecord.getTableId();
                hoodieRecord = genericRecord(tableId, insertRecord.getAfter());
            } else if (e instanceof TapUpdateRecordEvent) {
                tag = 2;
                TapUpdateRecordEvent updateRecord = (TapUpdateRecordEvent)e;
                tableId = updateRecord.getTableId();
                hoodieRecord = genericRecord(tableId, updateRecord.getAfter());
            } else if (e instanceof TapDeleteRecordEvent) {
                //@TODO how to delete
                tag = 3;
                TapDeleteRecordEvent deleteRecord = (TapDeleteRecordEvent)e;
                tableId = deleteRecord.getTableId();
                hoodieRecord = genericRecord(tableId, deleteRecord.getBefore());
            }

            if ((-1 != tempTag && tempTag != tag) || (null != tempTableId && !tempTableId.equals(tableId))) {
                recordsGroup.put(tempTag, recordsOneBatch);
                commitButch(tempTag, tempTableId, recordsOneBatch, deleteEventsKeys);
                recordsOneBatch = new ArrayList<>();
            }
            if (tag > 0 && null != hoodieRecord) {
                recordsOneBatch.add(hoodieRecord);
            }
        }

        return recordsGroup;
    }

    private void commitButch(int batchType, String tableId, List<HoodieRecord<HoodieAvroPayload>> batch, List<List<HoodieKey>> deleteEventsKeys) {
        ClientEntity clientEntity = getClientEntity(tableId);
        SparkRDDWriteClient<HoodieAvroPayload> client = clientEntity.getClient();
        switch (batchType) {
            case 1 :
            case 2 :
                String startCommit = client.startCommit();

                break;
            case 3 :
                String startDeleteCommit = client.startCommit();

                break;

        }
    }

    private HoodieRecord<HoodieAvroPayload> genericRecord(String tableId, Map<String, Object> record) {
        ClientEntity clientEntity = getClientEntity(tableId);
        Schema schema = clientEntity.getSchema();
        GenericRecord genericRecord = new GenericData.Record(schema);
        record.forEach(genericRecord::put);
        HoodieKey hoodieKey = new HoodieKey(clientEntity.getPrimaryKeys(), clientEntity.getPartitionKeys());
        HoodieAvroPayload payload = new HoodieAvroPayload(Option.of(genericRecord));
        return new HoodieAvroRecord<>(hoodieKey, payload);
    }

    private List<List<HoodieKey>> genericDeleteRecord(String tableId, Map<String, Object> record) {

        return null;
    }

    private HoodieKey genericHoodieKey(String tableId, Map<String, Object> record, Set<String> keyNames){
        TypedProperties props = new TypedProperties();
        props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "");
        new ComplexKeyGenerator(props);
        return null;
    }

    private void delete(List<TapRecordEvent> events) {

    }

    public void process() {
        String tableId = "";
        ClientEntity clientEntity = getClientEntity(tableId);

        SparkRDDWriteClient<HoodieAvroPayload> client = clientEntity.getClient();
        JavaSparkContext jsc = clientEntity.getJsc();



        // inserts
        String newCommitTime = client.startCommit();
        //LOG.info("Starting commit " + newCommitTime);

        List<HoodieRecord<HoodieAvroPayload>> records = dataGen.generateInserts(newCommitTime, 10);
        List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>(records);
        JavaRDD<HoodieRecord<HoodieAvroPayload>> writeRecords = jsc.parallelize(records, 1);
        client.upsert(writeRecords, newCommitTime);

        // updates
        newCommitTime = client.startCommit();
        //LOG.info("Starting commit " + newCommitTime);
        List<HoodieRecord<HoodieAvroPayload>> toBeUpdated = dataGen.generateUpdates(newCommitTime, 2);
        records.addAll(toBeUpdated);
        recordsSoFar.addAll(toBeUpdated);
        writeRecords = jsc.parallelize(records, 1);
        client.upsert(writeRecords, newCommitTime);

        // Delete
        newCommitTime = client.startCommit();
        //LOG.info("Starting commit " + newCommitTime);
        // just delete half of the records
        int numToDelete = recordsSoFar.size() / 2;
        List<HoodieKey> toBeDeleted = recordsSoFar.stream().map(HoodieRecord::getKey).limit(numToDelete).collect(Collectors.toList());
        JavaRDD<HoodieKey> deleteRecords = jsc.parallelize(toBeDeleted, 1);
        client.delete(deleteRecords, newCommitTime);

        // compaction
        if (HoodieTableType.valueOf(tableType) == HoodieTableType.MERGE_ON_READ) {
            Option<String> instant = client.scheduleCompaction(Option.empty());
            HoodieWriteMetadata<JavaRDD<WriteStatus>> writeStatues = client.compact(instant.get());
            client.commitCompaction(instant.get(), writeStatues.getCommitMetadata().get(), Option.empty());
        }

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

    public String file(String filePath) {
        System.out.println(filePath);
        return "D:\\Gavin\\kit\\IDEA\\hudi-demo\\java-client\\src\\main\\resources\\" + filePath;
    }

    private String startCommitAndGetCommitTime(ClientEntity clientEntity) {
        return clientEntity.getClient().startCommit();
    }
}
