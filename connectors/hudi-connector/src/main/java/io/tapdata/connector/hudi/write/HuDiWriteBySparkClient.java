package io.tapdata.connector.hudi.write;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.connector.hudi.util.FileUtil;
import io.tapdata.connector.hudi.util.Krb5Util;
import io.tapdata.connector.hudi.util.SiteXMLUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.ErrorKit;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;


public class HuDiWriteBySparkClient extends HudiWrite {
    private final ScheduledExecutorService cleanService = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledFuture<?> cleanFuture;
    private static final String DESCRIBE_EXTENDED_SQL = "describe Extended `%s`.`%s`";
    Configuration hadoopConf;

    String insertDmlPolicy;
    String updateDmlPolicy;
    WriteOperationType appendType;

    Log log;

    /**
     * @apiNote Use function getClientEntity(TableId) to get ClientEntity
     * */
    private ConcurrentHashMap<String, ClientEntity> tablePathMap;

    private ClientEntity getClientEntity(TapTable tapTable) {
        final String tableId = tapTable.getId();
        ClientEntity clientEntity;
        synchronized (clientEntityLock) {
            if (tablePathMap.containsKey(tableId)
                    && null != tablePathMap.get(tableId)
                    && null != (clientEntity = tablePathMap.get(tableId))) {
                clientEntity.updateTimestamp();
                return clientEntity;
            }
        }
        String tablePath = getTablePath(tableId);
        //config.authenticate(hadoopConf);
        clientEntity = new ClientEntity(
            ClientEntity.Param.witStart()
                    .withHadoopConf(hadoopConf)
                    .withDatabase(config.getDatabase())
                    .withTableId(tableId)
                    .withTablePath(tablePath)
                    .withTapTable(tapTable)
                    .withOperationType(appendType)
                    .withConfig(config)
                    .withLog(log));
        synchronized (clientEntityLock) {
            tablePathMap.put(tableId, clientEntity);
        }
        return clientEntity;
    }

    HudiConfig config;

    final Object clientEntityLock = new Object();
    public HuDiWriteBySparkClient(HiveJdbcContext hiveJdbcContext, HudiConfig hudiConfig) {
        super(hiveJdbcContext, hudiConfig);
        this.config = hudiConfig;
        init();
       // hudiConfig.authenticate(hadoopConf);
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
        initConfig();
    }

    private void cleanTableClientMap() {
        tablePathMap.values().stream().filter(Objects::nonNull).forEach(ClientEntity::doClose);
        tablePathMap.clear();
    }
    @Override
    public void onDestroy() {
        super.onDestroy();
        Optional.ofNullable(cleanFuture).ifPresent(e -> ErrorKit.ignoreAnyError(() -> e.cancel(true)));
        ErrorKit.ignoreAnyError(cleanService::shutdown);
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

    private void afterCommit(final WriteListResult<TapRecordEvent> writeResult, final AtomicLong insert,  final AtomicLong update,  final AtomicLong delete) {
        writeResult.incrementInserted(insert.get());
        insert.set(0);
        writeResult.incrementModified(update.get());
        update.set(0);
        writeResult.incrementRemove(delete.get());
        delete.set(0);
    }

    private WriteListResult<TapRecordEvent> groupRecordsByEventType(TapTable tapTable, List<TapRecordEvent> events, WriteListResult<TapRecordEvent> writeListResult) {
        TapRecordEvent errorRecord = null;
        List<HoodieRecord<HoodieRecordPayload>> recordsOneBatch = new ArrayList<>();
        List<HoodieKey> deleteEventsKeys = new ArrayList<>();
        TapRecordEvent batchFirstRecord = null;
        int tag = -1;
        //String tableId = null;
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        try {
            for (TapRecordEvent e : events) {
                if (!isAlive()) break;
                if (-1 == tag) {
                    batchFirstRecord = e;
                }
                HoodieRecord<HoodieRecordPayload> hoodieRecord = null;
                int tempTag = tag;
                //String tempTableId = tableId;
                try {
                    if (e instanceof TapInsertRecordEvent) {
                        tag = 1;
                        insert.incrementAndGet();
                        TapInsertRecordEvent insertRecord = (TapInsertRecordEvent) e;
                        //tableId = insertRecord.getTableId();
                        hoodieRecord = genericRecord(tapTable, insertRecord.getAfter());
                    } else if (e instanceof TapUpdateRecordEvent) {
                        tag = 1;
                        update.incrementAndGet();
                        TapUpdateRecordEvent updateRecord = (TapUpdateRecordEvent) e;
                        //tableId = updateRecord.getTableId();
                        hoodieRecord = genericRecord(tapTable, updateRecord.getAfter());
                    } else if (e instanceof TapDeleteRecordEvent) {
                        tag = 3;
                        delete.incrementAndGet();
                        TapDeleteRecordEvent deleteRecord = (TapDeleteRecordEvent) e;
                        //tableId = deleteRecord.getTableId();
                        deleteEventsKeys.add(genericDeleteRecord(tapTable, deleteRecord.getBefore()));
                    }

                    if ((-1 != tempTag && tempTag != tag)) { //|| (null != tempTableId && !tempTableId.equals(tableId))) {
                        commitButch(tempTag, tapTable, recordsOneBatch, deleteEventsKeys);
                        batchFirstRecord = e;
                        afterCommit(writeListResult, insert, update, delete);
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
                    afterCommit(writeListResult, insert, update, delete);
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

    private WriteListResult<TapRecordEvent> commitButch(int batchType, TapTable tapTable, List<HoodieRecord<HoodieRecordPayload>> batch, List<HoodieKey> deleteEventsKeys) {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        ClientEntity clientEntity = getClientEntity(tapTable);
        HoodieJavaWriteClient<HoodieRecordPayload> client = clientEntity.getClient();
        if (null == client) {
            throw new CoreException("Fail to write recodes, message: SparkRDDWriteClient can not be empty for table {}", clientEntity.tableId);
        }
        switch (batchType) {
            case 1 :
            case 2 :
                String startCommit = startCommitAndGetCommitTime(clientEntity);
                client.upsert(batch, startCommit);
                batch.clear();
                break;
            case 3 :
                String startDeleteCommit = startCommitAndGetCommitTime(clientEntity);
                client.delete(deleteEventsKeys, startDeleteCommit);
                deleteEventsKeys.clear();
                break;

        }
        return writeListResult;
    }

    private HoodieRecord<HoodieRecordPayload> genericRecord(TapTable tapTable, Map<String, Object> record) {
        ClientEntity clientEntity = getClientEntity(tapTable);
        GenericRecord genericRecord = getGenericRecordOfRecord(tapTable, record);
        HoodieKey hoodieKey =  genericHoodieKey(genericRecord, clientEntity.getPrimaryKeys(), clientEntity.getPartitionKeys());
        HoodieRecordPayload<HoodieAvroPayload> payload = new org.apache.hudi.common.model.HoodieAvroPayload(Option.of(genericRecord));
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
        SimpleKeyGenerator keyGenerator = new SimpleKeyGenerator(props);
        return keyGenerator.getKey(genericRecord);
    }

    public WriteListResult<TapRecordEvent> writeByClient(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) throws Throwable {
        this.insertDmlPolicy = dmlInsertPolicy(tapConnectorContext);
        this.updateDmlPolicy = dmlUpdatePolicy(tapConnectorContext);
        this.appendType = appendType(tapConnectorContext);
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

    private void initConfig() {
        String confPath = FileUtil.paths(config.getKrb5Path(), Krb5Util.KRB5_NAME);
        String krb5Path = confPath.replaceAll("\\\\","/");
        System.setProperty("HADOOP_USER_NAME","tapdata_test");
        System.setProperty("KERBEROS_USER_KEYTAB", krb5Path);
        hadoopConf = new Configuration();
        hadoopConf.clear();
        hadoopConf.set("dfs.namenode.kerberos.principal", config.getPrincipal());//"hdfs/hadoop.hadoop.com@HADOOP.COM");
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

    /**
     * @deprecated engine not support send writeStrategy to connector
     * hudi only support updateOrInsert or appendWrite now
     * */
    protected WriteOperationType appendType(TapConnectorContext tapConnectorContext) {
        DataMap configOptions = tapConnectorContext.getSpecification().getConfigOptions();
        String writeStrategy = String.valueOf(configOptions.get("writeStrategy"));
        switch (writeStrategy) {
            case "appendWrite": return WriteOperationType.INSERT;
            default: return WriteOperationType.UPSERT;
        }

    }
}
