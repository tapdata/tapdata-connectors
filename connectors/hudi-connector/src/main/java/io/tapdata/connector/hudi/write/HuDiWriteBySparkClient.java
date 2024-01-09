package io.tapdata.connector.hudi.write;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.connector.hudi.util.FileUtil;
import io.tapdata.connector.hudi.util.Krb5Util;
import io.tapdata.connector.hudi.util.SiteXMLUtil;
import io.tapdata.connector.hudi.write.generic.GenericDeleteRecord;
import io.tapdata.connector.hudi.write.generic.HoodieRecordGenericStage;
import io.tapdata.connector.hudi.write.generic.entity.NormalEntity;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.*;
import org.apache.hudi.exception.HoodieRollbackException;


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

    class CommitAcceptData {
        int type;
        Object data;
        public CommitAcceptData(Object data, int type) {
            this.type = type;
            this.data = data;
        }
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

    private WriteListResult<TapRecordEvent> afterCommit(final AtomicLong insert,  final AtomicLong update,  final AtomicLong delete, Consumer<WriteListResult<TapRecordEvent>> consumer) {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        writeListResult.incrementInserted(insert.get());
        writeListResult.incrementModified(update.get());
        writeListResult.incrementRemove(delete.get());
        consumer.accept(writeListResult);
        delete.set(0);
        update.set(0);
        insert.set(0);
        return writeListResult;
    }

    private WriteListResult<TapRecordEvent> groupRecordsByEventType(TapTable tapTable, List<TapRecordEvent> events, Consumer<WriteListResult<TapRecordEvent>> consumer) {
        WriteListResult<TapRecordEvent> writeListResult = new WriteListResult<>(0L, 0L, 0L, new HashMap<>());
        TapRecordEvent errorRecord = null;
        List<HoodieRecord<HoodieRecordPayload>> recordsOneBatch = new ArrayList<>();
        List<HoodieKey> deleteEventsKeys = new ArrayList<>();
        TapRecordEvent batchFirstRecord = null;
        int tag = -1;
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        final ClientEntity clientEntity = getClientEntity(tapTable);
        final NormalEntity entity = new NormalEntity().withClientEntity(clientEntity).withTapTable(tapTable);
        try {
            for (TapRecordEvent e : events) {
                if (!isAlive()) break;
                if (-1 == tag) {
                    batchFirstRecord = e;
                }
                HoodieRecord<HoodieRecordPayload> hoodieRecord = null;
                int tempTag = tag;
                try {
                    if (e instanceof TapInsertRecordEvent) {
                        tag = 1;
                        insert.incrementAndGet();
                        TapInsertRecordEvent insertRecord = (TapInsertRecordEvent) e;
                        hoodieRecord = HoodieRecordGenericStage.singleton().generic(insertRecord.getAfter(), entity);
                    } else if (e instanceof TapUpdateRecordEvent) {
                        tag = 1;
                        update.incrementAndGet();
                        TapUpdateRecordEvent updateRecord = (TapUpdateRecordEvent) e;
                        hoodieRecord = HoodieRecordGenericStage.singleton().generic(updateRecord.getAfter(), entity);
                    } else if (e instanceof TapDeleteRecordEvent) {
                        tag = 3;
                        delete.incrementAndGet();
                        TapDeleteRecordEvent deleteRecord = (TapDeleteRecordEvent) e;
                        deleteEventsKeys.add(GenericDeleteRecord.singleton().generic(deleteRecord.getBefore(), entity));
                    }

                    if ((-1 != tempTag && tempTag != tag)) {
                        commitButch(clientEntity, tempTag, recordsOneBatch, deleteEventsKeys);
                        batchFirstRecord = e;
                        afterCommit(insert, update, delete, consumer);
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
        } catch (Throwable e) {
            if (null != errorRecord) writeListResult.addError(errorRecord, e);
            throw e;
        } finally {
            if (!recordsOneBatch.isEmpty()) {
                try {
                    commitButch(clientEntity, 1, recordsOneBatch, deleteEventsKeys);
                    afterCommit(insert, update, delete, consumer);
                } catch (Exception fail) {
                    log.error("target database process message failed", "table name:{},error msg:{}", tapTable.getId(), fail.getMessage(), fail);
                    errorRecord = batchFirstRecord;
                    throw fail;
                }
            }
            if (!deleteEventsKeys.isEmpty()) {
                try {
                    commitButch(clientEntity, 3, recordsOneBatch, deleteEventsKeys);
                    afterCommit(insert, update, delete, consumer);
                } catch (Exception fail) {
                    log.error("target database process message failed", "table name:{},error msg:{}", tapTable.getId(), fail.getMessage(), fail);
                    errorRecord = batchFirstRecord;
                    throw fail;
                }
            }
        }
        return writeListResult;
    }

    private void commitButch(ClientEntity clientEntity, int batchType, List<HoodieRecord<HoodieRecordPayload>> batch, List<HoodieKey> deleteEventsKeys) {
        if (batchType != 1 && batchType != 2 && batchType != 3) return;
       //long s = System.currentTimeMillis();
        HoodieJavaWriteClient<HoodieRecordPayload> client = clientEntity.getClient();
        String startCommit;
        client.setOperationType(appendType);
        startCommit = startCommitAndGetCommitTime(clientEntity);
        ErrorKit.ignoreAnyError(()->Thread.sleep(500));

        try {
            switch (batchType) {
                case 1:
                case 2:
                    List<WriteStatus> insert;
                    if (WriteOperationType.INSERT.equals(this.appendType)) {
                        insert = client.insert(batch, startCommit);
                    } else {
                        insert = client.upsert(batch, startCommit);
                    }
                    client.commit(startCommit, insert);
                    batch.clear();
                    break;
                case 3:
                    if (!WriteOperationType.INSERT.equals(this.appendType)) {
                        List<WriteStatus> delete = client.delete(deleteEventsKeys, startCommit);
                        client.commit(startCommit, delete);
                        deleteEventsKeys.clear();
                    }
                    break;
            }
        } catch (HoodieRollbackException e) {
            client.rollback(startCommit);
            throw e;
        }
        //System.out.println("[TAP_QPS] one commit cost: " + (System.currentTimeMillis() - s));
    }

    public WriteListResult<TapRecordEvent> writeByClient(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        this.insertDmlPolicy = dmlInsertPolicy(tapConnectorContext);
        this.updateDmlPolicy = dmlUpdatePolicy(tapConnectorContext);
        this.appendType = appendType(tapConnectorContext, tapTable);
        return groupRecordsByEventType(tapTable, tapRecordEvents, consumer);
    }

    public WriteListResult<TapRecordEvent> writeRecord(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        return this.writeByClient(tapConnectorContext, tapTable, tapRecordEvents, consumer);
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

    /**
     * @deprecated engine not support send writeStrategy to connector
     * hudi only support updateOrInsert or appendWrite now,
     * the stage which name is Append-Only supported by table has not any one primary keys
     * */
    protected WriteOperationType appendType(TapConnectorContext tapConnectorContext, TapTable tapTable) {
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        if (null == primaryKeys || primaryKeys.isEmpty()) return WriteOperationType.INSERT;
        DataMap configOptions = tapConnectorContext.getSpecification().getConfigOptions();
        String writeStrategy = String.valueOf(configOptions.get("writeStrategy"));
        switch (writeStrategy) {
            case "appendWrite": return WriteOperationType.INSERT;
            default: return WriteOperationType.UPSERT;
        }
    }
}
