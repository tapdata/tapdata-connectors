package io.tapdata.connector.hudi.write;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.connector.hudi.util.AutoExpireInstance;
import io.tapdata.connector.hudi.util.FileUtil;
import io.tapdata.connector.hudi.util.Krb5Util;
import io.tapdata.connector.hudi.write.generic.GenericDeleteRecord;
import io.tapdata.connector.hudi.write.generic.GenericHoodieKey;
import io.tapdata.connector.hudi.write.generic.HoodieRecordGenericStage;
import io.tapdata.connector.hudi.write.generic.entity.KeyEntity;
import io.tapdata.connector.hudi.write.generic.entity.NormalEntity;
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
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.exception.HoodieRollbackException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.tapdata.base.ConnectorBase.list;
import static io.tapdata.base.ConnectorBase.toJson;


public class HuDiWriteBySparkClient extends HudiWrite {
    private final Configuration hadoopConf;
    private WriteOperationType appendType;
    private final Log log;
    private final Supplier<Boolean> isAlive;
    private String tableType;
    private final HudiConfig config;
    private final ClientHandler clientHandler;
    private final AutoExpireInstance<TapTable, String, ClientPerformer> autoExpireInstance;

    public HuDiWriteBySparkClient(HiveJdbcContext hiveJdbcContext, HudiConfig hudiConfig, Supplier<Boolean> isAlive, Log log) {
        super(hiveJdbcContext, hudiConfig);
        this.log = log;
        this.isAlive = isAlive;
        this.config = hudiConfig;
        this.clientHandler = new ClientHandler(config, hiveJdbcContext);
        String confPath = FileUtil.paths(config.getKrb5Path(), Krb5Util.KRB5_NAME);
        String krb5Path = confPath.replaceAll("\\\\","/");
        System.setProperty("HADOOP_USER_NAME", config.getUser());
        System.setProperty("KERBEROS_USER_KEYTAB", krb5Path);
        this.hadoopConf = this.clientHandler.getHadoopConf();
        autoExpireInstance = new AutoExpireInstance<>(
                TapTable::getId,
                (tapTable) -> {
                    final String tableId = tapTable.getId();
                    return new ClientPerformer(
                            ClientPerformer.Param.witStart()
                                    .withHadoopConf(hadoopConf)
                                    .withDatabase(config.getDatabase())
                                    .withTableId(tableId)
                                    .withTableType(tableType)
                                    .withTablePath(clientHandler.getTablePath(tableId))
                                    .withTapTable(tapTable)
                                    .withOperationType(appendType)
                                    .withConfig(config)
                                    .withSchema(this.clientHandler.getJDBCSchema(tableId, true, this.log))
                                    .withLog(log));
                },
                (tableId, clientPerformer) -> Optional.ofNullable(clientPerformer).ifPresent(ClientPerformer::close),
                AutoExpireInstance.Time.time(15, 15, TimeUnit.MINUTES),
                30 * 60 * 1000,
                log
        );
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
        List<HoodieRecord<HoodieRecordPayload>> recordsOneBatch = new ArrayList<>();
        List<HoodieKey> deleteEventsKeys = new ArrayList<>();
        AtomicLong insert = new AtomicLong(0);
        AtomicLong update = new AtomicLong(0);
        AtomicLong delete = new AtomicLong(0);
        autoExpireInstance.call(tapTable, (clientPerformer) -> {
            int tag = -1;
            TapRecordEvent errorRecord = null;
            TapRecordEvent batchFirstRecord = null;
            final NormalEntity entity = new NormalEntity().withClientEntity(clientPerformer).withTapTable(tapTable);
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
                            HoodieRecord<HoodieRecordPayload> before = null;
                            if (null != updateRecord.getBefore() && !updateRecord.getBefore().isEmpty()) {
                                try {
                                    before = HoodieRecordGenericStage.singleton().generic(updateRecord.getBefore(), entity);
                                } catch (Exception message) {
                                    log.warn("Can not generic before hoodie record from before data, error message: {}, table id: {}, before: {}", message.getMessage(), tapTable.getId(), toJson(updateRecord.getBefore()));
                                }
                            }
                            hoodieRecord = HoodieRecordGenericStage.singleton().generic(updateRecord.getAfter(), entity);

                            //如果是修改主键，则删除before后重新插入after，并且立马同步做一次删除
                            if (null != before && null != before.getKey() && !before.getKey().equals(hoodieRecord.getKey())) {
                                log.debug("An update event has modify primary keys, please ensure, primary key: {}, table: {}, before: {}, after: {}",
                                        tapTable.primaryKeys(true),
                                        tapTable.getId(),
                                        updateRecord.getBefore(),
                                        updateRecord.getAfter()
                                        );
                                try {
                                    batchFirstRecord = e;
                                    delete.addAndGet(commitBatch(clientPerformer, 3, recordsOneBatch, list(GenericDeleteRecord.singleton().generic(updateRecord.getBefore(), entity))));
                                    afterCommit(insert, update, delete, consumer);
                                } catch (Exception message) {
                                    log.warn("An update event has modify primary keys, can not delete before record, primary key: {}, table: {}, before: {}, after: {}, error message: {}",
                                            tapTable.primaryKeys(true),
                                            tapTable.getId(),
                                            updateRecord.getBefore(),
                                            updateRecord.getAfter(),
                                            message.getMessage()
                                    );
                                }
                            }
                        } else if (e instanceof TapDeleteRecordEvent) {
                            tag = 3;
                            delete.incrementAndGet();
                            TapDeleteRecordEvent deleteRecord = (TapDeleteRecordEvent) e;
                            deleteEventsKeys.add(GenericDeleteRecord.singleton().generic(deleteRecord.getBefore(), entity));
                        }

                        if ((-1 != tempTag && tempTag != tag)) {
                            batchFirstRecord = e;
                            commitBatch(clientPerformer, tempTag, recordsOneBatch, deleteEventsKeys);
                            afterCommit(insert, update, delete, consumer);
                        }
                        if (tag > 0 && null != hoodieRecord) {
                            recordsOneBatch.add(hoodieRecord);
                        }
                    } catch (Exception fail) {
                        log.warn("Target database process message failed, table name:{}, record: {}, error msg:{}", tapTable.getId(), toJson(e), fail.getMessage(), fail);
                        errorRecord = batchFirstRecord;
                        throw fail;
                    }
                }
                if (!recordsOneBatch.isEmpty()) {
                    try {
                        commitBatch(clientPerformer, 1, recordsOneBatch, deleteEventsKeys);
                        afterCommit(insert, update, delete, consumer);
                    } catch (Exception fail) {
                        throw fail;
                    }
                }
                if (!deleteEventsKeys.isEmpty()) {
                    try {
                        commitBatch(clientPerformer, 3, recordsOneBatch, deleteEventsKeys);
                        afterCommit(insert, update, delete, consumer);
                    } catch (Exception fail) {
                        throw fail;
                    }
                }
            } catch (Throwable e) {
                if (null != errorRecord) writeListResult.addError(errorRecord, e);
                throw e;
            }
        });
        return writeListResult;
    }

    private int commitBatch(ClientPerformer clientPerformer, int batchType, List<HoodieRecord<HoodieRecordPayload>> batch, List<HoodieKey> deleteEventsKeys) {
        if (batchType != 1 && batchType != 2 && batchType != 3) return 0;
        HoodieJavaWriteClient<HoodieRecordPayload> client = clientPerformer.getClient();
        String startCommit;
        client.setOperationType(appendType);
        startCommit = startCommitAndGetCommitTime(clientPerformer);
        try {
            switch (batchType) {
                case 1:
                case 2:
                    return commitInsertOrUpdate(startCommit, client, batch);
                case 3:
                    return commitDelete(startCommit, client, deleteEventsKeys);
            }
        } catch (HoodieRollbackException e) {
            client.restoreToInstant(startCommit, true);
            throw e;
        }
        return 0;
    }


    private int commitInsertOrUpdate(String startCommit, HoodieJavaWriteClient<HoodieRecordPayload> client, List<HoodieRecord<HoodieRecordPayload>> batch) {
        List<WriteStatus> insert;
        if (WriteOperationType.INSERT.equals(this.appendType)) {
            insert = client.insert(batch, startCommit);
        } else {
            insert = client.upsert(batch, startCommit);
        }
        int commitSize = insert.size();
        client.commit(startCommit, insert);
        batch.clear();
        return commitSize;
    }

    private int commitDelete(String startCommit, HoodieJavaWriteClient<HoodieRecordPayload> client, List<HoodieKey> deleteEventsKeys) {
        int deleteSize = 0;
        if (!WriteOperationType.INSERT.equals(this.appendType)) {
            List<WriteStatus> delete = client.delete(deleteEventsKeys, startCommit);
            client.commit(startCommit, delete);
            deleteSize = delete.size();
        } else {
            log.debug("Append mode: INSERT, ignore delete event: {}", deleteEventsKeys);
        }
        deleteEventsKeys.clear();
        return deleteSize;
    }

    public WriteListResult<TapRecordEvent> writeByClient(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        this.appendType = appendType(tapConnectorContext, tapTable);
        this.tableType = tapConnectorContext.getNodeConfig().getString("tableType");
        return groupRecordsByEventType(tapTable, tapRecordEvents, consumer);
    }

    public WriteListResult<TapRecordEvent> writeRecord(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> consumer) throws Throwable {
        return this.writeByClient(tapConnectorContext, tapTable, tapRecordEvents, consumer);
    }

    private String startCommitAndGetCommitTime(ClientPerformer clientPerformer) {
        return clientPerformer.getClient().startCommit();
    }

    /**
     * @deprecated engine not support send writeStrategy to connector
     * hudi only support updateOrInsert or appendWrite now,
     * the stage which name is Append-Only supported by table has not any one primary keys
     * */
    protected WriteOperationType appendType(TapConnectorContext tapConnectorContext, TapTable tapTable) {
        Collection<String> primaryKeys = tapTable.primaryKeys(true);
        DataMap nodeConfig = tapConnectorContext.getNodeConfig();

        //无主键表，并且开启了noPkAutoInsert开关，做仅插入操作
        if ((null == primaryKeys || primaryKeys.isEmpty()) && null != nodeConfig) {
            Object noPkAutoInsert = nodeConfig.getObject("noPkAutoInsert");
            if (noPkAutoInsert instanceof Boolean && (Boolean)noPkAutoInsert) {
                log.debug("Table not any primary keys, do append only mode to insert records, table id: {}", tapTable.getId());
                return WriteOperationType.INSERT;
            }
        }

        DataMap configOptions = tapConnectorContext.getSpecification().getConfigOptions();
        String writeStrategy = String.valueOf(configOptions.get("writeStrategy"));
        if ("appendWrite".equals(writeStrategy)) {
            return WriteOperationType.INSERT;
        }
        return WriteOperationType.UPSERT;
    }

    @Override
    protected boolean isAlive() {
        return null != isAlive && isAlive.get();
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        ErrorKit.ignoreAnyError(autoExpireInstance::close);
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
}
