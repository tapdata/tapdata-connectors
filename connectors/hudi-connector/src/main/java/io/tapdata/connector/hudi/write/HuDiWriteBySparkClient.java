package io.tapdata.connector.hudi.write;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.connector.hudi.util.AutoExpireInstance;
import io.tapdata.connector.hudi.util.FileUtil;
import io.tapdata.connector.hudi.util.Krb5Util;
import io.tapdata.connector.hudi.write.generic.GenericDeleteRecord;
import io.tapdata.connector.hudi.write.generic.HoodieRecordGenericStage;
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
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.exception.HoodieRollbackException;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;


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
                                    .withSchema(getJDBCSchema(tableId, true))
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
                            hoodieRecord = HoodieRecordGenericStage.singleton().generic(updateRecord.getAfter(), entity);
                        } else if (e instanceof TapDeleteRecordEvent) {
                            tag = 3;
                            delete.incrementAndGet();
                            TapDeleteRecordEvent deleteRecord = (TapDeleteRecordEvent) e;
                            deleteEventsKeys.add(GenericDeleteRecord.singleton().generic(deleteRecord.getBefore(), entity));
                        }

                        if ((-1 != tempTag && tempTag != tag)) {
                            commitBatch(clientPerformer, tempTag, recordsOneBatch, deleteEventsKeys);
                            batchFirstRecord = e;
                            afterCommit(insert, update, delete, consumer);
                        }
                        if (tag > 0 && null != hoodieRecord) {
                            recordsOneBatch.add(hoodieRecord);
                        }
                    } catch (Exception fail) {
                        log.error("target database process message failed", "table name:{}, record: {}, error msg:{}", tapTable.getId(), e, fail.getMessage(), fail);
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
                        commitBatch(clientPerformer, 1, recordsOneBatch, deleteEventsKeys);
                        afterCommit(insert, update, delete, consumer);
                    } catch (Exception fail) {
                        log.error("target database process message failed", "table name:{},error msg:{}", tapTable.getId(), fail.getMessage(), fail);
                        throw fail;
                    }
                }
                if (!deleteEventsKeys.isEmpty()) {
                    try {
                        commitBatch(clientPerformer, 3, recordsOneBatch, deleteEventsKeys);
                        afterCommit(insert, update, delete, consumer);
                    } catch (Exception fail) {
                        log.error("target database process message failed", "table name:{},error msg:{}", tapTable.getId(), fail.getMessage(), fail);
                        throw fail;
                    }
                }
            }
        });
        return writeListResult;
    }

    private void commitBatch(ClientPerformer clientPerformer, int batchType, List<HoodieRecord<HoodieRecordPayload>> batch, List<HoodieKey> deleteEventsKeys) {
        if (batchType != 1 && batchType != 2 && batchType != 3) return;
        HoodieJavaWriteClient<HoodieRecordPayload> client = clientPerformer.getClient();
        String startCommit;
        client.setOperationType(appendType);
        startCommit = startCommitAndGetCommitTime(clientPerformer);
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
                    } else {
                        log.debug("Append mode: INSERT, ignore delete event: {}", deleteEventsKeys);
                    }
                    deleteEventsKeys.clear();
                    break;
            }
        } catch (HoodieRollbackException e) {
            client.restoreToInstant(startCommit, true);
            throw e;
        }
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

    public Schema getJDBCSchema(String tableId, boolean nullable) {
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

    @Override
    protected boolean isAlive() {
        return null != isAlive && isAlive.get();
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        ErrorKit.ignoreAnyError(() -> autoExpireInstance.close());
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
