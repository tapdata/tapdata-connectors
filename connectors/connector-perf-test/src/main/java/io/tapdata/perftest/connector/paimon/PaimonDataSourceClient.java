package io.tapdata.perftest.connector.paimon;

import io.tapdata.connector.paimon.PaimonConnector;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.perftest.connector.paimon.support.PdkContextFactory;
import io.tapdata.perftest.core.client.DataSourceClient;
import io.tapdata.perftest.core.client.ReadResult;
import io.tapdata.perftest.core.client.WriteResult;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableV2Function;
import io.tapdata.pdk.apis.functions.connector.target.DropTableFunction;
import io.tapdata.pdk.apis.functions.connector.target.WriteRecordFunction;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DataSourceClient backed by PaimonConnector via PDK interfaces.
 *
 * Call chain for write:
 *   write() → WriteRecordFunction.writeRecord() → PaimonConnector → PaimonService.writeRecords()
 * Call chain for read:
 *   batchRead() → BatchReadFunction.batchRead() → PaimonConnector → PaimonService.batchRead()
 */
public class PaimonDataSourceClient implements DataSourceClient {

    private final PaimonDataSourceConfig config;

    private PaimonConnector       connector;
    private TapConnectionContext  connectionContext;
    private TapConnectorContext   connectorContext;
    private WriteRecordFunction   writeRecordFunction;
    private BatchReadFunction     batchReadFunction;
    private CreateTableV2Function createTableV2Function;
    private DropTableFunction     dropTableFunction;

    public PaimonDataSourceClient(PaimonDataSourceConfig config) {
        this.config = config;
    }

    @Override
    public void init() throws Exception {
        DataMap connectionConfig = buildConnectionConfig();

        connectionContext = PdkContextFactory.buildConnectionContext(connectionConfig);

        connector = new PaimonConnector();
        try {
            connector.onStart(connectionContext);
        } catch (Throwable t) {
            throw new Exception("PaimonConnector.onStart failed: " + t.getMessage(), t);
        }

        ConnectorFunctions functions = new ConnectorFunctions();
        connector.registerCapabilities(functions, new TapCodecsRegistry());

        writeRecordFunction   = functions.getWriteRecordFunction();
        batchReadFunction     = functions.getBatchReadFunction();
        createTableV2Function = functions.getCreateTableV2Function();
        dropTableFunction     = functions.getDropTableFunction();

        connectorContext = PdkContextFactory.buildConnectorContext(connectionConfig, config.getTapTable());

        System.out.println("[PaimonDataSourceClient] Initialized. warehouse=" + config.getWarehouse());
    }

    @Override
    public void createTable(TapTable tapTable) throws Exception {
        PdkContextFactory.updateTableMap(connectorContext, tapTable);
        if (createTableV2Function == null) return;
        TapCreateTableEvent event = new TapCreateTableEvent();
        event.setTable(tapTable);
        try {
            createTableV2Function.createTable(connectorContext, event);
        } catch (Throwable t) {
            throw new Exception("createTable failed: " + t.getMessage(), t);
        }
    }

    @Override
    public void dropTable(String tableName) throws Exception {
        if (dropTableFunction == null) return;
        TapDropTableEvent event = new TapDropTableEvent();
        event.setTableId(tableName);
        try {
            dropTableFunction.dropTable(connectorContext, event);
        } catch (Throwable t) {
            throw new Exception("dropTable failed: " + t.getMessage(), t);
        }
    }

    @Override
    public WriteResult write(List<TapRecordEvent> events) throws Exception {
        if (writeRecordFunction == null) {
            throw new IllegalStateException("WriteRecordFunction not registered by PaimonConnector");
        }
        TapTable table = config.getTapTable();
        AtomicLong inserted = new AtomicLong(0);
        AtomicLong updated  = new AtomicLong(0);
        AtomicLong deleted  = new AtomicLong(0);
        AtomicLong errors   = new AtomicLong(0);

        long t0 = System.currentTimeMillis();
        try {
            writeRecordFunction.writeRecord(connectorContext, events, table, result -> {
                inserted.addAndGet(result.getInsertedCount());
                updated.addAndGet(result.getModifiedCount());
                deleted.addAndGet(result.getRemovedCount());
                if (result.getErrorMap() != null) errors.addAndGet(result.getErrorMap().size());
            });
        } catch (Throwable t) {
            throw new Exception("writeRecord failed: " + t.getMessage(), t);
        }
        long elapsed = System.currentTimeMillis() - t0;

        return new WriteResult(inserted.get(), updated.get(), deleted.get(), errors.get(), elapsed);
    }

    @Override
    public ReadResult batchRead(TapTable table, int batchSize) throws Exception {
        if (batchReadFunction == null) {
            throw new UnsupportedOperationException("BatchReadFunction not supported by PaimonConnector");
        }
        AtomicLong rowCount = new AtomicLong(0);
        long t0 = System.currentTimeMillis();
        try {
            batchReadFunction.batchRead(connectorContext, table, null, batchSize,
                (evts, offset) -> rowCount.addAndGet(evts == null ? 0 : evts.size()));
        } catch (Throwable t) {
            throw new Exception("batchRead failed: " + t.getMessage(), t);
        }
        return new ReadResult(rowCount.get(), System.currentTimeMillis() - t0);
    }

    @Override
    public String getDataSourceType() {
        return "paimon";
    }

    @Override
    public void close() throws Exception {
        if (connector != null) {
            connector.onStop(connectionContext);
        }
    }

    private DataMap buildConnectionConfig() {
        DataMap m = DataMap.create();
        m.put("warehouse",             config.getWarehouse());
        m.put("storageType",           config.getStorageType());
        m.put("database",              config.getDatabase());
        m.put("writeBufferSize",       config.getWriteBufferSize());
        m.put("batchAccumulationSize", config.getBatchAccumulationSize());
        m.put("commitIntervalMs",      config.getCommitIntervalMs());
        m.put("enableAsyncCommit",     config.getEnableAsyncCommit());
        m.put("writeThreads",          config.getPaimonWriteThreads());
        m.put("enableAutoCompaction",  config.getEnableAutoCompaction());
        m.put("targetFileSize",        config.getTargetFileSize());
        m.put("bucketMode",            config.getBucketMode());
        m.put("bucketCount",           config.getBucketCount());
        if (config.getS3Endpoint() != null) {
            m.put("s3Endpoint",  config.getS3Endpoint());
            m.put("s3AccessKey", config.getS3AccessKey());
            m.put("s3SecretKey", config.getS3SecretKey());
            m.put("s3Region",    config.getS3Region());
        }
        if (config.getHdfsHost() != null) {
            m.put("hdfsHost", config.getHdfsHost());
            m.put("hdfsPort", config.getHdfsPort());
        }
        return m;
    }
}
