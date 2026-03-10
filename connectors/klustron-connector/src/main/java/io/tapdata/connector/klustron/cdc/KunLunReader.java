package io.tapdata.connector.klustron.cdc;

import io.tapdata.connector.klustron.config.KunLunCdcConfig;
import io.tapdata.connector.klustron.config.StorageNode;
import io.tapdata.connector.mysql.MysqlReaderV2;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/5 18:18 Create
 * @description
 */
public class KunLunReader implements AutoCloseable {
    //KunLunMysqlContext mysqlJdbcContext;
    List<MysqlConfig> mysqlConfigs;
    private final Log tapLogger;
    private List<String> tableList;
    private KVReadOnlyMap<TapTable> tableMap;
    private Object offsetState;
    private int recordSize;
    private StreamReadConsumer consumer;
    private final TimeZone timeZone;
    private final ThreadPoolExecutor executor;
    private final List<Future<?>> futures;

    public KunLunReader(KunLunCdcConfig cdcConfig, Log tapLogger, TimeZone timeZone) {
        this.futures = new ArrayList<>();
        this.tapLogger = tapLogger;
        this.mysqlConfigs = new ArrayList<>();
        this.timeZone = timeZone;
        List<StorageNode> storageNodes = cdcConfig.getStorageNode();
        for (StorageNode storageNode : storageNodes) {
            MysqlConfig mysqlConfig = new MysqlConfig();
            mysqlConfig.setDatabase(cdcConfig.getDatabase());
            mysqlConfig.setSchema(cdcConfig.getDatabase());
            mysqlConfig.setHost(storageNode.getHost());
            mysqlConfig.setPort(storageNode.getPort());
            mysqlConfig.setUser(storageNode.getUsername());
            mysqlConfig.setPassword(storageNode.getPassword());
            mysqlConfig.setZoneOffsetHour(cdcConfig.getZoneOffsetHour());
            mysqlConfigs.add(mysqlConfig);
        }
        this.executor = new ThreadPoolExecutor(
                mysqlConfigs.size() + 1,
                mysqlConfigs.size() + 1,
                0L,
                java.util.concurrent.TimeUnit.MILLISECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>()
        );
    }

    public KunLunReader init(List<String> tableList,
                             KVReadOnlyMap<TapTable> tableMap,
                             Object offsetState,
                             int recordSize,
                             StreamReadConsumer consumer) {
        this.tableList = tableList;
        this.tableMap = tableMap;
        this.offsetState = offsetState;
        this.recordSize = recordSize;
        this.consumer = consumer;
        return this;
    }

    public void startMiner(Supplier<Boolean> isAlive) {
        List<Throwable> throwables = new ArrayList<>();
        final KunLunOffset kunLunOffset;
        if (offsetState instanceof KunLunOffset kOffset) {
            kunLunOffset = kOffset;
        } else {
            kunLunOffset = null;
        }
        for (MysqlConfig mysqlConfig : mysqlConfigs) {
            Future<?> submit = this.executor.submit(() -> {
                Object offset = offsetState;
                if (kunLunOffset != null) {
                    offset = kunLunOffset.getOffset(mysqlConfig.getHost(), mysqlConfig.getPort());
                }
                try {
                    KunLunBinLogReader mysqlReaderV2 = new KunLunBinLogReader(mysqlConfig, tapLogger, timeZone);
                    mysqlReaderV2.init(tableList, tableMap, offset, recordSize, consumer);
                    mysqlReaderV2.startMiner(isAlive);
                } catch (Throwable e) {
                    tapLogger.error("KunLunReader {}:{} startMiner error: {}", mysqlConfig.getHost(), mysqlConfig.getPort(), e.getMessage(), e);
                    throwables.add(e);
                }
            });
            futures.add(submit);
        }
        while (isAlive.get() && throwables.isEmpty()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        if (futures == null || futures.isEmpty()) {
            return;
        }
        for (Future<?> future : futures) {
            try {
                future.cancel(true);
            } catch (Exception e) {
                //do nothing
            }
        }
        futures.clear();
        try {
            executor.shutdown();
        } catch (Exception e) {
            tapLogger.error("KunLunReader close error: {}", e.getMessage(), e);
        }
    }
}
