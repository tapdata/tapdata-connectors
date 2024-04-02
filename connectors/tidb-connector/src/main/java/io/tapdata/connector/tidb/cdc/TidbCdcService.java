package io.tapdata.connector.tidb.cdc;

import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.core.execution.JobClient;
import org.tikv.common.TiConfiguration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TidbCdcService implements Serializable {

    protected final static int LOG_QUEUE_SIZE = 5000;

    protected Log tapLogger;

    private TidbConfig tidbConfig;

    private static Map<String, LinkedBlockingQueue> logMap = new ConcurrentHashMap();

    private String database;

    private String tapContextId;

    private volatile AtomicBoolean started;

    private static Map<String, JobClient> streamExecutionEnvironment = new ConcurrentHashMap();


    public TidbCdcService(TidbConfig tidbConfig, Log tapLogger, AtomicBoolean started) {
        this.tidbConfig = tidbConfig;
        this.tapLogger = tapLogger;
        this.started = started;
    }


    public void readBinlog(TapConnectorContext nodeContext, List<String> tableList, Object offsetState, StreamReadConsumer consumer) throws Throwable {
        TiConfiguration tiConf = TDBSourceOptions.getTiConfiguration(
                tidbConfig.getPdServer(), new HashMap<>());
        database = tidbConfig.getDatabase();
        tapContextId = nodeContext.getId();
        LinkedBlockingQueue<TidbStreamEvent> logQueue = new LinkedBlockingQueue<>(LOG_QUEUE_SIZE);
        logMap.put(database + tapContextId, logQueue);
        Long offset;
        StreamData streamData = new StreamData();
        for (String table : tableList) {
            if (offsetState instanceof Long) {
                offset = (Long) offsetState;
            } else {
                offset = ((CdcOffset) offsetState).getOffset();
            }
            TiKVChangeEventDeserializationSchema tiKVChangeEventDeserializationSchema =
                    new TiKVChangeEventDeserializationSchemaImpl(database, table, nodeContext.getId(), tiConf, logMap);

            JobClient jobClient = streamData.startStream(database, table, tiConf, offset, tiKVChangeEventDeserializationSchema, tapLogger);
            streamExecutionEnvironment.put(database+tapContextId+table, jobClient);
        }
        tapLogger.info("Tidb cdc started");
        Long minOffset = 0L;
        while (started.get()) {
            try {
                TidbStreamEvent tidbStreamEvent = logQueue.poll(1, TimeUnit.SECONDS);
                if (tidbStreamEvent == null) {
                    continue;
                }
                List<TapEvent> tapEvents = new ArrayList<>();
                tapEvents.add(tidbStreamEvent.getTapEvent());
                if (tableList.size() > 1) {
                    if (minOffset == 0) {
                        minOffset = tidbStreamEvent.getCdcOffset();
                    } else {
                        if (minOffset > tidbStreamEvent.getCdcOffset()) {
                            minOffset = tidbStreamEvent.getCdcOffset();
                        }
                    }
                    tidbStreamEvent.setCdcOffset(minOffset);
                }
                consumer.accept(tapEvents, tidbStreamEvent.getCdcOffset());
            } catch (InterruptedException e) {
                tapLogger.error(" ReadBinlog error:{}",e.getMessage());
            }
        }

    }

    public void close() {
        logMap.remove(database + tapContextId);
        if (MapUtils.isNotEmpty(streamExecutionEnvironment)) {
            Iterator<Map.Entry<String, JobClient>> iterator = streamExecutionEnvironment.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, JobClient> entry = iterator.next();
                if (entry.getKey().contains(database + tapContextId)) {
                    try {
                        entry.getValue().cancel();
                    } catch (Exception e) {
                        if (e instanceof IllegalStateException && e.getMessage().contains("MiniCluster is not yet running or has already been shut down")) {
                            tapLogger.warn(" Cluster has shut down");
                        }
                    }
                    iterator.remove();
                }
            }
        }
        tapLogger.info("Tidb cdc stop");
    }

    public void setStarted(AtomicBoolean started) {
        this.started = started;
    }

}