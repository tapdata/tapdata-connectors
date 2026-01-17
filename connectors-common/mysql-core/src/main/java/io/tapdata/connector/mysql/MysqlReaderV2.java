package io.tapdata.connector.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.tapdata.base.ConnectorBase.list;

public class MysqlReaderV2 {

    private MysqlJdbcContextV2 mysqlJdbcContext;
    private MysqlConfig mysqlConfig;
    private String connectorId;
    private Log tapLogger;
    private List<String> tableList;
    private KVReadOnlyMap<TapTable> tableMap;
    private Object offsetState;
    private int recordSize;
    private StreamReadConsumer consumer;

    public MysqlReaderV2(MysqlJdbcContextV2 mysqlJdbcContext, String connectorId, Log tapLogger) {
        this.mysqlJdbcContext = mysqlJdbcContext;
        this.connectorId = connectorId;
        this.tapLogger = tapLogger;
        mysqlConfig = (MysqlConfig) mysqlJdbcContext.getConfig();
    }

    public void init(List<String> tableList, KVReadOnlyMap<TapTable> tableMap, Object offsetState, int recordSize, StreamReadConsumer consumer) throws Throwable {
        this.tableList = tableList;
        this.tableMap = tableMap;
        this.offsetState = offsetState;
        this.recordSize = recordSize;
        this.consumer = consumer;
    }

    public void startMiner(Supplier<Boolean> isAlive) throws Throwable {
        BinaryLogClient client = new BinaryLogClient(mysqlConfig.getHost(), mysqlConfig.getPort(), mysqlConfig.getUser(), mysqlConfig.getPassword());
        AtomicReference<List<TapEvent>> events = new AtomicReference<>(list());
        try (ConcurrentProcessor<Event, OffsetEvent> concurrentProcessor = TapExecutors.createSimple(8, 32, "MysqlReader-Processor")) {
            client.setServerId(randomServerId());
            client.registerEventListener(event -> concurrentProcessor.runAsync(event, this::emit));
            consumer.streamReadStarted();
            client.connect();
            MysqlBinlogPosition lastOffset;
            while (isAlive.get()) {
                OffsetEvent offsetEvent = concurrentProcessor.get(2, TimeUnit.SECONDS);
                if (EmptyKit.isNotNull(offsetEvent)) {
                    events.get().add(offsetEvent.getTapEvent());
                    lastOffset = offsetEvent.getMysqlBinlogPosition();
                    if (events.get().size() >= recordSize) {
                        consumer.accept(events.get(), lastOffset);
                        events.set(new ArrayList<>());
                    }
                } else {
                    if (!events.get().isEmpty()) {
                        consumer.accept(events.get(), offsetState);
                        events.set(list());
                    }
                }
            }
        } finally {
            consumer.streamReadEnded();
            client.disconnect();
        }
    }

    private OffsetEvent emit(Event event) {
        tapLogger.info("Emitting event {}", event);
        return null;
    }

    public int randomServerId() {
        int lowestServerId = 5400;
        int highestServerId = Integer.MAX_VALUE;
        return lowestServerId + new Random().nextInt(highestServerId - lowestServerId);
    }

    static class OffsetEvent {

        private TapEvent tapEvent;
        private MysqlBinlogPosition mysqlBinlogPosition;

        public OffsetEvent(TapEvent tapEvent, MysqlBinlogPosition mysqlBinlogPosition) {
            this.tapEvent = tapEvent;
            this.mysqlBinlogPosition = mysqlBinlogPosition;
        }

        public TapEvent getTapEvent() {
            return tapEvent;
        }

        public MysqlBinlogPosition getMysqlBinlogPosition() {
            return mysqlBinlogPosition;
        }

        public void setTapEvent(TapEvent tapEvent) {
            this.tapEvent = tapEvent;
        }

        public void setMysqlBinlogPosition(MysqlBinlogPosition mysqlBinlogPosition) {
            this.mysqlBinlogPosition = mysqlBinlogPosition;
        }
    }
}
