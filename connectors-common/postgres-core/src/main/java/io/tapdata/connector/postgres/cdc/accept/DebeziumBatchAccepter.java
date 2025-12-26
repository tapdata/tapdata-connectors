package io.tapdata.connector.postgres.cdc.accept;

import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/10 14:19 Create
 * @description
 */
public class DebeziumBatchAccepter
        extends PGEventAbstractAccepter<DebeziumBatchAccepter, StreamReadConsumer> {
    List<TapEvent> eventList = new ArrayList<>();
    StreamReadConsumer consumer;
    int batchSize = 1;
    long batchSizeTimeout = 1000L;

    @Override
    public void accept(TapEvent e) {
        if (null != e) {
            eventList.add(e);
        } else {
            if (!eventList.isEmpty()) {
                acceptOnce();
            }
        }
        if (eventList.size() >= getBatchSize()) {
            acceptOnce();
        }
    }

    void acceptOnce() {
        PostgresOffset postgresOffset = new PostgresOffset();
        postgresOffset.setSourceOffset(TapSimplify.toJson(offset));
        getConsumer().accept(eventList, postgresOffset);
        eventList = TapSimplify.list();
    }

    @Override
    public void accept(List<TapEvent> e, Object offset) {
        getConsumer().accept(e, offset);
    }

    @Override
    public DebeziumBatchAccepter setConsumer(StreamReadConsumer consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public DebeziumBatchAccepter setBatchSize(int size) {
        this.batchSize = size;
        return this;
    }

    @Override
    public DebeziumBatchAccepter setBatchSizeTimeout(long ms) {
        this.batchSizeTimeout = ms;
        return this;
    }

    @Override
    public void streamReadStarted() {
        this.consumer.streamReadStarted();
    }

    @Override
    public void streamReadEnded() {
        this.consumer.streamReadEnded();
    }

    @Override
    public StreamReadConsumer getConsumer() {
        return this.consumer;
    }
}
