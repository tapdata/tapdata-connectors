package io.tapdata.mongodb.reader.cdc.v4;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.reader.MongodbV4StreamReader;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/8 17:30 Create
 * @description
 */
public class NormalV4Acceptor extends V4Accept<NormalV4Acceptor, StreamReadConsumer> {
    StreamReadConsumer consumer;

    int batchSize = 1;

    long batchSizeTimeout = 1000L;

    MongodbV4StreamReader.OffsetEvent lastOffsetEvent = null;

    long lastSendTime;

    List<TapEvent> events = new ArrayList<>();

    // Calculate time window based on batch size
    // If batch size <= 500, use fixed 50ms window
    // If batch size > 500, use dynamic window = batch size / 10 (ms)
    @Override
    public void accept(MongodbV4StreamReader.OffsetEvent event) {
        if (EmptyKit.isNotNull(event)) {
            lastOffsetEvent = event;
            events.add(event.getEvent());
            // Check batch size OR time window
            if (events.size() >= batchSize ||
                    (System.currentTimeMillis() - lastSendTime > batchSizeTimeout)) {
                consumer.accept(events, event.getOffset());
                events.clear();
                lastSendTime = System.currentTimeMillis();
            }
        } else {
            // Send remaining events when queue is empty
            if (!events.isEmpty() && lastOffsetEvent != null) {
                consumer.accept(events, lastOffsetEvent.getOffset());
                events.clear();
                lastSendTime = System.currentTimeMillis();
            }
        }
    }

    @Override
    public void accept(List<TapEvent> e, Object offset) {
        this.consumer.accept(e, offset);
    }

    @Override
    public NormalV4Acceptor setConsumer(StreamReadConsumer consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public NormalV4Acceptor setBatchSize(int size) {
        this.batchSize = size;
        return this;
    }

    @Override
    public NormalV4Acceptor setBatchSizeTimeout(long ms) {
        this.batchSizeTimeout = ms;
        return this;
    }

    @Override
    public void streamReadStarted() {
        this.lastSendTime = System.currentTimeMillis();
        this.consumer.streamReadStarted();
    }

    @Override
    public void streamReadEnded() {
        this.consumer.streamReadEnded();
    }


    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public StreamReadConsumer getConsumer() {
        return this.consumer;
    }
}
