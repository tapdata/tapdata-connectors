package io.tapdata.mongodb.reader.cdc.v3;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.mongodb.reader.v3.MongoV3StreamOffset;
import io.tapdata.mongodb.reader.v3.TapEventOffset;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/8 17:30 Create
 * @description
 */
public class NormalV3Acceptor extends V3Accept<NormalV3Acceptor, StreamReadConsumer> {
    StreamReadConsumer consumer;

    int batchSize = 1;

    long batchSizeTimeout = TimeUnit.SECONDS.toMillis(1L);

    long lastConsumeTs;

    List<TapEvent> tapEvents = new ArrayList<>();

    // Calculate time window based on batch size
    // If batch size <= 500, use fixed 50ms window
    // If batch size > 500, use dynamic window = batch size / 10 (ms)
    @Override
    public void accept(TapEventOffset tapEventOffset) {
        if (tapEventOffset != null) {
            tapEvents.add(tapEventOffset.getTapEvent());
            this.offset.put(tapEventOffset.getReplicaSetName(), tapEventOffset.getOffset());
        }

        long consumeInterval = System.currentTimeMillis() - lastConsumeTs;
        if (tapEvents.size() >= batchSize
                || consumeInterval > batchSizeTimeout) {
            Map<String, MongoV3StreamOffset> newOffset = new ConcurrentHashMap<>(this.offset);
            consumer.accept(tapEvents, newOffset);
            tapEvents = new ArrayList<>(batchSize);
            lastConsumeTs = System.currentTimeMillis();
        }
    }

    @Override
    public void accept(List<TapEvent> e, Object offset) {
        consumer.accept(e, offset);
    }

    @Override
    public NormalV3Acceptor setConsumer(StreamReadConsumer consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public NormalV3Acceptor setBatchSize(int size) {
        this.batchSize = size;
        return this;
    }

    @Override
    public NormalV3Acceptor setBatchSizeTimeout(long ms) {
        this.batchSizeTimeout = ms;
        return this;
    }

    @Override
    public void streamReadStarted() {
        this.lastConsumeTs = System.currentTimeMillis();
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
