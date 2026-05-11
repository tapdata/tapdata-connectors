package io.tapdata.connector.mysql.accept;

import io.tapdata.connector.mysql.entity.MysqlStreamEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/16 09:21 Create
 * @description
 */
public class MysqlBatchAcceptor extends MysqlAbstractAcceptor<MysqlBatchAcceptor, StreamReadConsumer> {
    StreamReadConsumer consumer;
    int batchSize;
    long batchSizeTimeout;
    List<TapEvent> events = new ArrayList<>();

    @Override
    public void accept(MysqlStreamEvent e) {
        if (null == e) {
            return;
        }
        Optional.ofNullable(e.getMysqlStreamOffset()).ifPresent(this::updateOffset);
        events.add(e.getTapEvent());
    }

    @Override
    public void complete() {
        if (events.isEmpty()) {
            return;
        }
        consumer.accept(events, getOffset());
        events = new ArrayList<>();
    }

    @Override
    public void accept(List<TapEvent> e, Object offset) {
        consumer.accept(e, offset);
    }

    @Override
    public MysqlBatchAcceptor setConsumer(StreamReadConsumer consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public MysqlBatchAcceptor setBatchSize(int size) {
        this.batchSize = size;
        return this;
    }

    @Override
    public MysqlBatchAcceptor setBatchSizeTimeout(long ms) {
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

    @Override
    public int getBatchSize() {
        return this.batchSize;
    }
}
