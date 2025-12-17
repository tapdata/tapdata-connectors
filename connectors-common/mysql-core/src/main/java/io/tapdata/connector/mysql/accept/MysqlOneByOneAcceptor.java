package io.tapdata.connector.mysql.accept;

import io.tapdata.connector.mysql.entity.MysqlStreamEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.StreamReadOneByOneConsumer;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/16 09:23 Create
 * @description
 */
public class MysqlOneByOneAcceptor extends MysqlAbstractAcceptor<MysqlOneByOneAcceptor, StreamReadOneByOneConsumer> {
    StreamReadOneByOneConsumer consumer;

    @Override
    public void accept(MysqlStreamEvent e) {
        consumer.accept(e.getTapEvent(), e.getMysqlStreamOffset());
    }

    @Override
    public void accept(List<TapEvent> e, Object offset) {
        consumer.accept(e, offset);
    }

    @Override
    public MysqlOneByOneAcceptor setConsumer(StreamReadOneByOneConsumer consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public MysqlOneByOneAcceptor setBatchSize(int size) {
        return this;
    }

    @Override
    public MysqlOneByOneAcceptor setBatchSizeTimeout(long ms) {
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
    public int getBatchSize() {
        return this.consumer.getBatchSize();
    }

    @Override
    public StreamReadOneByOneConsumer getConsumer() {
        return this.consumer;
    }
}
