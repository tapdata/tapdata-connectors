package io.tapdata.connector.postgres.cdc.accept;

import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadOneByOneConsumer;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/10 14:20 Create
 * @description
 */
public class DebeziumOneByOneAccepter
        extends PGEventAbstractAccepter<DebeziumOneByOneAccepter, StreamReadOneByOneConsumer> {
    int acceptCount = 0;
    StreamReadOneByOneConsumer consumer;
    int batchSize = 1;
    long batchSizeTimeout = 1000L;

    @Override
    public void accept(TapEvent e) {
        if (null == e) {
            return;
        }
        acceptCount++;
        boolean calcOffset = acceptCount >= getBatchSize();
        PostgresOffset postgresOffset = null;
        if (calcOffset) {
            postgresOffset = new PostgresOffset();
            postgresOffset.setSourceOffset(TapSimplify.toJson(offset));
            acceptCount = 0;
        }
        getConsumer().accept(e, postgresOffset);
    }


    @Override
    public void accept(List<TapEvent> e, Object offset) {
        getConsumer().accept(e, offset);
    }

    @Override
    public DebeziumOneByOneAccepter setConsumer(StreamReadOneByOneConsumer consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public DebeziumOneByOneAccepter setBatchSize(int size) {
        this.batchSize = size;
        return this;
    }

    @Override
    public DebeziumOneByOneAccepter setBatchSizeTimeout(long ms) {
        this.batchSizeTimeout = ms;
        return this;
    }

    @Override
    public void streamReadStarted() {
        getConsumer().streamReadStarted();
    }

    @Override
    public void streamReadEnded() {
        getConsumer().streamReadEnded();
    }

    @Override
    public StreamReadOneByOneConsumer getConsumer() {
        return this.consumer;
    }

    @Override
    public int getBatchSize() {
        return getConsumer().getBatchSize();
    }
}
