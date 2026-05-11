package io.tapdata.connector.postgres.cdc.accept;

import io.tapdata.cdc.CustomAbstractAccepter;
import io.tapdata.connector.postgres.cdc.NormalRedo;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadOneByOneConsumer;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/10 14:19 Create
 * @description
 */
public class LogMinerOneByOneAccepter
        extends CustomAbstractAccepter<NormalRedo, LogMinerOneByOneAccepter, StreamReadOneByOneConsumer> {
    protected Function<NormalRedo, TapEvent> eventCreator;
    protected StreamReadOneByOneConsumer consumer;
    protected long batchSizeTimeout = 1000L;
    protected int batchSize = 1;

    @Override
    public void accept(NormalRedo redo) {
        if (EmptyKit.isNotNull(redo)) {
            TapEvent e = eventCreator.apply(redo);
            if (null == e) {
                return;
            }
            getConsumer().accept(e, redo.getCdcSequenceStr());
        }
    }

    @Override
    public void accept(List<TapEvent> e, Object offset) {
        getConsumer().accept(e, offset);
    }

    @Override
    public LogMinerOneByOneAccepter setConsumer(StreamReadOneByOneConsumer consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public LogMinerOneByOneAccepter setBatchSize(int size) {
        this.batchSize = size;
        return this;
    }

    @Override
    public LogMinerOneByOneAccepter setBatchSizeTimeout(long ms) {
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
    public StreamReadOneByOneConsumer getConsumer() {
        return this.consumer;
    }

    @Override
    public int getBatchSize() {
        return Optional.ofNullable(getConsumer()).map(StreamReadOneByOneConsumer::getBatchSize).orElse(1);
    }

    public LogMinerOneByOneAccepter setEventCreator(Function<NormalRedo, TapEvent> eventCreator) {
        this.eventCreator = eventCreator;
        return this;
    }
}
