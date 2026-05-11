package io.tapdata.connector.postgres.cdc.accept;

import io.tapdata.cdc.CustomAbstractAccepter;
import io.tapdata.connector.postgres.cdc.NormalRedo;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/10 14:19 Create
 * @description
 */
public class LogMinerBatchAccepter
        extends CustomAbstractAccepter<NormalRedo, LogMinerBatchAccepter, StreamReadConsumer> {
    protected NormalRedo lastRedo;
    protected List<TapEvent> events = new ArrayList<>();
    protected Function<NormalRedo, TapEvent> eventCreator;
    protected StreamReadConsumer consumer;
    protected int batchSize = 1;
    protected long batchSizeTimeout = 1000L;

    @Override
    public void accept(NormalRedo redo) {
        if (EmptyKit.isNotNull(redo)) {
            lastRedo = redo;
            TapEvent e = eventCreator.apply(redo);
            if (null == e) {
                return;
            }
            events.add(e);
            if (events.size() >= getBatchSize()) {
                getConsumer().accept(events, redo.getCdcSequenceStr());
                events = new ArrayList<>();
            }
        } else {
            if (!events.isEmpty()) {
                getConsumer().accept(events, lastRedo.getCdcSequenceStr());
                events = new ArrayList<>();
            }
        }
    }

    @Override
    public void accept(List<TapEvent> e, Object offset) {
        getConsumer().accept(e, offset);
    }

    @Override
    public LogMinerBatchAccepter setConsumer(StreamReadConsumer consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public LogMinerBatchAccepter setBatchSize(int size) {
        this.batchSize = size;
        return this;
    }

    @Override
    public LogMinerBatchAccepter setBatchSizeTimeout(long ms) {
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

    public LogMinerBatchAccepter setEventCreator(Function<NormalRedo, TapEvent> eventCreator) {
        this.eventCreator = eventCreator;
        return this;
    }
}
