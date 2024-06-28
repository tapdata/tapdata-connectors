package io.tapdata.connector.gauss.cdc;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.utils.StateListener;

import java.util.List;
import java.util.function.BiConsumer;

public class GaussDBStreamConsumer extends StreamReadConsumer {
    StreamReadConsumer streamReadConsumer;
    public GaussDBStreamConsumer(StreamReadConsumer consumer) {
        this.streamReadConsumer = consumer;
    }

    @Override
    public void asyncMethodAndNoRetry() {
        streamReadConsumer.asyncMethodAndNoRetry();
    }

    @Override
    public synchronized void streamReadStarted() {
        streamReadConsumer.streamReadStarted();
    }

    @Override
    public synchronized void streamReadEnded() {
        streamReadConsumer.streamReadEnded();
    }

    @Override
    public int getState() {
        return streamReadConsumer.getState();
    }

    @Override
    public boolean isAsyncMethodAndNoRetry() {
        return streamReadConsumer.isAsyncMethodAndNoRetry();
    }

    @Override
    public StreamReadConsumer stateListener(StateListener<Integer> stateListener) {
        return streamReadConsumer.stateListener(stateListener);
    }

    @Override
    public BiConsumer<List<TapEvent>, Object> andThen(BiConsumer<? super List<TapEvent>, ? super Object> after) {
        return streamReadConsumer.andThen(after);
    }

    @Override
    public void accept(List<TapEvent> events, Object offset) {
        streamReadConsumer.accept(events, offset instanceof CdcOffset ? ((CdcOffset)offset).toOffset() : offset);
    }
}
