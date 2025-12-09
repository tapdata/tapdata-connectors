package io.tapdata.mongodb.reader.cdc;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.TapStreamReadConsumer;

import java.util.List;

public interface Acceptor<Acc, EType, Consumer extends TapStreamReadConsumer<?, ?>> {

    void accept(EType e);

    void accept(List<TapEvent> e, Object offset);

    Acc setConsumer(Consumer consumer);

    Acc setBatchSize(int size);

    Acc setBatchSizeTimeout(long ms);

    void streamReadStarted();

    void streamReadEnded();

    default int getBatchSize() {
        return 1;
    }

    Consumer getConsumer();
}
