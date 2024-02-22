package io.tapdata.connector.gauss.cdc.logic.event.transcation.complex;

import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.event.EventFactory;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.nio.ByteBuffer;
import java.util.List;

public class LogicReplicationComplexImpl extends EventFactory<ByteBuffer> {
    StreamReadConsumer eventAccept;

    private LogicReplicationComplexImpl(StreamReadConsumer consumer) {
        this.eventAccept = consumer;
    }

    public static EventFactory<ByteBuffer> instance(StreamReadConsumer consumer) {
        return new LogicReplicationComplexImpl(consumer);
    }

    @Override
    public void emit(ByteBuffer logEvent, Log log) {
        final Event<List<TapEvent>> redirect = Event.redirect(logEvent);
        if (null == redirect) return;
        final Event.EventEntity<List<TapEvent>> tapEvent = redirect.process(logEvent, null);;
        eventAccept.accept(tapEvent.event(), null);
    }

    @Override
    protected void process() { }

    @Override
    protected void accept() {

    }
}
