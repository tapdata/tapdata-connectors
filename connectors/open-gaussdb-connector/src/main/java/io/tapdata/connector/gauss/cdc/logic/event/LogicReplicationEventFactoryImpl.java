package io.tapdata.connector.gauss.cdc.logic.event;

import io.tapdata.connector.gauss.cdc.logic.event.other.BeginTransaction;
import io.tapdata.connector.gauss.cdc.logic.event.other.CommitTransaction;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LogicReplicationEventFactoryImpl extends EventFactory<ByteBuffer> {
    String sid;
    List<TapEvent> event = new ArrayList<>();
    long sStartTime;
    StreamReadConsumer eventAccept;

    private LogicReplicationEventFactoryImpl(StreamReadConsumer consumer) {
        this.eventAccept = consumer;
    }

    public static EventFactory<ByteBuffer> instance(StreamReadConsumer consumer) {
        return new LogicReplicationEventFactoryImpl(consumer);
    }

    @Override
    public void emit(ByteBuffer logEvent) {
        final Event redirect = Event.redirect(logEvent);
        final Event.EventEntity tapEvent = redirect.process(logEvent, null);;
        if (redirect instanceof BeginTransaction) {
            sign(logEvent);
        }
        event.add(tapEvent.event());
        //@todo 提交事物
        if (redirect instanceof CommitTransaction) {
            process();
        }

        //@todo 事物事件超出预期

        //@todo 事物数太多

        //@todo 事物回滚

    }

    @Override
    protected void process() {
        try {
            event.stream().forEach(e -> {
                //todo
                eventAccept.accept(event, null);
            });
        } finally {
            cleanSign();
        }
    }

    private void sign(Object logEvent) {

    }

    private void cleanSign() {
        event = new ArrayList<>();
        sStartTime = 0;
        sid = null;
    }
}
