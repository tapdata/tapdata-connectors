package io.tapdata.connector.gauss.cdc.logic.event.other;

import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.entity.event.TapEvent;

import java.nio.ByteBuffer;

public class HeartBeatEvent implements Event<TapEvent> {
    private static HeartBeatEvent instance;
    private HeartBeatEvent() {

    }
    public static HeartBeatEvent instance() {
        if (null == instance) {
            synchronized (HeartBeatEvent.class) {
                if (null == instance) instance = new HeartBeatEvent();
            }
        }
        return instance;
    }
    @Override
    public EventEntity<TapEvent> process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public Event.EventEntity<TapEvent> analyze(ByteBuffer logEvent) {
        return null;
    }
}
