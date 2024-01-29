package io.tapdata.connector.gauss.cdc.logic.event.other;

import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;

import java.nio.ByteBuffer;

public class HeartBeatEvent implements Event {
    @Override
    public EventEntity process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public void analyze(ByteBuffer logEvent) {

    }
}
