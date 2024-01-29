package io.tapdata.connector.gauss.cdc.logic.event.dml;

import io.tapdata.connector.gauss.cdc.logic.event.DMLEvent;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import java.nio.ByteBuffer;

public class UpdateEvent implements DMLEvent {
    @Override
    public EventEntity process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public void analyze(ByteBuffer logEvent) {

    }
}
