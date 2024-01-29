package io.tapdata.connector.gauss.cdc.logic.event.ddl;

import io.tapdata.connector.gauss.cdc.logic.event.DDLEvent;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;

import java.nio.ByteBuffer;

public class AddField implements DDLEvent {
    @Override
    public EventEntity process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public void analyze(ByteBuffer logEvent) {

    }
}
