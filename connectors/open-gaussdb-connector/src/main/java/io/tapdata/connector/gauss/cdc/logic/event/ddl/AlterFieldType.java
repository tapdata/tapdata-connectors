package io.tapdata.connector.gauss.cdc.logic.event.ddl;

import io.tapdata.connector.gauss.cdc.logic.event.DDLEvent;
import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.entity.event.TapEvent;

import java.nio.ByteBuffer;
import java.util.List;

public class AlterFieldType implements DDLEvent {
    @Override
    public EventEntity<TapEvent> process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public Event.EventEntity<TapEvent> analyze(ByteBuffer logEvent) {
        return null;
    }
}
