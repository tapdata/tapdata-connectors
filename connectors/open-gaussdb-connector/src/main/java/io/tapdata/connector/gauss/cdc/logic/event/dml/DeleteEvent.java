package io.tapdata.connector.gauss.cdc.logic.event.dml;

import io.tapdata.connector.gauss.cdc.logic.event.DMLEvent;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;

import java.nio.ByteBuffer;

public class DeleteEvent implements DMLEvent {
    private static DeleteEvent instance;
    private DeleteEvent() {

    }
    public static DeleteEvent instance() {
        if (null == instance) {
            synchronized (DeleteEvent.class) {
                if (null == instance) instance = new DeleteEvent();
            }
        }
        return instance;
    }
    @Override
    public EventEntity<TapEvent> process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public EventEntity<TapEvent> collect(CollectEntity entity) {
        TapDeleteRecordEvent e = TapDeleteRecordEvent.create();
        e.table(entity.getTable());
        e.before(entity.getBefore());
        //tapInsertRecordEvent.referenceTime();
        return new EventEntity<TapEvent>(e, "", 0, 0, 0);
    }
}
