package io.tapdata.connector.gauss.cdc.logic.event.dml;

import io.tapdata.connector.gauss.cdc.logic.event.DMLEvent;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

import java.nio.ByteBuffer;


public class UpdateEvent implements DMLEvent {
    private static UpdateEvent instance;
    private UpdateEvent() {

    }
    public static UpdateEvent instance() {
        if (null == instance) {
            synchronized (UpdateEvent.class) {
                if (null == instance) instance = new UpdateEvent();
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
        TapUpdateRecordEvent e = TapUpdateRecordEvent.create();
        e.table(entity.getTable());
        e.after(entity.getAfter());
        e.before(entity.getBefore());
        //tapInsertRecordEvent.referenceTime();
        return new EventEntity<TapEvent>(e, "", 0, 0, 0);
    }
}
