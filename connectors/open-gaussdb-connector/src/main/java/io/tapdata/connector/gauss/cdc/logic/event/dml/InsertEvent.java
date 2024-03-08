package io.tapdata.connector.gauss.cdc.logic.event.dml;

import io.tapdata.connector.gauss.cdc.logic.event.DMLEvent;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;

import java.nio.ByteBuffer;

public class InsertEvent implements DMLEvent {
    private InsertEvent() {

    }
    public static InsertEvent instance() {
        return new InsertEvent();
    }
    @Override
    public EventEntity<TapEvent> process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public EventEntity<TapEvent> collect(CollectEntity entity) {
        TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create();
        tapInsertRecordEvent.table(entity.getTable());
        tapInsertRecordEvent.after(entity.getAfter());
        //tapInsertRecordEvent.referenceTime();
        return new EventEntity<TapEvent>().event(tapInsertRecordEvent).xid("").timestamp(0).lsn(0).csn(0);
    }
}
