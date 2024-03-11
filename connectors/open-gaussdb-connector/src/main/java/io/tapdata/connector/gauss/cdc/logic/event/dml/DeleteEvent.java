package io.tapdata.connector.gauss.cdc.logic.event.dml;

import io.tapdata.connector.gauss.cdc.logic.event.DMLEvent;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;

import java.nio.ByteBuffer;

public class DeleteEvent implements DMLEvent {
    private DeleteEvent() {

    }
    public static DeleteEvent instance() {
        return new DeleteEvent();
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
        return new EventEntity<TapEvent>().event(e).xid("").timestamp(0).lsn(0).csn(0);
    }
}
