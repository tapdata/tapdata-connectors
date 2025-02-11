package io.tapdata.connector.gauss.cdc.logic.event.transcation.discrete;

import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.connector.gauss.util.LogicUtil;
import io.tapdata.connector.gauss.util.TimeUtil;
import io.tapdata.entity.event.TapEvent;

import java.nio.ByteBuffer;
import java.util.Map;

public class CommitTransaction implements Event<TapEvent> {
    private CommitTransaction() {

    }

    public static CommitTransaction instance() {
        return new CommitTransaction();
    }

    @Override
    public EventEntity<TapEvent> process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public Event.EventEntity<TapEvent> analyze(ByteBuffer logEvent, Map<String, Map<String, String>> dataTypeMap) {
        byte[] xidTag = LogicUtil.read(logEvent, 1);
        String xTagChar = new String(xidTag);
        byte[] xid = null;
        if ("X".equalsIgnoreCase(xTagChar)) {
            xid = LogicUtil.read(logEvent, 8);
        }
        byte[] timestampTag = LogicUtil.read(logEvent, 1);
        String timestampTagChar = new String(timestampTag);
        byte[] timestamp = null;
        if ("T".equalsIgnoreCase(timestampTagChar)) {
            timestamp = LogicUtil.read(logEvent, 4, 32);
        }
        byte[] nextTag = LogicUtil.read(logEvent, 1);
        long timestampValue = null == timestamp ? 0 : TimeUtil.parseTimestamp(new String(timestamp), 3);
        return new EventEntity<TapEvent>().event(null).xid(null == xid ? "0" : "" + LogicUtil.byteToLong(xid)/* 64bit*/).timestamp(timestampValue).lsn(0).csn(0);
    }
}
