package io.tapdata.connector.gauss.cdc.logic.event.transcation.discrete;

import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.util.LogicUtil;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.connector.gauss.util.TimeUtil;
import io.tapdata.entity.event.TapEvent;

import java.nio.ByteBuffer;

public class CommitTransaction implements Event<TapEvent> {
    protected static CommitTransaction instance;
    private CommitTransaction() {

    }
    public static CommitTransaction instance() {
        if (null == instance) {
            synchronized (CommitTransaction.class) {
                if (null == instance) instance = new CommitTransaction();
            }
        }
        return instance;
    }
    @Override
    public EventEntity<TapEvent> process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public Event.EventEntity<TapEvent> analyze(ByteBuffer logEvent, AnalyzeParam param) {
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
        return new EventEntity<>(null, null == xid ? "0" : "" + LogicUtil.byteToLong(xid)/* 64bit*/, timestampValue, 0, 0);
    }
}
