package io.tapdata.connector.gauss.cdc.logic.event.transcation.discrete;

import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.connector.gauss.util.LogicUtil;
import io.tapdata.connector.gauss.util.TimeUtil;
import io.tapdata.entity.event.TapEvent;

import java.nio.ByteBuffer;
import java.util.Map;

public class BeginTransaction implements Event<TapEvent> {
    private BeginTransaction() {

    }

    public static BeginTransaction instance() {
        return new BeginTransaction();
    }

    @Override
    public EventEntity<TapEvent> process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public Event.EventEntity<TapEvent> analyze(ByteBuffer logEvent, Map<String, Map<String, String>> dataTypeMap) {
        byte[] csn = LogicUtil.read(logEvent, 8);
        byte[] firstLsn = LogicUtil.read(logEvent, 8);
        byte[] commitTimeTag = LogicUtil.read(logEvent, 1);
        String commitTimeTagChar = new String(commitTimeTag);
        byte[] commitTime = null;
        if ("T".equalsIgnoreCase(commitTimeTagChar)) {
            commitTime = LogicUtil.read(logEvent, 4, 32);
        }
        byte[] userTag = LogicUtil.read(logEvent, 1);
        String userTagChar = new String(userTag);
        byte[] user = null;
        if ("N".equalsIgnoreCase(userTagChar)) {
            user = LogicUtil.read(logEvent, 4, 32);
        }
        byte[] endTag = LogicUtil.read(logEvent, 1);
        long csnNumber = LogicUtil.byteToLong(csn);
        long lsnNumber = LogicUtil.byteToLong(firstLsn);
        long timestamp = null == commitTime ? 0 : TimeUtil.parseTimestamp(new String(commitTime), 3);
        return new EventEntity<TapEvent>().event(null).xid("").timestamp(timestamp).lsn(lsnNumber).csn(csnNumber);
    }
}
