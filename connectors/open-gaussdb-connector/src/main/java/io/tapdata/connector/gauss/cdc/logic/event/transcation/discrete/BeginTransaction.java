package io.tapdata.connector.gauss.cdc.logic.event.transcation.discrete;

import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.event.LogicUtil;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.connector.gauss.util.TimeUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.util.DateUtil;

import java.nio.ByteBuffer;

public class BeginTransaction implements Event<TapEvent> {
    private static BeginTransaction instance;
    private BeginTransaction() {

    }
    public static BeginTransaction instance() {
        if (null == instance) {
            synchronized (BeginTransaction.class) {
                if (null == instance) instance = new BeginTransaction();
            }
        }
        return instance;
    }
    @Override
    public EventEntity<TapEvent> process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public Event.EventEntity<TapEvent> analyze(ByteBuffer logEvent) {
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
        long csnNumber = LogicUtil.bytesToLong(csn);
        long lsnNumber = LogicUtil.bytesToLong(firstLsn);
        System.out.println(new String(commitTime));
        long timestamp = null == commitTime ? 0 :
                TimeUtil.parseTimestamp(new String(commitTime), "yyyy-MM-dd hh:mm:ss.ssssssXXX", 0);
        System.out.println("Begin: " + timestamp);
        return new EventEntity<>(null, "", timestamp, csnNumber, lsnNumber);
    }
}
