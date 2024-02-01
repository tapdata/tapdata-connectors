package io.tapdata.connector.gauss.cdc.logic.event;

import io.tapdata.connector.gauss.cdc.logic.AnalyzeLog;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;

import java.nio.ByteBuffer;
import java.util.Arrays;

public interface Event<E> extends AnalyzeLog<E> {
    EventEntity<E> process(ByteBuffer logEvent, EventParam processParam);

    public static Event redirect(ByteBuffer logEvent) {
        System.out.println("EVENT: " + Arrays.toString(logEvent.array()));
        return null;
    }

    public static class EventEntity<E> {
        private final E event;
        private final String xid;
        private final long timestamp;
        private final long csn;
        private final long lsn;

        public EventEntity(E event, String xid, long timestamp, long csn, long lsn) {
            this.event =  event;
            this.xid = xid;
            this.timestamp = timestamp;
            this.csn = csn;
            this.lsn = lsn;
        }

        public E event() {
            return this.event;
        }

        public String xid() {
            return this.xid;
        }

        public long timestamp() {
            return timestamp;
        }
        public long csn() {
            return csn;
        }
        public long lsn() {
            return lsn;
        }


    }
}
