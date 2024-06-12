package io.tapdata.connector.gauss.cdc.logic.event;

import io.tapdata.connector.gauss.cdc.logic.AnalyzeLog;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;

import java.nio.ByteBuffer;
import java.util.Arrays;

public interface Event<E> extends AnalyzeLog<E> {
    EventEntity<E> process(ByteBuffer logEvent, EventParam processParam);

    public static class EventEntity<E> {
        protected E event;
        protected String xid;
        protected long timestamp;
        protected long csn;
        protected long lsn;

        public EventEntity<E> event(E event) {
            this.event = event;
            return this;
        }
        public EventEntity<E> xid(String xid) {
            this.xid = xid;
            return this;
        }
        public EventEntity<E> timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }
        public EventEntity<E> csn(long csn) {
            this.csn = csn;
            return this;
        }
        public EventEntity<E> lsn(long lsn) {
            this.lsn = lsn;
            return this;
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
