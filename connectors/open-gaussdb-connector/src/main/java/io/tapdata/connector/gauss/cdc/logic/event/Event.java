package io.tapdata.connector.gauss.cdc.logic.event;

import io.tapdata.connector.gauss.cdc.logic.AnalyzeLog;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.entity.event.TapEvent;

import java.nio.ByteBuffer;

public interface Event extends AnalyzeLog {
    EventEntity process(ByteBuffer logEvent, EventParam processParam);

    public static Event redirect(ByteBuffer logEvent) {

        return null;
    }

    public static class EventEntity {
        private final TapEvent event;
        private final String xid;

        public EventEntity(TapEvent event, String xid) {
            this.event = event;
            this.xid = xid;
        }

        public TapEvent event() {
            return this.event;
        }

        public String xid() {
            return this.xid;
        }
    }
}
