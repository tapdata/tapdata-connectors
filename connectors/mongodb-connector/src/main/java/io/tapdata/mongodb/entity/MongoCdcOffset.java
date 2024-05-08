package io.tapdata.mongodb.entity;

import java.util.HashMap;
import java.util.Map;

public class MongoCdcOffset {
    private Object opLogOffset;
    private Object cdcOffset;

    public MongoCdcOffset() {
    }

    public MongoCdcOffset(Object opLogOffset, Object cdcOffset) {
        this.opLogOffset = opLogOffset;
        this.cdcOffset = cdcOffset;
    }

    public Object getOpLogOffset() {
        return opLogOffset;
    }

    public void setOpLogOffset(Object opLogOffset) {
        this.opLogOffset = opLogOffset;
    }

    public Object getCdcOffset() {
        return cdcOffset;
    }

    public void setCdcOffset(Object cdcOffset) {
        this.cdcOffset = cdcOffset;
    }

    protected static final String OP_LOG_OFFSET = "opLogOffset";
    protected static final String CDC_OFFSET = "cdcOffset";
    protected static final String MONGO_CDC_OFFSET_FLAG = "mongo_cdc_offset_flag";
    public static MongoCdcOffset fromOffset(Object mongoOffset) {
        if (mongoOffset instanceof Map) {
            Map<String, Object> offset = (Map<String, Object>) mongoOffset;
            if (offset.containsKey(MONGO_CDC_OFFSET_FLAG) && Boolean.TRUE.equals(offset.get(MONGO_CDC_OFFSET_FLAG))) {
                return new MongoCdcOffset(offset.get(OP_LOG_OFFSET), offset.get(CDC_OFFSET));
            }
            return new MongoCdcOffset(null, mongoOffset);
        }
        if (mongoOffset instanceof MongoCdcOffset) {
            return (MongoCdcOffset) mongoOffset;
        }
        return new MongoCdcOffset(null, mongoOffset);
    }

    public Object toOffset() {
        Map<String, Object> offset = new HashMap<>();
        offset.put(OP_LOG_OFFSET, opLogOffset);
        offset.put(CDC_OFFSET, cdcOffset);
        offset.put(MONGO_CDC_OFFSET_FLAG, true);
        return offset;
    }
}
