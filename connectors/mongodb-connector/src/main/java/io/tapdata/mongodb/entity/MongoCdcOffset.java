package io.tapdata.mongodb.entity;

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
}
