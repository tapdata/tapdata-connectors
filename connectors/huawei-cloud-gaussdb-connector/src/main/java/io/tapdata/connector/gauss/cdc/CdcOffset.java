package io.tapdata.connector.gauss.cdc;

import java.util.HashMap;
import java.util.Map;

public class CdcOffset {
    public static final String XID_INDEX = "xidIndex";
    public static final String LSN = "lsn";
    public static final String TRANSACTION_TIMESTAMP = "transactionTimestamp";
    private int xidIndex;
    private Long lsn;
    private long transactionTimestamp;

    public CdcOffset withXidIndex(int xidIndex) {
        setXidIndex(xidIndex);
        return this;
    }
    public CdcOffset withTransactionTimestamp(long transactionTimestamp) {
        setTransactionTimestamp(transactionTimestamp);
        return this;
    }

    public void setXidIndex(int xidIndex) {
        this.xidIndex = xidIndex;
    }

    public int getXidIndex() {
        return xidIndex;
    }

    public Object getLsn() {
        return lsn;
    }

    public void setLsn(Long lsn) {
        this.lsn = lsn;
    }

    public long getTransactionTimestamp() {
        return transactionTimestamp;
    }

    public void setTransactionTimestamp(long transactionTimestamp) {
        this.transactionTimestamp = transactionTimestamp;
    }

    public Map<String, Object> toOffset() {
        Map<String, Object> o = new HashMap<>();
        o.put(XID_INDEX, xidIndex);
        o.put(LSN, lsn);
        o.put(TRANSACTION_TIMESTAMP, transactionTimestamp);
        return o;
    }

    public static CdcOffset fromOffset(Object o) {
        CdcOffset offset = new CdcOffset();
        if (o instanceof Map) {
            Map<String, Object> cdc = (Map<String, Object>) o;
            Object x = cdc.get(XID_INDEX);
            if (x instanceof Number) {
                offset.setXidIndex(((Number)x).intValue());
            }
            Object l = cdc.get(LSN);
            if (l instanceof Number) {
                offset.setLsn(((Number)l).longValue());
            }
            Object t = cdc.get(TRANSACTION_TIMESTAMP);
            if (t instanceof Number) {
                offset.setTransactionTimestamp(((Number)t).longValue());
            }
        }
        return offset;
    }
}
