package io.tapdata.connector.gauss.cdc;

public class CdcOffset {
    private int xidIndex;
    private Long lsn;
    private long transactionTimestamp;
    public CdcOffset() {

    }
    public CdcOffset withXidIndex(int xidIndex) {
        this.xidIndex = xidIndex;
        return this;
    }
    public CdcOffset withLsn(Long lsn) {
        this.lsn = lsn;
        return this;
    }
    public CdcOffset withTransactionTimestamp(long transactionTimestamp) {
        this.transactionTimestamp = transactionTimestamp;
        return this;
    }

    public int getXidIndex() {
        return xidIndex;
    }

    public void setXidIndex(int xidIndex) {
        this.xidIndex = xidIndex;
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
}
