package io.tapdata.connector.gauss.cdc;

public class CdcOffset {
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
}
