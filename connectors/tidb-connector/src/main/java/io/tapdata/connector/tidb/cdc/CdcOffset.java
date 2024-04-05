package io.tapdata.connector.tidb.cdc;

public class CdcOffset {
    private  String table;
    private  long  offset;

    public CdcOffset(String table,long  offset){
        this.table = table;
        this.offset = offset;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
