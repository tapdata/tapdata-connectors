package io.tapdata.connector.tidb.cdc.process;


import io.tapdata.connector.tidb.cdc.process.ddl.entity.DDLObject;

public class TiData {
    /**
     * @see DDLObject#getTableVersion()
     * */
    Long tableVersion;

    public Long getTableVersion() {
        return tableVersion;
    }

    public void setTableVersion(Long tableVersion) {
        this.tableVersion = tableVersion;
    }
}
