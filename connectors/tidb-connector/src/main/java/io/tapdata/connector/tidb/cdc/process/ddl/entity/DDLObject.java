package io.tapdata.connector.tidb.cdc.process.ddl.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.tapdata.connector.tidb.cdc.process.TiData;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class DDLObject extends TiData implements Serializable {

    @JsonProperty("Table")
    String table;

    @JsonProperty("Schema")
    String schema;

    @JsonProperty("Version")
    Integer version;

    /**
     * TOS timestamp serial number
     * */
    @JsonProperty("TableVersion")
    Long tableVersion;

    @JsonProperty("Query")
    String query;

    @JsonProperty("Type")
    String type;

    @JsonProperty("TableColumns")
    List<Map<String, Object>> tableColumns;

    @JsonProperty("TableColumnsTotal")
    Integer tableColumnsTotal;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Long getTableVersion() {
        return tableVersion;
    }

    public void setTableVersion(Long tableVersion) {
        this.tableVersion = tableVersion;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Map<String, Object>> getTableColumns() {
        return tableColumns;
    }

    public void setTableColumns(List<Map<String, Object>> tableColumns) {
        this.tableColumns = tableColumns;
    }

    public Integer getTableColumnsTotal() {
        return tableColumnsTotal;
    }

    public void setTableColumnsTotal(Integer tableColumnsTotal) {
        this.tableColumnsTotal = tableColumnsTotal;
    }
}
