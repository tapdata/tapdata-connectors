package io.tapdata.connector.tidb.cdc.process.dml.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.tapdata.connector.tidb.cdc.process.TiData;
import io.tapdata.connector.tidb.cdc.process.ddl.convert.Convert;

import java.util.List;
import java.util.Map;

public class DMLObject extends TiData {

    @JsonProperty("id")
    Long id;

    @JsonProperty("database")
    String database;

    @JsonProperty("table")
    String table;

    @JsonProperty("pkNames")
    List<String> pkNames;

    @JsonProperty("isDdl")
    Boolean isDdl;

    /**
     * @see DMLType
     * */
    @JsonProperty("type")
    String type;

    @JsonProperty("es")
    Long es;

    @JsonProperty("ts")
    Long ts;

    @JsonProperty("sql")
    String sql;

    @JsonProperty("sqlType")
    Map<String, Integer> sqlType;

    @JsonProperty("mysqlType")
    Map<String, String> mysqlType;

    @JsonProperty("old")
    List<Map<String, Object>> old;

    @JsonProperty("data")
    List<Map<String, Object>> data;

    Map<String, Convert> tableColumnInfo;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Boolean getDdl() {
        return isDdl;
    }

    public void setDdl(Boolean ddl) {
        isDdl = ddl;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Map<String, Integer> getSqlType() {
        return sqlType;
    }

    public void setSqlType(Map<String, Integer> sqlType) {
        this.sqlType = sqlType;
    }

    public Map<String, String> getMysqlType() {
        return mysqlType;
    }

    public void setMysqlType(Map<String, String> mysqlType) {
        this.mysqlType = mysqlType;
    }

    public List<Map<String, Object>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, Object>> old) {
        this.old = old;
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public Map<String, Convert> getTableColumnInfo() {
        return tableColumnInfo;
    }

    public void setTableColumnInfo(Map<String, Convert> info) {
        this.tableColumnInfo = info;
    }
}
