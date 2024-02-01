package io.tapdata.connector.gauss.cdc.logic.event.dml;

import java.util.Map;

public class CollectEntity {
    Map<String, Object> before;
    Map<String, Object> after;
    Map<String, Integer> fieldType;
    String schema;
    String table;

    public static CollectEntity instance() {
        return new CollectEntity();
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public CollectEntity withBefore(Map<String, Object> before) {
        this.before = before;
        return this;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public CollectEntity withAfter(Map<String, Object> after) {
        this.after = after;
        return this;
    }

    public String getSchema() {
        return schema;
    }

    public CollectEntity withSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public String getTable() {
        return table;
    }

    public CollectEntity withTable(String table) {
        this.table = table;
        return this;
    }

    public Map<String, Integer> getFieldType() {
        return fieldType;
    }

    public CollectEntity withFieldType(Map<String, Integer> fieldType) {
        this.fieldType = fieldType;
        return this;
    }

    public void setBefore(Map<String, Object> before) {
        this.before = before;
    }

    public void setAfter(Map<String, Object> after) {
        this.after = after;
    }

    public void setFieldType(Map<String, Integer> fieldType) {
        this.fieldType = fieldType;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
