package io.tapdata.common.sqlparser;

import java.util.HashMap;
import java.util.Map;

public class ResultDO {
    private Operate op;
    private String schema;
    private String tableName;
    private Map<String, Object> data;

    public ResultDO(Operate op) {
        this(op, new HashMap<>());
    }

    public ResultDO(Operate op, Map<String, Object> data) {
        this.op = op;
        this.data = data;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Operate getOp() {
        return op;
    }

    public void setOp(Operate op) {
        this.op = op;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public void putData(String key, Object val) {
        this.data.put(key, val);
    }

    public void putIfAbsent(String key, Object val) {
        if (!this.data.containsKey(key)) {
            this.data.put(key, val);
        }
    }

    @Override
    public String toString() {
        return "ResultDO{" +
                "op=" + op +
                ", db='" + schema + '\'' +
                ", tableName='" + tableName + '\'' +
                ", data=" + data +
                '}';
    }
}
