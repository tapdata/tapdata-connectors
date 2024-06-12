package io.tapdata.connector.tidb.util.pojo;

import com.alibaba.fastjson.JSONObject;

public class ReplicaConfig {
    private JSONObject filter;

    private Sink sink;

    public JSONObject getFilter() {
        return filter;
    }

    public void setFilter(JSONObject filter) {
        this.filter = filter;
    }

    public Sink getSink() {
        return sink;
    }

    public void setSink(Sink sink) {
        this.sink = sink;
    }
}
