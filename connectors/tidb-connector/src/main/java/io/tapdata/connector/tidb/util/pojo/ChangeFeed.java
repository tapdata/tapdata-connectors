package io.tapdata.connector.tidb.util.pojo;

import com.alibaba.fastjson.JSONObject;

public class ChangeFeed {
    private Long startTs;
    private String sinkUri;
    // synchronize DDL
    private Boolean syncDdl;
    private String changefeedId;

    private ReplicaConfig replicaConfig;







    //Synchronize tables without valid indexes
    public Boolean forceReplicate;

    public Long getStartTs() {
        return startTs;
    }

    public void setStartTs(Long startTs) {
        this.startTs = startTs;
    }

    public Boolean getSyncDdl() {
        return syncDdl;
    }

    public void setSyncDdl(Boolean syncDdl) {
        this.syncDdl = syncDdl;
    }

    public Boolean getForceReplicate() {
        return forceReplicate;
    }

    public void setForceReplicate(Boolean forceReplicate) {
        this.forceReplicate = forceReplicate;
    }


    public String getSinkUri() {
        return sinkUri;
    }

    //ignore Synchronize tables without primary keys
    private Boolean ignoreIneligibleTable;

    public Boolean getIgnoreIneligibleTable() {
        return ignoreIneligibleTable;
    }


    public void setIgnoreIneligibleTable(Boolean ignoreIneligibleTable) {
        this.ignoreIneligibleTable = ignoreIneligibleTable;
    }

    public void setSinkUri(String sinkUri) {
        this.sinkUri = sinkUri;
    }

    public ReplicaConfig getReplicaConfig() {
        return replicaConfig;
    }

    public void setReplicaConfig(ReplicaConfig replicaConfig) {
        this.replicaConfig = replicaConfig;
    }

    public String getChangefeedId() {
        return changefeedId;
    }

    public void setChangefeedId(String changefeedId) {
        this.changefeedId = changefeedId;
    }



    public ChangeFeed() {
    }


    public ChangeFeed(String sinkUri, Boolean syncDdl, String changeFeedId, Boolean ignoreIneligibleTable) {
        this.sinkUri = sinkUri;
        this.syncDdl = syncDdl;
        this.changefeedId = changeFeedId;
    }
}
