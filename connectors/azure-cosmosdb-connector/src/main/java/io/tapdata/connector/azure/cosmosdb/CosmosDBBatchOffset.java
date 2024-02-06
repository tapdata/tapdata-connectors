package io.tapdata.connector.azure.cosmosdb;

import java.io.Serializable;

public class CosmosDBBatchOffset implements Serializable {

    private static final long serialVersionUID = -5066976974060426678L;
    private String value;
//    private String sortKey;

    public CosmosDBBatchOffset(String value) {
        if (value != null) {
            this.value = value;
        }
//        this.sortKey = sortKey;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

//    public String getSortKey() {
//        return sortKey;
//    }
//
//    public void setSortKey(String sortKey) {
//        this.sortKey = sortKey;
//    }
}
