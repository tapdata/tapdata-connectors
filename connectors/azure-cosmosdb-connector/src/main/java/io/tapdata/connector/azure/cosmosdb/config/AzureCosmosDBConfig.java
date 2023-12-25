package io.tapdata.connector.azure.cosmosdb.config;

import io.tapdata.common.CommonDbConfig;

import java.io.Serializable;

public class AzureCosmosDBConfig extends CommonDbConfig implements Serializable {
    private String host;
    private String accountKey;
    private String databaseName;

    private String consistencyLevel;

    public String getConsistencyLevel() {
        return consistencyLevel;
    }

    public void setConsistencyLevel(String consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    public String getAccountKey() {
        return accountKey;
    }

    public void setAccountKey(String masterKey) {
        this.accountKey = masterKey;
    }


    @Override
    public String getHost() {
        return host;
    }

    @Override
    public void setHost(String host) {
        this.host = host;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}
