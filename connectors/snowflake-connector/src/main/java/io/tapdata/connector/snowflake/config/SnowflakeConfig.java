package io.tapdata.connector.snowflake.config;

import io.tapdata.common.CommonDbConfig;

/**
 * Snowflake Configuration
 *
 * @author Jarad
 * @date 2026/03/24
 */
public class SnowflakeConfig extends CommonDbConfig {

    private String account;
    private String warehouse;
    private String role;

    public SnowflakeConfig() {
        setDbType("snowflake");
        setJdbcDriver("net.snowflake.client.jdbc.SnowflakeDriver");
        setEscapeChar('"');
    }

    @Override
    public String getConnectionString() {
        return "jdbc:snowflake://" + account + ".snowflakecomputing.com:443";
    }

    public String getDatabaseUrl() {
        return "jdbc:snowflake://" + account + ".snowflakecomputing.com:443" +
                "?warehouse=" + warehouse +
                "&db=" + getDatabase();
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public void setWarehouse(String warehouse) {
        this.warehouse = warehouse;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }
}

