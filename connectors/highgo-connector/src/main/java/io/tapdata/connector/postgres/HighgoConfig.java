package io.tapdata.connector.postgres;

import io.tapdata.connector.postgres.config.PostgresConfig;

public class HighgoConfig extends PostgresConfig {
    public HighgoConfig() {
        setDbType("highgo");
        setJdbcDriver("com.highgo.jdbc.Driver");
    }
}
