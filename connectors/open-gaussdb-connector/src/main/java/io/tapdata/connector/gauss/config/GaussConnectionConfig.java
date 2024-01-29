package io.tapdata.connector.gauss.config;


import io.tapdata.connector.gauss.enums.ConnectionEnum;
import io.tapdata.entity.utils.DataMap;

import java.util.Optional;

/**
 * @author Gavin'Xiao
 * @date 2024/01/16 18:50:00
 * connection config
 * */
public class GaussConnectionConfig {
    String database;
    private GaussConnectionConfig(String database) {
        this.database = Optional.ofNullable(database).orElse("");
    }

    public static class Builder {
        String database;
        public Builder() {

        }
        public Builder(DataMap map) {
            if (null != map && !map.isEmpty()) {
                this.database = map.getString(ConnectionEnum.DATABASE_TAG);
            }
        }
        public Builder withDataBase(String database) {
            this.database = database;
            return this;
        }
        public GaussConnectionConfig build() {
            return new GaussConnectionConfig(database);
        }
    }
}
