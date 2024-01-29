package io.tapdata.connector.gauss.config;

import io.tapdata.entity.utils.DataMap;


/**
 * @author Gavin'Xiao
 * @date 2024/01/16 18:50:00
 * node config
 * */
public class GaussNodeConfig {
    private GaussNodeConfig() {

    }

    public static class Builder {
        String database;
        public Builder() {

        }
        public Builder(DataMap map) {
            if (null != map && !map.isEmpty()) {

            }
        }
        public GaussNodeConfig build() {
            return new GaussNodeConfig();
        }
    }
}
