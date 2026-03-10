package io.tapdata.connector.klustron.config;

import io.tapdata.connector.mysql.config.MysqlConfig;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/4 17:20 Create
 * @description
 */
public class KunLunCdcConfig extends KunLunMysqlConfig {
    @Override
    public MysqlConfig load(Map<String, Object> map) {
        MysqlConfig c = super.load(map);
        c.setDatabase(String.format("%s_$$_%s", db, sc));
        c.setSchema(getDatabase());
        return c;
    }
}
