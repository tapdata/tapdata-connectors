package io.tapdata.connector.klustron.config;

import io.tapdata.common.CommonDbConfig;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.postgres.config.PostgresConfig;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/4 14:05 Create
 * @description
 */
public class KunLunPgConfig extends PostgresConfig {
    String username;
    String dataHost;
    Integer dataPort;
    String dataUsername;
    String dataPassword;
    String db;
    String sc;
    int pgPort;

    @Override
    public CommonDbConfig load(Map<String, Object> map) {
        CommonDbConfig c = super.load(map);
        c.setPort(pgPort);
        c.setDatabase(db);
        c.setSchema(sc);
        return c;
    }

    public String getDataHost() {
        return dataHost;
    }

    public void setDataHost(String dataHost) {
        this.dataHost = dataHost;
    }

    public Integer getDataPort() {
        return dataPort;
    }

    public void setDataPort(Integer dataPort) {
        this.dataPort = dataPort;
    }

    public String getDataUsername() {
        return dataUsername;
    }

    public void setDataUsername(String dataUsername) {
        this.dataUsername = dataUsername;
    }

    public String getDataPassword() {
        return dataPassword;
    }

    public void setDataPassword(String dataPassword) {
        this.dataPassword = dataPassword;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getPgPort() {
        return pgPort;
    }

    public void setPgPort(int pgPort) {
        this.pgPort = pgPort;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getSc() {
        return sc;
    }

    public void setSc(String sc) {
        this.sc = sc;
    }
}
