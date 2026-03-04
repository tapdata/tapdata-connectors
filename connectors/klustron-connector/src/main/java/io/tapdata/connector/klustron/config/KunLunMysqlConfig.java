package io.tapdata.connector.klustron.config;

import io.tapdata.connector.mysql.config.MysqlConfig;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/4 09:31 Create
 * @description
 */
public class KunLunMysqlConfig extends MysqlConfig {
    String dataHost;
    Integer dataPort;
    String dataUsername;
    String dataPassword;
    int mysqlPort;
    String db;
    String sc;

    @Override
    public MysqlConfig load(Map<String, Object> map) {
        MysqlConfig c = super.load(map);
        c.setPort(mysqlPort);
        if ("postgres".equals(db)) {
            c.setDatabase(sc);
        } else {
            c.setDatabase(String.format("%s.%s", db, sc));
        }
        c.setSchema(getDatabase());
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

    public int getMysqlPort() {
        return mysqlPort;
    }

    public void setMysqlPort(int mysqlPort) {
        this.mysqlPort = mysqlPort;
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
