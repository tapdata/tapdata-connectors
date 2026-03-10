package io.tapdata.connector.klustron.cdc;

import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;

import java.io.Serializable;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/5 17:08 Create
 * @description
 */
public class KunLunOffsetItem implements Serializable {
    String host;
    String port;
    MysqlBinlogPosition offset;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public MysqlBinlogPosition getOffset() {
        return offset;
    }

    public void setOffset(MysqlBinlogPosition offset) {
        this.offset = offset;
    }
}
