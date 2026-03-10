package io.tapdata.connector.klustron.cdc;

import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;

import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/5 17:10 Create
 * @description
 */
public class KunLunOffset {
    Map<String, MysqlBinlogPosition> offsetMap = new HashMap<>();

    public Map<String, MysqlBinlogPosition> getOffsetMap() {
        return offsetMap;
    }

    public void setOffsetMap(Map<String, MysqlBinlogPosition> offsetMap) {
        this.offsetMap = offsetMap;
    }

    public MysqlBinlogPosition getOffset(String host, Integer port) {
        return offsetMap.get(key(host, port));
    }

    public void setOffset(String host, Integer port, MysqlBinlogPosition offset) {
        offsetMap.put(key(host, port), offset);
    }

    String key(String host, int port) {
        return String.format("%s:%d", host, port);
    }
}
