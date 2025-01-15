package io.tapdata.connector.mysql;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    public static final Map<String, MysqlSchemaHistoryTransfer> mysqlSchemaHistoryTransferManager = new ConcurrentHashMap<>();
    public static MysqlSchemaHistoryTransfer getSchemaHistoryTransfer(String serverName){
        return mysqlSchemaHistoryTransferManager.get(serverName);
    }
}
