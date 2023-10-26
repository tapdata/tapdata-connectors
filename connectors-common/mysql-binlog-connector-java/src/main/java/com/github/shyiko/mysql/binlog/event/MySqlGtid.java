package com.github.shyiko.mysql.binlog.event;

import java.util.UUID;

public class MySqlGtid {
    private final UUID serverId;
    private final long transactionId;

    public MySqlGtid(UUID serverId, long transactionId) {
        this.serverId = serverId;
        this.transactionId = transactionId;
    }

    public static MySqlGtid fromString(String gtid) {
        String[] split = gtid.split(":");
        String sourceId = split[0];
        long transactionId = Long.parseLong(split[1]);
        return new MySqlGtid(UUID.fromString(sourceId), transactionId);
    }

    @Override
    public String toString() {
        return serverId.toString()+":"+transactionId;
    }

    public UUID getServerId() {
        return serverId;
    }

    public long getTransactionId() {
        return transactionId;
    }
}
