package io.tapdata.connector.mysql.entity;

import io.tapdata.entity.event.TapEvent;

/**
 * @author samuel
 * @Description
 * @create 2022-05-24 23:42
 **/
public class MysqlStreamEvent {
    private TapEvent tapEvent;
    private MysqlStreamOffset mysqlStreamOffset;
    private MysqlBinlogPosition mysqlBinlogPosition;
    private String partitionSetId;

    public MysqlStreamEvent(TapEvent tapEvent, MysqlStreamOffset mysqlStreamOffset) {
        this.tapEvent = tapEvent;
        this.mysqlStreamOffset = mysqlStreamOffset;
    }

    public MysqlStreamEvent(TapEvent tapEvent, MysqlBinlogPosition mysqlBinlogPosition, String partitionSetId) {
        this.tapEvent = tapEvent;
        this.mysqlBinlogPosition = mysqlBinlogPosition;
        this.partitionSetId = partitionSetId;
    }

    public TapEvent getTapEvent() {
        return tapEvent;
    }

    public MysqlStreamOffset getMysqlStreamOffset() {
        return mysqlStreamOffset;
    }

    public MysqlBinlogPosition getMysqlBinlogPosition() {
        return mysqlBinlogPosition;
    }

    public String getPartitionSetId() {
        return partitionSetId;
    }
}
