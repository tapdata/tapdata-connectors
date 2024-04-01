package io.tapdata.connector.tidb.cdc;

import io.tapdata.connector.mysql.entity.MysqlStreamOffset;
import io.tapdata.connector.tidb.cdc.CdcOffset;
import io.tapdata.entity.event.TapEvent;

public class TidbStreamEvent {

    private TapEvent tapEvent;
    private Long cdcOffset;

    public TidbStreamEvent(TapEvent tapEvent, Long cdcOffset) {
        this.tapEvent = tapEvent;
        this.cdcOffset = cdcOffset;
    }

    public TapEvent getTapEvent() {
        return tapEvent;
    }

    public Long getCdcOffset() {
        return cdcOffset;
    }

    public void setCdcOffset(Long cdcOffset) {
        this.cdcOffset = cdcOffset;
    }
}
