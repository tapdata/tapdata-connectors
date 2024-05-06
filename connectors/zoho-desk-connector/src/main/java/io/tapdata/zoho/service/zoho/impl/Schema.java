package io.tapdata.zoho.service.zoho.impl;

import io.tapdata.zoho.ZoHoConnector;
import io.tapdata.zoho.entity.ZoHoOffset;
import java.util.HashMap;

public class Schema {

    ZoHoConnector zoHoConnector;

    public synchronized boolean isAlive() {
        return null != zoHoConnector && this.zoHoConnector.isAlive();
    }

    public void init(ZoHoConnector zoHoConnector) {
        this.zoHoConnector = zoHoConnector;
    }

    public void out() {
        zoHoConnector = null;
    }

    public ZoHoOffset initOffset(Object offsetState) {
        if (!(offsetState instanceof ZoHoOffset)) {
            return ZoHoOffset.create(new HashMap<>());
        }
        return (ZoHoOffset) offsetState;
    }
}
