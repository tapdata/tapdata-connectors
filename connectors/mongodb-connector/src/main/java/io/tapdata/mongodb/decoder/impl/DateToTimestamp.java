package io.tapdata.mongodb.decoder.impl;

import io.tapdata.entity.error.CoreException;
import io.tapdata.mongodb.decoder.CustomSQLObject;

import java.util.Date;
import java.util.Map;

public class DateToTimestamp implements CustomSQLObject<Object, Map<String, Object>> {
    public static final String DATE_TO_TIMESTAMP = "$dateToTimestamp";

    @Override
    public Object execute(Object functionObj, Map<String, Object> curMap) {
        if (functionObj instanceof Date) {
            return ((Date)functionObj).getTime();
        }
        if (null != functionObj) {
            throw new CoreException("{} cannot resolve non type {} objects to timestamp", DATE_TO_TIMESTAMP, functionObj.getClass());
        }
        return null;
    }

    @Override
    public String getFunctionName() {
        return DATE_TO_TIMESTAMP;
    }
}
