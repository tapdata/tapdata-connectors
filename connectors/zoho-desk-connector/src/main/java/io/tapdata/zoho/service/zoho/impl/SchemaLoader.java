package io.tapdata.zoho.service.zoho.impl;

import cn.hutool.core.date.DateUtil;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.zoho.ZoHoConnector;
import io.tapdata.zoho.entity.ZoHoOffset;
import io.tapdata.zoho.utils.BeanUtil;
import io.tapdata.zoho.utils.ZoHoString;

import java.util.*;
import java.util.function.BiConsumer;

public interface SchemaLoader {
    String CONNECTION_MODE = "connectionMode";
    String MODIFIED_TIME = "modifiedTime";
    String CREATED_TIME = "createdTime";

    String tableName();

    boolean isAlive();

    void init(ZoHoConnector zoHoConnector);

    void out();

    SchemaLoader configSchema(TapConnectionContext tapConnectionContext);

    default void streamRead(Object offsetState, int recordSize, StreamReadConsumer consumer) {
        throw new CoreException("Table {} not support stream read", tableName());
    }

    void batchRead(Object offset, int batchCount, BiConsumer<List<TapEvent>, Object> consumer);

    long batchCount();

    static SchemaLoader loader(String tableName, ZoHoConnector zoHoConnector) {
        Object bean = BeanUtil.bean(SchemaLoader.class.getPackage().getName() + "." + tableName + "Schema");
        SchemaLoader schema = (SchemaLoader) bean;
        schema.init(zoHoConnector);
        return schema;
    }

    default void accept(BiConsumer<List<TapEvent>, Object> consumer, List<TapEvent> events, Object offset) {
        Object o = offset instanceof ZoHoOffset ? ((ZoHoOffset) offset).offset() : offset;
        consumer.accept(events, o);
    }

    default Long parseZoHoDatetime(String referenceTimeStr) {
        referenceTimeStr = ZoHoString.replace(referenceTimeStr, "Z", "");
        referenceTimeStr = ZoHoString.replace(referenceTimeStr, "T", " ");
        return DateUtil.parse(referenceTimeStr, "yyyy-MM-dd HH:mm:ss.SSS").getTime();
    }

    default void acceptOne(Map<String, Object> data, ZoHoOffset offset, boolean isStreamRead, List<TapEvent>[] events, int readSize, BiConsumer<List<TapEvent>, Object> consumer) {
        String tableName = tableName();
        long referenceTime = referenceTime(offset, data, tableName, isStreamRead);
        TapInsertRecordEvent event = TapSimplify.insertRecordEvent(data, tableName);
        if (isStreamRead) {
            event.referenceTime(referenceTime);
        }
        events[0].add(event);
        if (events[0].size() != readSize) return;
        accept(consumer, events[0], offset);
        events[0] = new ArrayList<>();
    }

    /**
     * stream read is sort by "modifiedTime",batch read is sort by "createdTime"
     */
    default long referenceTime(ZoHoOffset offset, Map<String, Object> data, String tableName, boolean isStreamRead) {
        Object modifiedTimeObj = referenceTimeObj(data, isStreamRead);
        long referenceTime = System.currentTimeMillis();
        if (modifiedTimeObj instanceof String) {
            referenceTime = parseZoHoDatetime((String) modifiedTimeObj);
            offset.getTableUpdateTimeMap().put(tableName, referenceTime);
        }
        return referenceTime;
    }

    default Object referenceTimeObj(Map<String, Object> data, boolean isStreamRead) {
        return isStreamRead ? data.get(SchemaLoader.MODIFIED_TIME) : data.get(SchemaLoader.CREATED_TIME);
    }
}
