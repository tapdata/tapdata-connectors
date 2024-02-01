package io.tapdata.connector.gauss.cdc.logic.event;

import io.tapdata.connector.gauss.cdc.logic.event.dml.CollectEntity;
import io.tapdata.entity.event.TapEvent;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public interface DMLEvent extends Event<TapEvent> {

    @Override
    public default Event.EventEntity<TapEvent> analyze(ByteBuffer logEvent) {
        CollectEntity instance = CollectEntity.instance();
        byte[] bOrA = null;
        int bOrACount = 2;
        byte[] hasNextTag = null;
        byte[] schema = LogicUtil.read(logEvent, 2, 32);
        byte[] table = LogicUtil.read(logEvent, 2, 32);
        instance.withSchema(new String(schema)).withTable(new String(table));
        while (logEvent.hasRemaining()) {
            bOrA = LogicUtil.read(logEvent, 1);
            String bOrAChar = new String(bOrA);
            boolean doBreak = false;
            Map<String, Object> kvMap = new HashMap<>();
            Map<String, Integer> kTypeMap = new HashMap<>();
            switch (bOrAChar.toUpperCase()) {
                case "N" :
                case "O" :
                    collectAttr(logEvent, kvMap, kTypeMap);
                    if (bOrACount > 1) {
                        bOrACount--;
                    } else {
                        doBreak = true;
                    }
                    break;
                case "P":
                    hasNextTag = bOrA;
                    doBreak = true;
                    break;
                default:
                    doBreak = true;
                    break;
            }
            if ("n".equalsIgnoreCase(bOrAChar)) {
                instance.withAfter(kvMap).withFieldType(kTypeMap);
            } else if ("o".equalsIgnoreCase(bOrAChar)) {
                instance.withBefore(kvMap);
            }
            if (doBreak) break;
        }
        hasNextTag = null == hasNextTag ? LogicUtil.read(logEvent, 1) : hasNextTag;
        if ("P".equalsIgnoreCase(new String(hasNextTag))) {
            //break;
        }

        return collect(instance);
    }

    public Event.EventEntity<TapEvent> collect(CollectEntity entity);

    public default void collectAttr(ByteBuffer logEvent, Map<String, Object> kvMap, Map<String, Integer> kTypeMap) {
        byte[] attrNum = LogicUtil.read(logEvent, 2);
        int attrNums = LogicUtil.bytesToInt(attrNum, 32);
        for (int index = 0; index < attrNums; index++) {
            byte[] attrName = LogicUtil.read(logEvent, 2, 32);
            byte[] oid = LogicUtil.read(logEvent, 4);
            byte[] value = LogicUtil.read(logEvent, 4, 32);
            if (0 == value.length) {
                value = "".getBytes();
            }
            String v = new String(value);
            if ("0xFFFFFFFF".equalsIgnoreCase(v)) {
                v = null;
                value = null;
            }
            String fieldName = new String(attrName);
            kvMap.put(fieldName, value);
            kTypeMap.put(fieldName, LogicUtil.bytesToInt(oid, 32));
        }
    }
}
