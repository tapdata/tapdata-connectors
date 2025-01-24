package io.tapdata.connector.gauss.cdc.logic.event;

import io.tapdata.connector.gauss.cdc.logic.event.dml.CollectEntity;
import io.tapdata.connector.gauss.entity.IllegalDataLengthException;
import io.tapdata.connector.gauss.util.LogicUtil;
import io.tapdata.entity.event.TapEvent;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public interface DMLEvent extends Event<TapEvent> {

    @Override
    default Event.EventEntity<TapEvent> analyze(ByteBuffer logEvent, Map<String, Map<String, String>> dataTypeMap) {
        CollectEntity instance = CollectEntity.instance();
        byte[] bOrA = null;
        int bOrACount = 2;
        byte[] hasNextTag = null;
        byte[] schema = LogicUtil.read(logEvent, 2, 32);
        byte[] table = LogicUtil.read(logEvent, 2, 32);
        instance.withSchema(new String(schema))
                .withTable(new String(table));
        while (logEvent.hasRemaining()) {
            bOrA = LogicUtil.read(logEvent, 1);
            String bOrAChar = new String(bOrA);
            boolean doBreak = false;
            Map<String, Object> kvMap = new HashMap<>();
            Map<String, Integer> kTypeMap = new HashMap<>();
            switch (bOrAChar.toUpperCase()) {
                case "N":
                case "O":
                    collectAttr(logEvent, kvMap, kTypeMap, dataTypeMap.get(new String(table)));
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

    Event.EventEntity<TapEvent> collect(CollectEntity entity);

    default void collectAttr(ByteBuffer logEvent, Map<String, Object> kvMap, Map<String, Integer> kTypeMap, Map<String, String> dataTypeMap) {
        byte[] attrNum = LogicUtil.read(logEvent, 2);
        int attrNums = LogicUtil.bytesToInt(attrNum);//, 32);
        Map<String, Object> temp = new HashMap<>();
        try {
            for (int index = 0; index < attrNums; index++) {
                byte[] attrName = LogicUtil.read(logEvent, 2, 32);
                byte[] oid = LogicUtil.read(logEvent, 4);
                byte[] value = LogicUtil.readValue(logEvent, 4, 32);
                String fieldName = new String(attrName);
                if (fieldName.startsWith("\"")) {
                    fieldName = fieldName.substring(1);
                }
                if (fieldName.endsWith("\"")) {
                    fieldName = fieldName.substring(0, fieldName.length() - 1);
                }
                int typeId = LogicUtil.bytesToInt(oid);//, 32);
                temp.put(fieldName, value);
                kTypeMap.put(fieldName, typeId);
            }
            kvMap.putAll(temp);
        } catch (IllegalDataLengthException e) {
            StringJoiner j = new StringJoiner(", ");
            temp.forEach((k, v) -> {
                String value;
                if (null == v) {
                    value = "null";
                } else {
                    value = new String((byte[]) v);
                }
                j.add(k + ":" + value);
            });
            throw new IllegalDataLengthException(e.getMessage() + "[" + j.toString() + "] ");
        }
    }
}
