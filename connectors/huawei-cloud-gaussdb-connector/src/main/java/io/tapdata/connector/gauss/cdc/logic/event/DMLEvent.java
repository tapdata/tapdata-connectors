package io.tapdata.connector.gauss.cdc.logic.event;

import io.tapdata.connector.gauss.cdc.logic.event.dml.CollectEntity;
import io.tapdata.connector.gauss.entity.IllegalDataLengthException;
import io.tapdata.connector.gauss.util.LogicUtil;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.StringKit;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public abstract class DMLEvent implements Event<TapEvent> {

    private int offsetHour = 0;


    @Override
    public Event.EventEntity<TapEvent> analyze(ByteBuffer logEvent, Map<String, Map<String, String>> dataTypeMap) {
        return analyze(logEvent, dataTypeMap, null);
    }

    @Override
    public Event.EventEntity<TapEvent> analyze(ByteBuffer logEvent, Map<String, Map<String, String>> dataTypeMap, String charset) {
        CollectEntity instance = CollectEntity.instance();
        byte[] bOrA = null;
        int bOrACount = 2;
        byte[] hasNextTag = null;
        byte[] schema = LogicUtil.read(logEvent, 2, 32);
        byte[] table = LogicUtil.read(logEvent, 2, 32);
        try {
            if (null == charset) {
                charset = "UTF-8";
            }
            instance.withSchema(new String(schema, charset))
                    .withTable(new String(table, charset));
            while (logEvent.hasRemaining()) {
                bOrA = LogicUtil.read(logEvent, 1);
                String bOrAChar = new String(bOrA, charset);
                boolean doBreak = false;
                Map<String, Object> kvMap = new HashMap<>();
                Map<String, Integer> kTypeMap = new HashMap<>();
                switch (bOrAChar.toUpperCase()) {
                    case "N":
                    case "O":
                        collectAttr(logEvent, kvMap, kTypeMap, dataTypeMap, new String(table, charset));
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
            if ("P".equalsIgnoreCase(new String(hasNextTag, charset))) {
                //break;
            }
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        return collect(instance);
    }

    protected abstract Event.EventEntity<TapEvent> collect(CollectEntity entity);

    public <T extends DMLEvent> T offsetHour(int offsetHour) {
        this.offsetHour = offsetHour;
        return (T) this;
    }

    public void collectAttr(ByteBuffer logEvent, Map<String, Object> kvMap, Map<String, Integer> kTypeMap, Map<String, Map<String, String>> dataTypeMap, String table) {
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
                temp.put(fieldName, filterDateTime(fieldName, value, dataTypeMap.get(table), table));
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
            throw new IllegalDataLengthException(e.getMessage() + "[" + j + "] ");
        }
    }

    private Object filterDateTime(String fieldName, byte[] value, Map<String, String> dataTypeMap, String table) {
        if (null == value) {
            return null;
        }
        if (null == dataTypeMap) {
            return value;
        }
        String dataType = dataTypeMap.get(fieldName);
        if (null == dataType) {
            return value;
        }
        String valueStr = new String(value);
        switch (dataType) {
            case "bit":
                return "1".equals(valueStr);
            case "bytea":
                return decodeBytea(value);
            case "json":
                return valueStr;
            case "blob":
                return StringKit.toByteArray(valueStr);
            case "money":
                return Double.parseDouble(valueStr.replace("$", ""));
            case "timestamp with time zone":
                return LocalDateTime.parse(valueStr.substring(0, valueStr.length() - 3).replace(" ", "T")).minusHours(Integer.parseInt(valueStr.substring(valueStr.length() - 3))).atZone(ZoneOffset.UTC);
            case "time with time zone":
                return LocalTime.parse(valueStr.substring(0, valueStr.length() - 3)).atDate(LocalDate.ofYearDay(1970, 1)).minusHours(Integer.parseInt(valueStr.substring(valueStr.length() - 3))).atZone(ZoneOffset.UTC);
            case "timestamp without time zone":
            case "smalldatetime":
                return LocalDateTime.parse(valueStr.replace(" ", "T")).minusHours(offsetHour);
            case "time without time zone":
                return LocalTime.parse(valueStr).atDate(LocalDate.ofYearDay(1970, 1)).minusHours(offsetHour);
        }
        return value;
    }

    private byte[] decodeBytea(byte[] bytes) {
        byte[] newBytes = new byte[bytes.length / 2 - 1];
        for (int i = 2; i < bytes.length; i += 2) {
            newBytes[i / 2 - 1] = (byte) ((bytes[i] - 48) * 16 + bytes[i + 1] - 48);
        }
        return newBytes;
    }
}
