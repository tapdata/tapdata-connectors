package io.tapdata.dummy.utils;

import io.tapdata.dummy.constants.RecordOperators;
import io.tapdata.dummy.constants.SyncStage;
import io.tapdata.dummy.po.DummyOffset;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.simplify.TapSimplify;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * TapEvent builder
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/8 15:21 Create
 */
public class TapEventBuilder {

    public static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final char[] RANDOM_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    private static final int RANDOM_CHARS_LENGTH = RANDOM_CHARS.length;
    public static final int DEFAULT_RANDOM_STRING_LENGTH = 8;
    public static final int LONG_RANDOM_STRING_LENGTH = 1000;
    public static final int DEFAULT_RANDOM_DATE_FRACTION = 0;

    private final AtomicLong eventIndex = new AtomicLong(0);
    private AtomicLong serial;
    private Integer serialStep;

    private DummyOffset offset;

    private final Consumer<Map<String, Object>> heartbeatSetter;
    private Map<String, String> cacheRString = new Object2ObjectOpenHashMap<>();
    private Map<String, Double> cacheRNumber = new Object2ObjectOpenHashMap<>();
    private Map<String, String> cacheRLongString = new HashMap<>();
    private Map<String, byte[]> cacheRLongBinary = new HashMap<>();
    private Map<String, Timestamp> cacheRDate = new HashMap<>();

    public TapEventBuilder() {
        this((data) -> {
        });
    }

    public TapEventBuilder(Consumer<Map<String, Object>> heartbeatSetter) {
        this.heartbeatSetter = heartbeatSetter;
    }

    public void reset(Object offsetState, SyncStage syncStage) {
        if (offsetState instanceof DummyOffset) {
            this.offset = (DummyOffset) offsetState;
        } else {
            this.offset = new DummyOffset();
            offset.setBeginTimes(System.currentTimeMillis());
        }
        offset.setSyncStage(syncStage);
    }

    private void updateOffset(String tableName, RecordOperators op, TapRecordEvent recordEvent) {
        recordEvent.setReferenceTime(System.currentTimeMillis());
        offset.setLastTN(eventIndex.addAndGet(1));
        offset.setLastTimes(recordEvent.getTime());
        offset.addCounts(op, tableName, 1);
    }

    public TapInsertRecordEvent generateInsertRecordEvent(TapTable table) {
        Map<String, Object> after = new HashMap<>(table.getNameFieldMap().size() * 2);
//        Map<String, Object> after = new Object2ObjectOpenHashMap<>(table.getNameFieldMap().size() * 2);
        for (TapField tapField : table.childItems()) {
            after.put(tapField.getName(), generateEventValue(tapField, RecordOperators.Insert));
        }
        heartbeatSetter.accept(after);

        TapInsertRecordEvent tapEvent = TapSimplify.insertRecordEvent(after, table.getName());
        updateOffset(table.getName(), RecordOperators.Insert, tapEvent);
        return tapEvent;
    }

    public TapInsertRecordEvent generateInsertRecordEvent(TapTable table, List<FieldTypeCode> fieldTypeCodes) {
        Map<String, Object> after = new Object2ObjectOpenHashMap<>(table.getNameFieldMap().size() * 2);
        for (FieldTypeCode fieldTypeCode : fieldTypeCodes) {
            after.put(fieldTypeCode.getTapField().getName(), generateEventValue(fieldTypeCode, RecordOperators.Insert));
        }
        heartbeatSetter.accept(after);

        TapInsertRecordEvent tapEvent = TapSimplify.insertRecordEvent(after, table.getName());
        updateOffset(table.getName(), RecordOperators.Insert, tapEvent);
        return tapEvent;
    }

    public TapInsertRecordEvent copy(TapTable table, TapInsertRecordEvent event) {
        TapInsertRecordEvent tapEvent = TapSimplify.insertRecordEvent(event.getAfter(), table.getName());
        updateOffset(table.getName(), RecordOperators.Insert, tapEvent);
        return tapEvent;
    }

    public TapUpdateRecordEvent generateUpdateRecordEvent(TapTable table, Map<String, Object> before) {
        before = (null == before) ? generateInsertRecordEvent(table).getAfter() : new HashMap<>(before);
        Map<String, Object> after = new HashMap<>(before);
        table.childItems().forEach(tapField -> {
            if (Boolean.FALSE.equals(tapField.getPrimaryKey())) {
                after.put(tapField.getName(), generateEventValue(tapField, RecordOperators.Update));
            }
        });
        heartbeatSetter.accept(after);

        String tableName = table.getName();
        TapUpdateRecordEvent updateRecordEvent = TapSimplify.updateDMLEvent(before, after, tableName);
        updateOffset(tableName, RecordOperators.Update, updateRecordEvent);
        return updateRecordEvent;
    }

    public TapDeleteRecordEvent generateDeleteRecordEvent(TapTable table, Map<String, Object> before) {
        String tableName = table.getName();
        if (null == before) {
            before = new HashMap<>();
            for (TapField tapField : table.childItems()) {
                before.put(tapField.getName(), generateEventValue(tapField, RecordOperators.Update));
            }
        } else {
            before = new HashMap<>(before);
        }
        heartbeatSetter.accept(before);

        TapDeleteRecordEvent deleteRecordEvent = TapSimplify.deleteDMLEvent(before, tableName);
        updateOffset(tableName, RecordOperators.Delete, deleteRecordEvent);
        return deleteRecordEvent;
    }

    public DummyOffset getOffset() {
        return offset;
    }

    /**
     * generate event value by TapField
     *
     * @param field TapField
     * @param op    operate type
     * @return TapEvent
     */
    protected Object generateEventValue(TapField field, RecordOperators op) {
        if (null != field.getDefaultValue()) {
            return field.getDefaultValue();
        }
        String ftype = field.getDataType();
        if ("uuid".equalsIgnoreCase(ftype)) {
            return UUID.randomUUID().toString();
        } else if ("now".equalsIgnoreCase(ftype)) {
            return Timestamp.from(Instant.now());
        } else {
            if (ftype.startsWith("rnumber")) {
                Double rNumberValue = cacheRNumber.get(ftype);
                if (null == rNumberValue) {
                    if (ftype.endsWith(")")) {
                        rNumberValue = SECURE_RANDOM.nextDouble() * Long.parseLong(ftype.substring(8, ftype.length() - 1));
                    } else {
                        rNumberValue = SECURE_RANDOM.nextDouble() * 4;
                    }
                    cacheRNumber.put(ftype, rNumberValue);
                }
                return rNumberValue;
            } else if (ftype.startsWith("rstring")) {
                String rStringValue = cacheRString.get(ftype);
                if (null == rStringValue) {
                    if (ftype.endsWith(")")) {
                        rStringValue = randomString(Integer.parseInt(ftype.substring(8, ftype.length() - 1)));
                    } else {
                        rStringValue = randomString(DEFAULT_RANDOM_STRING_LENGTH);
                    }
                    cacheRString.put(ftype, rStringValue);
                }
                return rStringValue;
            } else if (ftype.startsWith("rlongstring")) {
                String rStringValue = cacheRLongString.get(ftype);
                if (null == rStringValue) {
                    if (ftype.endsWith(")")) {
                        rStringValue = randomString(Integer.parseInt(ftype.substring(12, ftype.length() - 1)));
                    } else {
                        rStringValue = randomString(LONG_RANDOM_STRING_LENGTH);
                    }
                    cacheRLongString.put(ftype, rStringValue);
                }
                return rStringValue;
            } else if (ftype.startsWith("rlongbinary")) {
                byte[] rBinaryValue = cacheRLongBinary.get(ftype);
                if (null == rBinaryValue) {
                    if (ftype.endsWith(")")) {
                        rBinaryValue = randomString(Integer.parseInt(ftype.substring(12, ftype.length() - 1))).getBytes(StandardCharsets.UTF_8);
                    } else {
                        rBinaryValue = randomString(LONG_RANDOM_STRING_LENGTH).getBytes(StandardCharsets.UTF_8);
                    }
                    cacheRLongBinary.put(ftype, rBinaryValue);
                }
                return rBinaryValue;
            } else if (ftype.startsWith("rdatetime")) {
                Timestamp rDatetimeValue = cacheRDate.get(ftype);
                if (null == rDatetimeValue) {
                    if (ftype.endsWith(")")) {
                        rDatetimeValue = randomTimestamp(Integer.parseInt(ftype.substring(10, ftype.length() - 1)));
                    } else {
                        rDatetimeValue = randomTimestamp(DEFAULT_RANDOM_DATE_FRACTION);
                    }
                    cacheRDate.put(ftype, rDatetimeValue);
                }
                return rDatetimeValue;
            } else if (ftype.startsWith("serial")) {
                if (RecordOperators.Insert == op) {
                    if (null == serial) {
                        try {
                            String[] splitStr = ftype.substring(7, ftype.length() - 1).split(",");
                            serial = new AtomicLong(Integer.parseInt(splitStr[0]));
                            serialStep = Math.max(Integer.parseInt(splitStr[1]), 1);
                        } catch (Throwable e) {
                            serial = new AtomicLong(1);
                            serialStep = 1;
                        }
                    }
                    return serial.getAndAdd(serialStep);
                }
                return (int) (SECURE_RANDOM.nextDouble() * serial.get()) - serial.get() % serialStep;
            }
        }
        return field.getDefaultValue();
    }

    private String defaultRString;
    private Double defaultRNumber;

    protected Object generateEventValue(FieldTypeCode fieldTypeCode, RecordOperators op) {
        TapField tapField = fieldTypeCode.tapField;
        if (null != tapField.getDefaultValue()) {
            return tapField.getDefaultValue();
        }
        int type = fieldTypeCode.type;
        Integer length = fieldTypeCode.length;
        Object value;
        switch (type) {
            case 10:
                if (null == length) {
                    if (null == defaultRString) {
                        defaultRString = randomString(DEFAULT_RANDOM_STRING_LENGTH);
                    }
                    value = defaultRString;
                } else {
                    value = cacheRString.get(type + "_" + length);
                    if (null == value) {
                        value = randomString(length);
                        cacheRString.put(type + "_" + length, (String) value);
                    }
                }
                break;
            case 20:
                if (null == length) {
                    if (null == defaultRNumber) {
                        defaultRNumber = Math.round(SECURE_RANDOM.nextDouble() * 10000 * 10000) / (double) 10000;
                    }
                    value = defaultRNumber;
                } else {
                    double powNum = Math.pow(10, length);
                    value = Math.round(SECURE_RANDOM.nextDouble() * powNum * powNum) / powNum;
                    cacheRNumber.put(type + "_" + length, (Double) value);
                }
                break;
            case 30:
                if (RecordOperators.Insert == op) {
                    if (null == serial) {
                        serial = null == fieldTypeCode.getSerial() ? new AtomicLong(1) : fieldTypeCode.getSerial();
                        serialStep = null == fieldTypeCode.getStep() ? 1 : fieldTypeCode.getStep();
                    }
                    value = serial.getAndAdd(serialStep);
                } else {
                    value =  (int) (SECURE_RANDOM.nextDouble() * serial.get()) - serial.get() % serialStep;
                }
                break;
            default:
                value = null;
                break;
        }
        return value;
    }

    /**
     * Get random string by length
     *
     * @param length String length
     * @return string
     */
    private String randomString(int length) {
        StringBuilder buf = new StringBuilder();
        for (int i = length; i > 0; i--) {
            buf.append(RANDOM_CHARS[SECURE_RANDOM.nextInt(RANDOM_CHARS_LENGTH)]);
        }
        return buf.toString();
    }

    private Timestamp randomTimestamp(int fraction) {
        DateTime dateTime = new DateTime(new Date());
        long offsetSeconds = (long) ((Math.random() - 0.5) * 60 * 60 * 24 * 365 * 40);
        dateTime.setSeconds(dateTime.getSeconds() + offsetSeconds);
        dateTime.setNano((int) (Math.pow(10, 9 - fraction) * (long) (Math.random() * Math.pow(10, fraction))));
        return dateTime.toTimestamp();
    }
}
