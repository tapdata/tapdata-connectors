package io.tapdata.connector.kafka.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.constant.JsonType;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import org.apache.commons.collections4.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CustomParseUtil {
    private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
    public static final String OP_DML_TYPE = "dml";
    public static final String OP_DDL_TYPE = "ddl";
    public static final Map<Integer, Function<Map<String, Object>, TapBaseEvent>> HANDLER_MAP = new HashMap<>();
    public static final Map<String, Function<Map<String, Object>, TapBaseEvent>> HANDLER_MAP_DML = new HashMap<>();

    static {
        HANDLER_MAP.put(209, (record) -> {
            TapNewFieldEvent tapNewFieldEvent = new TapNewFieldEvent();
            List<Map<String, Object>> newFields = (List<Map<String, Object>>) record.get("newFields");
            List<TapField> newFieldList = newFields.stream().map(newFieldMap -> {
                TapField tapField = new TapField();
                tapField.setName(MapUtils.getString(newFieldMap, "name"));
                String dataType = MapUtils.getString(newFieldMap, "dataType");
                tapField.setDataType(dataType);
                return tapField;
            }).collect(Collectors.toList());
            tapNewFieldEvent.setTime(System.currentTimeMillis());
            tapNewFieldEvent.setReferenceTime(System.currentTimeMillis());
            tapNewFieldEvent.setNewFields(newFieldList);
            tapNewFieldEvent.setTableId(MapUtils.getString(record, "tableId"));
            return tapNewFieldEvent;
        });
        HANDLER_MAP.put(202, (record) -> {
            TapAlterFieldNameEvent tapAlterFieldNameEvent = new TapAlterFieldNameEvent();
            tapAlterFieldNameEvent.setReferenceTime(System.currentTimeMillis());
            tapAlterFieldNameEvent.setTime(System.currentTimeMillis());
            tapAlterFieldNameEvent.setTableId(MapUtils.getString(record, "tableId"));
            Map nameChange = MapUtils.getMap(record, "nameChange");
            String after = MapUtils.getString(nameChange, "after");
            String before = MapUtils.getString(nameChange, "before");
            ValueChange valueChange = new ValueChange();
            valueChange.setAfter(after);
            valueChange.setBefore(before);
            tapAlterFieldNameEvent.setNameChange(valueChange);
            return tapAlterFieldNameEvent;
        });
        HANDLER_MAP.put(207, (record) -> {
            TapDropFieldEvent tapDropFieldEvent = new TapDropFieldEvent();
            tapDropFieldEvent.setReferenceTime(System.currentTimeMillis());
            tapDropFieldEvent.setTime(System.currentTimeMillis());
            tapDropFieldEvent.setTableId(MapUtils.getString(record, "tableId"));
            String fieldName = MapUtils.getString(record, "fieldName");
            tapDropFieldEvent.setFieldName(fieldName);
            return tapDropFieldEvent;
        });
        HANDLER_MAP.put(201, (record) -> {
            TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = JSON.parseObject(JSON.toJSONString(record), TapAlterFieldAttributesEvent.class);

            return tapAlterFieldAttributesEvent;
        });
        HANDLER_MAP_DML.put("insert", (record) -> {
            Map<String, Object> after = (Map<String, Object>) MapUtils.getMap(record, "after");
            TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent().init().after(after).referenceTime(System.currentTimeMillis());
            return tapInsertRecordEvent;
        });
        HANDLER_MAP_DML.put("update", (record) -> {
            Map<String, Object> after = (Map<String, Object>) MapUtils.getMap(record, "after");
            Map<String, Object> before = (Map<String, Object>) MapUtils.getMap(record, "before");
            TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent().init().before(before).after(after).referenceTime(System.currentTimeMillis());
            return tapUpdateRecordEvent;
        });
        HANDLER_MAP_DML.put("delete", (record) -> {
            Map<String, Object> before = (Map<String, Object>) MapUtils.getMap(record, "before");
            TapDeleteRecordEvent tapDeleteRecordEvent = new TapDeleteRecordEvent().init().before(before).referenceTime(System.currentTimeMillis());
            return tapDeleteRecordEvent;
        });

    }

    public static TapBaseEvent applyCustomParse(Map<String, Object> record) {
        String op = MapUtils.getString(record, "op", "invalid");
        if (OP_DDL_TYPE.equals(op)) {
            Integer type = MapUtils.getInteger(record, "type");
            Function<Map<String, Object>, TapBaseEvent> mapTapBaseEventFunction = HANDLER_MAP.get(type);
            return mapTapBaseEventFunction.apply(record);
        } else {
            if (HANDLER_MAP_DML.containsKey(op)) {
                return HANDLER_MAP_DML.get(op).apply(record);
            } else {
                throw new RuntimeException("op type not support");
            }
        }
    }
}
