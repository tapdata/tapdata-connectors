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
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CustomParseUtil {
	public static final String OP_DML_TYPE = "dml";
	public static final String OP_DDL_TYPE = "ddl";
	public static final Map<Integer, Function<Map<String, Object>, TapBaseEvent>> HANDLER_MAP_DDL = new HashMap<>();
	public static final Map<String, Function<Map<String, Object>, TapBaseEvent>> HANDLER_MAP_DML = new HashMap<>();

	static {
		HANDLER_MAP_DDL.put(209, (record) -> {
			TapNewFieldEvent tapNewFieldEvent = null;
			try{
				tapNewFieldEvent=JSON.parseObject(JSON.toJSONString(record),TapNewFieldEvent.class);
			}catch (Exception e){
				throw new RuntimeException("cast custom tapNewFieldEvent failed");
			}
			tapNewFieldEvent.setTime(System.currentTimeMillis());
			tapNewFieldEvent.setReferenceTime(System.currentTimeMillis());
			return tapNewFieldEvent;
//			List<Map<String, Object>> newFields = (List<Map<String, Object>>) record.get("newFields");
//			List<TapField> newFieldList = newFields.stream().map(newFieldMap -> {
//				TapField tapField = new TapField();
//				String fieldName = MapUtils.getString(newFieldMap, "name");
//				if (StringUtils.isEmpty(fieldName))
//					throw new RuntimeException("custom newFieldEvent FieldName can not be null");
//				tapField.setName(fieldName);
//				String dataType = MapUtils.getString(newFieldMap, "dataType");
//				if (StringUtils.isEmpty(dataType))
//					throw new RuntimeException("custom newFieldEvent dataType can not be null");
//				tapField.setDataType(dataType);
//				return tapField;
//			}).collect(Collectors.toList());
//			tapNewFieldEvent.setNewFields(newFieldList);
//			tapNewFieldEvent.setTableId(MapUtils.getString(record, "tableId"));
		});
		HANDLER_MAP_DDL.put(202, (record) -> {
			TapAlterFieldNameEvent tapAlterFieldNameEvent = null;
			try {
				tapAlterFieldNameEvent = JSON.parseObject(JSON.toJSONString(record), TapAlterFieldNameEvent.class);
			} catch (Exception e) {
				throw new RuntimeException("cast custom tapAlterFieldNameEvent failed");
			}
			tapAlterFieldNameEvent.setReferenceTime(System.currentTimeMillis());
			tapAlterFieldNameEvent.setTime(System.currentTimeMillis());
			return tapAlterFieldNameEvent;
//			tapAlterFieldNameEvent.setTableId(MapUtils.getString(record, "tableId"));
//			Map nameChange = MapUtils.getMap(record, "nameChange");
//			String before = MapUtils.getString(nameChange, "before");
//			String after = MapUtils.getString(nameChange, "after");
//			if (StringUtils.isEmpty(before)) throw new RuntimeException("custom nameChangeEvent before can not be null");
//			if (StringUtils.isEmpty(after)) throw new RuntimeException("custom nameChangeEvent after can not be null");
//			ValueChange valueChange = new ValueChange();
//			valueChange.setAfter(after);
//			valueChange.setBefore(before);
//			tapAlterFieldNameEvent.setNameChange(valueChange);
		});
		HANDLER_MAP_DDL.put(207, (record) -> {
			TapDropFieldEvent tapDropFieldEvent = null;
			try {
				tapDropFieldEvent = JSON.parseObject(JSON.toJSONString(record), TapDropFieldEvent.class);
			} catch (Exception e) {
				throw new RuntimeException("cast custom DropFieldEvent failed");
			}
			tapDropFieldEvent.setReferenceTime(System.currentTimeMillis());
			tapDropFieldEvent.setTime(System.currentTimeMillis());
			return tapDropFieldEvent;
//			TapDropFieldEvent tapDropFieldEvent = new TapDropFieldEvent();
//			String fieldName = MapUtils.getString(record, "fieldName");
//			String tableId = MapUtils.getString(record, "tableId");
//			if (StringUtils.isEmpty(fieldName))
//				throw new RuntimeException("custom DropFieldEvent FieldName can not be null");
//			tapDropFieldEvent.setTableId(tableId);
//			tapDropFieldEvent.setFieldName(fieldName);
		});
		HANDLER_MAP_DDL.put(201, (record) -> {
			TapAlterFieldAttributesEvent tapAlterFieldAttributesEvent = null;
			try {
				tapAlterFieldAttributesEvent = JSON.parseObject(JSON.toJSONString(record), TapAlterFieldAttributesEvent.class);
			} catch (Exception e) {
				throw new RuntimeException("cast custom AlterFieldAttributeEvent failed");
			}
			return tapAlterFieldAttributesEvent;
		});
		HANDLER_MAP_DML.put("insert", (record) -> {
			Map<String, Object> after = (Map<String, Object>) MapUtils.getMap(record, "after");
			if (MapUtils.isEmpty(after)) throw new RuntimeException("custom InsertEvent after can not be null");
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent().init().after(after).referenceTime(System.currentTimeMillis());
			return tapInsertRecordEvent;
		});
		HANDLER_MAP_DML.put("update", (record) -> {
			Map<String, Object> before = (Map<String, Object>) MapUtils.getMap(record, "before");
			Map<String, Object> after = (Map<String, Object>) MapUtils.getMap(record, "after");
			if (MapUtils.isEmpty(after)) throw new RuntimeException("custom UpdateEvent after can not be null");
			TapUpdateRecordEvent tapUpdateRecordEvent = new TapUpdateRecordEvent().init().before(before).after(after).referenceTime(System.currentTimeMillis());
			return tapUpdateRecordEvent;
		});
		HANDLER_MAP_DML.put("delete", (record) -> {
			Map<String, Object> before = (Map<String, Object>) MapUtils.getMap(record, "before");
			if (MapUtils.isEmpty(before)) throw new RuntimeException("custom UpdateEvent after can not be null");
			TapDeleteRecordEvent tapDeleteRecordEvent = new TapDeleteRecordEvent().init().before(before).referenceTime(System.currentTimeMillis());
			return tapDeleteRecordEvent;
		});
	}

	public static TapBaseEvent applyCustomParse(Map<String, Object> record) {
		String op = MapUtils.getString(record, "op", "invalid");
		if (OP_DDL_TYPE.equals(op)) {
			Integer type = MapUtils.getInteger(record, "type");
			if (HANDLER_MAP_DDL.containsKey(type)) {
				Function<Map<String, Object>, TapBaseEvent> mapTapBaseEventFunction = HANDLER_MAP_DDL.get(type);
				return mapTapBaseEventFunction.apply(record);
			} else {
				throw new RuntimeException("ddl type" + type + " is not support");
			}
		} else {
			if (HANDLER_MAP_DML.containsKey(op)) {
				return HANDLER_MAP_DML.get(op).apply(record);
			} else {
				throw new RuntimeException("op type" + op + "is not support");
			}
		}
	}
}
