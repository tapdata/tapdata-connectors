package io.tapdata.connector.doris.streamload;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapDateTime;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-12-26 12:38
 **/
public class JsonSerializer implements MessageSerializer {

	public static final String LINE_END = ",";
	private ObjectMapper objectMapper;

	public JsonSerializer() {
		objectMapper = new ObjectMapper();
	}

	@Override
	public byte[] serialize(TapTable table, TapRecordEvent recordEvent, boolean isAgg) throws Throwable {
		String jsonString;
		if (recordEvent instanceof TapInsertRecordEvent) {
			final TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
			final Map<String, Object> after = insertRecordEvent.getAfter();
			jsonString = toJsonString(table, after, false, isAgg);
		} else if (recordEvent instanceof TapUpdateRecordEvent) {
			final TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
			Map<String, Object> after = updateRecordEvent.getAfter();
//			Map<String, Object> before = updateRecordEvent.getBefore();
//			before = before == null ? after : before;
//			jsonString = toJsonString(table, before, false);
//			jsonString += LINE_END;
			jsonString = toJsonString(table, after, false, isAgg);
		} else {
			final TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
			final Map<String, Object> before = deleteRecordEvent.getBefore();
			jsonString = toJsonString(table, before, true, isAgg);
		}
		return jsonString.getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public byte[] lineEnd() {
		return LINE_END.getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public byte[] batchStart() {
		return "[".getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public byte[] batchEnd() {
		return "]".getBytes(StandardCharsets.UTF_8);
	}

	private String toJsonString(TapTable tapTable, Map<String, Object> record, boolean delete, boolean isAgg) throws JsonProcessingException {
		if (null == tapTable) throw new IllegalArgumentException("TapTable cannot be null");
		LinkedHashMap<String, Object> linkedRecord = new LinkedHashMap<>();
		for (String field : tapTable.getNameFieldMap().keySet()) {
			if (record.containsKey(field) || isAgg) {
				Object value = record.get(field);
				if (null == value) {
					linkedRecord.put(field, null);
				} else {
					// Check if field is TapDateTime type and value is ISO 8601 format string
					TapField tapField = tapTable.getNameFieldMap().get(field);
					if (tapField != null && tapField.getTapType() instanceof TapDateTime && value instanceof String) {
						String dateTimeStr = (String) value;
						if (dateTimeStr.contains("T") && dateTimeStr.endsWith("Z")) {
							dateTimeStr = dateTimeStr.replace("T", " ").substring(0, 19);
							value = dateTimeStr;
						}
					}
					linkedRecord.put(field, value.toString());
				}
			}
		}
		linkedRecord.put(Constants.DORIS_DELETE_SIGN, delete ? 1 : 0);
		return objectMapper.writeValueAsString(linkedRecord);
	}
}