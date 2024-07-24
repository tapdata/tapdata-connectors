package io.tapdata.dummy.utils;

import io.tapdata.entity.schema.TapField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author samuel
 * @Description
 * @create 2024-07-04 17:10
 **/
public class TypeMappingUtils {
	private final static Map<String, Integer> TYPE_MAPPING;

	static {
		TYPE_MAPPING = new HashMap<>();
		TYPE_MAPPING.put("rstring", 10);
		TYPE_MAPPING.put("rnumber", 20);
		TYPE_MAPPING.put("serial", 30);
		TYPE_MAPPING.put("rlongstring", 40);
		TYPE_MAPPING.put("rlongbinary", 50);
		TYPE_MAPPING.put("rdatetime", 60);
		TYPE_MAPPING.put("uuid", 70);
		TYPE_MAPPING.put("now", 80);
		TYPE_MAPPING.put("static", 90);
	}

	private TypeMappingUtils() {
	}

	public static int getType(String type) {
		Integer typeCode;
		if (type.contains("(")) {
			typeCode = TYPE_MAPPING.get(type.substring(0, type.indexOf("(")));
		} else {
			typeCode = TYPE_MAPPING.get(type);
		}
		if (null == typeCode) {
			typeCode = TYPE_MAPPING.get("static");
		}
		return typeCode;
	}

	public static List<FieldTypeCode> toFieldCodes(Map<String, TapField> tapFieldMap) {
		List<FieldTypeCode> fieldTypeCodes = new ArrayList<>();
		for (TapField tapField : tapFieldMap.values()) {
			String name = tapField.getName();
			String dataType = tapField.getDataType();
			FieldTypeCode fieldTypeCode = new FieldTypeCode(tapField, getType(tapField.getDataType()));
			if (dataType.startsWith("rstring(") && dataType.endsWith(")")) {
				fieldTypeCode.setLength(Integer.parseInt(dataType.substring(8, dataType.length() - 1)));
			} else if (dataType.startsWith("rnumber(") && dataType.endsWith(")")) {
				fieldTypeCode.setLength(Integer.parseInt(dataType.substring(8, dataType.length() - 1)));
			} else if (dataType.startsWith("serial(") && dataType.endsWith(")")) {
				String[] splitStr = dataType.substring(7, dataType.length() - 1).split(",");
				fieldTypeCode.setSerial(new AtomicLong(Integer.parseInt(splitStr[0])));
				fieldTypeCode.setStep(Math.max(Integer.parseInt(splitStr[1]), 1));
			} else if (dataType.startsWith("rlongstring(") && dataType.endsWith(")")) {
				fieldTypeCode.setLength(Integer.parseInt(dataType.substring(12, dataType.length() - 1)));
			} else if (dataType.startsWith("rlongbinary(") && dataType.endsWith(")")) {
				fieldTypeCode.setLength(Integer.parseInt(dataType.substring(12, dataType.length() - 1)));
			} else if (dataType.startsWith("rdatetime(") && dataType.endsWith(")")) {
				fieldTypeCode.setLength(Integer.parseInt(dataType.substring(10, dataType.length() - 1)));
			}
			fieldTypeCodes.add(fieldTypeCode);
		}
		return fieldTypeCodes;
	}
}
