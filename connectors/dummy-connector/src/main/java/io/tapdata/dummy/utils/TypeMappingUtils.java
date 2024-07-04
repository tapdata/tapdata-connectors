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

	static{
		TYPE_MAPPING = new HashMap<>();
		TYPE_MAPPING.put("rstring", 10);
		TYPE_MAPPING.put("rnumber", 20);
		TYPE_MAPPING.put("serial", 30);
	}

	public static int getType(String type){
		return TYPE_MAPPING.get(type);
	}

	public static List<FieldTypeCode> toFieldCodes(Map<String, TapField> tapFieldMap) {
		List<FieldTypeCode> fieldTypeCodes = new ArrayList<>();
		for (TapField tapField : tapFieldMap.values()) {
			String name = tapField.getName();
			String dataType = tapField.getDataType();
			FieldTypeCode fieldTypeCode = new FieldTypeCode(tapField, getType(tapField.getDataType()));
			if (name.startsWith("rstring(")) {
				fieldTypeCode.setLength(Integer.parseInt(dataType.substring(8, dataType.length() - 1)));
			} else if (name.startsWith("rnumber(")) {
				fieldTypeCode.setLength(Integer.parseInt(dataType.substring(8, dataType.length() - 1)));
			} else if (name.startsWith("serial(")) {
				String[] splitStr = dataType.substring(7, dataType.length() - 1).split(",");
				fieldTypeCode.setSerial(new AtomicLong(Integer.parseInt(splitStr[0])));
				fieldTypeCode.setStep(Math.max(Integer.parseInt(splitStr[1]), 1));
			}
			fieldTypeCodes.add(fieldTypeCode);
		}
		return fieldTypeCodes;
	}
}
