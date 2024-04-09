package io.tapdata.connector.kafka.util;

import io.tapdata.constant.JsonType;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;

import java.util.LinkedHashMap;

public class KafkaTapModelDeclare {
    public void addField(TapTable tapTable, String fieldName, String tapType) throws Throwable {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null == nameFieldMap || nameFieldMap.containsKey(fieldName)) {
            return;
        }
        TapField field = buildField(fieldName, fieldName, tapType);
        tapTable.add(field);
    }

    public void updateField(TapTable tapTable, String fieldName, String tapType) throws Throwable {
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (null == nameFieldMap ||!tapTable.getNameFieldMap().containsKey(fieldName)) {
            return;
        }
        TapField field = buildField(fieldName, fieldName, tapType);
        tapTable.add(field);
    }

    private TapField buildField(String parentFieldName, String fieldName, String jsonType) {
        JsonType jsonType1 = JsonType.of(jsonType);
        TapField field = new TapField();
        field.setName(fieldName);
        field.setNullable(true);
        field.setDataType(jsonType1.name());
        return field;
    }

    public void removeField(TapTable tapTable, String fieldName) {
        tapTable.getNameFieldMap().remove(fieldName);
    }

}
