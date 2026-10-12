package io.tapdata.tdd.cli.support;

import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DataTypesHandlerTest {
    @Test
    void floatingPointMappingsGenerateTestFields() {
        DataTypesHandler handler = DataTypesHandler.create();

        for (Map<String, Object> info : floatingPointMappings()) {
            TapTable table = new TapTable("test", "test");
            table.setNameFieldMap(new LinkedHashMap<>());
            handler.fillTestFields(table, expression(info), mapping(info));

            assertFalse(table.getNameFieldMap().isEmpty(),
                    "Expected a test field for " + info.get("to"));
        }
    }

    @Test
    void coefficientMappingGeneratesFloatAndDoubleTestFields() {
        DataTypesHandler handler = DataTypesHandler.create();
        Map<String, Object> info = coefficientMapping();
        TapTable table = new TapTable("test", "test");
        table.setNameFieldMap(new LinkedHashMap<>());

        handler.fillTestFields(table, "float($precision)", mapping(info));

        assertFalse(table.getNameFieldMap().keySet().stream()
                .noneMatch(name -> name.contains("float(23)")));
        assertFalse(table.getNameFieldMap().keySet().stream()
                .noneMatch(name -> name.contains("float(53)")));
    }

    @Test
    void floatingPointMappingsGenerateDataTypeNames() {
        DataTypesHandler handler = DataTypesHandler.create();

        for (Map<String, Object> info : floatingPointMappings()) {
            TapMapping tapMapping = mapping(info);
            assertNotNull(handler.dataNameClassHandlers().handle(tapMapping, expression(info)),
                    "Expected a data type name for " + info.get("to"));
        }
    }

    @Test
    void variableFloatingPointMappingsGenerateConcreteDataTypes() {
        DataTypesHandler handler = DataTypesHandler.create();

        for (Map<String, Object> info : variableFloatingPointMappings()) {
            TapTable table = new TapTable("test", "test");
            table.setNameFieldMap(new LinkedHashMap<>());
            handler.fillTestFields(table, expression(info), mapping(info));

            assertFalse(table.getNameFieldMap().isEmpty(),
                    "Expected a test field for " + info.get("to"));
            table.getNameFieldMap().values().forEach(field -> {
                assertFalse(field.getDataType().contains("$"),
                        "Unresolved type variable in " + field.getDataType());
                assertFalse(field.getDataType().contains("["),
                        "Unresolved type bracket in " + field.getDataType());
                assertFalse(field.getDataType().contains("]"),
                        "Unresolved type bracket in " + field.getDataType());
            });

            String dataTypeName = handler.dataNameClassHandlers().handle(mapping(info), expression(info));
            assertFalse(dataTypeName.contains("$"), "Unresolved type variable in " + dataTypeName);
            assertFalse(dataTypeName.contains("["), "Unresolved type bracket in " + dataTypeName);
            assertFalse(dataTypeName.contains("]"), "Unresolved type bracket in " + dataTypeName);
        }
    }

    private List<Map<String, Object>> floatingPointMappings() {
        return Arrays.asList(
                mappingInfo("TapFloat", "float"),
                mappingInfo("TapDouble", "double"),
                coefficientMapping()
        );
    }

    private List<Map<String, Object>> variableFloatingPointMappings() {
        Map<String, Object> floatInfo = mappingInfo("TapFloat", "float($precision,$scale)[unsigned]");
        floatInfo.put("precision", Arrays.asList(1, 24));
        floatInfo.put("scale", Arrays.asList(0, 8));
        floatInfo.put("unsigned", "unsigned");

        Map<String, Object> doubleInfo = mappingInfo("TapDouble", "double[($precision,$scale)][unsigned]");
        doubleInfo.put("precision", Arrays.asList(1, 53));
        doubleInfo.put("scale", Arrays.asList(0, 12));
        doubleInfo.put("unsigned", "unsigned");

        return Arrays.asList(floatInfo, doubleInfo);
    }

    private Map<String, Object> mappingInfo(String to, String expression) {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("to", to);
        info.put("expression", expression);
        return info;
    }

    private Map<String, Object> coefficientMapping() {
        Map<String, Object> info = mappingInfo("TapCoefficientFloat", "float($precision)");
        Map<String, Object> coefficient = new LinkedHashMap<>();
        coefficient.put("TapFloat", Arrays.asList(1, 23));
        coefficient.put("TapDouble", Arrays.asList(24, 53));
        info.put("coefficient", coefficient);
        info.put("precision", Arrays.asList(1, 53));
        return info;
    }

    private String expression(Map<String, Object> info) {
        return (String) info.get("expression");
    }

    private TapMapping mapping(Map<String, Object> info) {
        Map<String, Object> mappingInfo = new LinkedHashMap<>(info);
        mappingInfo.remove("expression");
        TapMapping mapping = TapMapping.build(mappingInfo);
        assertNotNull(mapping, "Unable to build " + info.get("to") + " mapping");
        return mapping;
    }
}
