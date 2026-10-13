package io.tapdata.connector.mysql;

import io.tapdata.entity.conversion.impl.TableFieldTypesGeneratorImpl;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapFloat;
import io.tapdata.entity.utils.DataMap;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MysqlDataTypeMappingTest {
    @Test
    void floatWithPrecisionAndScaleUsesTwoParameterMapping() {
        DefaultExpressionMatchingMap dataTypesMap = DefaultExpressionMatchingMap.map(mysqlFloatMappings());
        TapField field = new TapField("amount", "float(6,2)");

        assertDoesNotThrow(() -> new TableFieldTypesGeneratorImpl().autoFill(field, dataTypesMap));
        assertTrue(field.getTapType() instanceof TapFloat);
    }

    private Map<String, DataMap> mysqlFloatMappings() {
        Map<String, DataMap> mappings = new LinkedHashMap<>();

        mappings.put("float($precision,$scale)[unsigned]", DataMap.create()
                .kv("to", "TapFloat")
                .kv("precision", Arrays.asList(1, 30))
                .kv("scale", Arrays.asList(0, 30))
                .kv("unsigned", "unsigned"));

        Map<String, Object> coefficient = new LinkedHashMap<>();
        coefficient.put("TapFloat", Arrays.asList(1, 24));
        coefficient.put("TapDouble", Arrays.asList(25, 53));
        mappings.put("float($precision)[unsigned]", DataMap.create()
                .kv("to", "TapCoefficientFloat")
                .kv("coefficient", coefficient)
                .kv("precision", Arrays.asList(1, 53))
                .kv("unsigned", "unsigned"));

        return mappings;
    }
}
