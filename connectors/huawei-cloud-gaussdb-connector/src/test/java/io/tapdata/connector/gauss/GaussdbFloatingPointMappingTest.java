package io.tapdata.connector.gauss;

import io.tapdata.entity.conversion.impl.TableFieldTypesGeneratorImpl;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapDouble;
import io.tapdata.entity.schema.type.TapFloat;
import io.tapdata.entity.utils.DataMap;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class GaussdbFloatingPointMappingTest {
    @Test
    void shouldMapRealToTapFloat() throws Exception {
        assertTrue(resolve("real").getTapType() instanceof TapFloat);
    }

    @Test
    void shouldMapDoublePrecisionToTapDouble() throws Exception {
        assertTrue(resolve("double precision").getTapType() instanceof TapDouble);
    }

    private TapField resolve(String dataType) throws Exception {
        TapField field = new TapField("amount", dataType);
        new TableFieldTypesGeneratorImpl().autoFill(field, dataTypes());
        return field;
    }

    private DefaultExpressionMatchingMap dataTypes() {
        Map<String, DataMap> mappings = new LinkedHashMap<>();
        mappings.put("real", DataMap.create()
                .kv("to", "TapFloat")
                .kv("precision", Arrays.asList(1, 6))
                .kv("scale", Arrays.asList(0, 6))
                .kv("fixed", false));
        mappings.put("double precision", DataMap.create()
                .kv("to", "TapDouble")
                .kv("precision", Arrays.asList(1, 17))
                .kv("scale", Arrays.asList(0, 17))
                .kv("fixed", false));
        return DefaultExpressionMatchingMap.map(mappings);
    }
}
