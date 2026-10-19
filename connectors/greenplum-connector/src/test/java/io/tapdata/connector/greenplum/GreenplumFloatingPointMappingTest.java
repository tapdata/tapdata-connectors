package io.tapdata.connector.greenplum;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.conversion.impl.TableFieldTypesGeneratorImpl;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.type.TapDouble;
import io.tapdata.entity.schema.type.TapFloat;
import io.tapdata.entity.utils.DataMap;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class GreenplumFloatingPointMappingTest {
    private final ObjectMapper mapper = new ObjectMapper();

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

    private DefaultExpressionMatchingMap dataTypes() throws Exception {
        try (InputStream input = getClass().getResourceAsStream("/spec_greenplum.json")) {
            JsonNode dataTypes = mapper.readTree(input).get("dataTypes");
            Map<String, DataMap> mappings = new LinkedHashMap<>();
            dataTypes.fields().forEachRemaining(entry -> mappings.put(entry.getKey(),
                    DataMap.create(mapper.convertValue(entry.getValue(), new TypeReference<Map<String, Object>>() {}))));
            return DefaultExpressionMatchingMap.map(mappings);
        }
    }
}
