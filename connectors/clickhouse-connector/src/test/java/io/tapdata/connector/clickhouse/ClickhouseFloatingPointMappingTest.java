package io.tapdata.connector.clickhouse;

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

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickhouseFloatingPointMappingTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void shouldMapFloat32ToTapFloat() throws Exception {
        assertTrue(resolve("Float32").getTapType() instanceof TapFloat);
    }

    @Test
    void shouldMapParameterizedFloat32ToTapFloat() throws Exception {
        assertTrue(resolve("Float32(6,2)").getTapType() instanceof TapFloat);
    }

    @Test
    void shouldMapFloat64ToTapDouble() throws Exception {
        assertTrue(resolve("Float64").getTapType() instanceof TapDouble);
    }

    private TapField resolve(String dataType) throws IOException {
        TapField field = new TapField("amount", dataType);
        new TableFieldTypesGeneratorImpl().autoFill(field, dataTypes());
        return field;
    }

    private DefaultExpressionMatchingMap dataTypes() throws IOException {
        try (InputStream input = getClass().getResourceAsStream("/spec_clickhouse.json")) {
            JsonNode dataTypes = mapper.readTree(input).get("dataTypes");
            Map<String, DataMap> mappings = new LinkedHashMap<>();
            dataTypes.fields().forEachRemaining(entry -> mappings.put(entry.getKey(),
                    DataMap.create(mapper.convertValue(entry.getValue(), new TypeReference<Map<String, Object>>() {}))));
            return DefaultExpressionMatchingMap.map(mappings);
        }
    }
}
