package io.tapdata.connector.starrocks;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.mapping.TypeExprResult;
import io.tapdata.entity.mapping.type.TapMapping;
import io.tapdata.entity.mapping.type.TapNumberMapping;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.utils.DataMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StarrocksDataTypeMappingTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void shouldPreservePaimonDecimalPrecisionAndScale() throws IOException {
        TypeExprResult<DataMap> mappingResult = dataTypesMap().get("decimal(38,10)");
        assertNotNull(mappingResult);

        TapNumberMapping mapping = tapNumberMapping(mappingResult);
        TapResult<String> result = mapping.fromTapType(mappingResult.getExpression(),
                new TapNumber().precision(38).scale(10));

        assertEquals("decimal(38,10)", result.getData());
        assertTrue(result.getResultItems() == null || result.getResultItems().isEmpty());
    }

    @Test
    void shouldKeepExistingDecimalMappingBehavior() throws IOException {
        TypeExprResult<DataMap> mappingResult = dataTypesMap().get("decimal(18,2)");
        assertNotNull(mappingResult);

        TapNumberMapping mapping = tapNumberMapping(mappingResult);
        TapResult<String> result = mapping.fromTapType(mappingResult.getExpression(),
                new TapNumber().precision(18).scale(2));

        assertEquals("decimal(18,2)", result.getData());
        assertTrue(result.getResultItems() == null || result.getResultItems().isEmpty());
    }

    @Test
    void shouldPreserveDecimalFixedMetadata() throws IOException {
        TypeExprResult<DataMap> mappingResult = dataTypesMap().get("decimal(10,4)");
        assertNotNull(mappingResult);

        Map<String, String> params = new LinkedHashMap<>();
        params.put("precision", "10");
        params.put("scale", "4");
        TapNumber number = (TapNumber) tapNumberMapping(mappingResult)
                .toTapType(mappingResult.getExpression(), params);

        assertTrue(number.getFixed());
        assertEquals(10, number.getPrecision());
        assertEquals(4, number.getScale());
    }

    private DefaultExpressionMatchingMap dataTypesMap() throws IOException {
        try (InputStream inputStream = getClass().getResourceAsStream("/spec_starrocks.json")) {
            assertNotNull(inputStream);
            JsonNode dataTypes = objectMapper.readTree(inputStream).get("dataTypes");
            Map<String, DataMap> mappings = new LinkedHashMap<>();
            dataTypes.fields().forEachRemaining(entry -> {
                Map<String, Object> values = objectMapper.convertValue(entry.getValue(),
                        new TypeReference<Map<String, Object>>() {});
                mappings.put(entry.getKey(), DataMap.create(values));
            });
            return DefaultExpressionMatchingMap.map(mappings);
        }
    }

    private TapNumberMapping tapNumberMapping(TypeExprResult<DataMap> mappingResult) {
        return (TapNumberMapping) mappingResult.getValue().get(TapMapping.FIELD_TYPE_MAPPING);
    }
}
