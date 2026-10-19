package io.tapdata.connector.rocketmq;

import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RocketmqFloatingPointTest {

    private final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);

    @Test
    void should_preserve_single_and_double_precision_values_in_message_body() {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("c_float", 1.25F);
        row.put("c_double", 2.718281828459045D);

        Map<String, Object> decoded = jsonParser.fromJsonBytes(jsonParser.toJsonBytes(row), Map.class);

        assertEquals(1.25D, ((Number) decoded.get("c_float")).doubleValue(), 0.0001D);
        assertEquals(2.718281828459045D, ((Number) decoded.get("c_double")).doubleValue(), 0.000000000000001D);
    }
}
