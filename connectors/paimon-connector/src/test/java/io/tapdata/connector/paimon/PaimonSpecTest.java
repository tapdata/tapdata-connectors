package io.tapdata.connector.paimon;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class PaimonSpecTest {

    @Test
    void fileFormatOptionsMustOnlyExposePackagedProviders() throws Exception {
        JsonObject spec = loadSpec();
        JsonArray formatOptions =
                spec.getAsJsonObject("configOptions")
                        .getAsJsonObject("node")
                        .getAsJsonObject("properties")
                        .getAsJsonObject("fileFormat")
                        .getAsJsonArray("enum");

        List<String> values = new ArrayList<>();
        for (JsonElement option : formatOptions) {
            values.add(option.getAsJsonObject().get("value").getAsString());
        }

        assertEquals(
                Arrays.asList("", "orc", "parquet", "avro", "csv", "json"),
                values);
    }

    private static JsonObject loadSpec() throws Exception {
        try (InputStream input =
                        PaimonSpecTest.class.getClassLoader().getResourceAsStream("spec.json")) {
            assertNotNull(input, "spec.json must be available on the test classpath");
            try (InputStreamReader reader =
                    new InputStreamReader(input, StandardCharsets.UTF_8)) {
                return JsonParser.parseReader(reader).getAsJsonObject();
            }
        }
    }
}
