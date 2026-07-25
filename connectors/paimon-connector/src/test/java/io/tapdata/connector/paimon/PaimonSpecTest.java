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
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void bucketOptionsMustExposeNativeModesAndPositiveFixedCount() throws Exception {
        JsonObject spec = loadSpec();
        JsonObject properties =
                spec.getAsJsonObject("configOptions")
                        .getAsJsonObject("node")
                        .getAsJsonObject("properties");
        JsonObject bucketMode = properties.getAsJsonObject("bucketMode");
        JsonArray modeOptions = bucketMode.getAsJsonArray("enum");
        List<String> modes = new ArrayList<>();
        for (JsonElement option : modeOptions) {
            modes.add(option.getAsJsonObject().get("value").getAsString());
        }

        assertEquals(Arrays.asList("dynamic", "postpone", "fixed"), modes);
        assertEquals(
                1,
                properties.getAsJsonObject("bucketCount")
                        .getAsJsonObject("x-component-props")
                        .get("min")
                        .getAsInt());
        assertTrue(
                bucketMode.getAsJsonArray("x-reactions")
                        .get(0)
                        .getAsJsonObject()
                        .toString()
                        .contains("$self.value==='fixed'"));

        JsonObject messages = spec.getAsJsonObject("messages");
        assertTrue(messages.getAsJsonObject("en_US").has("bucketMode_postpone"));
        assertTrue(messages.getAsJsonObject("zh_CN").has("bucketMode_postpone"));
        assertTrue(messages.getAsJsonObject("zh_TW").has("bucketMode_postpone"));
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
