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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonSpecTest {

    @Test
    void microBatchDefaultsAndPlaceholdersMustStayAligned() throws Exception {
        JsonObject spec = loadSpec();
        JsonObject properties =
                spec.getAsJsonObject("configOptions")
                        .getAsJsonObject("node")
                        .getAsJsonObject("properties");

        assertEquals(100000, properties.getAsJsonObject("batchAccumulationSize").get("default").getAsInt());
        assertEquals(30000, properties.getAsJsonObject("commitIntervalMs").get("default").getAsInt());
        assertTrue(properties.getAsJsonObject("enableAsyncCommit").get("default").getAsBoolean());
        JsonObject concurrency = properties.getAsJsonObject("asyncCommitConcurrency");
        assertEquals(4, concurrency.get("default").getAsInt());
        assertEquals(
                1,
                concurrency.getAsJsonObject("x-component-props").get("min").getAsInt());
        assertEquals(
                16,
                concurrency.getAsJsonObject("x-component-props").get("max").getAsInt());
        assertFalse(concurrency.has("x-perTable"));
        assertTrue(
                concurrency.getAsJsonArray("x-reactions")
                        .get(0)
                        .getAsJsonObject()
                        .toString()
                        .contains(".enableAsyncCommit"));

        JsonObject messages = spec.getAsJsonObject("messages");
        for (String locale : Arrays.asList("en_US", "zh_CN", "zh_TW")) {
            String placeholder =
                    messages.getAsJsonObject(locale)
                            .get("batchAccumulationSize_placeholder")
                            .getAsString();
            assertTrue(placeholder.contains("100000"), locale + " placeholder must show 100000");
            assertFalse(placeholder.contains("10000)"), locale + " placeholder must not show 10000");
            assertFalse(placeholder.contains("10000）"), locale + " placeholder must not show 10000");
            assertTrue(
                    messages.getAsJsonObject(locale)
                            .get("asyncCommitConcurrency_placeholder")
                            .getAsString()
                            .contains("4"),
                    locale + " placeholder must show concurrency default 4");
        }
    }

    @Test
    void flushOffsetCallbackCapabilityMustBeAdvertisedExactlyOnce() throws Exception {
        JsonArray capabilities =
                loadSpec()
                        .getAsJsonObject("configOptions")
                        .getAsJsonArray("capabilities");

        int count = 0;
        for (JsonElement capability : capabilities) {
            if ("flush_offset_callback".equals(
                    capability.getAsJsonObject().get("id").getAsString())) {
                count++;
            }
        }

        assertEquals(1, count);
    }

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

    @Test
    void flinkOnlyWriteThreadsMustNotBeExposed() throws Exception {
        JsonObject spec = loadSpec();
        JsonObject properties =
                spec.getAsJsonObject("configOptions")
                        .getAsJsonObject("node")
                        .getAsJsonObject("properties");

        assertFalse(properties.has("writeThreads"));

        JsonObject messages = spec.getAsJsonObject("messages");
        for (String locale : Arrays.asList("en_US", "zh_CN", "zh_TW")) {
            assertFalse(messages.getAsJsonObject(locale).has("writeThreads"));
            assertFalse(messages.getAsJsonObject(locale).has("writeThreads_placeholder"));
        }
    }

    @Test
    void timeTypeMustAdvertiseOnlyPaimonMillisecondPrecision() throws Exception {
        JsonObject timeType =
                loadSpec()
                        .getAsJsonObject("dataTypes")
                        .getAsJsonObject("TIME[($fraction)]");

        JsonArray fraction = timeType.getAsJsonArray("fraction");
        assertEquals(0, fraction.get(0).getAsInt());
        assertEquals(3, fraction.get(1).getAsInt());
        assertEquals(3, timeType.get("defaultFraction").getAsInt());
    }

    @Test
    void decimalTypeMustAdvertiseConnectorCompatibilityDefaults() throws Exception {
        JsonObject decimalType =
                loadSpec()
                        .getAsJsonObject("dataTypes")
                        .getAsJsonObject("DECIMAL[($precision,$scale)]");

        JsonArray precision = decimalType.getAsJsonArray("precision");
        assertEquals(1, precision.get(0).getAsInt());
        assertEquals(38, precision.get(1).getAsInt());

        JsonArray scale = decimalType.getAsJsonArray("scale");
        assertEquals(0, scale.get(0).getAsInt());
        assertEquals(38, scale.get(1).getAsInt());

        assertEquals(38, decimalType.get("defaultPrecision").getAsInt());
        assertEquals(10, decimalType.get("defaultScale").getAsInt());
        assertTrue(decimalType.get("fixed").getAsBoolean());
    }

    @Test
    void localizedDocsMustDescribePhysicalStringComplexTypesAndTimeContract() throws Exception {
        for (String resource :
                Arrays.asList(
                        "docs/paimon_en_US.md",
                        "docs/paimon_zh_CN.md",
                        "docs/paimon_zh_TW.md")) {
            String documentation = loadResourceText(resource);

            assertTrue(documentation.contains("| STRING | TapArray"), resource);
            assertTrue(documentation.contains("| STRING | TapMap"), resource);
            assertFalse(documentation.matches("(?s).*\\| (ARRAY|MAP|ROW) \\| Tap.*"), resource);
            assertTrue(documentation.contains("TIME(0-3)"), resource);
            assertTrue(documentation.contains("DECIMAL(38,10)"), resource);
        }
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

    private static String loadResourceText(String resource) throws Exception {
        try (InputStream input =
                PaimonSpecTest.class.getClassLoader().getResourceAsStream(resource)) {
            assertNotNull(input, resource + " must be available on the test classpath");
            try (InputStreamReader reader =
                    new InputStreamReader(input, StandardCharsets.UTF_8)) {
                StringBuilder text = new StringBuilder();
                char[] buffer = new char[1024];
                int read;
                while ((read = reader.read(buffer)) >= 0) {
                    text.append(buffer, 0, read);
                }
                return text.toString();
            }
        }
    }
}
