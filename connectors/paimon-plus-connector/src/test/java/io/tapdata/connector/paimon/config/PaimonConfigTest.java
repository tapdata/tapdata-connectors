package io.tapdata.connector.paimon.config;

import io.tapdata.entity.utils.DataMap;
import org.apache.paimon.table.BucketMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for PaimonConfig
 */
class PaimonConfigTest {

    @Test
    void stopBudgetsMustRejectFractionOverflowNonPositiveAndTableOverrides() {
        for (String key : Arrays.asList("stopTimeoutSeconds", "finalCompactionTimeoutSeconds", "compactionCancelGraceSeconds")) {
            for (Object value : Arrays.asList(0, -1, 1.5, Long.MAX_VALUE, "2147483648", true, "invalid")) {
                assertThrows(IllegalArgumentException.class, () -> new PaimonConfig().load(Collections.singletonMap(key, value)), key + "=" + value);
            }
            PaimonConfig config = new PaimonConfig();
            config.setTableConfig(Collections.singletonMap("a", DataMap.create().kv(key, 10)));
            assertThrows(IllegalArgumentException.class, config::validateStopBudgets);
            assertDoesNotThrow(() -> new PaimonConfig().load(Collections.singletonMap(key, Integer.MAX_VALUE)));
        }
    }

    @Test
    void stopBudgetsMissingNullAndLayeredDefaultsMustMatch() {
        PaimonConfig config = new PaimonConfig().load(Collections.emptyMap());
        assertEquals(180, config.getStopTimeoutSeconds());
        assertEquals(120, config.getFinalCompactionTimeoutSeconds());
        assertEquals(30, config.getCompactionCancelGraceSeconds());
        config.load(Collections.singletonMap("stopTimeoutSeconds", 20)).load(Collections.singletonMap("stopTimeoutSeconds", null));
        config.setFinalCompactionTimeoutSeconds(null); config.setCompactionCancelGraceSeconds(null);
        assertEquals(180, config.getStopTimeoutSeconds()); assertEquals(120, config.getFinalCompactionTimeoutSeconds());
        assertEquals(30, config.getCompactionCancelGraceSeconds());
    }

    @ParameterizedTest
    @MethodSource("unsupportedExpireModes")
    void expirationModeMustBeRejectedDuringLoadAndValidate(String value) {
        LinkedHashMap<String, String> property = new LinkedHashMap<>();
        property.put("propKey", "snapshot.expire.execution-mode");
        property.put("propValue", value);
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/tmp/paimon");
        config.setTableProperties(Collections.singletonList(property));
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, config::validate);
        assertTrue(failure.getMessage().contains("snapshot.expire.execution-mode"));
        assertTrue(failure.getMessage().contains("SYNC"));
        assertThrows(IllegalArgumentException.class,
                () -> new PaimonConfig().load(Collections.singletonMap(
                        "tableProperties", Collections.singletonList(property))));
    }

    private static Stream<Arguments> unsupportedExpireModes() {
        return Stream.of(Arguments.of("ASYNC"), Arguments.of("invalid"), Arguments.of(""));
    }

    @Test
    void expirationModeMustValidateTableOverridesWithoutChangingOtherOptions() {
        LinkedHashMap<String, String> property = new LinkedHashMap<>();
        property.put("propKey", "snapshot.expire.execution-mode");
        property.put("propValue", "ASYNC");
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/tmp/paimon");
        config.setTableConfig(Collections.singletonMap("orders", DataMap.create()
                .kv("tableProperties", Collections.singletonList(property))));
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, config::validate);
        assertTrue(failure.getMessage().contains("orders"));
        property.put("propValue", "SYNC");
        assertDoesNotThrow(config::validate);
        assertEquals("SYNC", config.getTableProperties("orders").get(0).get("propValue"));
        assertEquals(4, config.getAsyncCommitConcurrency());
    }

    @ParameterizedTest
    @MethodSource("asyncCommitConfigLayers")
    void asyncCommitDefaultsMustRespectConnectionThenNodeLoading(
            Map<String, Object> connection, Map<String, Object> node, int expected) {
        // 对齐 Connector.onStart 的真实 PDK 配置加载顺序，覆盖存量显式 1 与 null 回填。
        PaimonConfig config = new PaimonConfig().load(connection).load(node);
        assertEquals(expected, config.getAsyncCommitConcurrency());
    }

    private static Stream<Arguments> asyncCommitConfigLayers() {
        Map<String, Object> missing = Collections.emptyMap();
        Map<String, Object> one = Collections.singletonMap("asyncCommitConcurrency", 1);
        Map<String, Object> four = Collections.singletonMap("asyncCommitConcurrency", 4);
        Map<String, Object> explicitNull = Collections.singletonMap("asyncCommitConcurrency", null);
        return Stream.of(
                Arguments.of(missing, missing, 4),
                Arguments.of(missing, one, 1),
                Arguments.of(missing, four, 4),
                Arguments.of(missing, explicitNull, 4),
                Arguments.of(one, missing, 1),
                Arguments.of(one, explicitNull, 4),
                Arguments.of(one, four, 4));
    }

    @Test
    void microBatchDefaultsMustSurviveExplicitNullValues() {
        PaimonConfig config = new PaimonConfig();
        config.setBatchAccumulationSize(null);
        config.setCommitIntervalMs(null);
        config.setEnableAsyncCommit(null);
        config.setAsyncCommitConcurrency(null);

        assertEquals(100000, config.getBatchAccumulationSize());
        assertEquals(30000, config.getCommitIntervalMs());
        assertTrue(config.getEnableAsyncCommit());
        assertEquals(4, config.getAsyncCommitConcurrency());
    }

    @Test
    void explicitMicroBatchOverridesMustBePreserved() {
        PaimonConfig config = new PaimonConfig();
        config.setBatchAccumulationSize(0);
        config.setCommitIntervalMs(-1);
        config.setEnableAsyncCommit(false);
        config.setAsyncCommitConcurrency(1);

        assertEquals(0, config.getBatchAccumulationSize());
        assertEquals(-1, config.getCommitIntervalMs());
        assertFalse(config.getEnableAsyncCommit());
        assertEquals(1, config.getAsyncCommitConcurrency());
    }

    @Test
    void asyncCommitConcurrencyMustStayWithinTheBoundedSchedulerRange() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/tmp/paimon");
        config.setStorageType("local");

        config.setAsyncCommitConcurrency(0);
        IllegalArgumentException belowMinimum =
                assertThrows(IllegalArgumentException.class, config::validate);
        assertTrue(belowMinimum.getMessage().contains("between 1 and 16"));

        config.setAsyncCommitConcurrency(17);
        IllegalArgumentException aboveMaximum =
                assertThrows(IllegalArgumentException.class, config::validate);
        assertTrue(aboveMaximum.getMessage().contains("between 1 and 16"));

        config.setAsyncCommitConcurrency(16);
        assertDoesNotThrow(config::validate);
    }

    @Test
    void compactionIntervalMinutesDefaultMustMatchSpecJson() {
        // spec.json advertises default=30 (and en/zh_CN/zh_TW placeholders say
        // "default: 30"). The Java field initializer MUST agree; otherwise users
        // see "default: 30" in the UI but the runtime silently applies 60.
        assertEquals(30, new PaimonConfig().getCompactionIntervalMinutes());
    }

    @Test
    void localStorageWarehousePathMustUseFileScheme() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/tmp/paimon");
        config.setStorageType("local");

        String fullPath = config.getFullWarehousePath();
        assertEquals("file:///tmp/paimon", fullPath);
    }

    @Test
    void s3StorageWarehousePathMustUseS3aScheme() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("bucket/warehouse");
        config.setStorageType("s3");

        String fullPath = config.getFullWarehousePath();
        assertEquals("s3a://bucket/warehouse", fullPath);
    }

    @Test
    void hdfsStorageWarehousePathMustIncludeHostAndPort() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/warehouse");
        config.setStorageType("hdfs");
        config.setHdfsHost("namenode");
        config.setHdfsPort(9000);

        String fullPath = config.getFullWarehousePath();
        assertEquals("hdfs://namenode:9000/warehouse", fullPath);
    }

    @Test
    void validS3ConfigMustPassValidation() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("bucket/warehouse");
        config.setStorageType("s3");
        config.setS3Endpoint("https://s3.amazonaws.com");
        config.setS3AccessKey("access-key");
        config.setS3SecretKey("secret-key");

        assertDoesNotThrow(() -> config.validate());
    }

    @Test
    void s3ConfigMissingEndpointMustFailValidation() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("bucket/warehouse");
        config.setStorageType("s3");
        config.setS3AccessKey("access-key");
        config.setS3SecretKey("secret-key");

        Exception exception = assertThrows(IllegalArgumentException.class, () -> config.validate());
        assertTrue(exception.getMessage().contains("S3 endpoint"));
    }

    @Test
    void emptyWarehouseMustFailValidation() {
        PaimonConfig config = new PaimonConfig();
        config.setStorageType("local");

        Exception exception = assertThrows(IllegalArgumentException.class, () -> config.validate());
        assertTrue(exception.getMessage().contains("Warehouse path"));
    }

    @Test
    void unsupportedStorageTypeMustFailValidation() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/tmp/paimon");
        config.setStorageType("unsupported");

        Exception exception = assertThrows(IllegalArgumentException.class, () -> config.validate());
        assertTrue(exception.getMessage().contains("Unsupported storage type"));
    }

    @ParameterizedTest(name = "{0}, bucketCount={1} => {2}")
    @MethodSource("validBucketConfigurations")
    void resolveBucketMustMapPaimonNativeModes(
            String bucketMode, Integer bucketCount, int expectedBucket) {
        PaimonConfig config = new PaimonConfig();
        config.setBucketMode(bucketMode);
        config.setBucketCount(bucketCount);

        assertEquals(expectedBucket, config.resolveBucket("orders"));
    }

    private static Stream<Arguments> validBucketConfigurations() {
        return Stream.of(
                Arguments.of("dynamic", null, -1),
                Arguments.of("DYNAMIC", -2, -1),
                Arguments.of("postpone", null, BucketMode.POSTPONE_BUCKET),
                Arguments.of("POSTPONE", -2, BucketMode.POSTPONE_BUCKET),
                Arguments.of("fixed", 1, 1),
                Arguments.of("FIXED", 4, 4));
    }

    @ParameterizedTest(name = "fixed bucketCount={0}")
    @MethodSource("invalidFixedBucketCounts")
    void resolveBucketMustRejectNonPositiveFixedCount(Integer bucketCount) {
        PaimonConfig config = new PaimonConfig();
        config.setBucketMode("fixed");
        config.setBucketCount(bucketCount);

        IllegalArgumentException thrown =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> config.resolveBucket("orders"));

        assertTrue(thrown.getMessage().contains("Bucket count"));
    }

    private static Stream<Arguments> invalidFixedBucketCounts() {
        return Stream.of(
                Arguments.of((Integer) null),
                Arguments.of(0),
                Arguments.of(-1),
                Arguments.of(-2));
    }

    @ParameterizedTest(name = "bucketMode={0}")
    @MethodSource("invalidBucketModes")
    void resolveBucketMustRejectUnknownMode(String bucketMode) {
        PaimonConfig config = new PaimonConfig();
        config.setBucketMode(bucketMode);

        IllegalArgumentException thrown =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> config.resolveBucket("orders"));

        assertTrue(thrown.getMessage().contains("Bucket mode"));
    }

    private static Stream<Arguments> invalidBucketModes() {
        return Stream.of(
                Arguments.of((String) null),
                Arguments.of(""),
                Arguments.of("automatic"));
    }

    @Test
    void resolveBucketMustUseTableSpecificModeAndCount() {
        PaimonConfig config = new PaimonConfig();
        config.setBucketMode("fixed");
        config.setBucketCount(4);
        Map<String, DataMap> tableConfig = new LinkedHashMap<>();
        tableConfig.put(
                "postponed",
                DataMap.create()
                        .kv("bucketMode", "postpone")
                        .kv("bucketCount", -2));
        tableConfig.put(
                "single_bucket",
                DataMap.create()
                        .kv("bucketMode", "fixed")
                        .kv("bucketCount", 1));
        config.setTableConfig(tableConfig);

        assertEquals(BucketMode.POSTPONE_BUCKET, config.resolveBucket("postponed"));
        assertEquals(1, config.resolveBucket("single_bucket"));
        assertEquals(4, config.resolveBucket("inherited"));
    }

    @Test
    void validateMustAcceptPostponeAsNativeMode() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/tmp/paimon");
        config.setStorageType("local");
        config.setBucketMode("postpone");
        config.setBucketCount(null);

        assertDoesNotThrow(config::validate);
    }

    @Test
    void flinkOnlyWriteThreadsMustNotBeExposedByCoreWriterConfig() {
        assertTrue(
                Arrays.stream(PaimonConfig.class.getDeclaredFields())
                        .noneMatch(field -> "writeThreads".equals(field.getName())));
        assertTrue(
                Arrays.stream(PaimonConfig.class.getDeclaredMethods())
                        .noneMatch(
                                method ->
                                        "getWriteThreads".equals(method.getName())
                                                || "setWriteThreads".equals(method.getName())));
    }
}
