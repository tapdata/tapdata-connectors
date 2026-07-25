package io.tapdata.connector.paimon;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.utils.DataMap;
import org.apache.paimon.table.BucketMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for PaimonConfig
 */
public class PaimonConfigTest {

    @Test
    public void testLocalStorageWarehousePath() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/tmp/paimon");
        config.setStorageType("local");
        
        String fullPath = config.getFullWarehousePath();
        assertEquals("file:///tmp/paimon", fullPath);
    }

    @Test
    public void testS3StorageWarehousePath() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("bucket/warehouse");
        config.setStorageType("s3");
        
        String fullPath = config.getFullWarehousePath();
        assertEquals("s3a://bucket/warehouse", fullPath);
    }

    @Test
    public void testHdfsStorageWarehousePath() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/warehouse");
        config.setStorageType("hdfs");
        config.setHdfsHost("namenode");
        config.setHdfsPort(9000);
        
        String fullPath = config.getFullWarehousePath();
        assertEquals("hdfs://namenode:9000/warehouse", fullPath);
    }

    @Test
    public void testValidateS3Config() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("bucket/warehouse");
        config.setStorageType("s3");
        config.setS3Endpoint("https://s3.amazonaws.com");
        config.setS3AccessKey("access-key");
        config.setS3SecretKey("secret-key");
        
        assertDoesNotThrow(() -> config.validate());
    }

    @Test
    public void testValidateS3ConfigMissingEndpoint() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("bucket/warehouse");
        config.setStorageType("s3");
        config.setS3AccessKey("access-key");
        config.setS3SecretKey("secret-key");
        
        Exception exception = assertThrows(IllegalArgumentException.class, () -> config.validate());
        assertTrue(exception.getMessage().contains("S3 endpoint"));
    }

    @Test
    public void testValidateEmptyWarehouse() {
        PaimonConfig config = new PaimonConfig();
        config.setStorageType("local");
        
        Exception exception = assertThrows(IllegalArgumentException.class, () -> config.validate());
        assertTrue(exception.getMessage().contains("Warehouse path"));
    }

    @Test
    public void testValidateUnsupportedStorageType() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/tmp/paimon");
        config.setStorageType("unsupported");
        
        Exception exception = assertThrows(IllegalArgumentException.class, () -> config.validate());
        assertTrue(exception.getMessage().contains("Unsupported storage type"));
    }

    @ParameterizedTest(name = "{0}, bucketCount={1} => {2}")
    @MethodSource("validBucketConfigurations")
    public void resolveBucketMustMapPaimonNativeModes(
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
    public void resolveBucketMustRejectNonPositiveFixedCount(Integer bucketCount) {
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
    public void resolveBucketMustRejectUnknownMode(String bucketMode) {
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
    public void resolveBucketMustUseTableSpecificModeAndCount() {
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
    public void validateMustAcceptPostponeAsNativeMode() {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse("/tmp/paimon");
        config.setStorageType("local");
        config.setBucketMode("postpone");
        config.setBucketCount(null);

        assertDoesNotThrow(config::validate);
    }

    @Test
    public void flinkOnlyWriteThreadsMustNotBeExposedByCoreWriterConfig() {
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
