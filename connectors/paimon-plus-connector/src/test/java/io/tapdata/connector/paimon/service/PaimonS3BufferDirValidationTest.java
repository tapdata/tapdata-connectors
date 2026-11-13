package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.logger.Log;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * TAP-13250: {@code fs.s3a.buffer.dir} is user-overridable through S3 advanced properties, so the
 * directory that actually reaches S3A is the one set <em>after</em> those properties are applied.
 * Validating only the connector's own default left an overridden path neither created nor checked,
 * and Hadoop then reports it inside {@code LocalDirAllocator} as {@code max capacity ... is 0} -
 * a message that reads like a full disk while the real cause is a missing or unwritable path.
 */
class PaimonS3BufferDirValidationTest {

    @TempDir
    Path tempDir;

    @Test
    void overriddenBufferDirMustBeCreatedAndUsedInsteadOfTheDefault() throws Exception {
        Path overridden = tempDir.resolve("override").resolve("buffer");
        PaimonConfig config = s3Config(tempDir.resolve("default-spill"));
        config.setS3Properties(properties("fs.s3a.buffer.dir", overridden.toString()));

        Configuration hadoop = buildHadoopConfiguration(config);

        assertEquals(overridden.toString(), hadoop.get("fs.s3a.buffer.dir"));
        assertTrue(Files.isDirectory(overridden),
                "被覆盖的缓存目录必须真正创建，否则 S3A 会写入一个不存在的路径");
    }

    @Test
    void uncreatableOverrideMustFailWithTheConcretePath() throws Exception {
        // 父级是普通文件，子路径必然创建不出来。
        Path blocker = Files.write(tempDir.resolve("blocker"), new byte[] {1});
        Path overridden = blocker.resolve("buffer");
        PaimonConfig config = s3Config(tempDir.resolve("default-spill"));
        config.setS3Properties(properties("fs.s3a.buffer.dir", overridden.toString()));

        InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                () -> buildHadoopConfiguration(config));

        assertTrue(failure.getCause() instanceof IllegalStateException);
        String message = failure.getCause().getMessage();
        assertTrue(message.contains(overridden.toString()),
                "报错必须带出实际生效的目录，否则现场无法判断是哪个路径不可用：" + message);
        assertFalse(Files.exists(overridden));
    }

    @Test
    void overridePointingAtARegularFileMustFailWithTheConcretePath() throws Exception {
        Path regularFile = tempDir.resolve("not-a-directory");
        Files.write(regularFile, new byte[] {1});
        PaimonConfig config = s3Config(tempDir.resolve("default-spill"));
        config.setS3Properties(properties("fs.s3a.buffer.dir", regularFile.toString()));

        InvocationTargetException failure = assertThrows(InvocationTargetException.class,
                () -> buildHadoopConfiguration(config));

        assertTrue(failure.getCause() instanceof IllegalStateException);
        String message = failure.getCause().getMessage();
        assertTrue(message.contains("is not a directory"), message);
        assertTrue(message.contains(regularFile.toString()), message);
    }

    @Test
    void blankOverrideMustFallBackToTheConfiguredSpillRoot() throws Exception {
        Path spillRoot = tempDir.resolve("spill-root");
        PaimonConfig config = s3Config(spillRoot);
        config.setS3Properties(properties("fs.s3a.buffer.dir", "  "));

        Configuration hadoop = buildHadoopConfiguration(config);

        assertEquals(spillRoot.toString(), hadoop.get("fs.s3a.buffer.dir"));
        assertTrue(Files.isDirectory(spillRoot));
    }

    @Test
    void directoriesCreatedForARejectedListMustBeRolledBack() throws Exception {
        Path created = tempDir.resolve("valid-root");
        Path blocker = Files.write(tempDir.resolve("blocker-file"), new byte[] {1});
        assertThrows(IllegalStateException.class,
                () -> PaimonService.verifyS3ABufferDirs(created + "," + blocker.resolve("x")));

        assertFalse(Files.exists(created),
                "配置有问题时不能留下本次新建的半成品目录");
        assertTrue(Files.exists(blocker), "已存在的文件不能被清理逻辑误删");
    }

    @Test
    void relativeAndEmptyEntriesAreRejected() {
        IllegalArgumentException relative = assertThrows(IllegalArgumentException.class,
                () -> PaimonService.verifyS3ABufferDirs("relative/buffer"));
        assertTrue(relative.getMessage().contains("must be an absolute local directory"),
                relative.getMessage());

        IllegalArgumentException empty = assertThrows(IllegalArgumentException.class,
                () -> PaimonService.verifyS3ABufferDirs(tempDir + ","));
        assertTrue(empty.getMessage().contains("not an empty entry"), empty.getMessage());
    }

    @Test
    void warehouseBucketOverrideIsValidatedInsteadOfAnUnusedGlobalPath() throws Exception {
        PaimonConfig config = s3Config(tempDir.resolve("default"));
        config.setWarehouse("s3a://customer/warehouse");
        Path blocker = Files.write(tempDir.resolve("unused-global"), new byte[]{1});
        List<LinkedHashMap<String, String>> values = properties("fs.s3a.buffer.dir", blocker.resolve("buffer").toString());
        values.addAll(properties("fs.s3a.bucket.customer.buffer.dir", tempDir.resolve("bucket-buffer").toString()));
        config.setS3Properties(values);
        Configuration hadoop = buildHadoopConfiguration(config);
        assertEquals(tempDir.resolve("bucket-buffer").toString(),
                org.apache.hadoop.fs.s3a.S3AUtils.propagateBucketOptions(hadoop, "customer").get("fs.s3a.buffer.dir"));
        assertTrue(Files.isDirectory(tempDir.resolve("bucket-buffer")));
    }

    @Test
    void blankGlobalAndBucketValuesBothFallBackToConfiguredSpillRoot() throws Exception {
        Path spill = tempDir.resolve("custom-spill");
        PaimonConfig config = s3Config(spill);
        config.setWarehouse("s3a://customer/warehouse");
        List<LinkedHashMap<String, String>> values = properties("fs.s3a.buffer.dir", "  ");
        values.addAll(properties("fs.s3a.bucket.customer.buffer.dir", "  "));
        config.setS3Properties(values);
        Configuration hadoop = buildHadoopConfiguration(config);
        assertEquals(spill.toString(), org.apache.hadoop.fs.s3a.S3AUtils
                .propagateBucketOptions(hadoop, "customer").getTrimmed("fs.s3a.buffer.dir"));
        assertTrue(Files.isDirectory(spill));
    }

    private PaimonConfig s3Config(Path diskTmpDir) {
        PaimonConfig config = new PaimonConfig();
        config.setStorageType("s3");
        config.setWarehouse("s3a://paimon-buffer-dir-" + System.nanoTime() + "/");
        config.setS3Endpoint("http://127.0.0.1:9");
        config.setS3Region("us-east-1");
        config.setS3AccessKey("local-test");
        config.setS3SecretKey("local-test");
        config.setDiskTmpDir(diskTmpDir.toString());
        return config;
    }

    private static Configuration buildHadoopConfiguration(PaimonConfig config) throws Exception {
        Method method = PaimonService.class.getDeclaredMethod("buildHadoopConfiguration");
        method.setAccessible(true);
        return (Configuration) method.invoke(new PaimonService(config, mock(Log.class)));
    }

    private static List<LinkedHashMap<String, String>> properties(String key, String value) {
        LinkedHashMap<String, String> property = new LinkedHashMap<>();
        property.put("propKey", key);
        property.put("propValue", value);
        List<LinkedHashMap<String, String>> properties = new ArrayList<>();
        properties.add(property);
        return properties;
    }
}
