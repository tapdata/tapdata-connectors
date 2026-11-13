package io.tapdata.connector.paimon.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import io.tapdata.connector.paimon.util.PaimonLocalDirectories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/** 保留 S3A 的缓存与认证规则，仅隔离客户端初始化的线程组。 */
public final class PaimonS3AFileSystem extends S3AFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonS3AFileSystem.class);

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        // Hadoop 3.3.6 的 initThreadPools 在 initialize 内固定保存当前 ThreadGroup。
        // https://github.com/apache/hadoop/blob/rel/release-3.3.6/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L776
        // Paimon 1.3.2 HadoopFileIO.createFileSystem 仍通过原 path.getFileSystem(conf) 入口。
        Configuration verified = verifiedConfiguration(name, conf);
        PaimonFileSystemInitialization.run(() -> super.initialize(name, verified));
    }
    /** Mirrors Hadoop's bucket propagation before any network access or upload allocation. */
    public static Configuration verifiedConfiguration(URI name, Configuration conf) throws IOException {
        String bucket = name.getHost();
        if (bucket == null || bucket.isEmpty()) {
            throw new IOException("S3A URI must include a bucket");
        }
        String bucketKey = "fs.s3a.bucket." + bucket + ".buffer.dir";
        Configuration effective = S3AUtils.propagateBucketOptions(conf, bucket);
        String configured = effective.getTrimmed("fs.s3a.buffer.dir");
        if (configured == null || configured.isEmpty()) {
            configured = conf.getTrimmed("fs.s3a.buffer.dir");
        }
        if (configured == null || configured.isEmpty()) {
            configured = io.tapdata.connector.paimon.util.PaimonSpillDirCleaner.resolveTmpDirs(null);
        }
        String source = conf.get(bucketKey) == null ? "fs.s3a.buffer.dir" : bucketKey;
        final String verified;
        try {
            verified = PaimonLocalDirectories.verifyS3ABufferDirs(configured);
        } catch (RuntimeException failure) {
            throw new IOException("S3A buffer validation failed: bucket=" + bucket
                    + ", source=" + source + ": " + failure.getMessage(), failure);
        }
        // super.initialize propagates again; normalize the bucket value as well, keeping the
        // caller's shared Configuration untouched (other authorities may use other paths).
        effective.set("fs.s3a.buffer.dir", verified);
        if (conf.get(bucketKey) != null) {
            effective.set(bucketKey, verified);
        }
        for (String path : verified.split(",")) {
            LOG.info("Paimon S3A buffer verified: bucket={}, source={}, directoryState={}",
                    bucket, source, PaimonLocalDirectories.describe(java.nio.file.Paths.get(path)));
        }
        return effective;
    }
}
