package io.tapdata.connector.paimon.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;

import java.io.IOException;
import java.net.URI;

/** 保留 S3A 的缓存与认证规则，仅隔离客户端初始化的线程组。 */
public final class PaimonS3AFileSystem extends S3AFileSystem {
    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        // Hadoop 3.3.6 的 initThreadPools 在 initialize 内固定保存当前 ThreadGroup。
        // https://github.com/apache/hadoop/blob/rel/release-3.3.6/hadoop-tools/hadoop-aws/src/main/java/org/apache/hadoop/fs/s3a/S3AFileSystem.java#L776
        // Paimon 1.3.2 HadoopFileIO.createFileSystem 仍通过原 path.getFileSystem(conf) 入口。
        PaimonFileSystemInitialization.run(() -> super.initialize(name, conf));
    }
}
