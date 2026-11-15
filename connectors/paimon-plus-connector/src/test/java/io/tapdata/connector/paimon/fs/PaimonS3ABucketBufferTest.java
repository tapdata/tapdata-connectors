package io.tapdata.connector.paimon.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.*;

class PaimonS3ABucketBufferTest {
    @TempDir Path root;

    @Test
    void validatesActualBucketOverrideBeforeNetworkAccess() throws Exception {
        Configuration conf = config();
        Path blocker = Files.write(root.resolve("blocker"), new byte[]{1});
        conf.set("fs.s3a.bucket.customer.buffer.dir", blocker.resolve("buffer").toString());
        IOException failure = assertThrows(IOException.class,
                () -> new PaimonS3AFileSystem().initialize(URI.create("s3a://customer/table"), conf));
        assertTrue(failure.getMessage().contains("fs.s3a.bucket.customer.buffer.dir"));
        assertTrue(failure.getMessage().contains(blocker.toString()));
        assertNotNull(failure.getCause().getCause(), "Preserve the OS mkdir failure");
    }

    @Test
    void bucketDirectoriesAreIndependentAndSurviveSecondHadoopPropagation() throws Exception {
        Configuration conf = config();
        conf.set("fs.s3a.bucket.first.buffer.dir", root.resolve("first").toString());
        conf.set("fs.s3a.bucket.second.buffer.dir", root.resolve("second").toString());
        for (String bucket : new String[]{"first", "second"}) {
            Configuration verified = PaimonS3AFileSystem.verifiedConfiguration(URI.create("s3a://" + bucket), conf);
            assertEquals(root.resolve(bucket).toString(),
                    S3AUtils.propagateBucketOptions(verified, bucket).get("fs.s3a.buffer.dir"));
            assertTrue(Files.isDirectory(root.resolve(bucket)));
        }
        assertEquals(root.resolve("global").toString(), conf.get("fs.s3a.buffer.dir"));
    }

    @Test
    void blankBucketFallsBackToGlobalWithoutMutatingSharedConfiguration() throws Exception {
        Configuration conf = config();
        conf.set("fs.s3a.bucket.customer.buffer.dir", "  ");
        Configuration verified = PaimonS3AFileSystem.verifiedConfiguration(URI.create("s3a://customer"), conf);
        assertEquals(root.resolve("global").toString(),
                S3AUtils.propagateBucketOptions(verified, "customer").getTrimmed("fs.s3a.buffer.dir"));
        assertEquals("  ", conf.get("fs.s3a.bucket.customer.buffer.dir"));
    }

    private Configuration config() {
        Configuration conf = new Configuration(false);
        conf.set("fs.s3a.buffer.dir", root.resolve("global").toString());
        return conf;
    }
}
