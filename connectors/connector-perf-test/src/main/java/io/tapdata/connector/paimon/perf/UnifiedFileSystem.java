package io.tapdata.connector.paimon.perf;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * 统一文件系统接口 - 支持本地和 S3
 */
public interface UnifiedFileSystem {
    
    boolean exists(String path) throws IOException;
    boolean isDirectory(String path) throws IOException;
    FileStatus[] listStatus(String path) throws IOException;
    List<FileStatus> listFilesRecursive(String path) throws IOException;
    URI getUri();
    void close() throws IOException;
    
    static UnifiedFileSystem createLocal() {
        return new LocalFileSystemImpl();
    }
    
    static UnifiedFileSystem createS3(String endpoint, String accessKey, String secretKey, String region, String bucket) {
        return new S3FileSystemImpl(endpoint, accessKey, secretKey, region, bucket);
    }

    /**
     * 本地文件系统实现
     */
    class LocalFileSystemImpl implements UnifiedFileSystem {
        private final org.apache.hadoop.fs.FileSystem fs;
        
        LocalFileSystemImpl() {
            try {
                this.fs = org.apache.hadoop.fs.FileSystem.getLocal(new org.apache.hadoop.conf.Configuration());
            } catch (IOException e) {
                throw new RuntimeException("Failed to create local filesystem", e);
            }
        }
        
        @Override
        public boolean exists(String path) throws IOException {
            return fs.exists(new org.apache.hadoop.fs.Path(path));
        }
        
        @Override
        public boolean isDirectory(String path) throws IOException {
            return fs.isDirectory(new org.apache.hadoop.fs.Path(path));
        }
        
        @Override
        public FileStatus[] listStatus(String path) throws IOException {
            return fs.listStatus(new org.apache.hadoop.fs.Path(path));
        }
        
        @Override
        public List<FileStatus> listFilesRecursive(String path) throws IOException {
            List<FileStatus> files = new ArrayList<>();
            org.apache.hadoop.fs.Path rootPath = new org.apache.hadoop.fs.Path(path);
            if (!fs.exists(rootPath)) return files;
            
            org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> iterator = 
                fs.listFiles(rootPath, true);
            while (iterator.hasNext()) {
                files.add(iterator.next());
            }
            return files;
        }
        
        @Override
        public URI getUri() {
            return fs.getUri();
        }
        
        @Override
        public void close() throws IOException {
            fs.close();
        }
    }
    
    /**
     * S3 文件系统实现（使用 AWS S3 SDK）
     */
    class S3FileSystemImpl implements UnifiedFileSystem {
        private final AmazonS3 s3Client;
        private final String endpoint;
        private final String bucket;
        private final URI endpointUri;

        S3FileSystemImpl(String endpoint, String accessKey, String secretKey, String region, String bucket) {
            try {
                this.endpoint = endpoint;
                this.bucket = bucket;
                this.endpointUri = URI.create(endpoint);
                
                System.out.println("  [DEBUG] S3 配置 - Endpoint: " + endpoint + ", Bucket: " + bucket);
                
                // 创建 AWS 凭证
                BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey, secretKey);
                
                // 创建 S3 客户端
                this.s3Client = AmazonS3ClientBuilder.standard()
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                    .withPathStyleAccessEnabled(true)  // MinIO 需要路径样式访问
                    .disableChunkedEncoding()  // 某些 MinIO 版本需要禁用
                    .build();
                
                System.out.println("  [DEBUG] S3 客户端创建成功");
                
            } catch (Exception e) {
                throw new RuntimeException("Failed to create S3 filesystem: " + e.getMessage(), e);
            }
        }

        @Override
        public boolean exists(String path) throws IOException {
            try {
                String key = extractKey(path);
                if (key == null || key.isEmpty()) {
                    // 检查 bucket 是否存在
                    return s3Client.doesBucketExistV2(bucket);
                }
                
                // 检查对象是否存在
                return s3Client.doesObjectExist(bucket, key + "/schema/schema-0");
            } catch (Exception e) {
                System.err.println("  [WARN] 检查路径存在失败: " + path + " - " + e.getMessage());
                e.printStackTrace();
                return false;
            }
        }

        @Override
        public boolean isDirectory(String path) throws IOException {
            try {
                String key = extractKey(path);
                if (key == null || key.isEmpty() || key.endsWith("/")) {
                    return true;  // bucket 或目录
                }
                
                // 检查是否是对象（文件）
                if (s3Client.doesObjectExist(bucket, key)) {
                    ObjectMetadata metadata = s3Client.getObjectMetadata(bucket, key);
                    // S3 中目录通常以 / 结尾且内容为空
                    return key.endsWith("/") || metadata.getContentLength() == 0;
                }
                
                // 检查是否有以此键为前缀的对象（说明是目录）
                com.amazonaws.services.s3.model.ObjectListing listing = s3Client.listObjects(
                    new com.amazonaws.services.s3.model.ListObjectsRequest()
                        .withBucketName(bucket)
                        .withPrefix(key.endsWith("/") ? key : key + "/")
                        .withMaxKeys(1)
                );
                
                return !listing.getObjectSummaries().isEmpty();
            } catch (Exception e) {
                System.err.println("  [WARN] 检查目录失败: " + path + " - " + e.getMessage());
                return false;
            }
        }

        @Override
        public FileStatus[] listStatus(String path) throws IOException {
            try {
                String key = extractKey(path);
                String prefix = key.endsWith("/") ? key : (key.isEmpty() ? "" : key + "/");
                
                List<FileStatus> statuses = new ArrayList<>();
                com.amazonaws.services.s3.model.ListObjectsRequest request = new com.amazonaws.services.s3.model.ListObjectsRequest()
                    .withBucketName(bucket)
                    .withPrefix(prefix)
                    .withDelimiter("/")
                    .withMaxKeys(1000);
                
                com.amazonaws.services.s3.model.ObjectListing listing = s3Client.listObjects(request);
                
                // 添加目录（CommonPrefixes）
                for (String commonPrefix : listing.getCommonPrefixes()) {
                    String dirPath = commonPrefix;
                    statuses.add(new FileStatus(
                        0,
                        true,  // isDir
                        1,
                        128 * 1024 * 1024,
                        0,
                        new Path("s3://" + bucket + "/" + dirPath)
                    ));
                }
                
                // 添加文件
                for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                    // 跳过目录标记对象
                    if (summary.getKey().endsWith("/") && summary.getSize() == 0) {
                        continue;
                    }
                    statuses.add(new FileStatus(
                        summary.getSize(),
                        false,  // isDir
                        1,
                        128 * 1024 * 1024,
                        summary.getLastModified().getTime(),
                        new Path("s3://" + bucket + "/" + summary.getKey())
                    ));
                }
                
                return statuses.toArray(new FileStatus[0]);
            } catch (Exception e) {
                throw new IOException("Failed to list status: " + path, e);
            }
        }

        @Override
        public List<FileStatus> listFilesRecursive(String path) throws IOException {
            List<FileStatus> files = new ArrayList<>();
            String key = extractKey(path);
            String prefix = key.endsWith("/") ? key : (key.isEmpty() ? "" : key + "/");
            
            try {
                // 递归列出所有对象
                com.amazonaws.services.s3.model.ObjectListing listing = null;
                do {
                    if (listing == null) {
                        listing = s3Client.listObjects(bucket, prefix);
                    } else {
                        listing = s3Client.listNextBatchOfObjects(listing);
                    }
                    
                    for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                        // 跳过目录标记
                        if (summary.getKey().endsWith("/") && summary.getSize() == 0) {
                            continue;
                        }
                        files.add(new FileStatus(
                            summary.getSize(),
                            false,
                            1,
                            128 * 1024 * 1024,
                            summary.getLastModified().getTime(),
                            new Path("s3://" + bucket + "/" + summary.getKey())
                        ));
                    }
                } while (listing.isTruncated());
                
            } catch (Exception e) {
                throw new IOException("Failed to list files recursively: " + path, e);
            }
            
            return files;
        }

        /**
         * 从 S3 路径提取 key（去掉 bucket 部分）
         * 输入: s3://bucket/path/to/file 或 /bucket/path/to/file
         * 输出: path/to/file
         */
        private String extractKey(String path) {
            if (path == null || path.isEmpty()) {
                return "";
            }
            
            // 去掉 s3:// 前缀
            if (path.startsWith("s3://")) {
                path = path.substring(5);
            }
            
            // 去掉 bucket 名称
            int slashIdx = path.indexOf('/');
            if (slashIdx >= 0) {
                return path.substring(slashIdx + 1);
            }
            
            return "";
        }

        @Override
        public URI getUri() {
            return endpointUri;
        }

        @Override
        public void close() throws IOException {
            if (s3Client != null) {
                s3Client.shutdown();
            }
        }
    }
}
