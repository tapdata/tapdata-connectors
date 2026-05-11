package io.tapdata.connector.paimon.perf;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;

/**
 * S3 文件扫描调试测试（使用 AWS S3 SDK）
 */
public class S3FileScanDebugTest {

    // S3 配置（根据你的实际情况修改）
    private static final String S3_ENDPOINT = "http://192.168.1.184:9081";
    private static final String S3_ACCESS_KEY = "admin";
    private static final String S3_SECRET_KEY = "admin123";
    private static final String S3_REGION = "us-east-1";
    private static final String S3_BUCKET = "luke";
    private static final String S3_WAREHOUSE = "warehouse-paimon-perf";

    public static void main(String[] args) {
        System.out.println("=== S3 文件扫描调试测试（AWS S3 SDK） ===\n");

        try {
            test1_S3Connection();
            test2_ListBucket();
            test3_ScanWarehouse();

            System.out.println("\n=== ✅ 所有测试通过 ===");
        } catch (Exception e) {
            System.err.println("\n❌ 测试失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 测试 1: S3 连接验证
     */
    private static void test1_S3Connection() {
        System.out.println("[测试 1] 验证 S3 连接...");
        System.out.println("  Endpoint: " + S3_ENDPOINT);
        System.out.println("  Bucket: " + S3_BUCKET);
        System.out.println("  Region: " + S3_REGION);

        AmazonS3 s3Client = createS3Client();

        try {
            // 验证 bucket 是否存在
            boolean exists = s3Client.doesBucketExistV2(S3_BUCKET);
            System.out.println("  Bucket 存在: " + exists);

            if (exists) {
                System.out.println("  ✅ S3 连接成功");
            } else {
                System.out.println("  ⚠️  Bucket 不存在，请检查配置");
            }
        } catch (Exception e) {
            System.out.println("  ❌ 连接失败: " + e.getMessage());
        } finally {
            s3Client.shutdown();
        }
    }

    /**
     * 测试 2: 列出 Bucket 内容
     */
    private static void test2_ListBucket() {
        System.out.println("\n[测试 2] 列出 Bucket 内容...");
        AmazonS3 s3Client = createS3Client();

        try {
            com.amazonaws.services.s3.model.ObjectListing listing = s3Client.listObjects(S3_BUCKET);

            System.out.println("  对象总数: " + listing.getObjectSummaries().size());

            if (!listing.getObjectSummaries().isEmpty()) {
                System.out.println("  前 10 个对象:");
                int count = 0;
                for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                    if (count >= 10) break;
                    System.out.printf("    - %s (%s)%n", summary.getKey(), formatSize(summary.getSize()));
                    count++;
                }
            }

            System.out.println("  ✅ Bucket 列出成功");
        } catch (Exception e) {
            System.out.println("  ❌ 列出失败: " + e.getMessage());
        } finally {
            s3Client.shutdown();
        }
    }

    /**
     * 测试 3: 扫描 Warehouse 目录
     */
    private static void test3_ScanWarehouse() {
        System.out.println("\n[测试 3] 扫描 Warehouse 目录...");
        String warehousePath = S3_WAREHOUSE;
        System.out.println("  Warehouse: " + warehousePath);

        AmazonS3 s3Client = createS3Client();

        try {
            // 递归列出所有对象
            int fileCount = 0;
            long totalSize = 0;
            int parquetCount = 0;

            com.amazonaws.services.s3.model.ObjectListing listing = null;
            do {
                if (listing == null) {
                    listing = s3Client.listObjects(S3_BUCKET, warehousePath);
                } else {
                    listing = s3Client.listNextBatchOfObjects(listing);
                }

                for (S3ObjectSummary summary : listing.getObjectSummaries()) {
                    String key = summary.getKey();
                    fileCount++;
                    totalSize += summary.getSize();

                    if (key.endsWith(".parquet")) {
                        parquetCount++;
                    }

                    // 显示前 10 个 parquet 文件
                    if (parquetCount <= 10 && key.endsWith(".parquet")) {
                        System.out.printf("    📄 %s (%s)%n", key, formatSize(summary.getSize()));
                    }
                }
            } while (listing.isTruncated());

            System.out.println("\n  统计信息:");
            System.out.printf("    总文件数: %d%n", fileCount);
            System.out.printf("    Parquet 文件: %d%n", parquetCount);
            System.out.printf("    总大小: %s%n", formatSize(totalSize));

            if (fileCount > 0) {
                System.out.println("  ✅ Warehouse 扫描成功");
            } else {
                System.out.println("  ⚠️  未找到文件");
            }

        } catch (Exception e) {
            System.out.println("  ❌ 扫描失败: " + e.getMessage());
            e.printStackTrace();
        } finally {
            s3Client.shutdown();
        }
    }

    /**
     * 创建 S3 客户端
     */
    private static AmazonS3 createS3Client() {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(S3_ACCESS_KEY, S3_SECRET_KEY);

        return AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(S3_ENDPOINT, S3_REGION))
            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
            .withPathStyleAccessEnabled(true)  // MinIO 需要
            .disableChunkedEncoding()
            .build();
    }

    private static String formatSize(long size) {
        if (size < 1024) return size + " B";
        if (size < 1024 * 1024) return String.format("%.2f KB", size / 1024.0);
        if (size < 1024 * 1024 * 1024) return String.format("%.2f MB", size / (1024.0 * 1024));
        return String.format("%.2f GB", size / (1024.0 * 1024 * 1024));
    }
}
