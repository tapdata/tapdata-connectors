package io.tapdata.connector.paimon;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 高性能写入测试 - 使用最原生的 Paimon API + 8线程并发向 S3 写入 1000 万条数据
 * <p>
 * 特点：
 * - 直接使用 Paimon 原生 API，无任何中间层
 * - 8 线程并发写入，最高性能
 * - 每个线程独立的 Writer，共享 Commit
 * - 10 个字段（id, name, age, email, phone, address, city, country, created_at, updated_at）
 * - 1000 万条数据
 * - 批量提交优化
 * <p>
 * 使用前请配置 S3 连接信息
 */
public class PaimonPerformanceTest {

    // ===== S3 配置 - 请根据实际情况修改 =====
    private static final String S3_ENDPOINT = "http://127.0.0.1:9000";
    private static final String S3_ACCESS_KEY = "19CAkIyPfUcHRTd8";
    private static final String S3_SECRET_KEY = "xa8HcHPei6cA0LvZF6kh3yokDjZiU5Vc";
    private static final String S3_REGION = "";
    private static final String S3_BUCKET = "jarad";

    // ===== 测试配置 =====
    private static final int TOTAL_RECORDS = 10_000_000;  // 1000 万条数据
    private static final int WRITE_THREADS = 8;            // 8 个写入线程
    private static final int COMMIT_BATCH_SIZE = 100000;   // 每 100000 条提交一次
    private static final String DATABASE_NAME = "default";
    private static final String TABLE_NAME = "user_data";

    // Paimon 原生对象
    private Catalog catalog;
    private List<StreamTableWrite> writers;  // 每个线程一个 Writer
    private StreamTableCommit commit;
    private AtomicLong commitIdentifier = new AtomicLong(0);

    // 线程池
    private ExecutorService executorService;

    @BeforeEach
    public void setUp() throws Exception {
        System.out.println("========================================");
        System.out.println("Paimon 原生 API + 8线程并发写入测试");
        System.out.println("========================================");
        System.out.println("目标: 写入 " + formatNumber(TOTAL_RECORDS) + " 条数据到 S3");
        System.out.println("写入线程: " + WRITE_THREADS + " 个");
        System.out.println("提交批次: " + COMMIT_BATCH_SIZE + " 条/提交");
        System.out.println("========================================\n");

        // 初始化 Paimon Catalog
        System.out.println("初始化 Paimon Catalog...");
        initCatalog();

        // 创建表
        System.out.println("创建表: " + DATABASE_NAME + "." + TABLE_NAME);
        createTable();
        System.out.println("表创建成功\n");

        // 初始化 Writers 和 Commit
        System.out.println("初始化 " + WRITE_THREADS + " 个 Writers 和 Commit...");
        initWritersAndCommit();
        System.out.println("初始化完成\n");

        // 创建线程池
        executorService = Executors.newFixedThreadPool(WRITE_THREADS);
    }

    @AfterEach
    public void tearDown() throws Exception {
        System.out.println("\n正在关闭资源...");

        // 关闭线程池
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }

        // 关闭所有 Writers
        if (writers != null) {
            for (StreamTableWrite writer : writers) {
                try {
                    writer.close();
                } catch (Exception e) {
                    // Ignore
                }
            }
        }

        if (commit != null) {
            try {
                commit.close();
            } catch (Exception e) {
                // Ignore
            }
        }

        if (catalog != null) {
            try {
                catalog.close();
            } catch (Exception e) {
                // Ignore
            }
        }

        System.out.println("资源已关闭");
    }

    /**
     * 初始化 Paimon Catalog
     */
    private void initCatalog() throws Exception {
        // 创建 Catalog Options
        Options options = new Options();
        options.set("warehouse", "s3a://" + S3_BUCKET + "/paimon-warehouse");

        // 创建 Hadoop Configuration（S3 配置）
        Configuration hadoopConf = new Configuration();

        // S3A 基本配置
        hadoopConf.set("fs.s3a.endpoint", S3_ENDPOINT);
        hadoopConf.set("fs.s3a.access.key", S3_ACCESS_KEY);
        hadoopConf.set("fs.s3a.secret.key", S3_SECRET_KEY);
        hadoopConf.set("fs.s3a.endpoint.region", S3_REGION);
        hadoopConf.set("fs.s3a.path.style.access", "false");

        // ===== S3A 性能优化配置 =====
        // 连接池优化
        hadoopConf.set("fs.s3a.connection.maximum", "100");
        hadoopConf.set("fs.s3a.threads.max", "50");
        hadoopConf.set("fs.s3a.connection.establish.timeout", "10000");
        hadoopConf.set("fs.s3a.connection.timeout", "200000");

        // 快速上传配置
        hadoopConf.set("fs.s3a.fast.upload", "true");
        hadoopConf.set("fs.s3a.fast.upload.buffer", "disk");
        hadoopConf.set("fs.s3a.multipart.size", "268435456"); // 256 MB
        hadoopConf.set("fs.s3a.multipart.threshold", "134217728"); // 128 MB

        // 减少 HeadObject 调用
        hadoopConf.set("fs.s3a.directory.marker.retention", "keep");
        hadoopConf.set("fs.s3a.change.detection.mode", "none");

        // 重试策略
        hadoopConf.set("fs.s3a.attempts.maximum", "10");
        hadoopConf.set("fs.s3a.retry.limit", "5");

        // 创建 Catalog Context
        CatalogContext context = CatalogContext.create(options, hadoopConf);

        // 创建 Catalog
        catalog = CatalogFactory.createCatalog(context);

        // 确保数据库存在
        try {
            catalog.getDatabase(DATABASE_NAME);
        } catch (Catalog.DatabaseNotExistException e) {
            catalog.createDatabase(DATABASE_NAME, true);
        }
    }

    /**
     * 创建表
     */
    private void createTable() throws Exception {
        Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);

        // 检查表是否已存在
        try {
            catalog.getTable(identifier);
            System.out.println("表已存在，将使用现有表");
            return;
        } catch (Catalog.TableNotExistException e) {
            // 表不存在，创建新表
        }

        // 创建 Schema Builder
        Schema.Builder schemaBuilder = Schema.newBuilder();

        // 添加字段
        schemaBuilder.column("id", DataTypes.BIGINT(), "主键ID");
        schemaBuilder.column("name", DataTypes.STRING(), "用户名");
        schemaBuilder.column("age", DataTypes.INT(), "年龄");
        schemaBuilder.column("email", DataTypes.STRING(), "邮箱");
        schemaBuilder.column("phone", DataTypes.STRING(), "电话");
        schemaBuilder.column("address", DataTypes.STRING(), "地址");
        schemaBuilder.column("city", DataTypes.STRING(), "城市");
        schemaBuilder.column("country", DataTypes.STRING(), "国家");
        schemaBuilder.column("created_at", DataTypes.TIMESTAMP(3), "创建时间");
        schemaBuilder.column("updated_at", DataTypes.TIMESTAMP(3), "更新时间");

        // 设置主键
        schemaBuilder.primaryKey("id");

        // ===== 性能优化选项 =====
        // 写入缓冲区：1GB
        schemaBuilder.option("write-buffer-size", "1024mb");

        // 目标文件大小：512MB
        schemaBuilder.option("target-file-size", "512mb");

        // 文件格式：Parquet
        schemaBuilder.option("file.format", "parquet");

        // 压缩格式：ZSTD
        schemaBuilder.option("file.compression", "zstd");

        // 分桶模式：动态分桶（-1 表示动态）
        schemaBuilder.option("bucket", "8");

        // 快照保留策略
        schemaBuilder.option("snapshot.num-retained.min", "10");
        schemaBuilder.option("snapshot.num-retained.max", "100");
        schemaBuilder.option("snapshot.time-retained", "1h");

        // 禁用自动压缩（写入阶段）
        schemaBuilder.option("compaction.min.file-num", "999999");
        schemaBuilder.option("compaction.max.file-num", "999999");

        // 并行度
        schemaBuilder.option("sink.parallelism", "16");
        schemaBuilder.option("write-parallelism", "8");
        schemaBuilder.option("write-buffer-spillable", "true");
        schemaBuilder.option("write-buffer-for-append", "true");
        schemaBuilder.option("write-buffer-spill-threshold", "64mb");
        schemaBuilder.option("commit.async", "true");
        schemaBuilder.option("commit.interval", "5s");
        schemaBuilder.option("commit.timeout", "2m");
        schemaBuilder.option("s3.upload.max-concurrency", "50");
        schemaBuilder.option("s3.upload.part-size", "16mb");
        schemaBuilder.option("s3.fast-upload", "true");

//
//
//                // 小文件合并
//                "  'auto-merge' = 'true',\n" +
//                "  'merge-engine' = 'deduplicate',\n" +
//                "  'merge.max-file-size' = '256mb',\n" +
//                "  'merge.min-file-size' = '16mb',\n" +
//                "  'merge.trigger.interval' = '10m',\n" +
//
//
//
//                // S3优化
//                "  's3.upload.max-concurrency' = '50',\n" +
//                "  's3.upload.part-size' = '16mb',\n" +
//                "  's3.fast-upload' = 'true'\n" +

        // 创建表
        catalog.createTable(identifier, schemaBuilder.build(), false);
    }

    /**
     * 初始化多个 Writers 和 Commit
     */
    private void initWritersAndCommit() throws Exception {
        Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);
        Table table = catalog.getTable(identifier);

        // 创建 StreamWriteBuilder
        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();

        // 为每个线程创建一个 Writer
        writers = new ArrayList<>(WRITE_THREADS);
        for (int i = 0; i < WRITE_THREADS; i++) {
            writers.add(writeBuilder.newWrite());
        }

        // 创建共享的 Commit
        commit = writeBuilder.newCommit();
    }


    /**
     * 主测试方法：使用 8 线程并发写入 1000 万条数据
     */
    public void testWrite10MillionRecords() throws Exception {
        System.out.println("开始 8 线程并发写入测试...\n");

        long startTime = System.currentTimeMillis();

        // 全局统计
        AtomicLong totalWritten = new AtomicLong(0);
        AtomicLong lastReportTime = new AtomicLong(startTime);
        AtomicLong lastReportCount = new AtomicLong(0);

        // 每个线程处理的记录数
        int recordsPerThread = TOTAL_RECORDS / WRITE_THREADS;

        // 用于收集所有线程的 CommitMessage
        ConcurrentLinkedQueue<CommitMessage> allMessages = new ConcurrentLinkedQueue<>();

        // 用于同步提交
        Object commitLock = new Object();
        AtomicLong accumulatedRecords = new AtomicLong(0);

        // 创建写入任务
        List<Future<?>> futures = new ArrayList<>();

        for (int threadId = 0; threadId < WRITE_THREADS; threadId++) {
            final int tid = threadId;
            final long startRecordId = (long) tid * recordsPerThread;
            final long endRecordId = (tid == WRITE_THREADS - 1) ? TOTAL_RECORDS : (tid + 1) * recordsPerThread;
            final StreamTableWrite threadWriter = writers.get(tid);

            Future<?> future = executorService.submit(() -> {
                try {
                    Random random = new Random(tid); // 每个线程独立的随机数生成器
                    List<CommitMessage> localMessages = new ArrayList<>();

                    for (long recordId = startRecordId; recordId < endRecordId; recordId++) {
                        // 创建 GenericRow
                        GenericRow row = createRow(recordId, random);

                        // 写入数据（使用线程 ID 作为 bucket）
                        threadWriter.write(row, tid);

                        long written = totalWritten.incrementAndGet();
                        long accumulated = accumulatedRecords.incrementAndGet();

                        // 每 COMMIT_BATCH_SIZE 条记录提交一次
                        if (accumulated >= COMMIT_BATCH_SIZE) {
                            synchronized (commitLock) {
                                // 再次检查，避免重复提交
                                if (accumulatedRecords.get() >= COMMIT_BATCH_SIZE) {
                                    commitAllWriters();
                                    accumulatedRecords.set(0);
                                }
                            }
                        }

                        // 每 10 秒或每 100000 条记录报告一次进度（只由线程 0 报告）
                        if (tid == 0) {
                            long currentTime = System.currentTimeMillis();
                            if (written % 100000 == 0 || (currentTime - lastReportTime.get()) >= 10000) {
                                long timeDiff = currentTime - lastReportTime.get();
                                long countDiff = written - lastReportCount.get();

                                if (timeDiff > 0) {
                                    double progress = (double) written / TOTAL_RECORDS * 100;
                                    double currentThroughput = (countDiff * 1000.0 / timeDiff);
                                    double overallThroughput = (written * 1000.0 / (currentTime - startTime));

                                    System.out.printf("进度: %.2f%% | 已写入: %s 条 | 当前吞吐: %s 条/秒 | 平均吞吐: %s 条/秒\n",
                                            progress,
                                            formatNumber(written),
                                            formatNumber((long) currentThroughput),
                                            formatNumber((long) overallThroughput));

                                    lastReportTime.set(currentTime);
                                    lastReportCount.set(written);
                                }
                            }
                        }
                    }

                } catch (Exception e) {
                    System.err.println("线程 " + tid + " 写入失败: " + e.getMessage());
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            });

            futures.add(future);
        }

        // 等待所有线程完成
        System.out.println("等待所有写入线程完成...");
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                System.err.println("线程执行失败: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // 提交剩余数据
        if (accumulatedRecords.get() > 0) {
            System.out.println("\n正在提交剩余数据...");
            commitAllWriters();
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        // 输出最终统计
        printFinalStatistics(totalWritten.get(), totalTime);
    }

    /**
     * 创建一行数据
     */
    private GenericRow createRow(long id, Random random) {
        GenericRow row = new GenericRow(10);

        // 字段 0: id
        row.setField(0, id);

        // 字段 1: name
        row.setField(1, org.apache.paimon.data.BinaryString.fromString("User_" + id));

        // 字段 2: age
        row.setField(2, 18 + random.nextInt(60));

        // 字段 3: email
        row.setField(3, org.apache.paimon.data.BinaryString.fromString("user" + id + "@example.com"));

        // 字段 4: phone
        row.setField(4, org.apache.paimon.data.BinaryString.fromString(generatePhone(random)));

        // 字段 5: address
        row.setField(5, org.apache.paimon.data.BinaryString.fromString(generateAddress(random)));

        // 字段 6: city
        row.setField(6, org.apache.paimon.data.BinaryString.fromString(generateCity(random)));

        // 字段 7: country
        row.setField(7, org.apache.paimon.data.BinaryString.fromString(generateCountry(random)));

        // 字段 8: created_at
        row.setField(8, Timestamp.fromEpochMillis(System.currentTimeMillis()));

        // 字段 9: updated_at
        row.setField(9, Timestamp.fromEpochMillis(System.currentTimeMillis()));

        return row;
    }

    /**
     * 提交所有 Writers 的数据
     */
    private void commitAllWriters() throws Exception {
        long commitId = commitIdentifier.incrementAndGet();

        // 收集所有 Writers 的 CommitMessage
        List<CommitMessage> allMessages = new ArrayList<>();
        for (StreamTableWrite writer : writers) {
            List<CommitMessage> messages = writer.prepareCommit(false, commitId);
            allMessages.addAll(messages);
        }

        // 统一提交
        commit.commit(commitId, allMessages);
    }

    /**
     * 生成随机电话号码
     */
    private String generatePhone(Random random) {
        return String.format("+1-%03d-%03d-%04d",
                random.nextInt(1000),
                random.nextInt(1000),
                random.nextInt(10000));
    }

    /**
     * 生成随机地址
     */
    private String generateAddress(Random random) {
        String[] streets = {"Main St", "Oak Ave", "Maple Dr", "Pine Rd", "Cedar Ln"};
        return random.nextInt(9999) + " " + streets[random.nextInt(streets.length)];
    }

    /**
     * 生成随机城市
     */
    private String generateCity(Random random) {
        String[] cities = {"New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
                "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"};
        return cities[random.nextInt(cities.length)];
    }

    /**
     * 生成随机国家
     */
    private String generateCountry(Random random) {
        String[] countries = {"USA", "Canada", "UK", "Germany", "France", "Japan", "China", "Australia"};
        return countries[random.nextInt(countries.length)];
    }

    /**
     * 打印最终统计信息
     */
    private void printFinalStatistics(long totalRecords, long totalTimeMs) {
        System.out.println("\n========================================");
        System.out.println("写入测试完成！");
        System.out.println("========================================");
        System.out.println("总记录数: " + formatNumber(totalRecords));
        System.out.println("写入线程数: " + WRITE_THREADS);
        System.out.println("总耗时: " + formatTime(totalTimeMs));

        double throughput = totalTimeMs > 0 ? (totalRecords * 1000.0 / totalTimeMs) : 0;
        System.out.println("平均吞吐量: " + formatNumber((long) throughput) + " 条/秒");

        double perThreadThroughput = throughput / WRITE_THREADS;
        System.out.println("单线程吞吐量: " + formatNumber((long) perThreadThroughput) + " 条/秒");

        long totalCommits = (totalRecords + COMMIT_BATCH_SIZE - 1) / COMMIT_BATCH_SIZE;
        System.out.println("总提交次数: " + formatNumber(totalCommits));

        double avgCommitTime = totalCommits > 0 ? (totalTimeMs / (double) totalCommits) : 0;
        System.out.println("平均提交耗时: " + String.format("%.2f", avgCommitTime) + " 毫秒");

        // 估算数据大小（每条记录约 200 字节）
        long estimatedDataSize = totalRecords * 200;
        System.out.println("估算数据大小: " + formatBytes(estimatedDataSize));

        double dataThroughput = totalTimeMs > 0 ? (estimatedDataSize * 1000.0 / totalTimeMs) : 0;
        System.out.println("数据吞吐量: " + formatBytes((long) dataThroughput) + "/秒");

        System.out.println("\n性能提示:");
        System.out.println("- 使用原生 Paimon API，无中间层开销");
        System.out.println("- " + WRITE_THREADS + " 线程并发写入，充分利用多核 CPU");
        System.out.println("- 批量提交优化（每 " + formatNumber(COMMIT_BATCH_SIZE) + " 条提交一次）");
        System.out.println("- S3A 性能优化已启用");
        System.out.println("- 建议在写入完成后手动执行压缩以优化查询性能");

        System.out.println("========================================\n");
    }

    /**
     * 格式化数字（添加千分位分隔符）
     */
    private String formatNumber(long number) {
        DecimalFormat formatter = new DecimalFormat("#,###");
        return formatter.format(number);
    }

    /**
     * 格式化时间
     */
    private String formatTime(long milliseconds) {
        long seconds = milliseconds / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;

        if (hours > 0) {
            return String.format("%d 小时 %d 分钟 %d 秒", hours, minutes % 60, seconds % 60);
        } else if (minutes > 0) {
            return String.format("%d 分钟 %d 秒", minutes, seconds % 60);
        } else {
            return String.format("%d 秒", seconds);
        }
    }

    /**
     * 格式化字节大小
     */
    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }
}
