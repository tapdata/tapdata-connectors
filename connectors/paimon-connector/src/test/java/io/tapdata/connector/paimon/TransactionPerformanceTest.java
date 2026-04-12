//package io.tapdata.connector.paimon;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.paimon.catalog.Catalog;
//import org.apache.paimon.catalog.CatalogContext;
//import org.apache.paimon.catalog.CatalogFactory;
//import org.apache.paimon.catalog.Identifier;
//import org.apache.paimon.data.BinaryString;
//import org.apache.paimon.data.Decimal;
//import org.apache.paimon.data.GenericRow;
//import org.apache.paimon.data.Timestamp;
//import org.apache.paimon.options.Options;
//import org.apache.paimon.schema.Schema;
//import org.apache.paimon.table.Table;
//import org.apache.paimon.table.sink.CommitMessage;
//import org.apache.paimon.table.sink.StreamTableCommit;
//import org.apache.paimon.table.sink.StreamTableWrite;
//import org.apache.paimon.table.sink.StreamWriteBuilder;
//import org.apache.paimon.types.DataTypes;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//import java.math.BigDecimal;
//import java.text.DecimalFormat;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Random;
//import java.util.UUID;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicLong;
//
///**
// * Paimon 1.3.1 性能测试 - 交易明细表（29 字段，单条 1KB）
// *
// * 特点：
// * - 基于真实业务表结构（TransactionDetails）
// * - 29 个字段，单条记录约 1KB
// * - 主键表（Id 为主键）
// * - 支持分区测试（天/月/年）
// * - 8 线程并发写入
// * - 本地文件系统存储
// *
// * @author Tapdata Connectors Team
// * @version 2.0 (Paimon 1.3.1)
// */
//public class TransactionPerformanceTest {
//
//    // ===== 仓库路径配置 =====
//    private static final String WAREHOUSE_PATH = "/Users/your-username/workspace/tapdata-connectors/test-output/paimon-warehouse";
//
//    // ===== 测试配置 =====
//    private static final int BENCHMARK_RECORDS = 100_000;    // 基准测试：10 万条
//    private static final int PRESSURE_RECORDS = 1_000_000;   // 压力测试：100 万条
//    private static final int WRITE_THREADS = 8;              // 8 个写入线程
//    private static final int COMMIT_BATCH_SIZE = 10000;      // 每 1 万条提交一次
//    private static final String DATABASE_NAME = "ods_ewallet";
//
//    // Paimon 原生对象
//    private Catalog catalog;
//    private List<StreamTableWrite> writers;
//    private StreamTableCommit commit;
//    private AtomicLong commitIdentifier = new AtomicLong(0);
//
//    // 线程池
//    private ExecutorService executorService;
//
//    // 随机数生成器
//    private final Random random = new Random();
//
//    // 数据常量
//    private static final String[] TRANSACTION_TYPES = {"PAYMENT", "REFUND", "TRANSFER", "WITHDRAWAL", "DEPOSIT"};
//    private static final String[] CHANNELS = {"MOBILE_APP", "WEB", "POS", "ATM", "COUNTER"};
//    private static final String[] CURRENCIES = {"SGD", "USD", "EUR", "MYR", "THB"};
//    private static final String[] OUTLETS = {"Marina Bay Sands", "Sentosa Resort", "Changi Airport", "Orchard Road", "Bugis Junction"};
//    private static final String[] PROPERTIES = {"CASINO", "HOTEL", "RESTAURANT", "RETAIL", "ENTERTAINMENT"};
//
//    @BeforeEach
//    public void setUp() throws Exception {
//        System.out.println("========================================");
//        System.out.println("Paimon 1.3.1 交易明细表性能测试");
//        System.out.println("========================================");
//        System.out.println("仓库路径：" + WAREHOUSE_PATH);
//        System.out.println("数据库：" + DATABASE_NAME);
//        System.out.println("========================================\n");
//
//        // 初始化 Paimon Catalog
//        System.out.println("初始化 Paimon Catalog...");
//        initCatalog();
//
//        // 创建线程池
//        executorService = Executors.newFixedThreadPool(WRITE_THREADS);
//    }
//
//    @AfterEach
//    public void tearDown() throws Exception {
//        System.out.println("\n正在关闭资源...");
//
//        // 关闭线程池
//        if (executorService != null) {
//            executorService.shutdown();
//            try {
//                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
//                    executorService.shutdownNow();
//                }
//            } catch (InterruptedException e) {
//                executorService.shutdownNow();
//            }
//        }
//
//        // 关闭所有 Writers
//        if (writers != null) {
//            for (StreamTableWrite writer : writers) {
//                try {
//                    writer.close();
//                } catch (Exception e) {
//                    // Ignore
//                }
//            }
//        }
//
//        if (commit != null) {
//            try {
//                commit.close();
//            } catch (Exception e) {
//                // Ignore
//            }
//        }
//
//        if (catalog != null) {
//            try {
//                catalog.close();
//            } catch (Exception e) {
//                // Ignore
//            }
//        }
//
//        System.out.println("资源已关闭");
//    }
//
//    /**
//     * 初始化 Paimon Catalog
//     */
//    private void initCatalog() throws Exception {
//        Options options = new Options();
//        options.set("warehouse", "file://" + WAREHOUSE_PATH);
//
//        Configuration hadoopConf = new Configuration();
//        CatalogContext context = CatalogContext.create(options, hadoopConf);
//        catalog = CatalogFactory.createCatalog(context);
//
//        // 确保数据库存在
//        try {
//            catalog.getDatabase(DATABASE_NAME);
//        } catch (Catalog.DatabaseNotExistException e) {
//            catalog.createDatabase(DATABASE_NAME, true);
//        }
//    }
//
//    /**
//     * 创建按天分区表
//     */
//    protected void createDailyPartitionTable(String tableName) throws Exception {
//        Identifier identifier = Identifier.create(DATABASE_NAME, tableName);
//
//        // 检查表是否已存在
//        try {
//            catalog.getTable(identifier);
//            System.out.println("表已存在：" + tableName);
//            return;
//        } catch (Catalog.TableNotExistException e) {
//            // 表不存在，创建新表
//        }
//
//        Schema.Builder schemaBuilder = createSchemaBuilder();
//
//        // 按天分区配置
//        schemaBuilder.partitionKeys("pt_created_date");
//
//        // 性能优化参数
//        schemaBuilder.option("bucket", "-1")                    // 动态分桶
//                .option("target-file-size", "256mb")            // 目标文件大小
//                .option("write-buffer-size", "512mb")           // 写入缓冲区
//                .option("sink.parallelism", "8")                // 并发度
//                .option("compaction.async.enabled", "true")     // 异步 Compaction
//                .option("compaction.optimization-interval", "10min")
//                .option("num-sorted-run.compaction-trigger", "4")
//                .option("num-sorted-run.stop-trigger", "8")
//                .option("changelog-producer", "input")
//                .option("changelog-producer.lookup-wait", "false")
//                .option("snapshot.num-retained.min", "5")
//                .option("snapshot.num-retained.max", "500")
//                .option("snapshot.time-retained", "7d")
//                .option("commit.force-compact", "false")
//                .option("file.format", "parquet")
//                .option("file.compression", "zstd");
//
//        catalog.createTable(identifier, schemaBuilder.build(), false);
//        System.out.println("创建按天分区表：" + tableName);
//    }
//
//    /**
//     * 创建按月分区表
//     */
//    protected void createMonthlyPartitionTable(String tableName) throws Exception {
//        Identifier identifier = Identifier.create(DATABASE_NAME, tableName);
//
//        try {
//            catalog.getTable(identifier);
//            System.out.println("表已存在：" + tableName);
//            return;
//        } catch (Catalog.TableNotExistException e) {
//            // 表不存在
//        }
//
//        Schema.Builder schemaBuilder = createSchemaBuilder();
//        schemaBuilder.partitionKeys("pt_created_date");
//
//        // 按月分区配置
//        schemaBuilder.option("bucket", "-1")
//                .option("target-file-size", "256mb")
//                .option("write-buffer-size", "512mb")
//                .option("sink.parallelism", "8")
//                .option("compaction.async.enabled", "true")
//                .option("compaction.optimization-interval", "10min")
//                .option("num-sorted-run.compaction-trigger", "4")
//                .option("num-sorted-run.stop-trigger", "8")
//                .option("changelog-producer", "input")
//                .option("changelog-producer.lookup-wait", "false")
//                .option("snapshot.num-retained.min", "5")
//                .option("snapshot.num-retained.max", "500")
//                .option("snapshot.time-retained", "7d")
//                .option("commit.force-compact", "false")
//                .option("file.format", "parquet")
//                .option("file.compression", "zstd");
//
//        catalog.createTable(identifier, schemaBuilder.build(), false);
//        System.out.println("创建按月分区表：" + tableName);
//    }
//
//    /**
//     * 创建按年分区表
//     */
//    protected void createYearlyPartitionTable(String tableName) throws Exception {
//        Identifier identifier = Identifier.create(DATABASE_NAME, tableName);
//
//        try {
//            catalog.getTable(identifier);
//            System.out.println("表已存在：" + tableName);
//            return;
//        } catch (Catalog.TableNotExistException e) {
//            // 表不存在
//        }
//
//        Schema.Builder schemaBuilder = createSchemaBuilder();
//        schemaBuilder.partitionKeys("pt_created_date");
//
//        // 按年分区配置
//        schemaBuilder.option("bucket", "-1")
//                .option("target-file-size", "256mb")
//                .option("write-buffer-size", "512mb")
//                .option("sink.parallelism", "8")
//                .option("compaction.async.enabled", "true")
//                .option("compaction.optimization-interval", "10min")
//                .option("num-sorted-run.compaction-trigger", "4")
//                .option("num-sorted-run.stop-trigger", "8")
//                .option("changelog-producer", "input")
//                .option("changelog-producer.lookup-wait", "false")
//                .option("snapshot.num-retained.min", "5")
//                .option("snapshot.num-retained.max", "500")
//                .option("snapshot.time-retained", "7d")
//                .option("commit.force-compact", "false")
//                .option("file.format", "parquet")
//                .option("file.compression", "zstd");
//
//        catalog.createTable(identifier, schemaBuilder.build(), false);
//        System.out.println("创建按年分区表：" + tableName);
//    }
//
//    /**
//     * 创建 Schema Builder（29 个字段）
//     */
//    protected Schema.Builder createSchemaBuilder() {
//        Schema.Builder schemaBuilder = Schema.newBuilder();
//
//        // 添加 29 个字段
//        schemaBuilder.column("Id", DataTypes.VARCHAR(900), "主键 ID");
//        schemaBuilder.column("BalanceDetailId", DataTypes.VARCHAR(8000), "余额明细 ID");
//        schemaBuilder.column("BeforeDetailBalance", DataTypes.DECIMAL(18, 0), "明细前余额");
//        schemaBuilder.column("Amount", DataTypes.DECIMAL(18, 0), "交易金额");
//        schemaBuilder.column("ExpiryDate", DataTypes.TIMESTAMP(6), "过期时间");
//        schemaBuilder.column("ComporId", DataTypes.VARCHAR(10), "组件 ID");
//        schemaBuilder.column("TransactionType", DataTypes.VARCHAR(50), "交易类型");
//        schemaBuilder.column("Channel", DataTypes.VARCHAR(200), "渠道");
//        schemaBuilder.column("POSReference", DataTypes.VARCHAR(500), "POS 参考号");
//        schemaBuilder.column("Outlet", DataTypes.VARCHAR(500), "门店名称");
//        schemaBuilder.column("Remark", DataTypes.VARCHAR(500), "备注");
//        schemaBuilder.column("CreatedTime", DataTypes.TIMESTAMP(6), "创建时间");
//        schemaBuilder.column("PaymentDetailId", DataTypes.VARCHAR(8000), "支付明细 ID");
//        schemaBuilder.column("PaymentId", DataTypes.BIGINT(), "支付 ID");
//        schemaBuilder.column("CreatedBy", DataTypes.VARCHAR(100), "创建人");
//        schemaBuilder.column("op", DataTypes.VARCHAR(1024), "CDC 操作类型");
//        schemaBuilder.column("AfterDetailBalance", DataTypes.DECIMAL(18, 0), "明细后余额");
//        schemaBuilder.column("SourceSystem", DataTypes.VARCHAR(200), "源系统");
//        schemaBuilder.column("DollarTypeId", DataTypes.VARCHAR(50), "币种 ID");
//        schemaBuilder.column("ExceptionBalance", DataTypes.DECIMAL(18, 0), "异常余额");
//        schemaBuilder.column("PatronId", DataTypes.VARCHAR(80), "客户 ID");
//        schemaBuilder.column("SourceKey", DataTypes.VARCHAR(200), "源键值");
//        schemaBuilder.column("DeviceId", DataTypes.VARCHAR(50), "设备 ID");
//        schemaBuilder.column("AfterBalance", DataTypes.DECIMAL(18, 0), "交易后余额");
//        schemaBuilder.column("BeforeBalance", DataTypes.DECIMAL(18, 0), "交易前余额");
//        schemaBuilder.column("OutletCode", DataTypes.VARCHAR(15), "门店代码");
//        schemaBuilder.column("ods_updated_at", DataTypes.TIMESTAMP(3), "ODS 更新时间");
//        schemaBuilder.column("Property", DataTypes.VARCHAR(200), "物业");
//        schemaBuilder.column("DisplaySourceKey", DataTypes.VARCHAR(200), "显示源键");
//        schemaBuilder.column("SourceSystemExtraReference", DataTypes.VARCHAR(200), "源系统扩展参考");
//        schemaBuilder.column("SegmentCode", DataTypes.VARCHAR(40), "分段代码");
//        schemaBuilder.column("pt_created_date", DataTypes.INT(), "分区字段");
//
//        // 设置主键
//        schemaBuilder.primaryKey("Id");
//
//        return schemaBuilder;
//    }
//
//    /**
//     * 初始化 Writers 和 Commit
//     */
//    protected void initWritersAndCommit(String tableName) throws Exception {
//        Identifier identifier = Identifier.create(DATABASE_NAME, tableName);
//        Table table = catalog.getTable(identifier);
//
//        StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
//
//        writers = new ArrayList<>(WRITE_THREADS);
//        for (int i = 0; i < WRITE_THREADS; i++) {
//            writers.add(writeBuilder.newWrite());
//        }
//
//        commit = writeBuilder.newCommit();
//    }
//
//    /**
//     * 测试按天分区写入性能
//     */
//    @Test
//    public void testDailyPartitionWrite() throws Exception {
//        String tableName = "TransactionDetails_day";
//
//        System.out.println("\n========================================");
//        System.out.println("测试：按天分区写入性能");
//        System.out.println("表名：" + tableName);
//        System.out.println("数据量：" + PRESSURE_RECORDS + " 条");
//        System.out.println("========================================\n");
//
//        // 创建表
//        createDailyPartitionTable(tableName);
//
//        // 初始化 Writers
//        initWritersAndCommit(tableName);
//
//        // 执行写入
//        writeRecords(tableName, PRESSURE_RECORDS);
//    }
//
//    /**
//     * 测试按月分区写入性能
//     */
//    @Test
//    public void testMonthlyPartitionWrite() throws Exception {
//        String tableName = "TransactionDetails_month";
//
//        System.out.println("\n========================================");
//        System.out.println("测试：按月分区写入性能");
//        System.out.println("表名：" + tableName);
//        System.out.println("数据量：" + PRESSURE_RECORDS + " 条");
//        System.out.println("========================================\n");
//
//        createMonthlyPartitionTable(tableName);
//        initWritersAndCommit(tableName);
//        writeRecords(tableName, PRESSURE_RECORDS);
//    }
//
//    /**
//     * 测试按年分区写入性能
//     */
//    @Test
//    public void testYearlyPartitionWrite() throws Exception {
//        String tableName = "TransactionDetails_year";
//
//        System.out.println("\n========================================");
//        System.out.println("测试：按年分区写入性能");
//        System.out.println("表名：" + tableName);
//        System.out.println("数据量：" + PRESSURE_RECORDS + " 条");
//        System.out.println("========================================\n");
//
//        createYearlyPartitionTable(tableName);
//        initWritersAndCommit(tableName);
//        writeRecords(tableName, PRESSURE_RECORDS);
//    }
//
//    /**
//     * 执行数据写入
//     */
//    protected void writeRecords(String tableName, int totalRecords) throws Exception {
//        long startTime = System.currentTimeMillis();
//
//        AtomicLong totalWritten = new AtomicLong(0);
//        AtomicLong lastReportTime = new AtomicLong(startTime);
//        AtomicLong lastReportCount = new AtomicLong(0);
//
//        int recordsPerThread = totalRecords / WRITE_THREADS;
//        ConcurrentLinkedQueue<CommitMessage> allMessages = new ConcurrentLinkedQueue<>();
//        Object commitLock = new Object();
//        AtomicLong accumulatedRecords = new AtomicLong(0);
//
//        List<Future<?>> futures = new ArrayList<>();
//
//        for (int threadId = 0; threadId < WRITE_THREADS; threadId++) {
//            final int tid = threadId;
//            final long startRecordId = (long) tid * recordsPerThread;
//            final long endRecordId = (tid == WRITE_THREADS - 1) ? totalRecords : (tid + 1) * recordsPerThread;
//            final StreamTableWrite threadWriter = writers.get(tid);
//
//            Future<?> future = executorService.submit(() -> {
//                try {
//                    Random threadRandom = new Random(tid);
//
//                    for (long recordId = startRecordId; recordId < endRecordId; recordId++) {
//                        // 创建交易数据行
//                        GenericRow row = createTransactionRow(recordId, threadRandom);
//
//                        // 计算分区（按天）
//                        int partitionDate = calculatePartitionDate(recordId);
//                        row.setField(29, partitionDate);
//
//                        // 写入数据
//                        threadWriter.write(row, tid);
//
//                        long written = totalWritten.incrementAndGet();
//                        long accumulated = accumulatedRecords.incrementAndGet();
//
//                        // 批量提交
//                        if (accumulated >= COMMIT_BATCH_SIZE) {
//                            synchronized (commitLock) {
//                                if (accumulatedRecords.get() >= COMMIT_BATCH_SIZE) {
//                                    commitAllWriters();
//                                    accumulatedRecords.set(0);
//                                }
//                            }
//                        }
//
//                        // 报告进度（仅线程 0）
//                        if (tid == 0 && written % 10000 == 0) {
//                            long currentTime = System.currentTimeMillis();
//                            long timeDiff = currentTime - lastReportTime.get();
//                            long countDiff = written - lastReportCount.get();
//
//                            if (timeDiff > 0) {
//                                double progress = (double) written / totalRecords * 100;
//                                double currentThroughput = (countDiff * 1000.0 / timeDiff);
//                                double overallThroughput = (written * 1000.0 / (currentTime - startTime));
//
//                                System.out.printf("进度：%.1f%% | 已写入：%s 条 | 当前吞吐：%s 条/秒 | 平均吞吐：%s 条/秒\n",
//                                        progress,
//                                        formatNumber(written),
//                                        formatNumber((long) currentThroughput),
//                                        formatNumber((long) overallThroughput));
//
//                                lastReportTime.set(currentTime);
//                                lastReportCount.set(written);
//                            }
//                        }
//                    }
//
//                } catch (Exception e) {
//                    System.err.println("线程 " + tid + " 写入失败：" + e.getMessage());
//                    e.printStackTrace();
//                    throw new RuntimeException(e);
//                }
//            });
//
//            futures.add(future);
//        }
//
//        // 等待所有线程完成
//        System.out.println("等待所有写入线程完成...");
//        for (Future<?> future : futures) {
//            try {
//                future.get();
//            } catch (Exception e) {
//                System.err.println("线程执行失败：" + e.getMessage());
//                e.printStackTrace();
//            }
//        }
//
//        // 提交剩余数据
//        if (accumulatedRecords.get() > 0) {
//            System.out.println("\n正在提交剩余数据...");
//            commitAllWriters();
//        }
//
//        long endTime = System.currentTimeMillis();
//        long totalTime = endTime - startTime;
//
//        // 输出统计
//        printFinalStatistics(totalWritten.get(), totalTime, tableName);
//    }
//
//    /**
//     * 创建交易数据行（29 个字段，单条约 1KB）
//     */
//    protected GenericRow createTransactionRow(long id, Random random) {
//        GenericRow row = new GenericRow(30); // 29 个字段 + 1 个分区字段
//
//        long timestamp = System.currentTimeMillis() - random.nextInt(86400000); // 随机时间（1 天内）
//        Timestamp ts = Timestamp.fromEpochMillis(timestamp);
//
//        // 字段 0: Id (主键)
//        row.setField(0, BinaryString.fromString("TXN_" + System.currentTimeMillis() + "_" + id));
//
//        // 字段 1: BalanceDetailId
//        row.setField(1, BinaryString.fromString("BAL_" + System.currentTimeMillis() + "_" + id));
//
//        // 字段 2: BeforeDetailBalance (DECIMAL)
//        row.setField(2, Decimal.fromBigDecimal(new BigDecimal(random.nextLong(1000000, 10000000L)), 18, 0));
//
//        // 字段 3: Amount (DECIMAL)
//        row.setField(3, Decimal.fromBigDecimal(new BigDecimal(random.nextLong(1000, 500000)), 18, 0));
//
//        // 字段 4: ExpiryDate
//        row.setField(4, Timestamp.fromEpochMillis(timestamp + 86400000L * 30)); // 30 天后过期
//
//        // 字段 5: ComporId
//        row.setField(5, BinaryString.fromString("COMP_" + String.format("%03d", random.nextInt(1000))));
//
//        // 字段 6: TransactionType
//        row.setField(6, BinaryString.fromString(TRANSACTION_TYPES[random.nextInt(TRANSACTION_TYPES.length)]));
//
//        // 字段 7: Channel
//        row.setField(7, BinaryString.fromString(CHANNELS[random.nextInt(CHANNELS.length)]));
//
//        // 字段 8: POSReference
//        row.setField(8, BinaryString.fromString("POS_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16)));
//
//        // 字段 9: Outlet
//        row.setField(9, BinaryString.fromString(OUTLETS[random.nextInt(OUTLETS.length)]));
//
//        // 字段 10: Remark
//        row.setField(10, BinaryString.fromString("Transaction processed successfully at " + ts.toString()));
//
//        // 字段 11: CreatedTime
//        row.setField(11, ts);
//
//        // 字段 12: PaymentDetailId
//        row.setField(12, BinaryString.fromString("PAY_DTL_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12)));
//
//        // 字段 13: PaymentId
//        row.setField(13, random.nextLong(1000000000L, 9999999999L));
//
//        // 字段 14: CreatedBy
//        row.setField(14, BinaryString.fromString("system"));
//
//        // 字段 15: op (CDC 操作类型)
//        row.setField(15, BinaryString.fromString(random.nextBoolean() ? "INSERT" : "UPDATE"));
//
//        // 字段 16: AfterDetailBalance
//        row.setField(16, Decimal.fromBigDecimal(new BigDecimal(random.nextLong(1000000, 10000000L)), 18, 0));
//
//        // 字段 17: SourceSystem
//        row.setField(17, BinaryString.fromString("WALLET_CORE"));
//
//        // 字段 18: DollarTypeId
//        row.setField(18, BinaryString.fromString(CURRENCIES[random.nextInt(CURRENCIES.length)]));
//
//        // 字段 19: ExceptionBalance
//        row.setField(19, Decimal.fromBigDecimal(BigDecimal.ZERO, 18, 0));
//
//        // 字段 20: PatronId
//        row.setField(20, BinaryString.fromString("CUST_" + String.format("%05d", random.nextInt(100000))));
//
//        // 字段 21: SourceKey
//        row.setField(21, BinaryString.fromString("SRC_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12)));
//
//        // 字段 22: DeviceId
//        row.setField(22, BinaryString.fromString("DEV_" + String.format("%03d", random.nextInt(1000))));
//
//        // 字段 23: AfterBalance
//        row.setField(23, Decimal.fromBigDecimal(new BigDecimal(random.nextLong(1000000, 20000000L)), 18, 0));
//
//        // 字段 24: BeforeBalance
//        row.setField(24, Decimal.fromBigDecimal(new BigDecimal(random.nextLong(1000000, 20000000L)), 18, 0));
//
//        // 字段 25: OutletCode
//        row.setField(25, BinaryString.fromString("OUT_" + String.format("%03d", random.nextInt(1000))));
//
//        // 字段 26: ods_updated_at
//        row.setField(26, Timestamp.fromEpochMillis(timestamp));
//
//        // 字段 27: Property
//        row.setField(27, BinaryString.fromString(PROPERTIES[random.nextInt(PROPERTIES.length)]));
//
//        // 字段 28: DisplaySourceKey
//        row.setField(28, BinaryString.fromString("DISP_" + String.format("%03d", random.nextInt(1000))));
//
//        // 字段 29: SourceSystemExtraReference
//        row.setField(29, BinaryString.fromString("EXT_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12)));
//
//        // 字段 30: SegmentCode (实际是第 29 个字段，索引 28)
//        // 字段 31: pt_created_date (分区字段，索引 29)
//
//        return row;
//    }
//
//    /**
//     * 计算分区日期
//     */
//    protected int calculatePartitionDate(long recordId) {
//        // 模拟 30 天内的数据
//        long daysAgo = recordId % 30;
//        long timestamp = System.currentTimeMillis() - daysAgo * 86400000L;
//        return Integer.parseInt(new java.text.SimpleDateFormat("yyyyMMdd").format(new java.util.Date(timestamp)));
//    }
//
//    /**
//     * 提交所有 Writers 的数据
//     */
//    protected void commitAllWriters() throws Exception {
//        long commitId = commitIdentifier.incrementAndGet();
//
//        List<CommitMessage> allMessages = new ArrayList<>();
//        for (StreamTableWrite writer : writers) {
//            List<CommitMessage> messages = writer.prepareCommit(false, commitId);
//            allMessages.addAll(messages);
//        }
//
//        commit.commit(commitId, allMessages);
//    }
//
//    /**
//     * 输出最终统计信息
//     */
//    protected void printFinalStatistics(long totalRecords, long totalTimeMs, String tableName) {
//        System.out.println("\n========================================");
//        System.out.println("写入测试完成！");
//        System.out.println("========================================");
//        System.out.println("表名：" + tableName);
//        System.out.println("总记录数：" + formatNumber(totalRecords));
//        System.out.println("写入线程数：" + WRITE_THREADS);
//        System.out.println("总耗时：" + formatTime(totalTimeMs));
//
//        double throughput = totalTimeMs > 0 ? (totalRecords * 1000.0 / totalTimeMs) : 0;
//        System.out.println("平均吞吐量：" + formatNumber((long) throughput) + " 条/秒");
//
//        double perThreadThroughput = throughput / WRITE_THREADS;
//        System.out.println("单线程吞吐量：" + formatNumber((long) perThreadThroughput) + " 条/秒");
//
//        long totalCommits = (totalRecords + COMMIT_BATCH_SIZE - 1) / COMMIT_BATCH_SIZE;
//        System.out.println("总提交次数：" + formatNumber(totalCommits));
//
//        double avgCommitTime = totalCommits > 0 ? (totalTimeMs / (double) totalCommits) : 0;
//        System.out.println("平均提交耗时：" + String.format("%.2f", avgCommitTime) + " 毫秒");
//
//        // 估算数据大小（每条记录约 1KB）
//        long estimatedDataSize = totalRecords * 1024;
//        System.out.println("估算数据大小：" + formatBytes(estimatedDataSize));
//
//        double dataThroughput = totalTimeMs > 0 ? (estimatedDataSize * 1000.0 / totalTimeMs) : 0;
//        System.out.println("数据吞吐量：" + formatBytes((long) dataThroughput) + "/秒");
//
//        System.out.println("========================================\n");
//    }
//
//    /**
//     * 格式化数字
//     */
//    protected String formatNumber(long number) {
//        DecimalFormat formatter = new DecimalFormat("#,###");
//        return formatter.format(number);
//    }
//
//    /**
//     * 格式化时间
//     */
//    protected String formatTime(long milliseconds) {
//        long seconds = milliseconds / 1000;
//        long minutes = seconds / 60;
//        long hours = minutes / 60;
//
//        if (hours > 0) {
//            return String.format("%d 小时 %d 分钟 %d 秒", hours, minutes % 60, seconds % 60);
//        } else if (minutes > 0) {
//            return String.format("%d 分钟 %d 秒", minutes, seconds % 60);
//        } else {
//            return String.format("%d 秒", seconds);
//        }
//    }
//
//    /**
//     * 格式化字节大小
//     */
//    protected String formatBytes(long bytes) {
//        if (bytes < 1024) {
//            return bytes + " B";
//        } else if (bytes < 1024 * 1024) {
//            return String.format("%.2f KB", bytes / 1024.0);
//        } else if (bytes < 1024 * 1024 * 1024) {
//            return String.format("%.2f MB", bytes / (1024.0 * 1024));
//        } else {
//            return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
//        }
//    }
//}
