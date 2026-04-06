package io.tapdata.connector.paimon.perf;

import org.apache.paimon.catalog.*;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataTypes;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Paimon 写入性能参数调优测试运行器 - 自动模式（无需交互）
 */
public class AutoTestRunner {

    private static final String WAREHOUSE_BASE = "/tmp/paimon-perf-test-" + System.currentTimeMillis();
    private static final String DATABASE = "perf_db";
    private static final List<String> PRIMARY_KEYS = Collections.singletonList("id");
    
    private static final List<TestCase> TEST_CASES = Arrays.asList(
        new TestCase("CASE-001", "基准配置 (Baseline)", 
            map("bucket", "-1", "write-buffer-size", "256mb", "target-file-size", "128mb", 
                "file.format", "parquet", "compaction.async.enabled", "true",
                "num-sorted-run.compaction-trigger", "3", "num-sorted-run.stop-trigger", "5",
                "commit.force-compact", "false", "sink.parallelism", "4"),
            50000, 0.3),
            
        new TestCase("CASE-002", "大 write-buffer (512MB)",
            map("bucket", "-1", "write-buffer-size", "512mb", "target-file-size", "256mb",
                "file.format", "parquet", "compaction.async.enabled", "true",
                "num-sorted-run.compaction-trigger", "3", "num-sorted-run.stop-trigger", "5",
                "commit.force-compact", "false", "sink.parallelism", "4"),
            50000, 0.3),
            
        new TestCase("CASE-003", "超大 write-buffer (1GB) + 大文件 (512MB)",
            map("bucket", "-1", "write-buffer-size", "1gb", "target-file-size", "512mb",
                "file.format", "parquet", "compaction.async.enabled", "true",
                "num-sorted-run.compaction-trigger", "5", "num-sorted-run.stop-trigger", "10",
                "commit.force-compact", "false", "sink.parallelism", "8"),
            50000, 0.3),
            
        new TestCase("CASE-004", "bucket=-2 (延迟分桶/Deferred Bucket)",
            map("bucket", "-2", "write-buffer-size", "512mb", "target-file-size", "256mb",
                "file.format", "parquet", "compaction.async.enabled", "true",
                "num-sorted-run.compaction-trigger", "3", "num-sorted-run.stop-trigger", "8",
                "commit.force-compact", "false", "sink.parallelism", "4"),
            50000, 0.3),
            
        new TestCase("CASE-005", "无 Compaction (关闭合并)",
            map("bucket", "-1", "write-buffer-size", "512mb", "target-file-size", "256mb",
                "file.format", "parquet", "compaction.async.enabled", "false",
                "commit.force-compact", "false", "sink.parallelism", "4"),
            50000, 0.3),
            
        new TestCase("CASE-006", "ORC 格式 + 大缓冲区",
            map("bucket", "-1", "write-buffer-size", "512mb", "target-file-size", "256mb",
                "file.format", "orc", "compaction.async.enabled", "true",
                "num-sorted-run.compaction-trigger", "3", "num-sorted-run.stop-trigger", "5",
                "commit.force-compact", "false", "sink.parallelism", "4"),
            50000, 0.3),
            
        new TestCase("CASE-007", "高并行度 (sink.parallelism=16)",
            map("bucket", "-1", "write-buffer-size", "512mb", "target-file-size", "256mb",
                "file.format", "parquet", "compaction.async.enabled", "true",
                "num-sorted-run.compaction-trigger", "5", "num-sorted-run.stop-trigger", "10",
                "commit.force-compact", "false", "sink.parallelism", "16"),
            50000, 0.3)
    );

    public static void main(String[] args) throws Exception {
        printHeader();
        
        List<TestResult> results = new ArrayList<>();
        
        for (TestCase tc : TEST_CASES) {
            System.out.println("\n" + "─".repeat(70));
            System.out.println(">>> 执行: " + tc.id + " - " + tc.name);
            System.out.println("─".repeat(70));
            
            TestResult result = runSingleTest(tc);
            results.add(result);
            
            printResult(result);
        }
        
        generateReport(results);
    }

    private static TestResult runSingleTest(TestCase tc) throws Exception {
        String warehouse = WAREHOUSE_BASE + "/" + tc.id;
        Files.createDirectories(Paths.get(warehouse));
        
        System.out.println("\n  [配置]");
        System.out.printf("    数据量: %,d 条 | 重复率: %.0f%%%n", tc.totalRecords, tc.duplicateRate * 100);
        System.out.println("    参数:");
        for (Map.Entry<String, String> e : tc.tableOptions.entrySet()) {
            System.out.printf("      %-40s = %s%n", e.getKey(), e.getValue());
        }

        Options options = new Options();
        options.set("warehouse", warehouse);
        CatalogContext context = CatalogContext.create(options);
        Catalog catalog = CatalogFactory.createCatalog(context);
        
        try {
            catalog.createDatabase(DATABASE, true);
            
            String tableName = "test_table";
            Identifier tableId = Identifier.create(DATABASE, tableName);
            
            Schema.Builder schemaBuilder = Schema.newBuilder()
                    .column("id", DataTypes.BIGINT().notNull())
                    .column("balance_detail_id", DataTypes.STRING())
                    .column("before_detail_balance", DataTypes.DECIMAL(18, 0))
                    .column("amount", DataTypes.DECIMAL(18, 0))
                    .column("expiry_date", DataTypes.TIMESTAMP(6))
                    .column("compor_id", DataTypes.STRING())
                    .column("transaction_type", DataTypes.STRING())
                    .column("channel", DataTypes.STRING())
                    .column("pos_reference", DataTypes.STRING())
                    .column("outlet", DataTypes.STRING())
                    .column("remark", DataTypes.STRING())
                    .column("created_time", DataTypes.TIMESTAMP(6))
                    .column("payment_detail_id", DataTypes.STRING())
                    .column("payment_id", DataTypes.BIGINT())
                    .column("created_by", DataTypes.STRING())
                    .column("op", DataTypes.STRING())
                    .column("after_detail_balance", DataTypes.DECIMAL(18, 0))
                    .column("source_system", DataTypes.STRING())
                    .column("dollar_type_id", DataTypes.STRING())
                    .column("exception_balance", DataTypes.DECIMAL(18, 0))
                    .column("patron_id", DataTypes.STRING())
                    .column("source_key", DataTypes.STRING())
                    .column("device_id", DataTypes.STRING())
                    .column("after_balance", DataTypes.DECIMAL(18, 0))
                    .column("before_balance", DataTypes.DECIMAL(18, 0))
                    .column("outlet_code", DataTypes.STRING())
                    .column("ods_updated_at", DataTypes.TIMESTAMP(3))
                    .column("property", DataTypes.STRING())
                    .column("pt_created_date", DataTypes.INT())
                    .primaryKey(PRIMARY_KEYS);
            
            for (Map.Entry<String, String> opt : tc.tableOptions.entrySet()) {
                schemaBuilder.option(opt.getKey(), opt.getValue());
            }
            
            catalog.createTable(tableId, schemaBuilder.build(), false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(tableId);
            
            System.out.println("  [写入] 开始...");
            
            long startTime = System.currentTimeMillis();
            AtomicLong writeCount = new AtomicLong(0);
            
            StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();
            try (StreamTableWrite writer = writeBuilder.newWrite();
                 StreamTableCommit committer = writeBuilder.newCommit()) {
                
                PaimonDataGenerator generator = new PaimonDataGenerator(tc.totalRecords, 0, tc.duplicateRate, 10000);
                
                while (generator.hasMore()) {
                    InternalRow row = generator.nextRecord();
                    if (row == null) break;

                    // 在动态分桶模式下，使用 0 作为默认 bucket 值
                    writer.write(row, 0);
                    writeCount.incrementAndGet();
                    
                    if (writeCount.get() % 10000 == 0) {
                        long cpId = writeCount.get() / 10000;
                        List<org.apache.paimon.table.sink.CommitMessage> messages = writer.prepareCommit(false, cpId);
                        if (!messages.isEmpty()) {
                            committer.commit(cpId, messages);
                        }
                    }
                }

                long finalCp = (writeCount.get() / 10000) + 1;
                List<org.apache.paimon.table.sink.CommitMessage> finalMessages = writer.prepareCommit(true, finalCp);
                if (!finalMessages.isEmpty()) {
                    committer.commit(finalCp, finalMessages);
                }
            }
            
            long elapsed = System.currentTimeMillis() - startTime;
            double throughput = writeCount.get() * 1000.0 / Math.max(elapsed, 1);
            
            System.out.printf("  [完成] %,d 条, %,d ms, %.0f 条/秒%n", 
                writeCount.get(), elapsed, throughput);
            
            FileScanResult fileScan = scanFiles(warehouse);
            
            return new TestResult(tc, writeCount.get(), elapsed, throughput, fileScan);
            
        } finally {
            catalog.close();
        }
    }

    private static FileScanResult scanFiles(String warehouse) throws Exception {
        FileScanResult result = new FileScanResult();
        String tablePath = warehouse + "/" + DATABASE + ".db/test_table";
        File tableDir = new File(tablePath);
        
        if (!tableDir.exists()) return result;
        scanDirectory(tableDir, result);
        return result;
    }

    private static void scanDirectory(File dir, FileScanResult result) {
        File[] files = dir.listFiles();
        if (files == null) return;
        
        for (File f : files) {
            if (f.isDirectory()) {
                scanDirectory(f, result);
            } else if (f.getName().endsWith(".parquet") || f.getName().endsWith(".orc")) {
                result.fileCount++;
                result.totalSize += f.length();
                result.files.add(new FileInfo(f.getPath(), f.length()));
            }
        }
    }

    private static void printHeader() {
        System.out.println("\n" + "═".repeat(70));
        System.out.println("  Paimon 1.3.1 写入性能参数调优测试 (自动模式)");
        System.out.println("  日期: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        System.out.println("  Warehouse: " + WAREHOUSE_BASE);
        System.out.println("  CPU: " + Runtime.getRuntime().availableProcessors() + " 核");
        System.out.println("  JVM: " + (Runtime.getRuntime().maxMemory() / 1024 / 1024) + " MB");
        System.out.println("═".repeat(70));
        
        System.out.println("\n【测试用例】");
        for (TestCase tc : TEST_CASES) {
            System.out.printf("  %-10s | %-40s | %,d 条%n", tc.id, tc.name, tc.totalRecords);
        }
    }

    private static void printResult(TestResult result) {
        System.out.printf("%n  [结果] %s%n", result.testCase.id);
        System.out.printf("    吞吐: %.0f 条/秒 | 文件: %d 个 | 大小: %.2f MB%n",
            result.throughput, result.fileScan.fileCount, result.fileScan.totalSize / 1024.0 / 1024.0);
    }

    private static void generateReport(List<TestResult> results) throws Exception {
        System.out.println("\n" + "═".repeat(70));
        System.out.println("  测试报告");
        System.out.println("═".repeat(70));
        
        System.out.printf("%n%-10s | %-8s | %-8s | %-10s | %-8s | %-10s%n",
            "用例", "记录数", "耗时(ms)", "吞吐(条/s)", "文件数", "大小(MB)");
        System.out.println("-".repeat(70));
        
        TestResult best = null, fewest = null;
        
        for (TestResult r : results) {
            System.out.printf("%-10s | %,8d | %,8d | %,10.0f | %,8d | %,10.2f%n",
                r.testCase.id, r.recordCount, r.elapsedMs, r.throughput, 
                r.fileScan.fileCount, r.fileScan.totalSize / 1024.0 / 1024.0);
            
            if (best == null || r.throughput > best.throughput) best = r;
            if (fewest == null || r.fileScan.fileCount < fewest.fileScan.fileCount) fewest = r;
        }
        
        System.out.println("\n【核心结论】");
        System.out.println("1. 最高吞吐: " + best.testCase.id + " (" + best.testCase.name + ")");
        System.out.printf("   吞吐量: %.0f 条/秒%n", best.throughput);
        
        System.out.println("\n2. 最少文件: " + fewest.testCase.id + " (" + fewest.testCase.name + ")");
        System.out.printf("   文件数: %d 个, 总大小: %.2f MB%n", 
            fewest.fileScan.fileCount, fewest.fileScan.totalSize / 1024.0 / 1024.0);
        
        TestResult bucketDeferred = results.stream()
            .filter(r -> r.testCase.id.equals("CASE-004")).findFirst().orElse(null);
        
        if (bucketDeferred != null) {
            System.out.println("\n3. bucket=-2 (延迟分桶):");
            System.out.printf("   吞吐: %.0f vs 基准 %.0f (%.1f%%)%n",
                bucketDeferred.throughput, results.get(0).throughput,
                bucketDeferred.throughput / results.get(0).throughput * 100 - 100);
        }
        
        System.out.println("\n【推荐生产参数】");
        System.out.println("  'write-buffer-size' = '512mb'");
        System.out.println("  'target-file-size' = '256mb'");
        System.out.println("  'file.format' = 'parquet'");
        System.out.println("  'compaction.async.enabled' = 'true'");
        System.out.println("  'num-sorted-run.compaction-trigger' = '5'");
        System.out.println("  'num-sorted-run.stop-trigger' = '10'");
        System.out.println("  'commit.force-compact' = 'false'");
        System.out.println("  'sink.parallelism' = '8'");
        
        System.out.println("\n" + "═".repeat(70));
        System.out.println("  Warehouse: " + WAREHOUSE_BASE);
        System.out.println("  查看: ls -lh " + WAREHOUSE_BASE);
        System.out.println("  详情: du -sh " + WAREHOUSE_BASE + "/*");
        System.out.println("═".repeat(70) + "\n");
    }

    private static Map<String, String> map(String... kvs) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kvs.length; i += 2) m.put(kvs[i], kvs[i + 1]);
        return m;
    }

    static class TestCase {
        String id, name;
        Map<String, String> tableOptions;
        long totalRecords;
        double duplicateRate;
        TestCase(String id, String name, Map<String, String> opts, long records, double dupRate) {
            this.id = id; this.name = name; this.tableOptions = opts;
            this.totalRecords = records; this.duplicateRate = dupRate;
        }
    }

    static class TestResult {
        TestCase testCase;
        long recordCount, elapsedMs;
        double throughput;
        FileScanResult fileScan;
        TestResult(TestCase tc, long records, long elapsed, double tp, FileScanResult scan) {
            this.testCase = tc; this.recordCount = records; this.elapsedMs = elapsed;
            this.throughput = tp; this.fileScan = scan;
        }
    }

    static class FileScanResult {
        int fileCount = 0;
        long totalSize = 0;
        List<FileInfo> files = new ArrayList<>();
    }

    static class FileInfo {
        String path; long size;
        FileInfo(String p, long s) { path = p; size = s; }
    }
}
