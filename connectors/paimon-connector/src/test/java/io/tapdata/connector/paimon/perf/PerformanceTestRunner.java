package io.tapdata.connector.paimon.perf;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.service.PaimonService;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import org.mockito.Mockito;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Paimon 写入性能参数调优测试主类
 *
 * <p>运行方式：
 * <pre>
 *   ./run-perf-test.sh [mode]
 *   mode: basic | buffer | target | bucket | compaction | nosmallfile | format | pkupdate | parallelism | all | auto
 * </pre>
 *
 * <p>交互模式（默认）：每个用例前后按回车键继续；auto 模式：无需交互，全自动运行。
 */
public class PerformanceTestRunner {

    // ─── 常量 ─────────────────────────────────────────────────────────────────

    private static final String BASE_TEST_DIR = "/tmp/paimon-perf-test";
    private static final String DATABASE = "default";
    private static final String TABLE_NAME = "test_table";
    private static final int    BATCH_SIZE = 5_000;   // 每批次写入记录数
    private static final int    BATCH_ACCUMULATE = 10_000; // PaimonService 累积批次大小

    // ─── 实例变量 ─────────────────────────────────────────────────────────────

    private final String baseDir;
    private final String database;
    private final String tableName;
    private boolean interactive;
    private final Log logger;

    // ─── 构造函数 ─────────────────────────────────────────────────────────────

    public PerformanceTestRunner(String baseDir, String database, String tableName) {
        this.baseDir    = baseDir;
        this.database   = database;
        this.tableName  = tableName;
        this.interactive = true;
        this.logger     = buildConsoleLog();
    }

    public void setInteractive(boolean interactive) {
        this.interactive = interactive;
    }

    // ─── 简单控制台 Log 实现 ──────────────────────────────────────────────────

    private static Log buildConsoleLog() {
        return new Log() {
            @Override public void debug(String m, Object... p) {}
            @Override public void info(String m, Object... p)  { print("[INFO] ", m, p); }
            @Override public void warn(String m, Object... p)  { print("[WARN] ", m, p); }
            @Override public void error(String m, Object... p) { print("[ERROR]", m, p); }
            @Override public void error(String m, Throwable t) { System.err.println("[ERROR] " + m + (t != null ? ": " + t.getMessage() : "")); }
            @Override public void fatal(String m, Object... p) { print("[FATAL]", m, p); }
            @Override public void trace(String m, Object... p) {}
            private void print(String prefix, String m, Object[] p) {
                if (p != null) { for (Object o : p) m = m.replaceFirst("\\{}", String.valueOf(o)); }
                System.out.println(prefix + " " + m);
            }
        };
    }

    // ─── 测试用例目录管理 ────────────────────────────────────────────────────

    private String warehouseForCase(TestCase tc) {
        // 每个用例独立仓库，避免 schema 冲突
        return baseDir + "/" + tc.getId();
    }

    // ─── PaimonService 工厂 ───────────────────────────────────────────────────

    private PaimonService buildPaimonService(TestCase tc) throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setWarehouse(warehouseForCase(tc));
        config.setStorageType("local");
        config.setDatabase(database);
        config.setBatchAccumulationSize(BATCH_ACCUMULATE);
        config.setCommitIntervalMs(0);         // 关闭时间触发，依靠数量触发
        config.setEnableAsyncCommit(false);    // 测试中关闭异步 commit

        // 设置 bucketMode & bucketCount（先给一个安全默认，后续 tableProperties 可覆盖）
        Map<String, String> params = tc.getParameters();
        String bucketStr = params.getOrDefault("bucket", "-1");
        int bucket;
        try { bucket = Integer.parseInt(bucketStr); } catch (NumberFormatException e) { bucket = -1; }

        if (bucket > 0) {
            config.setBucketMode("fixed");
            config.setBucketCount(bucket);
        } else {
            config.setBucketMode("dynamic");
        }

        // ── 核心参数 → PaimonConfig setters ──────────────────────────────────
        applyConfigSetters(config, params);

        // ── 所有参数追加到 tableProperties（最高优先级覆盖）─────────────────
        List<LinkedHashMap<String, String>> tableProps = buildTableProperties(params);
        config.setTableProperties(tableProps);

        // ── 写入线程 / 并行度 ─────────────────────────────────────────────────
        String parallelism = params.get("sink.parallelism");
        if (parallelism != null) {
            try { config.setWriteThreads(Integer.parseInt(parallelism)); }
            catch (NumberFormatException ignored) {}
        }

        PaimonService service = new PaimonService(config);
        service.init();
        return service;
    }

    private void applyConfigSetters(PaimonConfig config, Map<String, String> params) {
        // write-buffer-size: e.g. "256mb" → 256
        String wbs = params.get("write-buffer-size");
        if (wbs != null) {
            try { config.setWriteBufferSize(parseSizeMb(wbs)); } catch (Exception ignored) {}
        }
        String tfs = params.get("target-file-size");
        if (tfs != null) {
            try { config.setTargetFileSize(parseSizeMb(tfs)); } catch (Exception ignored) {}
        }
        config.setProperties(map2prop(params));

    }

    private Properties map2prop(Map<String, String> params) {
        Properties props = new Properties();
        for (Map.Entry<String, String> e : params.entrySet()) {
            props.setProperty(e.getKey(), e.getValue());
        }
        return props;
    }

    /**
     * 将所有参数转为 tableProperties 键值对列表（最终覆盖 createTable 中的硬编码值）
     */
    private List<LinkedHashMap<String, String>> buildTableProperties(Map<String, String> params) {
        List<LinkedHashMap<String, String>> props = new ArrayList<>();
        for (Map.Entry<String, String> e : params.entrySet()) {
            LinkedHashMap<String, String> m = new LinkedHashMap<>();
            m.put("propKey",   e.getKey());
            m.put("propValue", e.getValue());
            props.add(m);
        }
        return props;
    }

    /** 解析带单位的大小，返回 MB 数（如 "512mb" → 512，"1gb" → 1024） */
    private static int parseSizeMb(String s) {
        s = s.trim().toLowerCase();
        if (s.endsWith("gb")) return Integer.parseInt(s.replace("gb", "").trim()) * 1024;
        if (s.endsWith("mb")) return Integer.parseInt(s.replace("mb", "").trim());
        return Integer.parseInt(s);
    }

    // ─── 创建测试表 ────────────────────────────────────────────────────────────

    private TapTable createTapTable() {
        DataGenerator dg = new DataGenerator(0, tableName);
        return dg.generateTapTable();
    }

    private void createFreshTable(PaimonService service) throws Exception {
        try { service.dropTable(tableName); } catch (Exception ignored) {}
        TapTable table = createTapTable();
        service.createTable(table, logger);
    }

    // ─── 核心执行方法 ──────────────────────────────────────────────────────────

    /**
     * 执行单个测试用例
     */
    public TestResult runTestCase(TestCase tc) {
        printSeparator("=");
        System.out.printf("  用例 %-8s: %s%n", tc.getId(), tc.getName());
        System.out.printf("  组 别: %-20s  描述: %s%n", tc.getGroup(), tc.getDescription());
        printSeparator("-");
        printParameters(tc.getParameters());

        if (interactive) {
            System.out.println("\n  [按 Enter 开始本用例，Ctrl+C 退出]");
            waitForEnter();
        }

        PaimonService service = null;
        long startMs = 0;
        long endMs   = 0;
        AtomicLong written = new AtomicLong(0);
        String error = null;
        PaimonFileObserver observer = new PaimonFileObserver(warehouseForCase(tc), database, tableName);

        try {
            // 1. 初始化服务并创建表
            System.out.println("\n  >> 初始化 PaimonService...");
            service = buildPaimonService(tc);
            createFreshTable(service);

            TapConnectorContext tapConnectorContext = new TapConnectorContext(Mockito.mock(TapNodeSpecification.class), new DataMap(), new DataMap(), new HashMap<>(), Mockito.mock(Log.class));

            System.out.printf("  >> 仓库路径: %s%n", warehouseForCase(tc));
            System.out.printf("  >> 开始写入 %,d 条记录 (主键重复率 %d%%, QPS限制 %s)%n",
                tc.getDataSize(), tc.getPrimaryKeyDuplicateRate(),
                tc.getQps() > 0 ? tc.getQps() + "" : "无限制");

            // 2. 执行写入
            TapTable tapTable = createTapTable();
            DataGenerator gen = new DataGenerator(tc.getPrimaryKeyDuplicateRate(), tableName);

            startMs = System.currentTimeMillis();
            long total  = tc.getDataSize();
            long remain = total;
            long qpsSlotStartMs = System.currentTimeMillis();
            long qpsSlotWritten = 0;

            while (remain > 0) {
                int batchSz = (int) Math.min(BATCH_SIZE, remain);
                List<TapRecordEvent> batch = new ArrayList<>(batchSz);
                for (int i = 0; i < batchSz; i++) {
                    Map<String, Object> rec = gen.generateRecord();
                    TapInsertRecordEvent evt = new TapInsertRecordEvent();
                    evt.setAfter(rec);
                    evt.setTableId(tableName);
                    evt.setReferenceTime(System.currentTimeMillis());
                    batch.add(evt);
                }
                service.writeRecords(batch, tapTable, tapConnectorContext);
                remain -= batchSz;
                written.addAndGet(batchSz);
                qpsSlotWritten += batchSz;

                // QPS throttle
                if (tc.getQps() > 0) {
                    long elapsed = System.currentTimeMillis() - qpsSlotStartMs;
                    long expected = qpsSlotWritten * 1000L / tc.getQps();
                    long sleep = expected - elapsed;
                    if (sleep > 0) {
                        try { Thread.sleep(sleep); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                    }
                }

                // 进度打印
                long pct = (written.get() * 100) / total;
                if (written.get() % (BATCH_SIZE * 10) == 0 || remain == 0) {
                    double elapsed = (System.currentTimeMillis() - startMs) / 1000.0;
                    double throughput = elapsed > 0 ? written.get() / elapsed : 0;
                    System.out.printf("  >> 进度: %,d/%,d (%d%%) | 吞吐: %.0f 条/秒%n",
                        written.get(), total, pct, throughput);
                }
            }

            // 3. 强制 flush
            System.out.println("  >> 执行最终 flush...");
            service.flushAll();
            endMs = System.currentTimeMillis();

        } catch (Throwable e) {
            endMs = System.currentTimeMillis();
            error = e.getClass().getSimpleName() + ": " + e.getMessage();
            System.err.println("  [ERROR] 用例执行异常: " + error);
            if (System.getProperty("perf.verbose", "false").equals("true")) e.printStackTrace();
        } finally {
            if (service != null) {
                try { service.close(); } catch (Exception ignored) {}
            }
        }

        // 4. 统计文件
        List<PaimonFileObserver.FileInfo> files = Collections.emptyList();
        try { files = observer.scanAllFiles(); } catch (Exception e) {
            System.err.println("  [WARN] 文件扫描失败: " + e.getMessage());
        }

        long durationMs = endMs - startMs;
        double throughput = durationMs > 0 ? written.get() * 1000.0 / durationMs : 0;

        TestResult result = new TestResult(tc, written.get(), durationMs, throughput, files, error);

        // 5. 打印结果
        printResult(result, observer);

        if (interactive) {
            System.out.println("\n  [按 Enter 继续下一个用例]");
            waitForEnter();
        }
        return result;
    }

    /**
     * 运行整个测试组
     */
    public List<TestResult> runTestGroup(String groupName) throws Exception {
        List<TestCase> cases;
        switch (groupName.toLowerCase()) {
            case "basic":       cases = TestCase.createBasicTests(); break;
            case "buffer":      cases = TestCase.createWriteBufferTests(); break;
            case "target":      cases = TestCase.createTargetFileSizeTests(); break;
            case "bucket":      cases = TestCase.createBucketTests(); break;
            case "compaction":  cases = TestCase.createCompactionTests(); break;
            case "nosmallfile": cases = TestCase.createNoSmallFileTests(); break;
            case "format":      cases = TestCase.createFormatCompressionTests(); break;
            case "pkupdate":    cases = TestCase.createPrimaryKeyUpdateTests(); break;
            case "parallelism": cases = TestCase.createParallelismTests(); break;
            case "all":         cases = TestCase.createAllTests(); break;
            default:
                System.out.println("  [WARN] 未知测试组: " + groupName);
                System.out.println("  可用: " + String.join(", ", TestCase.getGroupDescriptions().keySet()));
                return Collections.emptyList();
        }

        printSeparator("=");
        System.out.printf("  运行测试组: %s  共 %d 个用例%n", groupName, cases.size());
        printSeparator("=");

        List<TestResult> results = new ArrayList<>();
        for (int i = 0; i < cases.size(); i++) {
            TestCase tc = cases.get(i);
            System.out.printf("%n  >>> 用例 [%d/%d]%n", i + 1, cases.size());
            results.add(runTestCase(tc));
        }
        return results;
    }

    // ─── 打印工具 ──────────────────────────────────────────────────────────────

    private static void printSeparator(String ch) {
        System.out.println(ch.repeat(70));
    }

    private static void printParameters(Map<String, String> params) {
        System.out.println("  参数配置:");
        // 分组打印
        String[][] groups = {
            {"[写入缓冲区]", "write-buffer-size", "write-buffer-spillable", "write-buffer-spill.max-disk-size"},
            {"[文件大小]",   "target-file-size", "file.format", "file.compression", "spill-compression"},
            {"[分桶策略]",   "bucket", "dynamic-bucket.target-row-num"},
            {"[合并控制]",   "compaction.async.enabled", "num-sorted-run.compaction-trigger",
                            "num-sorted-run.stop-trigger", "compaction.size-ratio",
                            "commit.force-compact", "write-only"},
            {"[排序/合并]",  "local-merge-buffer-size", "sort-spill-buffer-size"},
            {"[并行度]",     "sink.parallelism"},
        };
        for (String[] group : groups) {
            boolean hasAny = false;
            for (int i = 1; i < group.length; i++) {
                if (params.containsKey(group[i])) { hasAny = true; break; }
            }
            if (!hasAny) continue;
            System.out.println("    " + group[0]);
            for (int i = 1; i < group.length; i++) {
                String val = params.get(group[i]);
                if (val != null) {
                    System.out.printf("      %-45s = %s%n", group[i], val);
                }
            }
        }
        // 打印剩余未分组的参数
        Set<String> grouped = new HashSet<>();
        for (String[] g : groups) for (int i = 1; i < g.length; i++) grouped.add(g[i]);
        boolean hasOther = false;
        for (String k : params.keySet()) {
            if (!grouped.contains(k)) {
                if (!hasOther) { System.out.println("    [其他]"); hasOther = true; }
                System.out.printf("      %-45s = %s%n", k, params.get(k));
            }
        }
    }

    private void printResult(TestResult r, PaimonFileObserver observer) {
        printSeparator("-");
        System.out.println("  ┌─ 测试结果: " + r.testCase.getName());
        if (r.error != null) {
            System.out.println("  │  [FAILED] 错误: " + r.error);
        }
        System.out.printf("  │  写入记录: %,d 条  用时: %.2f s  吞吐: %.0f 条/秒%n",
            r.recordCount, r.durationMs / 1000.0, r.throughput);
        System.out.printf("  │  文件数量: %d 个  总大小: %s  平均: %s%n",
            r.fileCount, PaimonFileObserver.formatSize(r.totalFileSize),
            r.fileCount > 0 ? PaimonFileObserver.formatSize(r.totalFileSize / r.fileCount) : "N/A");
        System.out.printf("  │  最小: %s  最大: %s%n",
            PaimonFileObserver.formatSize(r.minFileSize),
            PaimonFileObserver.formatSize(r.maxFileSize));

        // 文件大小分布
        System.out.println("  │  分布:");
        for (Map.Entry<String, Long> e : r.sizeDistribution.entrySet()) {
            if (e.getValue() > 0) {
                String bar = "█".repeat(Math.min(30, Math.max(1, e.getValue().intValue())));
                System.out.printf("  │    %-15s: %3d %s%n", e.getKey(), e.getValue(), bar);
            }
        }
        System.out.println("  └─");

        // 展示实际文件列表（最多15个）
        try { observer.printCompact(); } catch (Exception ignored) {}
    }

    // ─── 报告生成 ──────────────────────────────────────────────────────────────

    public String generateReport(List<TestResult> results, String reportPath) throws IOException {
        StringBuilder sb = new StringBuilder();
        String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        sb.append("# Paimon 写入性能测试报告\n\n");
        sb.append("> 生成时间：").append(now).append("  \n");
        sb.append("> Paimon 版本：1.3.1  ").append("基仓库：").append(baseDir).append("\n\n");

        // ── 概览 ─────────────────────────────────────────────────────────────
        sb.append("## 测试概览\n\n");
        sb.append("| 指标 | 值 |\n|------|-----|\n");
        sb.append("| 测试用例总数 | ").append(results.size()).append(" |\n");
        long successCount = results.stream().filter(r -> r.error == null).count();
        sb.append("| 成功用例数 | ").append(successCount).append(" |\n");
        sb.append("| 失败用例数 | ").append(results.size() - successCount).append(" |\n");
        OptionalDouble avgTp = results.stream().filter(r -> r.error == null).mapToDouble(r -> r.throughput).average();
        OptionalDouble maxTp = results.stream().filter(r -> r.error == null).mapToDouble(r -> r.throughput).max();
        sb.append(String.format("| 平均吞吐量 | %.0f 条/秒 |\n", avgTp.orElse(0)));
        sb.append(String.format("| 最高吞吐量 | %.0f 条/秒 |\n", maxTp.orElse(0)));
        sb.append("\n");

        // ── 分组详细结果 ─────────────────────────────────────────────────────
        sb.append("## 详细测试结果\n\n");
        Map<String, List<TestResult>> grouped = new LinkedHashMap<>();
        for (TestResult r : results) grouped.computeIfAbsent(r.testCase.getGroup(), k -> new ArrayList<>()).add(r);

        for (Map.Entry<String, List<TestResult>> entry : grouped.entrySet()) {
            sb.append("### ").append(entry.getKey()).append("\n\n");
            sb.append("| 用例ID | 名称 | 吞吐(条/s) | 用时(s) | 文件数 | 总大小 | 平均大小 | 状态 |\n");
            sb.append("|--------|------|-----------|---------|--------|--------|----------|------|\n");
            for (TestResult r : entry.getValue()) {
                String status = r.error == null ? "✅" : "❌";
                sb.append(String.format("| %s | %s | %.0f | %.2f | %d | %s | %s | %s |\n",
                    r.testCase.getId(), r.testCase.getName(), r.throughput,
                    r.durationMs / 1000.0, r.fileCount,
                    PaimonFileObserver.formatSize(r.totalFileSize),
                    r.fileCount > 0 ? PaimonFileObserver.formatSize(r.totalFileSize / r.fileCount) : "N/A",
                    status));
            }
            sb.append("\n");
        }

        // ── 文件大小分布 ──────────────────────────────────────────────────────
        sb.append("## 文件大小分布对比\n\n");
        sb.append("| 用例ID | <1KB | 1KB-1MB | 1MB-10MB | 10MB-100MB | 100MB-500MB | >500MB |\n");
        sb.append("|--------|------|---------|----------|-----------|-------------|--------|\n");
        for (TestResult r : results) {
            Map<String, Long> d = r.sizeDistribution;
            sb.append(String.format("| %s | %d | %d | %d | %d | %d | %d |\n",
                r.testCase.getId(),
                d.getOrDefault("< 1KB",       0L),
                d.getOrDefault("1KB - 1MB",   0L),
                d.getOrDefault("1MB - 10MB",  0L),
                d.getOrDefault("10MB-100MB",  0L),
                d.getOrDefault("100MB-500MB", 0L),
                d.getOrDefault("> 500MB",     0L)));
        }
        sb.append("\n");

        // ── 参数效果分析 ─────────────────────────────────────────────────────
        sb.append("## 参数效果分析\n\n");
        appendParamAnalysis(sb, results);

        // ── 推荐配置 ──────────────────────────────────────────────────────────
        sb.append("## 生产推荐配置\n\n");
        appendRecommendedConfig(sb, results);

        // ── 核心结论 ──────────────────────────────────────────────────────────
        sb.append("## 核心结论\n\n");
        appendConclusions(sb, results);

        // 写入文件
        File f = new File(reportPath);
        f.getParentFile().mkdirs();
        try (BufferedWriter w = new BufferedWriter(new FileWriter(f))) {
            w.write(sb.toString());
        }
        System.out.println("\n  ✅ 报告已生成: " + f.getAbsolutePath());
        return sb.toString();
    }

    private void appendParamAnalysis(StringBuilder sb, List<TestResult> results) {
        // 按组分析
        Map<String, List<TestResult>> grouped = new LinkedHashMap<>();
        for (TestResult r : results) grouped.computeIfAbsent(r.testCase.getGroup(), k -> new ArrayList<>()).add(r);

        for (Map.Entry<String, List<TestResult>> entry : grouped.entrySet()) {
            if (entry.getValue().size() < 2) continue;
            sb.append("### ").append(entry.getKey()).append("\n\n");
            List<TestResult> list = entry.getValue().stream().filter(r -> r.error == null)
                .sorted(Comparator.comparingDouble(r -> -r.throughput)).collect(Collectors.toList());
            if (list.isEmpty()) { sb.append("（所有用例失败，无法分析）\n\n"); continue; }
            TestResult best = list.get(0);
            TestResult worst = list.get(list.size() - 1);
            sb.append(String.format("- 最高吞吐：**%s** (%.0f 条/秒)%n", best.testCase.getId(), best.throughput));
            sb.append(String.format("- 最低吞吐：**%s** (%.0f 条/秒)%n", worst.testCase.getId(), worst.throughput));
            if (worst.throughput > 0) {
                sb.append(String.format("- 性能差异：**%.1f 倍**%n", best.throughput / worst.throughput));
            }
            // 最少文件
            list.stream().min(Comparator.comparingInt(r -> r.fileCount)).ifPresent(r ->
                sb.append(String.format("- 最少文件：**%s** (%d 个文件，avg=%s)%n",
                    r.testCase.getId(), r.fileCount, r.fileCount > 0 ? PaimonFileObserver.formatSize(r.totalFileSize / r.fileCount) : "N/A")));
            sb.append("\n");
        }
    }

    private void appendRecommendedConfig(StringBuilder sb, List<TestResult> results) {
        // 找无小文件组最优
        Optional<TestResult> noSmallBest = results.stream()
            .filter(r -> r.error == null && "无小文件".equals(r.testCase.getGroup()))
            .max(Comparator.comparingDouble(r -> r.throughput));

        // 找全局最高吞吐
        Optional<TestResult> globalBest = results.stream()
            .filter(r -> r.error == null)
            .max(Comparator.comparingDouble(r -> r.throughput));

        sb.append("### 场景1：导入无 Compact，大文件优先（推荐生产导入场景）\n\n");
        sb.append("```properties\n");
        sb.append("# 最大化写入性能，禁止合并，溢写磁盘保证大文件\n");
        sb.append("write-buffer-size         = 512mb\n");
        sb.append("write-buffer-spillable    = true\n");
        sb.append("target-file-size          = 256mb\n");
        sb.append("bucket                    = -1\n");
        sb.append("compaction.async.enabled  = false\n");
        sb.append("write-only                = true\n");
        sb.append("num-sorted-run.compaction-trigger = 100\n");
        sb.append("num-sorted-run.stop-trigger       = 200\n");
        sb.append("file.format               = parquet\n");
        sb.append("file.compression          = zstd\n");
        sb.append("sink.parallelism          = 4\n");
        sb.append("```\n\n");

        sb.append("### 场景2：实时写入，允许合并\n\n");
        sb.append("```properties\n");
        sb.append("write-buffer-size         = 256mb\n");
        sb.append("write-buffer-spillable    = true\n");
        sb.append("target-file-size          = 128mb\n");
        sb.append("bucket                    = -1\n");
        sb.append("compaction.async.enabled  = true\n");
        sb.append("num-sorted-run.compaction-trigger = 5\n");
        sb.append("num-sorted-run.stop-trigger       = 10\n");
        sb.append("write-only                = false\n");
        sb.append("file.format               = parquet\n");
        sb.append("file.compression          = zstd\n");
        sb.append("```\n\n");

        if (noSmallBest.isPresent()) {
            TestResult r = noSmallBest.get();
            sb.append("### 场景3：基于测试数据的最佳无小文件配置（").append(r.testCase.getId()).append("）\n\n");
            sb.append("```properties\n");
            for (Map.Entry<String, String> e : r.testCase.getParameters().entrySet()) {
                sb.append(String.format("%-40s = %s%n", e.getKey(), e.getValue()));
            }
            sb.append("```\n\n");
            sb.append(String.format("**实测结果**: 吞吐 %.0f 条/秒，%d 个文件，总大小 %s%n%n",
                r.throughput, r.fileCount, PaimonFileObserver.formatSize(r.totalFileSize)));
        }

        if (globalBest.isPresent()) {
            TestResult r = globalBest.get();
            sb.append("### 场景4：全局最高吞吐配置（").append(r.testCase.getId()).append("）\n\n");
            sb.append(String.format("**实测吞吐**: %.0f 条/秒  文件数: %d%n%n", r.throughput, r.fileCount));
        }
    }

    private void appendConclusions(StringBuilder sb, List<TestResult> results) {
        sb.append("1. **write-buffer-size**：缓冲区越大，单次 flush 数据越多，生成文件越大，吞吐越高；" +
                  "但受 JVM 堆限制，建议 256MB-512MB。\n");
        sb.append("2. **write-buffer-spillable=true**：允许溢写磁盘，是大批量导入的关键保障；" +
                  "可避免 OOM，同时保证数据一次性落盘。\n");
        sb.append("3. **target-file-size**：控制 LSM 层 L0 文件的目标大小；" +
                  "256MB+ 可显著减少文件碎片，查询效率更高。\n");
        sb.append("4. **write-only=true**：完全跳过 compaction 和快照过期，最大化写入吞吐；" +
                  "适合纯导入场景，导入完成后再手动 compact。\n");
        sb.append("5. **bucket=-2 vs -1**：bucket=-2（延迟分桶）在数据分布均匀时性能接近 -1；" +
                  "实际效果依赖 Paimon 1.3.1 对 -2 的支持情况（如不支持会退回 -1 行为）。\n");
        sb.append("6. **num-sorted-run.compaction-trigger**：增大阈值（如 100）可延迟合并触发，" +
                  "显著降低写入期间的 I/O 压力，但会增加 sorted run 数量（影响查询）。\n");
        sb.append("7. **file.format=parquet + file.compression=zstd**：" +
                  "压缩比最佳，推荐生产默认配置。\n");
        sb.append("8. **无小文件最优组合**：write-buffer-size=512mb + target-file-size=256mb + " +
                  "write-only=true + write-buffer-spillable=true。\n");
        sb.append("\n");
    }

    // ─── 交互等待 ──────────────────────────────────────────────────────────────

    private void waitForEnter() {
        try {
            System.in.read();
            while (System.in.available() > 0) System.in.read();
        } catch (IOException ignored) {}
    }

    // ─── 测试结果 ──────────────────────────────────────────────────────────────

    public static class TestResult {
        public final TestCase testCase;
        public final long     recordCount;
        public final long     durationMs;
        public final double   throughput;
        public final int      fileCount;
        public final long     totalFileSize;
        public final long     minFileSize;
        public final long     maxFileSize;
        public final Map<String, Long> sizeDistribution;
        public final String   error;

        public TestResult(TestCase tc, long recordCount, long durationMs, double throughput,
                          List<PaimonFileObserver.FileInfo> files, String error) {
            this.testCase     = tc;
            this.recordCount  = recordCount;
            this.durationMs   = durationMs;
            this.throughput   = throughput;
            this.fileCount    = files.size();
            this.totalFileSize = files.stream().mapToLong(PaimonFileObserver.FileInfo::getSize).sum();
            this.minFileSize   = files.stream().mapToLong(PaimonFileObserver.FileInfo::getSize).min().orElse(0L);
            this.maxFileSize   = files.stream().mapToLong(PaimonFileObserver.FileInfo::getSize).max().orElse(0L);
            this.sizeDistribution = calcDistribution(files);
            this.error         = error;
        }

        private static Map<String, Long> calcDistribution(List<PaimonFileObserver.FileInfo> files) {
            Map<String, Long> d = new LinkedHashMap<>();
            d.put("< 1KB",       files.stream().filter(f -> f.getSize() < 1024L).count());
            d.put("1KB - 1MB",   files.stream().filter(f -> f.getSize() >= 1024L && f.getSize() < 1024 * 1024L).count());
            d.put("1MB - 10MB",  files.stream().filter(f -> f.getSize() >= 1024 * 1024L && f.getSize() < 10 * 1024 * 1024L).count());
            d.put("10MB-100MB",  files.stream().filter(f -> f.getSize() >= 10 * 1024 * 1024L && f.getSize() < 100 * 1024 * 1024L).count());
            d.put("100MB-500MB", files.stream().filter(f -> f.getSize() >= 100 * 1024 * 1024L && f.getSize() < 500 * 1024 * 1024L).count());
            d.put("> 500MB",     files.stream().filter(f -> f.getSize() >= 500 * 1024 * 1024L).count());
            return d;
        }
    }

    // ─── main ─────────────────────────────────────────────────────────────────

    /**
     * 初始化 Tapdata PDK 运行时，使 PaimonConfig/CommonDbConfig 的静态字段可以正常初始化。
     * 必须在任何 PaimonConfig 创建之前调用。
     */
    private static void initRuntime() {
        try {
            // 触发 TapRuntime 扫描 io.tapdata 包，注册 JsonParser / BeanUtils 等实现
            Class<?> runtimeClass = Class.forName("io.tapdata.pdk.core.runtime.TapRuntime");
            java.lang.reflect.Method getInstance = runtimeClass.getDeclaredMethod("getInstance");
            getInstance.invoke(null);
        } catch (Exception e) {
            System.err.println("[WARN] TapRuntime 初始化失败（可能影响 PaimonConfig 创建）: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        // 优先初始化 PDK 运行时，避免 CommonDbConfig 静态初始化失败
        initRuntime();

        printWelcome();

        String mode = args.length > 0 ? args[0].trim().toLowerCase() : "";

        // 如果没有参数则交互式选择
        if (mode.isEmpty()) {
            mode = interactiveChooseMode();
        }

        boolean autoMode = "auto".equals(mode);
        String group     = autoMode ? "all" : resolveMode(mode);

        PerformanceTestRunner runner = new PerformanceTestRunner(BASE_TEST_DIR, DATABASE, TABLE_NAME);
        runner.setInteractive(!autoMode);

        // 确保基础目录存在
        new File(BASE_TEST_DIR).mkdirs();

        System.out.printf("%n  运行模式: %s  自动: %s%n", group, autoMode ? "是" : "否");

        List<TestResult> results;
        switch (group) {
            case "single":
                List<TestCase> basics = TestCase.createBasicTests();
                if (!basics.isEmpty()) {
                    results = Collections.singletonList(runner.runTestCase(basics.get(0)));
                } else {
                    results = Collections.emptyList();
                }
                break;
            default:
                results = runner.runTestGroup(group);
        }

        if (!results.isEmpty()) {
            String reportPath = BASE_TEST_DIR + "/test-report-" +
                new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date()) + ".md";
            String report = runner.generateReport(results, reportPath);

            // 同时保存一份固定名称方便脚本引用
            String fixedPath = BASE_TEST_DIR + "/test-report.md";
            try (BufferedWriter w = new BufferedWriter(new FileWriter(fixedPath))) {
                w.write(report);
            }

            printSummaryTable(results);

            printSeparator("=");
            System.out.println("  全部测试完成！");
            System.out.println("  报告路径: " + fixedPath);
            System.out.println("  数据目录: " + BASE_TEST_DIR);
            System.out.println("  查看文件: ls -lhR " + BASE_TEST_DIR + "/TC-*/");
            printSeparator("=");
        }
    }

    private static String resolveMode(String mode) {
        switch (mode) {
            case "1": case "basic":        return "basic";
            case "2": case "all":          return "all";
            case "3": case "nosmallfile":  return "nosmallfile";
            case "4": case "single":       return "single";
            case "bucket":                 return "bucket";
            case "compaction":             return "compaction";
            case "buffer":                 return "buffer";
            case "target":                 return "target";
            case "format":                 return "format";
            case "pkupdate":               return "pkupdate";
            case "parallelism":            return "parallelism";
            default:
                System.out.println("  [WARN] 未知模式: " + mode + "，使用 basic");
                return "basic";
        }
    }

    private static String interactiveChooseMode() throws IOException {
        System.out.println("\n  请选择测试模式（回车默认 1）:");
        System.out.println("    1  basic         - 基础用例组(TC-01~03)");
        System.out.println("    2  all            - 全量测试(所有组)");
        System.out.println("    3  nosmallfile    - 无小文件测试(TC-50~53)");
        System.out.println("    4  single         - 单个基准用例(TC-01)");
        System.out.println("    bucket           - 分桶策略测试(TC-30~35)");
        System.out.println("    compaction       - 合并策略测试(TC-40~45)");
        System.out.println("    buffer           - 写入缓冲区测试(TC-10~16)");
        System.out.println("    target           - 目标文件大小测试(TC-20~23)");
        System.out.println("    format           - 文件格式压缩测试(TC-60~64)");
        System.out.println("    auto             - 全自动运行(无需交互)");
        System.out.print("  > ");
        System.out.flush();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String line = br.readLine();
        return (line == null || line.trim().isEmpty()) ? "1" : line.trim();
    }

    private static void printWelcome() {
        printSeparator("═");
        System.out.println("  Paimon 1.3.1 写入性能参数调优测试");
        System.out.printf("  时 间: %s%n", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        System.out.printf("  仓 库: %s%n", BASE_TEST_DIR);
        System.out.printf("  Java : %s  堆: %dMB  CPU: %d 核%n",
            System.getProperty("java.version"),
            Runtime.getRuntime().maxMemory() / 1024 / 1024,
            Runtime.getRuntime().availableProcessors());
        printSeparator("═");
    }

    private static void printSummaryTable(List<TestResult> results) {
        printSeparator("─");
        System.out.println("  汇总表");
        printSeparator("─");
        System.out.printf("  %-8s %-30s %12s %8s %6s %10s%n",
            "用例", "名称", "吞吐(条/s)", "用时(s)", "文件数", "总大小");
        printSeparator("-");
        for (TestResult r : results) {
            String status = r.error == null ? "✅" : "❌";
            System.out.printf("  %-8s %-30s %12.0f %8.2f %6d %10s  %s%n",
                r.testCase.getId(),
                truncate(r.testCase.getName(), 28),
                r.throughput,
                r.durationMs / 1000.0,
                r.fileCount,
                PaimonFileObserver.formatSize(r.totalFileSize),
                status);
        }
        printSeparator("─");
        // 最佳配置
        results.stream().filter(r -> r.error == null).max(Comparator.comparingDouble(r -> r.throughput))
            .ifPresent(r -> System.out.printf("  最高吞吐: %s (%.0f 条/秒)%n", r.testCase.getId(), r.throughput));
        results.stream().filter(r -> r.error == null && r.fileCount > 0).min(Comparator.comparingInt(r -> r.fileCount))
            .ifPresent(r -> System.out.printf("  最少文件: %s (%d 个，avg %s)%n",
                r.testCase.getId(), r.fileCount,
                PaimonFileObserver.formatSize(r.totalFileSize / r.fileCount)));
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }
}
