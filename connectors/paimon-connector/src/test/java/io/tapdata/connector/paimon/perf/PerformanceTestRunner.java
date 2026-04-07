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
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
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

    public static final String BASE_TEST_DIR = "/tmp/paimon-perf-test";
    private static final String DATABASE = "default";
    private static final String TABLE_NAME = "test_table";
    public static final int TOTAL_RECORDS = 5_000;   // 数据集总大小
    private static final int BATCH_SIZE = 1_000; // 每批次写入记录数，也是PaimonService 累积批次大小
    private static final int INIT_TOTAL_RECORDS = 0; //模拟初始化阶段全表数据量

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
        config.setBatchAccumulationSize(BATCH_SIZE);
        config.setCommitIntervalMs(0);         // 关闭时间触发，依靠数量触发
        config.setEnableAsyncCommit(false);    // 测试中关闭异步 commit
        config.setCreateAutoInc(true);

        // 设置 bucketMode & bucketCount（先给一个安全默认，后续 tableProperties 可覆盖）
        Map<String, String> params = tc.getParameters();
        String bucketStr = params.getOrDefault("bucket", "-1");
        int bucket;
        try { bucket = Integer.parseInt(bucketStr); } catch (NumberFormatException e) { bucket = -1; }

        if (bucket > 0) {
            config.setBucketMode("fixed");
            config.setBucketCount(bucket);
        } else if (bucket == -2) {
//            config.setBucketMode("fixed");
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

            // 2. 验证表参数是否生效
            System.out.println("  >> 验证表配置参数...");
            validateTableParameters(tc, service);

            TapConnectorContext tapConnectorContext = new TapConnectorContext(Mockito.mock(TapNodeSpecification.class), new DataMap(), new DataMap(), new HashMap<>(), logger);

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
                    Map<String, Object> info = new HashMap<>(1);
                    info.put("batchOffset",i);
                    evt.setInfo(info);
                    batch.add(evt);
                    if (remain <= BATCH_SIZE || qpsSlotWritten >= INIT_TOTAL_RECORDS) {
                        // 模拟最后一个batch为增量cdc、已写数据量 大于 初始化阶段全表数据量：
                        evt.getInfo().put(TapRecordEvent.INFO_KEY_SYNC_STAGE, "CDC");
                    }
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

    /**
     * 从 Paimon Catalog 中读取表的实际配置参数
     */
    private Map<String, String> readActualTableOptions(String warehouse, String database, String tableName) {
        try {
            Options catalogOptions = new Options();
            catalogOptions.set("warehouse", warehouse);
            CatalogContext context = CatalogContext.create(catalogOptions);
            Catalog catalog = CatalogFactory.createCatalog(context);
            Identifier identifier = Identifier.create(database, tableName);
            Table table = catalog.getTable(identifier);
            Map<String, String> options = table.options();
            catalog.close();
            return options;
        } catch (Exception e) {
            System.err.println("  [WARN] 读取表配置失败: " + e.getMessage());
            return Collections.emptyMap();
        }
    }

    /**
     * 验证表参数是否已生效：对比预期参数和实际表配置
     */
    private void validateTableParameters(TestCase tc, PaimonService service) {
        Map<String, String> expectedParams = tc.getParameters();
        Map<String, String> actualOptions = readActualTableOptions(warehouseForCase(tc), database, tableName);

        if (actualOptions.isEmpty()) {
            System.out.println("  [WARN] 无法读取表配置，跳过参数验证");
            return;
        }

        // 分类验证
        List<String> serviceOnlyParams = Arrays.asList(
            "write-buffer-spillable", "write-buffer-spill.max-disk-size"
        );

        System.out.println("  参数验证结果:");
        int validated = 0;
        int matched = 0;
        int mismatched = 0;

        for (Map.Entry<String, String> entry : expectedParams.entrySet()) {
            String key = entry.getKey();
            String expectedValue = entry.getValue();

            // Service 专有参数不在表选项中
            if (serviceOnlyParams.contains(key)) {
                System.out.printf("    [Service] %-45s = %-20s ✓ 作用于 PaimonService%n", key, expectedValue);
                validated++;
                continue;
            }

            // 检查表选项中是否有该参数
            String actualValue = null;
            for (Map.Entry<String, String> opt : actualOptions.entrySet()) {
                if (opt.getKey().equals(key)) {
                    actualValue = opt.getValue();
                    break;
                }
            }

            validated++;
            if (actualValue != null) {
                // 标准化后比较（去除单位差异）
                String normalizedExpected = normalizeParamValue(key, expectedValue);
                String normalizedActual = normalizeParamValue(key, actualValue);
                
                if (normalizedExpected.equals(normalizedActual)) {
                    System.out.printf("    [表选项]  %-45s = %-20s ✅ 已生效（实际: %s）%n", key, expectedValue, actualValue);
                    matched++;
                } else {
                    System.out.printf("    [表选项]  %-45s = %-20s ⚠️  值不一致（实际: %s）%n", key, expectedValue, actualValue);
                    mismatched++;
                }
            } else {
                // 某些参数可能被 PaimonService 的 createTable 硬编码覆盖
                System.out.printf("    [表选项]  %-45s = %-20s ❌ 未在表配置中找到%n", key, expectedValue);
                mismatched++;
            }
        }

        System.out.println();
        System.out.printf("  验证统计: 共验证 %d 个参数，%d 个已生效，%d 个不匹配%n%n", validated, matched, mismatched);
    }

    /**
     * 标准化参数值以便比较（去除单位差异，如 "256mb" vs "256 MB"）
     */
    private String normalizeParamValue(String key, String value) {
        if (value == null) return "";
        String normalized = value.trim().toLowerCase();
        
        // 对于大小相关参数，统一转换为 MB 数值
        if (key.contains("size") || key.contains("buffer")) {
            try {
                if (normalized.endsWith("gb")) {
                    int mb = (int) (Double.parseDouble(normalized.replace("gb", "").trim()) * 1024);
                    return mb + "mb";
                } else if (normalized.endsWith("mb")) {
                    return normalized;
                } else if (normalized.endsWith("kb")) {
                    double mb = Double.parseDouble(normalized.replace("kb", "").trim()) / 1024.0;
                    return String.format("%.2fmb", mb);
                } else {
                    // 假设是字节，尝试转换为 MB
                    try {
                        long bytes = Long.parseLong(normalized);
                        double mb = bytes / (1024.0 * 1024.0);
                        return String.format("%.2fmb", mb);
                    } catch (NumberFormatException ignored) {}
                }
            } catch (Exception ignored) {}
        }
        
        return normalized;
    }

    /**
     * 参数分类信息
     */
    private static class ParamInfo {
        String key;
        String value;
        ParamCategory category;  // 参数分类
        String targetComponent;  // 作用于哪个组件
        boolean applied;         // 是否已生效
        String actualValue;      // 实际值（用于验证）
        String description;      // 参数说明

        enum ParamCategory {
            SERVICE_CONFIG,      // PaimonService 配置
            TABLE_OPTION,        // Paimon 表选项
            INTERNAL             // 内部参数（不直接传递）
        }

        ParamInfo(String key, String value, ParamCategory category, String targetComponent, String description) {
            this.key = key;
            this.value = value;
            this.category = category;
            this.targetComponent = targetComponent;
            this.description = description;
            this.applied = false;
            this.actualValue = null;
        }
    }

    /**
     * 打印参数（增强版）：分类、作用目标、预期效果、验证状态
     */
    private void printParameters(Map<String, String> params) {
        System.out.println();
        System.out.println("  ┌─ 参数配置详情" + "─".repeat(50));
        System.out.println("  │");

        // 定义参数分组及其元数据
        String[][] paramGroups = {
            // 组名 | 作用目标分类
            {"[1/6] 写入缓冲区配置", "Service 配置"},
            {"[2/6] 文件大小与格式", "Paimon 表选项"},
            {"[3/6] 分桶策略", "混合（Service + 表选项）"},
            {"[4/6] Compaction 合并控制", "Paimon 表选项"},
            {"[5/6] 排序与合并优化", "Paimon 表选项"},
            {"[6/6] 并行度与线程", "Service 配置 → 表选项"},
        };

        String[][][] groupParams = {
            {
                {"write-buffer-size", "写入缓冲区大小（MB），控制内存缓冲容量", "256", "增大可减少 flush 频率，提升吞吐"},
                {"write-buffer-spillable", "是否允许溢写到磁盘", "false", "true 可避免 OOM，适合大数据量"},
                {"write-buffer-spill.max-disk-size", "溢写磁盘最大大小", "不限", "限制磁盘占用"},
            },
            {
                {"target-file-size", "LSM L0 层目标文件大小（MB）", "128", "影响文件碎片化和查询效率"},
                {"file.format", "文件格式（parquet/orc）", "parquet", "Parquet 压缩比更好，查询更快"},
                {"file.compression", "文件压缩算法", "zstd", "zstd 压缩比优于 lz4/snappy"},
                {"spill-compression", "溢写时压缩算法", "lz4", "减少磁盘 I/O"},
            },
            {
                {"bucket", "分桶数量（-1=动态，>0=固定）", "-1", "动态分桶适合未知数据分布"},
                {"dynamic-bucket.target-row-num", "动态分桶目标行数", "不限", "控制动态桶粒度"},
            },
            {
                {"compaction.async.enabled", "是否启用异步 compaction", "true", "异步可减少写入阻塞"},
                {"num-sorted-run.compaction-trigger", "触发 compaction 的 sorted run 数量", "5", "值越大延迟合并"},
                {"num-sorted-run.stop-trigger", "停止写入的 sorted run 阈值", "8", "防止内存溢出"},
                {"compaction.size-ratio", "Compaction 大小比率", "不限", "影响合并策略"},
                {"commit.force-compact", "提交时强制 compact", "false", "true 可保证读性能"},
                {"write-only", "仅写入模式（跳过 compact）", "false", "true 最大化导入吞吐"},
            },
            {
                {"local-merge-buffer-size", "本地合并缓冲区大小", "不限", "影响 merge 性能"},
                {"sort-spill-buffer-size", "排序溢写缓冲区大小", "不限", "控制排序内存占用"},
            },
            {
                {"sink.parallelism", "写入并行度（线程数）", "4", "影响并发写入能力"},
            },
        };

        // 打印每个分组的参数
        for (int g = 0; g < paramGroups.length; g++) {
            String[][] currentGroupParams = groupParams[g];
            boolean hasAny = false;
            
            // 检查该分组是否有任何参数
            for (String[] paramMeta : currentGroupParams) {
                if (params.containsKey(paramMeta[0])) {
                    hasAny = true;
                    break;
                }
            }
            
            if (!hasAny) continue;

            // 打印组名和作用目标
            System.out.printf("  │  %s → %s%n", paramGroups[g][0], paramGroups[g][1]);

            // 打印该组的参数
            for (String[] paramMeta : currentGroupParams) {
                String key = paramMeta[0];
                String desc = paramMeta[1];
                String defaultVal = paramMeta[2];
                String effect = paramMeta[3];
                
                if (!params.containsKey(key)) continue;
                
                String value = params.get(key);
                
                // 判断参数类型和作用目标
                String paramType;
                String targetComponent;
                
                if (key.equals("write-buffer-size") || key.equals("write-buffer-spillable") || 
                    key.equals("write-buffer-spill.max-disk-size")) {
                    paramType = "Service";
                    targetComponent = "PaimonService 运行时配置";
                } else if (key.equals("sink.parallelism")) {
                    paramType = "混合";
                    targetComponent = "Service.writeThreads → 表选项 sink.parallelism";
                } else if (key.equals("bucket")) {
                    paramType = "混合";
                    targetComponent = "Service.bucketMode/bucketCount + 表选项 bucket";
                } else {
                    paramType = "表选项";
                    targetComponent = "Paimon 表 schema OPTIONS";
                }
                
                // 打印参数值、预期效果和作用目标
                System.out.printf("  │    %-40s = %-15s [%s]%n", key, value, paramType);
                System.out.printf("  │      ↳ 作用: %s%n", targetComponent);
                System.out.printf("  │      ↳ 预期: %s%n", effect);
            }
            System.out.println("  │");
        }

        // 打印未分组的参数
        Set<String> allGroupedKeys = new HashSet<>();
        for (String[][] gp : groupParams) {
            for (String[] paramMeta : gp) {
                allGroupedKeys.add(paramMeta[0]);
            }
        }
        
        boolean hasOther = false;
        for (String k : params.keySet()) {
            if (!allGroupedKeys.contains(k)) {
                if (!hasOther) { 
                    System.out.println("  │  [其他参数] → Paimon 表选项");
                    hasOther = true; 
                }
                System.out.printf("  │    %-40s = %-15s [表选项]%n", k, params.get(k));
            }
        }

        // 打印参数统计
        System.out.println("  │");
        System.out.printf("  │  参数统计: 共 %d 个参数 | ", params.size());
        
        long serviceCount = params.keySet().stream()
            .filter(k -> k.equals("write-buffer-size") || k.equals("write-buffer-spillable") || 
                        k.equals("write-buffer-spill.max-disk-size") || k.equals("sink.parallelism"))
            .count();
        long tableOptionCount = params.size() - serviceCount;
        
        System.out.printf("Service 配置: %d 个 | 表选项: %d 个%n", serviceCount, tableOptionCount);
        System.out.println("  │");
        System.out.println("  └" + "─".repeat(69));
        System.out.println();
        System.out.println("  参数分类说明:");
        System.out.println("    • Service 配置  → 通过 PaimonConfig setter 设置，影响运行时行为（缓冲区、线程数等）");
        System.out.println("    • Paimon 表选项 → 通过 CREATE TABLE 的 OPTIONS 设置，定义表的物理存储特性");
        System.out.println("    • 混合类型      → 同时作用于 Service 和表选项（如 bucket、parallelism）");
        System.out.println("    ✓ 参数生效验证将在表创建后自动执行");
        System.out.println();
    }

    /**
     * 参数元数据
     */
    private static class ParamMeta {
        String groupName;
        String targetComponent;
        String description;
        String key;
        String defaultValue;
        String validationMethod;

        ParamMeta(String groupName, String targetComponent, String description,
                  String key, String defaultValue, String validationMethod) {
            this.groupName = groupName;
            this.targetComponent = targetComponent;
            this.description = description;
            this.key = key;
            this.defaultValue = defaultValue;
            this.validationMethod = validationMethod;
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

    /**
     * 基于测试结果动态生成推荐配置
     * 分析实际测试数据，从各场景最优用例中提取配置参数
     */
    private void appendRecommendedConfig(StringBuilder sb, List<TestResult> results) {
        // 筛选成功的测试结果
        List<TestResult> successResults = results.stream()
            .filter(r -> r.error == null)
            .collect(Collectors.toList());

        if (successResults.isEmpty()) {
            sb.append("> ⚠️ 所有测试用例均失败，无法生成推荐配置\n\n");
            return;
        }

        // 按场景分类获取最优配置
        ScenarioRecommendation importRecommend = findBestForScenario(successResults, "无小文件", 
            "导入无 Compact 场景", Comparator.comparingDouble(r -> -r.throughput));
        
        ScenarioRecommendation realtimeRecommend = findBestForScenario(successResults, "合并策略", 
            "实时写入允许合并场景", Comparator.comparingDouble(r -> r.throughput / Math.max(1, r.fileCount)));
        
        ScenarioRecommendation smallFileRecommend = findBestForScenario(successResults, "无小文件", 
            "最少文件数场景", Comparator.comparingInt(r -> r.fileCount));
        
        ScenarioRecommendation globalThroughputRecommend = findBestForScenario(successResults, null, 
            "全局最高吞吐", Comparator.comparingDouble(r -> -r.throughput));
        
        ScenarioRecommendation balancedRecommend = findBestForScenario(successResults, null, 
            "均衡场景（吞吐/文件数综合）", Comparator.comparingDouble(r -> r.throughput / Math.max(1, r.fileCount)));

        // 场景 1：导入无 Compact
        sb.append("### 场景1：大批量导入无 Compact（基于实测最优）\n\n");
        if (importRecommend != null) {
            sb.append(String.format("**推荐用例**: %s - %s%n", importRecommend.result.testCase.getId(), 
                importRecommend.result.testCase.getName()));
            sb.append(String.format("**实测吞吐**: %.0f 条/秒 | **文件数**: %d | **总大小**: %s%n%n", 
                importRecommend.result.throughput, importRecommend.result.fileCount, 
                PaimonFileObserver.formatSize(importRecommend.result.totalFileSize)));
            appendConfigBlock(sb, importRecommend.result.testCase.getParameters(), 
                "大批量导入优化配置（追求最大吞吐，禁止合并）");
            
            sb.append("\n**配置说明**:\n");
            appendConfigExplanation(sb, importRecommend);
        } else {
            sb.append("> ⚠️ 未找到无小文件测试组结果，请运行 nosmallfile 测试组\n\n");
        }

        // 场景 2：实时写入允许合并
        sb.append("\n### 场景2：实时写入允许合并（基于实测最优）\n\n");
        if (realtimeRecommend != null) {
            sb.append(String.format("**推荐用例**: %s - %s%n", realtimeRecommend.result.testCase.getId(), 
                realtimeRecommend.result.testCase.getName()));
            sb.append(String.format("**实测吞吐**: %.0f 条/秒 | **文件数**: %d | **平均大小**: %s%n%n", 
                realtimeRecommend.result.throughput, realtimeRecommend.result.fileCount,
                realtimeRecommend.result.fileCount > 0 ? 
                    PaimonFileObserver.formatSize(realtimeRecommend.result.totalFileSize / realtimeRecommend.result.fileCount) : "N/A"));
            appendConfigBlock(sb, realtimeRecommend.result.testCase.getParameters(), 
                "实时写入优化配置（允许异步合并，平衡读写性能）");
            
            sb.append("\n**配置说明**:\n");
            appendConfigExplanation(sb, realtimeRecommend);
        } else {
            sb.append("> ⚠️ 未找到合并策略测试组结果，请运行 compaction 测试组\n\n");
        }

        // 场景 3：最少文件数
        sb.append("\n### 场景3：最少文件数优化（基于实测最少）\n\n");
        if (smallFileRecommend != null) {
            sb.append(String.format("**推荐用例**: %s - %s%n", smallFileRecommend.result.testCase.getId(), 
                smallFileRecommend.result.testCase.getName()));
            sb.append(String.format("**实测吞吐**: %.0f 条/秒 | **文件数**: %d（最少）| **平均大小**: %s%n%n", 
                smallFileRecommend.result.throughput, smallFileRecommend.result.fileCount,
                smallFileRecommend.result.fileCount > 0 ? 
                    PaimonFileObserver.formatSize(smallFileRecommend.result.totalFileSize / smallFileRecommend.result.fileCount) : "N/A"));
            appendConfigBlock(sb, smallFileRecommend.result.testCase.getParameters(), 
                "最少文件数优化配置（减少文件碎片，便于后续查询）");
            
            sb.append("\n**配置说明**:\n");
            appendConfigExplanation(sb, smallFileRecommend);
        } else {
            sb.append("> ⚠️ 未找到相关测试结果\n\n");
        }

        // 场景 4：全局最高吞吐
        sb.append("\n### 场景4：全局最高吞吐（基于实测数据）\n\n");
        if (globalThroughputRecommend != null) {
            sb.append(String.format("**推荐用例**: %s - %s%n", globalThroughputRecommend.result.testCase.getId(), 
                globalThroughputRecommend.result.testCase.getName()));
            sb.append(String.format("**实测吞吐**: %.0f 条/秒 | **文件数**: %d | **总大小**: %s%n%n", 
                globalThroughputRecommend.result.throughput, globalThroughputRecommend.result.fileCount, 
                PaimonFileObserver.formatSize(globalThroughputRecommend.result.totalFileSize)));
            appendConfigBlock(sb, globalThroughputRecommend.result.testCase.getParameters(), 
                "全局最高吞吐配置（所有测试用例中的最佳表现）");
            
            sb.append("\n**配置说明**:\n");
            appendConfigExplanation(sb, globalThroughputRecommend);
        }

        // 场景 5：均衡配置
        sb.append("\n### 场景5：吞吐与文件数均衡（综合最优）\n\n");
        if (balancedRecommend != null) {
            sb.append(String.format("**推荐用例**: %s - %s%n", balancedRecommend.result.testCase.getId(), 
                balancedRecommend.result.testCase.getName()));
            sb.append(String.format("**实测吞吐**: %.0f 条/秒 | **文件数**: %d | **效能**: %.0f 条/秒/文件%n%n", 
                balancedRecommend.result.throughput, balancedRecommend.result.fileCount,
                balancedRecommend.result.fileCount > 0 ? 
                    balancedRecommend.result.throughput / balancedRecommend.result.fileCount : 0));
            appendConfigBlock(sb, balancedRecommend.result.testCase.getParameters(), 
                "吞吐与文件数均衡配置（适合一般生产场景）");
            
            sb.append("\n**配置说明**:\n");
            appendConfigExplanation(sb, balancedRecommend);
        }

        // 参数对比分析
        sb.append("\n### 配置参数对比分析\n\n");
        appendParameterComparison(sb, importRecommend, realtimeRecommend, smallFileRecommend, globalThroughputRecommend);
    }

    /**
     * 场景推荐结果
     */
    private static class ScenarioRecommendation {
        TestResult result;
        String scenarioName;
        
        ScenarioRecommendation(TestResult result, String scenarioName) {
            this.result = result;
            this.scenarioName = scenarioName;
        }
    }

    /**
     * 根据场景查找最优配置
     */
    private ScenarioRecommendation findBestForScenario(List<TestResult> results, String groupName, 
            String scenarioName, Comparator<TestResult> comparator) {
        return results.stream()
            .filter(r -> groupName == null || groupName.equals(r.testCase.getGroup()))
            .max(comparator)
            .map(r -> new ScenarioRecommendation(r, scenarioName))
            .orElse(null);
    }

    /**
     * 打印配置参数块
     */
    private void appendConfigBlock(StringBuilder sb, Map<String, String> params, String comment) {
        sb.append("```properties\n");
        sb.append("# " + comment + "\n");
        
        // 按分类排序打印
        String[] orderedKeys = {
            "write-buffer-size", "write-buffer-spillable", "write-buffer-spill.max-disk-size",
            "target-file-size", "file.format", "file.compression", "spill-compression",
            "bucket", "dynamic-bucket.target-row-num",
            "compaction.async.enabled", "num-sorted-run.compaction-trigger", 
            "num-sorted-run.stop-trigger", "compaction.size-ratio",
            "commit.force-compact", "write-only",
            "local-merge-buffer-size", "sort-spill-buffer-size",
            "sink.parallelism", "changelog-producer"
        };
        
        Set<String> printed = new HashSet<>();
        for (String key : orderedKeys) {
            if (params.containsKey(key)) {
                sb.append(String.format("%-45s = %s%n", key, params.get(key)));
                printed.add(key);
            }
        }
        
        // 打印剩余参数
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (!printed.contains(entry.getKey())) {
                sb.append(String.format("%-45s = %s%n", entry.getKey(), entry.getValue()));
            }
        }
        
        sb.append("```\n");
    }

    /**
     * 打印配置说明
     */
    private void appendConfigExplanation(StringBuilder sb, ScenarioRecommendation rec) {
        Map<String, String> params = rec.result.testCase.getParameters();
        
        // 缓冲区说明
        if (params.containsKey("write-buffer-size")) {
            String size = params.get("write-buffer-size");
            sb.append(String.format("- **write-buffer-size=%s**: 写入缓冲区大小，", size));
            if (size.contains("512") || size.contains("1024")) {
                sb.append("大缓冲区可减少 flush 频率，提升大批量导入吞吐\n");
            } else if (size.contains("64") || size.contains("128")) {
                sb.append("中小缓冲区适合内存受限场景\n");
            } else {
                sb.append("默认配置，平衡吞吐和内存占用\n");
            }
        }
        
        // 溢写说明
        if ("true".equals(params.get("write-buffer-spillable"))) {
            sb.append("- **write-buffer-spillable=true**: 允许溢写磁盘，避免 OOM，适合大数据量导入\n");
        }
        
        // 目标文件大小说明
        if (params.containsKey("target-file-size")) {
            String size = params.get("target-file-size");
            sb.append(String.format("- **target-file-size=%s**: 控制生成文件的大小，", size));
            if (size.contains("256") || size.contains("512")) {
                sb.append("大文件减少碎片，适合查询优化\n");
            } else {
                sb.append("中小文件适合频繁读取场景\n");
            }
        }
        
        // 合并说明
        if (params.containsKey("write-only")) {
            if ("true".equals(params.get("write-only"))) {
                sb.append("- **write-only=true**: 完全跳过 compaction，最大化写入性能\n");
            } else {
                sb.append("- **write-only=false**: 允许后台合并，保证读性能\n");
            }
        }
        
        if (params.containsKey("compaction.async.enabled")) {
            if ("true".equals(params.get("compaction.async.enabled"))) {
                sb.append("- **compaction.async.enabled=true**: 异步合并不阻塞写入\n");
            } else {
                sb.append("- **compaction.async.enabled=false**: 关闭异步合并\n");
            }
        }
        
        // 分桶说明
        if (params.containsKey("bucket")) {
            String bucket = params.get("bucket");
            sb.append(String.format("- **bucket=%s**: ", bucket));
            if ("-1".equals(bucket)) {
                sb.append("动态分桶，自动适应数据分布\n");
            } else if ("-2".equals(bucket)) {
                sb.append("延迟分桶，Paimon 1.3+ 新特性\n");
            } else {
                sb.append(String.format("固定 %s 桶，适合已知数据量的场景\n", bucket));
            }
        }
        
        // 并行度说明
        if (params.containsKey("sink.parallelism")) {
            sb.append(String.format("- **sink.parallelism=%s**: 写入并行度，影响并发线程数\n", 
                params.get("sink.parallelism")));
        }
    }

    /**
     * 打印参数对比分析
     */
    private void appendParameterComparison(StringBuilder sb, ScenarioRecommendation... recommendations) {
        // 收集所有出现过的参数
        Set<String> allParams = new LinkedHashSet<>();
        for (ScenarioRecommendation rec : recommendations) {
            if (rec != null) {
                allParams.addAll(rec.result.testCase.getParameters().keySet());
            }
        }
        
        if (allParams.isEmpty()) return;
        
        sb.append("| 参数 | ");
        for (ScenarioRecommendation rec : recommendations) {
            if (rec != null) {
                sb.append(rec.result.testCase.getId()).append(" | ");
            }
        }
        sb.append("\n|------|");
        for (ScenarioRecommendation rec : recommendations) {
            if (rec != null) sb.append("------|");
        }
        sb.append("\n");
        
        for (String param : allParams) {
            sb.append("| ").append(param).append(" | ");
            for (ScenarioRecommendation rec : recommendations) {
                if (rec != null) {
                    String val = rec.result.testCase.getParameters().getOrDefault(param, "-");
                    sb.append(val).append(" | ");
                }
            }
            sb.append("\n");
        }
        sb.append("\n");
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
        // 委托给 TestModeConfig 统一处理
        String groupKey = TestModeConfig.resolveToGroupKey(mode);
        if (groupKey != null) {
            return groupKey;
        }
        
        // 未找到匹配的模式，给出警告并返回默认
        System.out.println("  [WARN] 未知模式: " + mode + "，使用默认 basic");
        return TestModeConfig.DEFAULT_MODE_GROUP;
    }

    private static String interactiveChooseMode() throws IOException {
        // 使用 TestModeConfig 生成的菜单文本
        System.out.print(TestModeConfig.getInteractiveMenuText());
        System.out.flush();
        
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String line = br.readLine();
        String choice = (line == null || line.trim().isEmpty()) ? 
            TestModeConfig.DEFAULT_MODE_ID : line.trim();
        
        // 解析选择并返回 group key
        String groupKey = TestModeConfig.resolveToGroupKey(choice);
        if (groupKey == null) {
            System.out.println("  [WARN] 无效选项: " + choice + "，使用默认 basic");
            return TestModeConfig.DEFAULT_MODE_GROUP;
        }
        
        return groupKey;
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
