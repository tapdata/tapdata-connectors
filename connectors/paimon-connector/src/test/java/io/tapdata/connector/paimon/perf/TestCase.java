package io.tapdata.connector.paimon.perf;

import java.util.*;

import static io.tapdata.connector.paimon.perf.PerformanceTestRunner.TOTAL_RECORDS;
import static io.tapdata.connector.paimon.perf.PerformanceTestRunner.BASE_TEST_DIR;

/**
 * 性能测试用例定义
 * 参数键直接对应 Paimon CoreOptions 表选项名称，可通过 tableProperties 直接注入
 * 
 * 注意：所有测试用例的 dataSize 统一使用 BATCH_SIZE，便于集中控制测试数据量
 */
public class TestCase {
    private final String id;
    private final String name;
    private final String group;
    private final Map<String, String> parameters;
    private final int primaryKeyDuplicateRate; // 0-100
    private final int qps;                    // 0 = unlimited
    private final String description;

    public TestCase(String id, String name, String group,
                    Map<String, String> parameters,
                    int primaryKeyDuplicateRate, int qps,
                    String description) {
        this.id = id;
        this.name = name;
        this.group = group;
        this.parameters = parameters;
        this.primaryKeyDuplicateRate = primaryKeyDuplicateRate;
        this.qps = qps;
        this.description = description;
    }

    // ─── Accessors ────────────────────────────────────────────────────────────

    public String getId() { return id; }
    public String getName() { return name; }
    public String getGroup() { return group; }
    public Map<String, String> getParameters() { return parameters; }
    
    /**
     * 获取数据量：统一使用 BATCH_SIZE 控制
     * 所有测试用例的数据量都由 PerformanceTestRunner.BATCH_SIZE 统一管理
     */
    public long getDataSize() { return TOTAL_RECORDS; }
    
    public int getPrimaryKeyDuplicateRate() { return primaryKeyDuplicateRate; }
    public int getQps() { return qps; }
    public String getDescription() { return description; }

    @Override
    public String toString() {
        return String.format("[%s] %s (%s) - %s [数据量: %,d]", 
            id, name, group, description, TOTAL_RECORDS);
    }

    // ─── Builder helper ───────────────────────────────────────────────────────

    private static Map<String, String> params(String... kvs) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kvs.length; i += 2) m.put(kvs[i], kvs[i + 1]);
        return m;
    }

    // ─── 5.1 基础测试用例组 ────────────────────────────────────────────────────

    public static List<TestCase> createBasicTests() {
        return Arrays.asList(
            new TestCase("TC-01", "基准测试(默认配置)", "基础测试",
                params(
                    "write-buffer-size", "256mb",
                    "target-file-size", "128mb",
                    "file.format", "parquet",
                    "file.compression", "zstd",
                    "bucket", "-1",
                    "compaction.async.enabled", "true",
                    "num-sorted-run.compaction-trigger", "5",
                    "num-sorted-run.stop-trigger", "8",
                    "write-only", "false"
                ),
                0, 0,
                "默认配置基线，建立性能参考点"),

            new TestCase("TC-02", "大数据量测试", "基础测试",
                params(
                    "write-buffer-size", "512mb",
                    "write-buffer-spillable", "true",
                    "target-file-size", "256mb",
                    "file.format", "parquet",
                    "file.compression", "zstd",
                    "bucket", "-1",
                    "compaction.async.enabled", "false",
                    "write-only", "true"
                ),
                0, 0,
                "大数据量写入，验证稳定性和文件大小"),

            new TestCase("TC-03", "小批量测试", "基础测试",
                params(
                    "write-buffer-size", "64mb",
                    "target-file-size", "64mb",
                    "file.format", "parquet",
                    "bucket", "-1",
                    "compaction.async.enabled", "true"
                ),
                0, 0,
                "小数据量，验证最小写入场景")
        );
    }

    // ─── 5.2 写入缓冲区测试组 ─────────────────────────────────────────────────

    public static List<TestCase> createWriteBufferTests() {
        return Arrays.asList(
            new TestCase("TC-10", "缓冲区-64MB", "写入缓冲区",
                params("write-buffer-size", "64mb", "write-buffer-spillable", "true",
                    "target-file-size", "128mb", "bucket", "-1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "64MB缓冲区，验证小缓冲区写入行为和文件数量"),

            new TestCase("TC-11", "缓冲区-128MB", "写入缓冲区",
                params("write-buffer-size", "128mb", "write-buffer-spillable", "true",
                    "target-file-size", "128mb", "bucket", "-1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "128MB缓冲区，中等缓冲区写入行为"),

            new TestCase("TC-12", "缓冲区-256MB(默认)", "写入缓冲区",
                params("write-buffer-size", "256mb", "write-buffer-spillable", "true",
                    "target-file-size", "128mb", "bucket", "-1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "256MB缓冲区(默认值)"),

            new TestCase("TC-13", "缓冲区-512MB", "写入缓冲区",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "256mb", "bucket", "-1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "512MB缓冲区，大缓冲区减少flush次数"),

            new TestCase("TC-14", "缓冲区-1024MB", "写入缓冲区",
                params("write-buffer-size", "1024mb", "write-buffer-spillable", "true",
                    "target-file-size", "512mb", "bucket", "-1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "1GB缓冲区，超大缓冲区验证极限"),

            new TestCase("TC-15", "缓冲区-不可溢出", "写入缓冲区",
                params("write-buffer-size", "256mb", "write-buffer-spillable", "false",
                    "target-file-size", "128mb", "bucket", "-1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "关闭溢写，缓冲区满时触发flush，验证OOM风险"),

            new TestCase("TC-16", "缓冲区-可溢出", "写入缓冲区",
                params("write-buffer-size", "64mb", "write-buffer-spillable", "true",
                    "write-buffer-spill.max-disk-size", "1gb",
                    "target-file-size", "128mb", "bucket", "-1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "小缓冲区+溢写磁盘，验证溢写行为和最终文件大小")
        );
    }

    // ─── 5.3 目标文件大小测试组 ───────────────────────────────────────────────

    public static List<TestCase> createTargetFileSizeTests() {
        return Arrays.asList(
            new TestCase("TC-20", "目标文件-64MB", "目标文件大小",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "64mb", "bucket", "-1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "64MB目标文件，验证小目标文件的文件数量"),

            new TestCase("TC-21", "目标文件-128MB(默认)", "目标文件大小",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "128mb", "bucket", "-1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "128MB目标文件(默认值)"),

            new TestCase("TC-22", "目标文件-256MB", "目标文件大小",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "256mb", "bucket", "-1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "256MB目标文件，减少文件数量"),

            new TestCase("TC-23", "目标文件-512MB", "目标文件大小",
                params("write-buffer-size", "1024mb", "write-buffer-spillable", "true",
                    "target-file-size", "512mb", "bucket", "-1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "512MB目标文件，超大目标文件")
        );
    }

    // ─── 5.4 分桶策略测试组 ───────────────────────────────────────────────────

    public static List<TestCase> createBucketTests() {
        return Arrays.asList(
            new TestCase("TC-30", "分桶-bucket=-2(延迟分桶)", "分桶策略",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "256mb", "bucket", "-2",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "bucket=-2 延迟分桶，验证自动调整分桶性能和文件布局"),

            new TestCase("TC-31", "分桶-动态(bucket=-1)", "分桶策略",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "256mb", "bucket", "-1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "bucket=-1 动态分桶，基准对比"),

            new TestCase("TC-32", "分桶-固定4桶", "分桶策略",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "256mb", "bucket", "4",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "固定4桶，验证固定分桶文件分布"),

            new TestCase("TC-33", "分桶-固定8桶", "分桶策略",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "256mb", "bucket", "8",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "固定8桶，验证更多分桶的文件分布"),

            new TestCase("TC-34", "分桶-固定16桶", "分桶策略",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "256mb", "bucket", "16",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "固定16桶"),

            new TestCase("TC-35", "分桶-固定32桶", "分桶策略",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "256mb", "bucket", "32",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "固定32桶，桶数过多时的文件碎片化")
        );
    }

    // ─── 5.5 合并(Compaction)策略测试组 ──────────────────────────────────────

    public static List<TestCase> createCompactionTests() {
        return Arrays.asList(
            new TestCase("TC-40", "合并-激进(trigger=2)", "合并策略",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "bucket", "-1",
                    "compaction.async.enabled", "true",
                    "num-sorted-run.compaction-trigger", "2",
                    "num-sorted-run.stop-trigger", "5",
                    "compaction.size-ratio", "1",
                    "write-only", "false"),
                0, 0, "激进合并：trigger=2，合并最频繁，验证合并开销"),

            new TestCase("TC-41", "合并-默认(trigger=5)", "合并策略",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "bucket", "-1",
                    "compaction.async.enabled", "true",
                    "num-sorted-run.compaction-trigger", "5",
                    "num-sorted-run.stop-trigger", "8",
                    "write-only", "false"),
                0, 0, "默认合并参数"),

            new TestCase("TC-42", "合并-保守(trigger=10)", "合并策略",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "bucket", "-1",
                    "compaction.async.enabled", "true",
                    "num-sorted-run.compaction-trigger", "10",
                    "num-sorted-run.stop-trigger", "20",
                    "write-only", "false"),
                0, 0, "保守合并：trigger=10，减少合并次数"),

            new TestCase("TC-43", "合并-超保守(trigger=100)", "合并策略",
                params("write-buffer-size", "512mb", "target-file-size", "256mb",
                    "bucket", "-1",
                    "compaction.async.enabled", "true",
                    "num-sorted-run.compaction-trigger", "100",
                    "num-sorted-run.stop-trigger", "200",
                    "write-only", "false"),
                0, 0, "超保守：trigger=100，几乎不触发合并，接近write-only效果"),

            new TestCase("TC-44", "合并-禁用异步", "合并策略",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "bucket", "-1",
                    "compaction.async.enabled", "false",
                    "write-only", "false"),
                0, 0, "关闭异步合并，合并仅在写入时同步执行"),

            new TestCase("TC-45", "合并-write-only模式", "合并策略",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "256mb", "bucket", "-1",
                    "compaction.async.enabled", "false",
                    "write-only", "true"),
                0, 0, "write-only=true：完全跳过合并，最大化写入吞吐")
        );
    }

    // ─── 5.6 无小文件写入测试组(重点) ────────────────────────────────────────

    public static List<TestCase> createNoSmallFileTests() {
        return Arrays.asList(
            new TestCase("TC-50", "无小文件-大缓冲无合并", "无小文件",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "256mb", "bucket", "1",
                    "compaction.async.enabled", "false", "write-only", "true",
                    "num-sorted-run.compaction-trigger", "100",
                    "num-sorted-run.stop-trigger", "200"),
                0, 0,
                "大缓冲(512MB)+大目标文件(256MB)+禁止合并，一次性写入验证"),

            new TestCase("TC-51", "无小文件-溢写验证", "无小文件",
                params("write-buffer-size", "128mb", "write-buffer-spillable", "true",
                    "write-buffer-spill.max-disk-size", "2gb",
                    "write-buffer-spill.tmp-dirs", BASE_TEST_DIR + "/tmp," + BASE_TEST_DIR + "/tmp2",
                    "target-file-size", "256mb", "bucket", "1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0,
                "小缓冲(128MB)+磁盘溢写，验证溢写后文件是否仍然大"),

            new TestCase("TC-52", "无小文件-bucket=-2组合", "无小文件",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "256mb", "bucket", "-2",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0,
                "性能最好配置：bucket=-2+大缓冲+禁止合并，验证延迟分桶的无小文件效果，但需要compaction数据才能可见"),

            new TestCase("TC-53", "无小文件-增加local-merge-buffer", "无小文件",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                    "target-file-size", "512mb", "bucket", "1",
                    "compaction.async.enabled", "false", "write-only", "true",
                    "local-merge-buffer-size", "64mb",
                    "sort-spill-buffer-size", "128mb",
                    "num-sorted-run.compaction-trigger", "100",
                    "num-sorted-run.stop-trigger", "200"),
                0, 0,
                "多bucket最优生产配置：所有参数协同优化，预期最少文件数、最大文件"),
            new TestCase("TC-54", "无小文件-去除changelog", "无小文件",
                params("write-buffer-size", "512mb", "write-buffer-spillable", "true",
                        "target-file-size", "512mb", "bucket", "1",
                        "compaction.async.enabled", "false", "write-only", "true",
                        "local-merge-buffer-size", "64mb",
                        "sort-spill-buffer-size", "128mb",
                        "num-sorted-run.compaction-trigger", "100",
                        "num-sorted-run.stop-trigger", "200",
                        "changelog-producer", "input"),
                0, 0,
                "多bucket最优生产配置：所有参数协同优化，预期最少文件数、最大文件")
        );
    }

    // ─── 5.7 文件格式和压缩测试组 ─────────────────────────────────────────────

    public static List<TestCase> createFormatCompressionTests() {
        return Arrays.asList(
            new TestCase("TC-60", "格式-Parquet+ZSTD(默认)", "文件格式",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "file.format", "parquet", "file.compression", "zstd",
                    "bucket", "-1", "write-only", "true"),
                0, 0, "Parquet+ZSTD，默认配置基准"),

            new TestCase("TC-61", "格式-Parquet+LZ4", "文件格式",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "file.format", "parquet", "file.compression", "lz4",
                    "bucket", "-1", "write-only", "true"),
                0, 0, "Parquet+LZ4，更快压缩速度但压缩率稍低"),

            new TestCase("TC-62", "格式-Parquet+无压缩", "文件格式",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "file.format", "parquet", "file.compression", "none",
                    "bucket", "-1", "write-only", "true"),
                0, 0, "Parquet+无压缩，最快写入速度但文件最大"),

            new TestCase("TC-63", "格式-ORC+ZSTD", "文件格式",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "file.format", "orc", "file.compression", "zstd",
                    "bucket", "-1", "write-only", "true"),
                0, 0, "ORC+ZSTD，列存格式写入性能对比"),

            new TestCase("TC-64", "格式-ORC+LZ4", "文件格式",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "file.format", "orc", "file.compression", "lz4",
                    "bucket", "-1", "write-only", "true"),
                0, 0, "ORC+LZ4")
        );
    }

    // ─── 5.8 主键更新测试组 ───────────────────────────────────────────────────

    public static List<TestCase> createPrimaryKeyUpdateTests() {
        return Arrays.asList(
            new TestCase("TC-70", "主键-纯插入(0%重复)", "主键更新",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "bucket", "-1", "compaction.async.enabled", "true",
                    "write-only", "false"),
                0, 0, "0%主键重复，纯INSERT性能基准"),

            new TestCase("TC-71", "主键-10%重复", "主键更新",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "bucket", "-1", "compaction.async.enabled", "true",
                    "write-only", "false"),
                10, 0, "10%主键重复(低更新率)，触发部分UPDATE合并"),

            new TestCase("TC-72", "主键-30%重复", "主键更新",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "bucket", "-1", "compaction.async.enabled", "true",
                    "write-only", "false"),
                30, 0, "30%主键重复(中等更新率)"),

            new TestCase("TC-73", "主键-50%重复", "主键更新",
                params("write-buffer-size", "256mb", "target-file-size", "128mb",
                    "bucket", "-1", "compaction.async.enabled", "true",
                    "write-only", "false"),
                50, 0, "50%主键重复(高更新率)，大量合并开销")
        );
    }

    // ─── 5.9 并行度测试组 ─────────────────────────────────────────────────────

    public static List<TestCase> createParallelismTests() {
        return Arrays.asList(
            new TestCase("TC-80", "并行度-1线程", "写入并行度",
                params("write-buffer-size", "512mb", "target-file-size", "256mb",
                    "bucket", "1", "sink.parallelism", "1",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "单线程写入，最少文件数"),

            new TestCase("TC-81", "并行度-2线程", "写入并行度",
                params("write-buffer-size", "512mb", "target-file-size", "256mb",
                    "bucket", "2", "sink.parallelism", "2",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "2线程写入"),

            new TestCase("TC-82", "并行度-4线程", "写入并行度",
                params("write-buffer-size", "512mb", "target-file-size", "256mb",
                    "bucket", "4", "sink.parallelism", "4",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "4线程写入(默认)"),

            new TestCase("TC-83", "并行度-8线程", "写入并行度",
                params("write-buffer-size", "512mb", "target-file-size", "256mb",
                    "bucket", "8", "sink.parallelism", "8",
                    "compaction.async.enabled", "false", "write-only", "true"),
                0, 0, "8线程写入")
        );
    }

    // ─── 组合工厂方法 ─────────────────────────────────────────────────────────

    public static List<TestCase> createAllTests() {
        List<TestCase> all = new ArrayList<>();
        all.addAll(createBasicTests());
        all.addAll(createWriteBufferTests());
        all.addAll(createTargetFileSizeTests());
        all.addAll(createBucketTests());
        all.addAll(createCompactionTests());
        all.addAll(createNoSmallFileTests());
        all.addAll(createFormatCompressionTests());
        all.addAll(createPrimaryKeyUpdateTests());
        all.addAll(createParallelismTests());
        return all;
    }

    /**
     * 获取所有支持的测试组名称
     */
    public static Map<String, String> getGroupDescriptions() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("basic",       "基础测试用例(TC-01~03) - 默认配置基线");
        m.put("buffer",      "写入缓冲区测试(TC-10~16) - write-buffer-size / spillable");
        m.put("target",      "目标文件大小测试(TC-20~23) - target-file-size");
        m.put("bucket",      "分桶策略测试(TC-30~35) - bucket=-2/-1/固定");
        m.put("compaction",  "合并策略测试(TC-40~45) - compaction trigger/stop/write-only");
        m.put("nosmallfile", "无小文件写入测试(TC-50~53) - 生产最优配置");
        m.put("format",      "文件格式&压缩测试(TC-60~64) - parquet/orc + zstd/lz4");
        m.put("pkupdate",    "主键更新测试(TC-70~73) - 重复率 0%/10%/30%/50%");
        m.put("parallelism", "写入并行度测试(TC-80~83) - sink.parallelism");
        m.put("all",         "全量测试(所有组)");
        return m;
    }
}
