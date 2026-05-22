# Connector Benchmark Framework — 使用文档

## 概述

`connector-perf-test` 是一个通用的 tapdata-connector 性能测试框架。通过统一的 `DataSourceClient` 接口驱动任意 connector，测量写入吞吐量、读取性能、延迟分位数（p50/p99/p999/max）及长时稳定性，输出标准化 Markdown 报告。

框架不绑定任何具体数据源。当前内置 **Paimon** 实现；扩展新数据源只需增加配置类与客户端类，引擎代码零改动。

---

## 快速开始

### 前置条件

| 项目 | 要求 |
|---|---|
| JDK | 11+ |
| Maven | 3.6+ |
| paimon-connector | 已 `mvn install` 到本地 `.m2` |

### 1. 安装依赖

```bash
# 先构建 paimon-connector（框架依赖它）
cd connectors/paimon-connector
mvn clean install -DskipTests

# 回到测试工程目录
cd ../connector-perf-test
```

### 2. 运行内置 Paimon 写入测试

```bash
mvn exec:java -Pnew -Dexec.jvmArgs="-Xmx2g"
```

默认参数：

| 参数 | 默认值 |
|---|---|
| 写入总量 | 100 万条 |
| 批次大小 | 10,000 条 |
| 写入线程 | 2 |
| 仓库路径 | `/tmp/connector-perf-test/paimon` |
| 报告输出 | `/tmp/connector-perf-test/reports/benchmark-report.md` |

### 3. 运行旧版 Paimon 测试（向后兼容）

```bash
mvn exec:java -Dexec.args="basic"
```

---

## 测试场景

### WriteScenario — 写入性能测试

测量批量写入吞吐量和延迟。支持多线程并发写入。

```java
DataGenerationConfig genConfig = new DataGenerationConfig()
    .totalRecords(5_000_000)     // 总写入量
    .batchSize(5_000)            // 每批大小
    .writeThreads(4)             // 并发线程数
    .pkDuplicateRate(0);         // 主键重复率（0=全新插入，30=30% UPDATE）

WriteScenario scenario = new WriteScenario(
    "My Write Test",
    tapTable,
    genConfig,
    new DefaultRecordGenerator(genConfig.getTotalRecords(), genConfig.getPrimaryKeyDuplicateRate())
);
```

### ReadScenario — 读取性能测试

测量批量读取吞吐量。每轮读取一次，重复 `rounds` 次取平均值。

```java
ReadScenario scenario = new ReadScenario(
    "My Read Test",
    tapTable,
    10_000,   // batchSize
    5         // rounds（读取轮数）
);
```

> **注意**：Paimon 的 `PaimonConnector` 注册了 `BatchReadFunction`，ReadScenario 可用。  
> 若目标 connector 不支持批量读取，会捕获 `UnsupportedOperationException` 并在结果中记录错误信息，不会使测试崩溃。

### MixedScenario — 并发读写混合测试

写入线程与读取线程同时运行，模拟读写竞争压力。报告以写入侧指标为主。

```java
MixedScenario scenario = new MixedScenario(
    "Mixed RW",
    tapTable,
    genConfig,
    new DefaultRecordGenerator(genConfig.getTotalRecords(), 0),
    2    // readThreads（并发读取线程数）
);
```

### DrillTestScenario — 长时稳定性测试

以**时间**而非行数为终止条件，持续写入直到超时，适合评估 compaction、GC、内存泄漏等长期行为。

```java
DrillTestScenario scenario = new DrillTestScenario(
    "30-min Drill",
    tapTable,
    genConfig,
    new DefaultRecordGenerator(genConfig.getTotalRecords(), 0),
    Duration.ofMinutes(30)    // 运行时长
);
```

数据耗尽后自动 reset 重新生成，保证持续写入。

---

## 完整示例代码

```java
import io.tapdata.entity.schema.*;
import io.tapdata.entity.schema.type.*;
import io.tapdata.perftest.connector.paimon.PaimonDataSourceConfig;
import io.tapdata.perftest.core.config.*;
import io.tapdata.perftest.core.data.DefaultRecordGenerator;
import io.tapdata.perftest.core.engine.BenchmarkEngine;
import io.tapdata.perftest.core.scenario.*;
import java.time.Duration;

public class MyBenchmark {
    public static void main(String[] args) throws Exception {

        // 1. 定义测试表 Schema
        TapTable table = new TapTable("bench_table");
        table.add(new TapField("id",    "string").tapType(new TapString()).primaryKeyPos(1));
        table.add(new TapField("name",  "string").tapType(new TapString()));
        table.add(new TapField("value", "double").tapType(new TapNumber().scale(2)));
        table.add(new TapField("ts",    "datetime").tapType(new TapDateTime()));

        // 2. Paimon 数据源配置
        PaimonDataSourceConfig dsConfig = new PaimonDataSourceConfig()
            .warehouse("/data/paimon-warehouse")
            .storageType("local")
            .writeBufferSizeMb(512)
            .batchAccumulationSize(200_000)
            .enableAutoCompaction(false);
        dsConfig.setDatabase("benchmark");
        dsConfig.setTapTable(table);

        // 3. 数据生成配置
        DataGenerationConfig genConfig = new DataGenerationConfig()
            .totalRecords(10_000_000)
            .batchSize(10_000)
            .writeThreads(4)
            .pkDuplicateRate(10);   // 10% 主键重复（模拟 UPDATE）

        DefaultRecordGenerator generator = new DefaultRecordGenerator(
            genConfig.getTotalRecords(),
            genConfig.getPrimaryKeyDuplicateRate()
        );

        // 4. 组合多个场景
        BenchmarkConfig benchConfig = new BenchmarkConfig()
            .addScenario(new WriteScenario("Write 10M", table, genConfig, generator))
            .addScenario(new ReadScenario("Read 10K×5", table, 10_000, 5))
            .addScenario(new DrillTestScenario(
                "Drill 10min", table, genConfig,
                new DefaultRecordGenerator(genConfig.getTotalRecords(), 0),
                Duration.ofMinutes(10)
            ))
            .reportOutputDir("/tmp/my-benchmark/reports")
            .reportTitle("Paimon Performance Benchmark");

        // 5. 运行
        BenchmarkEngine engine = new BenchmarkEngine(benchConfig, dsConfig);
        engine.run().getResults().forEach(r ->
            System.out.printf("%-20s | %,.0f rec/s | p99=%.1fms%n",
                r.getScenarioName(), r.getThroughputPerSec(), r.getP99LatencyMs())
        );
    }
}
```

---

## Paimon 数据源配置参数

`PaimonDataSourceConfig` 完整参数：

| 方法 | 默认值 | 说明 |
|---|---|---|
| `.warehouse(String)` | `/tmp/perf-test-warehouse` | Paimon 仓库根路径 |
| `.storageType(String)` | `"local"` | 存储类型：`local` / `s3` / `hdfs` / `oss` |
| `.s3(ep, ak, sk, region)` | — | S3 连接参数（storageType=s3 时必填） |
| `.hdfs(host, port)` | — | HDFS 连接参数（storageType=hdfs 时必填） |
| `.writeBufferSizeMb(int)` | `256` | 写缓冲区大小（MB） |
| `.batchAccumulationSize(int)` | `100_000` | 批次积累行数 |
| `.commitIntervalMs(int)` | `30_000` | 提交间隔（ms） |
| `.enableAsyncCommit(boolean)` | `true` | 异步提交 |
| `.writeThreads(int)` | `4` | Paimon 内部写线程数 |
| `.enableAutoCompaction(boolean)` | `false` | 自动压缩（测试期间建议关闭） |
| `.targetFileSizeMb(int)` | `128` | 目标文件大小（MB） |
| `.bucketMode(String)` | `"dynamic"` | 桶模式：`dynamic` / `fixed` |
| `.bucketCount(int)` | `4` | 固定桶模式下的桶数量 |

`.setDatabase(String)` 和 `.setTapTable(TapTable)` 必填，通过继承自 `DataSourceConfig` 的 setter 设置。

---

## 指标说明

每个场景执行后返回 `ScenarioResult`，包含以下指标：

| 指标 | 含义 |
|---|---|
| `totalRecords` | 实际写入/读取记录总数 |
| `elapsedMs` | 场景总耗时（ms） |
| `throughputPerSec` | 整体吞吐量（records/sec） |
| `avgLatencyMs` | 每条记录平均延迟（ms） |
| `p50LatencyMs` | P50 延迟（中位数） |
| `p99LatencyMs` | P99 延迟 |
| `p999LatencyMs` | P999 延迟 |
| `maxLatencyMs` | 最大延迟 |
| `errorCount` | 错误次数 |

> 延迟统计基于**批次级别**：`batchLatencyMs / batchSize`，反映单条记录的均摊处理时间。  
> 使用 2048 桶固定直方图，覆盖 0–20,000ms 范围，分辨率约 10ms。

---

## 报告格式

测试结束后自动在 `reportOutputDir` 生成 `benchmark-report.md`，包含：

- 汇总表（所有场景对比）
- 每场景详细指标

示例：

```
| Scenario       | DataSource | Total Records | Throughput (rec/s) | P99 Latency (ms) | Errors |
|---|---|---:|---:|---:|---:|
| Write 10M      | paimon     | 10,000,000    | 35,214             | 3.20             | 0      |
| Read 10K×5     | paimon     | 50,000        | 82,400             | 0.95             | 0      |
```

---

## 常见问题

**Q: 运行时报 `ClassNotFoundException: io.tapdata.connector.paimon.PaimonConnector`**  
A: paimon-connector 未安装到本地 `.m2`，执行 `mvn install -DskipTests` 后重试。

**Q: 写入速度远低于预期**  
A: 检查 `enableAutoCompaction`（测试期建议 `false`），增大 `writeBufferSizeMb` 和 `batchAccumulationSize`，确认 JVM 堆内存足够（`-Xmx4g` 以上）。

**Q: ReadScenario 报 `UnsupportedOperationException`**  
A: 目标 connector 未注册 `BatchReadFunction`，该场景对此 connector 不适用。

**Q: 多线程写入时遇到死锁或 OOM**  
A: `WriteScenario` 共用同一个 `DataSourceClient` 实例（由 Paimon 内部锁保护）。若 connector 不是线程安全的，需在 `createClient()` 中为每个线程创建独立实例并自行管理生命周期。
