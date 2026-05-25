# 扩展新数据源指南

本文以 **MySQL connector** 为例，完整演示如何将一个新的 tapdata-connector 接入性能测试框架。实际适用于框架内任何实现了 PDK 接口的 connector（Kafka、MongoDB、StarRocks、Fluss、RisingWave 等）。

---

## 架构回顾

框架通过依赖倒置将测试引擎与具体数据源完全解耦：

```
BenchmarkEngine
    └── DataSourceConfig.createClient()
            └── DataSourceClient (interface)
                    ├── PaimonDataSourceClient   ← 已实现
                    ├── MySQLDataSourceClient    ← 本文新增
                    └── ...                      ← 未来扩展
```

**引擎代码零改动**。新增数据源只需两步：

1. 创建 `XxxDataSourceConfig`（持有 connector 的连接参数）
2. 创建 `XxxDataSourceClient`（通过 PDK 接口调用 connector）

---

## 步骤一：添加 Maven 依赖

在 `connector-perf-test/pom.xml` 中引入目标 connector 的依赖。以 MySQL 为例：

```xml
<dependency>
    <groupId>io.tapdata</groupId>
    <artifactId>mysql-connector</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

确保 mysql-connector 已通过 `mvn install -DskipTests` 安装到本地 `.m2`。

---

## 步骤二：创建配置类

配置类继承 `DataSourceConfig`，持有目标 connector 所需的全部连接参数，并实现工厂方法 `createClient()`。

**文件路径**：`src/main/java/io/tapdata/perftest/connector/mysql/MySQLDataSourceConfig.java`

```java
package io.tapdata.perftest.connector.mysql;

import io.tapdata.perftest.core.client.DataSourceClient;
import io.tapdata.perftest.core.config.DataSourceConfig;

public class MySQLDataSourceConfig extends DataSourceConfig {

    // ── 连接参数（与 MysqlConnector 的 connectionConfig 字段一一对应）──
    private String  host     = "127.0.0.1";
    private Integer port     = 3306;
    private String  username = "root";
    private String  password = "";

    @Override
    public DataSourceClient createClient() {
        return new MySQLDataSourceClient(this);
    }

    // ── Fluent setters ────────────────────────────────────────────────
    public MySQLDataSourceConfig host(String v)     { this.host = v;     return this; }
    public MySQLDataSourceConfig port(int v)        { this.port = v;     return this; }
    public MySQLDataSourceConfig username(String v) { this.username = v; return this; }
    public MySQLDataSourceConfig password(String v) { this.password = v; return this; }

    // ── Getters ───────────────────────────────────────────────────────
    public String  getHost()     { return host; }
    public Integer getPort()     { return port; }
    public String  getUsername() { return username; }
    public String  getPassword() { return password; }
}
```

---

## 步骤三：创建客户端类

客户端类实现 `DataSourceClient` 接口，通过 PDK 标准流程调用 connector。

**文件路径**：`src/main/java/io/tapdata/perftest/connector/mysql/MySQLDataSourceClient.java`

```java
package io.tapdata.perftest.connector.mysql;

import io.tapdata.connector.mysql.MysqlConnector;   // 目标 connector
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.ddl.table.*;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.perftest.connector.paimon.support.PdkContextFactory;  // 复用工具类
import io.tapdata.perftest.core.client.*;
import io.tapdata.pdk.apis.context.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.source.BatchReadFunction;
import io.tapdata.pdk.apis.functions.connector.target.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MySQLDataSourceClient implements DataSourceClient {

    private final MySQLDataSourceConfig config;

    private MysqlConnector        connector;
    private TapConnectionContext  connectionContext;
    private TapConnectorContext   connectorContext;
    private WriteRecordFunction   writeRecordFunction;
    private BatchReadFunction     batchReadFunction;
    private CreateTableV2Function createTableV2Function;
    private DropTableFunction     dropTableFunction;

    public MySQLDataSourceClient(MySQLDataSourceConfig config) {
        this.config = config;
    }

    // ── PDK 标准三步初始化流程 ────────────────────────────────────────

    @Override
    public void init() throws Exception {
        DataMap connectionConfig = buildConnectionConfig();

        // 步骤 1：构建 TapConnectionContext（使用 Mockito mock 的 TapNodeSpecification）
        connectionContext = PdkContextFactory.buildConnectionContext(connectionConfig);

        // 步骤 2：启动 connector（建立连接、初始化内部状态）
        connector = new MysqlConnector();
        try {
            connector.onStart(connectionContext);
        } catch (Throwable t) {
            throw new Exception("MysqlConnector.onStart failed: " + t.getMessage(), t);
        }

        // 步骤 3：注册能力，获取函数引用
        ConnectorFunctions functions = new ConnectorFunctions();
        connector.registerCapabilities(functions, new TapCodecsRegistry());

        writeRecordFunction   = functions.getWriteRecordFunction();
        batchReadFunction     = functions.getBatchReadFunction();
        createTableV2Function = functions.getCreateTableV2Function();
        dropTableFunction     = functions.getDropTableFunction();

        // 步骤 4：构建带 tableMap 的 TapConnectorContext
        connectorContext = PdkContextFactory.buildConnectorContext(connectionConfig, config.getTapTable());

        System.out.println("[MySQLDataSourceClient] Connected to " + config.getHost() + ":" + config.getPort());
    }

    @Override
    public void createTable(TapTable tapTable) throws Exception {
        PdkContextFactory.updateTableMap(connectorContext, tapTable);
        if (createTableV2Function == null) return;
        TapCreateTableEvent event = new TapCreateTableEvent();
        event.setTable(tapTable);
        try {
            createTableV2Function.createTable(connectorContext, event);
        } catch (Throwable t) {
            throw new Exception("createTable failed: " + t.getMessage(), t);
        }
    }

    @Override
    public void dropTable(String tableName) throws Exception {
        if (dropTableFunction == null) return;
        TapDropTableEvent event = new TapDropTableEvent();
        event.setTableId(tableName);
        try {
            dropTableFunction.dropTable(connectorContext, event);
        } catch (Throwable t) {
            throw new Exception("dropTable failed: " + t.getMessage(), t);
        }
    }

    @Override
    public WriteResult write(List<TapRecordEvent> events) throws Exception {
        if (writeRecordFunction == null) {
            throw new IllegalStateException("WriteRecordFunction not registered");
        }
        TapTable table = config.getTapTable();
        AtomicLong inserted = new AtomicLong(0);
        AtomicLong updated  = new AtomicLong(0);
        AtomicLong deleted  = new AtomicLong(0);
        AtomicLong errors   = new AtomicLong(0);

        long t0 = System.currentTimeMillis();
        try {
            writeRecordFunction.writeRecord(connectorContext, events, table, result -> {
                inserted.addAndGet(result.getInsertedCount());
                updated.addAndGet(result.getModifiedCount());
                deleted.addAndGet(result.getRemovedCount());
                if (result.getErrorMap() != null) errors.addAndGet(result.getErrorMap().size());
            });
        } catch (Throwable t) {
            throw new Exception("writeRecord failed: " + t.getMessage(), t);
        }
        return new WriteResult(inserted.get(), updated.get(), deleted.get(), errors.get(),
            System.currentTimeMillis() - t0);
    }

    @Override
    public ReadResult batchRead(TapTable table, int batchSize) throws Exception {
        if (batchReadFunction == null) {
            throw new UnsupportedOperationException("BatchReadFunction not supported");
        }
        AtomicLong rowCount = new AtomicLong(0);
        long t0 = System.currentTimeMillis();
        try {
            batchReadFunction.batchRead(connectorContext, table, null, batchSize,
                (evts, offset) -> rowCount.addAndGet(evts == null ? 0 : evts.size()));
        } catch (Throwable t) {
            throw new Exception("batchRead failed: " + t.getMessage(), t);
        }
        return new ReadResult(rowCount.get(), System.currentTimeMillis() - t0);
    }

    @Override
    public String getDataSourceType() {
        return "mysql";
    }

    @Override
    public void close() throws Exception {
        if (connector != null) {
            connector.onStop(connectionContext);
        }
    }

    // ── 构建 connectionConfig DataMap（字段名与 connector spec.json 一致）──

    private DataMap buildConnectionConfig() {
        DataMap m = DataMap.create();
        m.put("host",     config.getHost());
        m.put("port",     config.getPort());
        m.put("username", config.getUsername());
        m.put("password", config.getPassword());
        m.put("database", config.getDatabase());
        return m;
    }
}
```

---

## 步骤四：编写测试入口

```java
import io.tapdata.entity.schema.*;
import io.tapdata.entity.schema.type.*;
import io.tapdata.perftest.connector.mysql.MySQLDataSourceConfig;
import io.tapdata.perftest.core.config.*;
import io.tapdata.perftest.core.data.DefaultRecordGenerator;
import io.tapdata.perftest.core.engine.BenchmarkEngine;
import io.tapdata.perftest.core.scenario.*;

public class MySQLBenchmarkMain {
    public static void main(String[] args) throws Exception {

        // 1. 表定义
        TapTable table = new TapTable("perf_test");
        table.add(new TapField("id",    "string").tapType(new TapString()).primaryKeyPos(1));
        table.add(new TapField("name",  "string").tapType(new TapString()));
        table.add(new TapField("value", "double").tapType(new TapNumber().scale(2)));
        table.add(new TapField("ts",    "datetime").tapType(new TapDateTime()));

        // 2. MySQL 数据源配置
        MySQLDataSourceConfig dsConfig = new MySQLDataSourceConfig()
            .host("127.0.0.1")
            .port(3306)
            .username("root")
            .password("your_password");
        dsConfig.setDatabase("benchmark_db");
        dsConfig.setTapTable(table);

        // 3. 数据生成配置
        DataGenerationConfig genConfig = new DataGenerationConfig()
            .totalRecords(1_000_000)
            .batchSize(1_000)
            .writeThreads(2);

        // 4. 组装并运行
        BenchmarkConfig benchConfig = new BenchmarkConfig()
            .addScenario(new WriteScenario(
                "MySQL Write 1M", table, genConfig,
                new DefaultRecordGenerator(genConfig.getTotalRecords(), 0)
            ))
            .addScenario(new ReadScenario("MySQL Read 1K×10", table, 1_000, 10))
            .reportOutputDir("/tmp/mysql-benchmark/reports")
            .reportTitle("MySQL Benchmark");

        new BenchmarkEngine(benchConfig, dsConfig).run();
    }
}
```

---

## 关键注意事项

### 1. connectionConfig 字段名必须与 spec.json 一致

每个 connector 的 `PaimonConfig`、`MysqlConfig` 等都通过 `DataMap` 中的字段名与 `@JsonProperty` 或字段声明绑定。字段名写错会导致 connector 使用空值或默认值，不会报错但行为异常。

验证方法：
```bash
# 查看 connector 的配置类字段名（以 MySQL 为例）
grep -n "@JsonProperty\|private.*String\|private.*Integer" \
  connectors/mysql-connector/src/main/java/.../MysqlConfig.java
```

### 2. 确认 connector 注册了哪些 PDK 函数

不同 connector 支持的能力集不同。使用前确认目标函数已注册：

```bash
grep -n "supportWriteRecord\|supportBatchRead\|supportCreateTable\|supportDropTable" \
  connectors/mysql-connector/src/main/java/.../MysqlConnector.java
```

| 函数 | 用途 | 未注册时的行为 |
|---|---|---|
| `WriteRecordFunction` | `write()` | 抛出 `IllegalStateException` |
| `BatchReadFunction` | `batchRead()` | 抛出 `UnsupportedOperationException` |
| `CreateTableV2Function` | `createTable()` | 静默跳过 |
| `DropTableFunction` | `dropTable()` | 静默跳过 |

### 3. 线程安全性

`BenchmarkEngine` 以 **单个 `DataSourceClient` 实例** 服务于所有写入线程（`WriteScenario` 多线程共享）。

- **线程安全的 connector**（如 Paimon，内部有 `synchronized`）：直接共享，无需改动。
- **非线程安全的 connector**：重写 `DataSourceConfig.createClient()`，在 `WriteScenario` 的每个线程内通过 `ThreadLocal<DataSourceClient>` 管理独立实例，或在配置类层面通过连接池实现。

### 4. onStart / onStop 必须配对调用

`BenchmarkEngine.run()` 使用 `try-with-resources` 确保 `client.close()` 始终执行：

```java
try (DataSourceClient client = dataSourceConfig.createClient()) {
    client.init();   // → connector.onStart()
    ...
}                    // → client.close() → connector.onStop()
```

`close()` 内应调用 `connector.onStop(connectionContext)`，保证 connector 正确释放连接、刷新缓冲区、关闭文件句柄。

### 5. `PdkContextFactory` 可直接复用

`PdkContextFactory`（位于 `connector/paimon/support/`）不含任何 Paimon 特定逻辑，任何 connector 均可直接复用：

```java
connectionContext = PdkContextFactory.buildConnectionContext(connectionConfig);
connectorContext  = PdkContextFactory.buildConnectorContext(connectionConfig, tapTable);
PdkContextFactory.updateTableMap(connectorContext, newTable);
```

内部使用 `Mockito.mock(TapNodeSpecification.class)`，与 `PerformanceTestRunner` 现有做法一致，无需完整 Engine 运行时环境。

---

## 扩展检查清单

新增一个数据源时，按以下步骤逐项确认：

- [ ] `pom.xml` 添加了目标 connector 的依赖，并已 `mvn install`
- [ ] 配置类继承 `DataSourceConfig`，字段名与 connector spec 一致
- [ ] `createClient()` 返回新的客户端实例
- [ ] `init()` 中调用了 `connector.onStart()` 和 `registerCapabilities()`
- [ ] `close()` 中调用了 `connector.onStop()`
- [ ] `getDataSourceType()` 返回可读的数据源标识字符串
- [ ] 确认了 `WriteRecordFunction` / `BatchReadFunction` 是否已注册
- [ ] 验证了 connector 的线程安全性，按需处理
- [ ] 测试入口中设置了 `dsConfig.setTapTable(table)` 和 `dsConfig.setDatabase(...)`

---

## 目录结构约定

```
src/main/java/io/tapdata/perftest/
└── connector/
    ├── paimon/                   # 已有
    │   ├── PaimonDataSourceConfig.java
    │   ├── PaimonDataSourceClient.java
    │   └── support/
    │       ├── PdkContextFactory.java    ← 所有 connector 共用
    │       ├── SimpleKVMap.java
    │       └── SimpleKVReadOnlyMap.java
    ├── mysql/                    # 新增
    │   ├── MySQLDataSourceConfig.java
    │   └── MySQLDataSourceClient.java
    └── kafka/                    # 未来扩展
        ├── KafkaDataSourceConfig.java
        └── KafkaDataSourceClient.java
```

每个 connector 一个子包，配置类和客户端类各一个文件，保持结构一致。
