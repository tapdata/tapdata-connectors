# File Connector Integration Test Design

## 1. Goal and scope

为 `csv-connector`、`excel-connector`、`file-stream-connector`、`json-connector` 和 `xml-connector` 增加与 `mysql-connector/src/it` 同等级别的集成测试入口，并复用 TapData connector IT 的上下文、能力注册、offset 和事件验证方式。

本次工作覆盖：

- 总结并对齐 MySQL connector IT 的组织方式：真实 connector 初始化、schema discovery、batch read、stream read、offset resume，以及 connector-specific 行为回归。
- 为五个文件型 connector 各增加 `src/it/java` 和必要的 `src/it/resources`。
- 为五个模块补齐 Maven Failsafe 集成测试生命周期，使测试默认跳过、通过 `-DskipITs=false` 显式执行。
- 所有测试资源都提交到对应 connector 的 `src/it/resources`，测试不读取开发机目录、系统属性或环境变量中的外部 fixture。

除了 IT 本身，本次还修复了由真实场景暴露的实现缺陷：文件流轮询间隔可配置但默认行为保持 60 秒；JSON null 值正确消费 token；XML schema 采样能识别被 dom4j 包装的停止信号，且混合子节点中的重复名称聚合为列表；XML 嵌套子字段缺少类型时按字符串处理。所有修复都由对应 IT 回归覆盖。

## 2. Existing MySQL IT pattern

`mysql-connector/src/it` 由三层组成：

1. `MySQLConnectorIT` 提供 MySQL 连接、表结构和数据准备，并继承共享的 DB connector IT，用于覆盖 source/target、全量/增量、DDL、事务、offset、类型和大字段。
2. `MySQLTpccAdapter` 和 `MySQLPerformanceAdapter` 把 TPCC 或性能场景接入统一测试框架，并通过 profile 控制成本较高的测试。
3. `src/it/resources/config/mysql-connection.json` 保存连接配置；POM 通过 build-helper 添加 `src/it`，通过 Failsafe 执行 `*IT`。

文件 connector 没有 MySQL 的表、事务、DDL 和数据库 verifier，因此不会直接继承 DB-centric 的完整 `ConnectorIT` 套件。它们将沿用相同的生命周期和 `ConnectorTestContext` 结构，但针对文件源建立专用的事件/schema 断言。

## 3. Test harness architecture

每个 connector 的 IT 类独立放在自己的模块中，命名为 `<Connector>ConnectorIT`。测试使用真实 connector 实例和真实 local-file storage，不 mock 文件读取过程。

统一的测试流程如下：

1. 从当前 connector 的 `src/it/resources/fixtures` 加载资源，并复制到测试专属临时目录；connector 配置只引用该临时目录。
2. 创建 `TapNodeSpecification`、`TapNodeContext`、`TapConnectorContext`、`TestStateMap`、`ConnectorFunctions` 和 `TapCodecsRegistry`，注册被测 connector 的 capability。
3. 调用 connector 的 `init`、`discoverSchema`、`batchRead`、`timestampToStreamOffset` 或 `streamRead`，通过 callback 收集 schema、offset 和 `TapEvent`。
4. 在每个测试结束时停止 connector 并释放 context，避免线程和 local-file storage 状态泄漏到后续测试。
5. 对事件数量、字段名、字段值、事件类型、文件名和 offset 内容作明确断言；不依赖日志文本作为测试结果。

测试按能力分组，但不要求所有 connector 具备相同的写入能力：

- `schema`: discovery 输出包含预期字段和类型。
- `batch`: 一次性读取返回预期文件或记录，并产生结束事件。
- `offset`: 从 batch 结束位置得到可序列化 offset；在 fixture 不变时可以恢复到同一文件集合，不重复产生历史数据。
- `stream`: 启动 stream 后添加或修改 fixture，确认新文件或新版本被捕获，再主动结束读取线程。
- `connector-specific`: 验证各格式实现中最容易回归的解析语义。

## 4. Test matrix

### 4.1 CSV

Fixture 使用模块内 `src/it/resources/fixtures/csv/1.csv`。测试覆盖：

- capability 注册：batchCount、batchRead、streamRead、timestampToStreamOffset 和 writeRecord 均可用。
- schema discovery：默认推断与 `justString=true` 两种模式。
- batch read：默认 header、显式 header、空行跳过、短行补 null、tab 分隔和 off-standard 正则分支。
- streamRead：从 timestamp offset 开始发现新 CSV 文件。
- writeRecord：insert/update/delete 三种 marker、按 record 字段切文件、按日期表达式分区文件。

### 4.2 Excel

Excel 的输入数据保存在模块内 `src/it/resources/fixtures/excel/data.csv`。测试从 classpath 读取该资源，再用 Apache POI 在临时目录生成真实 xlsx 工作簿，确保 Excel 日期/时间单元格格式由测试资源稳定复现。

测试覆盖：

- capability 注册以及无 writeRecord 能力的边界。
- 指定 sheet、header line、data start line 和列范围的 schema discovery。
- 有 header 与无 header workbook 的 batch read。
- 单 sheet 与多 sheet 读取，验证 `sheetLocation` 范围。
- `date`、`time`、`datetime` 单元格的字段类型和精确值，验证 `CellValueConvert` 的日期/时间语义。
- `justString` 配置下的字符串化读取。
- timestamp offset 和新 workbook 的 streamRead。

Excel connector 当前没有 `writeRecord` 能力，因此不添加 target write 测试，也不把文件 target 语义强行套用到该模块。

### 4.3 File stream

File-stream connector 的每个文件对应一条 `TapInsertRecordEvent`，固定 schema 为 `file_name`、`file_path`、`file_size`、`last_modified` 和 `file_data`。测试覆盖：

- schema discovery 返回固定字段和预期类型。
- batch count 等于过滤后的文件数量。
- batch read 每个文件只产生一条记录，文件元信息与 fixture 一致，`file_data` 可以完整读取。
- 递归目录和正则过滤只返回匹配文件。
- streamRead 从 timestamp offset 发现新文件。
- writeRecord 只接受带 `file_data` 的 insert，忽略 update、delete 和缺失内容的 insert。

### 4.4 JSON

使用模块内小型 fixture 覆盖 JSON object root、JSON array root、嵌套 object/array、decimal、boolean、null 和多文件目录。

测试覆盖：

- array root：每个数组对象产生一条记录，字段类型和记录数正确。
- object root：每个 property value 产生一条记录，`__key` 被注入且 key/value 对应正确。
- schema discovery 与 batch read 使用相同的根类型配置，验证 primitive、nested object、array 和 null。
- 多文件 batch、timestamp offset 和新文件 streamRead。
- JSON 数字按实现统一为 `BigDecimal`/`NUMBER`，不把整数值误断言成另一种 Java 类型。

### 4.5 XML

使用模块内 `src/it/resources/fixtures/xml/items.xml` 的 RSS 结构验证 XPath 选择。核心 XPath 为 `/rss/channel/item/info`。

测试覆盖：

- XPath 命中的节点数量和字段 schema。
- SAX batch read 的记录内容和结束事件。
- `info` 节点中注释和 processing instruction 不进入字段值，验证 `BigSaxDataHandler` 的节点过滤语义。
- 嵌套子节点展开、混合子节点中的重复名称列表以及 `justString` schema。
- timestamp offset 和新文件 streamRead。
- XML 文本、CDATA 和嵌套结果按当前实现断言，不对属性或未支持节点类型作超出实现范围的保证。

## 5. Fixture strategy

测试资源统一从模块 `src/it/resources/fixtures` 的 classpath 加载，然后复制到测试专属临时目录。这样测试不依赖开发机目录，也不允许通过绝对路径绕过仓库资源管理。

Excel 测试从 `fixtures/excel/data.csv` 读取稳定数据，再使用 POI 创建临时 workbook，覆盖日期、时间、日期时间、字符串和数值列；测试同时在内存中生成无表头和多 Sheet workbook。临时文件使用测试专属临时目录，生命周期结束后删除。其余格式的文本 fixture 均通过 classpath 加载后复制到临时目录。

## 6. Maven and execution design

五个模块的 POM 增加相同的 IT 基础配置：

- `skipITs` 默认值为 `true`。
- 添加 `io.tapdata:tapdata-connector-it:1.0-SNAPSHOT` test dependency，以复用 context、state map、codec registry 和 connector test helpers。
- 使用 build-helper 把 `src/it/java` 和 `src/it/resources` 加入测试源码及资源。
- 使用 `maven-failsafe-plugin` 3.5.6 的 `integration-test` 和 `verify` executions，配置 `<skipITs>${skipITs}</skipITs>` 和当前 JDK 所需的 module opens。
- 保留各模块现有 Surefire/unit-test 配置；集成测试只由 Failsafe 的 `*IT` 命名约定触发。

执行方式：

```bash
mvn -pl connectors/csv-connector,connectors/excel-connector,connectors/file-stream-connector,connectors/json-connector,connectors/xml-connector -am verify -DskipITs=false
```

只运行一个模块时，可以将 `-pl` 缩小到目标模块；测试资源仍由该模块的 classpath 提供。

## 7. Failure handling and isolation

- 所有 callback 收集器都使用有界等待或明确结束事件，测试失败时输出已收到的事件类型和数量。
- stream 测试必须在成功或失败路径都调用停止逻辑，避免 Failsafe 卡住。
- 每个测试使用独立临时目录或只读 classpath fixture；写测试不修改仓库内资源。
- offset 断言只依赖 public offset 内容和文件集合，不比较对象 identity 或日志顺序。
- 测试不要求网络、数据库、外部服务或人工操作。

## 8. Acceptance criteria

实现完成后满足以下条件：

1. 五个模块均存在可发现的 `*ConnectorIT`，并能通过 Failsafe 编译。
2. 每个模块至少覆盖 schema、batch read 和 offset；具备 stream 能力的模块还覆盖新增文件事件；file-stream 额外覆盖 writeRecord。
3. CSV、Excel、JSON、XML 的格式特有语义均有至少一个明确回归断言；Excel 使用日期/时间字段，XML 使用注释/processing-instruction 过滤，JSON 同时覆盖 object/array root。
4. 所有 fixture 都位于模块 `src/it/resources`，在不同开发机和 CI 环境中具有相同输入。
5. 默认 Maven 构建不执行 IT；显式 `-DskipITs=false` 时能够执行目标 IT，失败信息可定位到 connector 和 fixture。
6. 单元测试和已有模块构建不被破坏；生产修复必须由暴露缺陷的 IT 覆盖，且默认配置行为保持兼容。

## 9. Risks and decisions

- 共享 `ConnectorIT` 假设数据库表和 verifier，直接继承会制造不适用的 CRUD、DDL 和 target 测试，因此选择“复用上下文能力、专用文件断言”。
- stream reader 的轮询周期较长，测试通过添加文件后等待 callback 事件并设置有界 timeout；必要时使用 connector 提供的结束 callback，而不是等待自然超时。
- 开发机 fixture 的绝对路径不能成为测试输入，因此所有测试数据都必须进入 `src/it/resources` 或由其中的文本数据确定性生成。
- 五个 connector 的 schema 输出存在格式差异，公共 helper 只负责生命周期、fixture 和事件收集，不抽象字段断言，避免掩盖格式-specific 回归。
