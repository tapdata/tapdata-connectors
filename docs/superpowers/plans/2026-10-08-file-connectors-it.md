# File Connector Integration Test Implementation Plan

> 执行约束：先写 IT 断言和 fixture，再补齐使其可执行的 Maven wiring；每一步完成后运行对应的最小验证命令，失败时先定位 API/生命周期问题，再继续扩展覆盖面。若真实能力场景暴露实现缺陷，则保留回归用例并做最小兼容修复。

## 1. Baseline and test harness red phase

### Files

- 新增五个模块的 `src/it/java` 目录。
- 新增五个模块的最小 `*ConnectorIT` 测试类和最小 fallback fixture。

### Work

1. 保持当前工作区已有提交不变，确认 `git status --short` 只显示本次新增内容。
2. 为每个 connector 先写一个最小 IT 场景：创建真实 connector、注册 capability、初始化 local-file context、读取 fixture、断言至少一个公开行为。
3. 每个测试类使用 JUnit 5，类名分别为：
   - `io.tapdata.connector.csv.CsvConnectorIT`
   - `io.tapdata.connector.excel.ExcelConnectorIT`
   - `io.tapdata.connector.json.FileStreamConnectorIT`
   - `io.tapdata.connector.json.JsonConnectorIT`
   - `io.tapdata.connector.xml.XmlConnectorIT`
4. 在测试侧写 module-local `FileITSupport`，不增加新的生产模块。该 helper 只负责：
   - fixture 根目录解析；
   - `TapConnectorContext`、`TestStateMap`、`ConnectorFunctions`、`TapCodecsRegistry` 的组装；
   - `TapEvent`/offset 收集和有界等待；
   - connector 停止和临时目录清理。
5. 先运行 IT compile/discovery 命令，确认预期的 red 状态（POM 尚未将 `src/it` 纳入测试生命周期或共享 IT API 尚未接入），记录具体编译错误；不得用修改生产代码的方式绕过错误。

### Verification

```bash
mvn -pl connectors/csv-connector,connectors/excel-connector,connectors/file-stream-connector,connectors/json-connector,connectors/xml-connector -am test-compile -DskipITs=true
```

该阶段的目的是真正让测试源码先表达行为需求；若 Maven 尚未编译 `src/it`，下一步只补测试生命周期 wiring。

## 2. Add Maven integration-test wiring

### Files

- `connectors/csv-connector/pom.xml`
- `connectors/excel-connector/pom.xml`
- `connectors/file-stream-connector/pom.xml`
- `connectors/json-connector/pom.xml`
- `connectors/xml-connector/pom.xml`

### Work

在五个 POM 中保持一致地增加：

1. `<skipITs>true</skipITs>`，确保普通 `verify` 不启动文件 IT。
2. `io.tapdata:tapdata-connector-it:1.0-SNAPSHOT` 的 test dependency，复用 MySQL IT 已使用的 context、state map、codec registry 和 specification helper。
3. `build-helper-maven-plugin:3.2.0` 的 `generate-test-sources` execution：加入 `src/it/java` 和 `src/it/resources`。
4. `maven-failsafe-plugin:3.5.6` 的 `integration-test`/`verify` goals，配置 `${skipITs}`、`useModulePath=false`、Failsafe report 目录以及当前 JDK 运行 file connector 所需的 `--add-opens` 参数。
5. 保留现有 Surefire、assembly、bundle、compiler 和资源复制配置，不把 IT 绑定到 unit-test phase。

### Verification

```bash
mvn -pl connectors/csv-connector,connectors/excel-connector,connectors/file-stream-connector,connectors/json-connector,connectors/xml-connector -am test-compile -DskipITs=true
mvn -pl connectors/csv-connector -am verify -DskipITs=false -Dit.test=CsvConnectorIT
```

第二条命令先允许失败，目标是确认 `CsvConnectorIT` 已被 Failsafe 发现并进入测试执行，而不是被 Surefire 或 Maven 资源阶段静默忽略。

## 3. Implement fixture resolver and lifecycle support

### Files

- `connectors/csv-connector/src/it/java/io/tapdata/connector/it/FileITSupport.java`
- `connectors/excel-connector/src/it/java/io/tapdata/connector/it/FileITSupport.java`
- `connectors/file-stream-connector/src/it/java/io/tapdata/connector/it/FileITSupport.java`
- `connectors/json-connector/src/it/java/io/tapdata/connector/it/FileITSupport.java`
- `connectors/xml-connector/src/it/java/io/tapdata/connector/it/FileITSupport.java`

### Work

每个模块保留同名、同职责的 test-only helper，避免为共享测试代码新建生产 artifact。helper 的行为固定为：

1. fixture 只从当前模块 `src/it/resources/fixtures` 的 classpath 加载，并复制到测试专属临时目录。
2. 配置 local protocol、绝对 `filePathString`、`modelName`、过滤规则、header/data start line 和格式-specific 参数。
3. 按 `MySQLConnectorIT.createContext` 的模式创建 node context；每个 connector 使用自己的 `spec_*.json`。
4. 对 `discoverSchema`、`batchRead` 和 timestamp offset 提供同步收集方法；callback 未结束时使用明确 timeout，并把已收集 event 类型和 fixture 路径带入失败信息。
5. `streamRead` 使用独立线程和 `StreamReadConsumer`，成功或异常都调用 `streamReadEnded`/停止逻辑；测试结束调用 connector 的 `onStop`，释放 storage 和 executor。
6. 对 `InputStream` 类型字段在断言时读取并关闭，避免 file-stream 测试留下打开的文件句柄。

### Verification

- 只运行一个 connector 的最小 IT，确认 context 初始化和 teardown 都执行。
- 故意移除一个 classpath fixture 运行一次，确认错误中包含缺失的 fixture 名称，而非长时间阻塞。

## 4. CSV integration tests

### Files

- `connectors/csv-connector/src/it/java/io/tapdata/connector/csv/CsvConnectorIT.java`
- `connectors/csv-connector/src/it/resources/fixtures/csv/1.csv`

### Work

1. 使用模块内 `src/it/resources/fixtures/csv/1.csv`，不读取外部目录。
2. 添加 schema test：配置 header line 1、data start line 2，断言单表、字段名 `ID`/`Name`/`Age` 以及 discovery 的类型推断。
3. 添加 batch test：收集 insert record 和结束 offset，断言 9 条记录以及 `Alice`/`Bob` 等稳定值。
4. 添加 timestamp offset test：确认 offset 包含当前过滤文件集合，序列化/恢复后路径和数据行信息保持一致。
5. 校验 stream capability 已注册，并验证 timestamp offset 可生成。
6. 只在 CSV 已注册 `writeRecord` 的前提下增加一个最小写入回归；写入路径使用临时目录，不污染仓库资源。

### Verification

```bash
mvn -pl connectors/csv-connector -am verify -DskipITs=false -Dit.test=CsvConnectorIT
```

## 5. Excel integration tests

### Files

- `connectors/excel-connector/src/it/java/io/tapdata/connector/excel/ExcelConnectorIT.java`
- `connectors/excel-connector/src/it/resources/fixtures/excel/data.csv`

### Work

1. 从 `fixtures/excel/data.csv` 读取测试数据。
2. 在测试运行期用 Apache POI 创建小型 workbook，sheet 为 `data`，包含 id、name、date、time、datetime 列，并设置日期/时间单元格格式。
3. 添加 schema test：设置 `sheetLocation=data`、header line 和 data start line，断言字段名及 date/time/datetime 类型。
4. 添加 batch test：断言 fixture 中全部记录和事件完成。
5. 添加 `justString=true` test，确认日期和数值输出为字符串，不改变字段数量或表名。
6. 添加 timestamp offset test，确认 workbook 文件在 offset 中被记录，重复使用同一 offset 不会错误地把路径解析成其他 sheet。
7. Excel 不添加 writeRecord target test，因为实现没有注册该 capability。

### Verification

```bash
mvn -pl connectors/excel-connector -am verify -DskipITs=false -Dit.test=ExcelConnectorIT
```

## 6. File-stream integration tests

### Files

- `connectors/file-stream-connector/src/it/java/io/tapdata/connector/json/FileStreamConnectorIT.java`
- `connectors/file-stream-connector/src/it/resources/fixtures/file-stream/payload.txt`

### Work

1. schema test 断言固定表 `file` 以及 `file_name`、`file_path`、`file_size`、`last_modified`、`file_data` 五个字段和类型。
2. batch count/read test 使用两个临时文件，断言 count 等于过滤后的文件数；每个文件只生成一条 insert event，元信息与实际文件一致，`file_data` 内容可完整读取。
3. timestamp offset test 断言 offset 保存目录中的文件列表，包含 file path、length 或 last-modified 信息。
4. stream test 在已有 offset 后新增一个文件，等待新增文件事件；由于公共 `FileConnector` 的轮询实现固定为约 60 秒，使用单个新增文件和 75 秒上限，并在 finally 中结束 consumer。
5. writeRecord test 通过 registered write function 写入 `payload.txt` 内容到临时目录，重新读取目标文件并断言字节一致；只提交 insert event，其他 event 类型确认不会错误计数。

### Verification

```bash
mvn -pl connectors/file-stream-connector -am verify -DskipITs=false -Dit.test=FileStreamConnectorIT
```

## 7. JSON integration tests

### Files

- `connectors/json-connector/src/it/java/io/tapdata/connector/json/JsonConnectorIT.java`
- `connectors/json-connector/src/it/resources/fixtures/json/array.json`
- `connectors/json-connector/src/it/resources/fixtures/json/object.json`

### Work

1. array fixture 使用对象数组，至少包含数值、字符串、布尔和嵌套对象；断言每个数组元素产生一条 insert record，schema 与值类型一致。
2. object fixture 使用 property-to-object 映射；断言每个 property value 产生一条记录，并且 `__key` 等于 property name。
3. 两种 `jsonType` 分别运行 discovery 和 batch read，保证 schema 与实际读取配置一致。
4. 添加多文件 batch 和 timestamp offset test，确认文件路径排序不会影响 object root 的 `__key` 值。
5. 添加 stream 新文件 test，使用小型 JSON 文件和有界等待；断言新增文件的记录被捕获后正常停止。
6. 对空文件或非对象数组元素只断言当前实现的稳定结果，不写入未实现的字段顺序或额外 coercion 假设。

### Verification

```bash
mvn -pl connectors/json-connector -am verify -DskipITs=false -Dit.test=JsonConnectorIT
```

## 8. XML integration tests

### Files

- `connectors/xml-connector/src/it/java/io/tapdata/connector/xml/XmlConnectorIT.java`
- `connectors/xml-connector/src/it/resources/fixtures/xml/items.xml`

### Work

1. 使用模块内 `src/it/resources/fixtures/xml/items.xml`，保留 item/info、CDATA、注释和 processing instruction。
2. 配置 `XPath=/rss/channel/item/info`（按实际 fixture root 调整为等价绝对路径），schema test 断言命中节点和字段。
3. batch test 收集 SAX 事件，断言文本和 CDATA 内容；明确断言注释和 processing instruction 不进入 `info` 字段值。
4. 添加多文件 batch 和 timestamp offset test，确认 XPath 配置作用于每个过滤文件。
5. 添加 stream 新文件 test，使用临时目录和有界等待，成功后通过 consumer end signal 退出。
6. XML 测试不对属性、entity reference 或未被 `BigSaxDataHandler` 支持的节点类型增加额外保证。

### Verification

```bash
mvn -pl connectors/xml-connector -am verify -DskipITs=false -Dit.test=XmlConnectorIT
```

## 9. Full verification and cleanup

### Work

1. 先运行五个模块的 unit test，确保 IT wiring 没有污染现有 Surefire。
2. 运行五个模块的完整 IT，确认所有测试只使用模块内 classpath fixture。
3. 检查 Failsafe reports、线程是否残留、临时文件是否清理，以及所有 stream 测试是否在失败路径结束 consumer。
4. 检查新增资源大小，确认大型 Excel 没有进入 Git；检查 POM 中五个模块的 Failsafe 配置一致。
5. 运行 `git diff --check`、相关模块构建和最终 `git status --short`；只保留本次 IT、fixture、POM 和文档变更。

### Verification commands

```bash
mvn -pl connectors/csv-connector,connectors/excel-connector,connectors/file-stream-connector,connectors/json-connector,connectors/xml-connector -am test -DskipITs=true
mvn -pl connectors/csv-connector,connectors/excel-connector,connectors/file-stream-connector,connectors/json-connector,connectors/xml-connector -am verify -DskipITs=false
git diff --check
```

成功标准是五个 `*ConnectorIT` 均被 Failsafe 执行且通过，默认 `verify` 仍跳过 IT，单元测试无回归，生产修复由相应 IT 覆盖且不改变默认配置语义。

## 10. Actual coverage and implementation findings

最终五个 IT 共覆盖 48 个场景：CSV 13、Excel 9、file-stream 8、JSON 10、XML 8。除公共生命周期外，分别覆盖了格式特有的 header/分隔符/写入 marker、Excel headerless/multi-sheet/column range/temporal cells、文件元信息和过滤、JSON object/array/nested/null/decimal、XML XPath/CDATA/comment/PI/nested/repeated children。

真实运行过程中保留并修复了以下问题：

1. `FileConnector` 的流轮询等待从硬编码改为 `streamReadInterval`，默认仍为 60 秒，IT 设置为 1 秒以保证有界执行。
2. `JsonReaderUtil` 的 `NULL` 分支消费 `nextNull()`，JSON schema 采样和 batch read 均能保留 null。
3. XML schema 采样识别 dom4j 包装的 `StopException`；`MatchUtil` 对未单独声明的 XML 嵌套子字段回退字符串；`HandlerBase` 聚合混合子节点中的重复名称为列表。

所有格式文本 fixture 都位于各模块 `src/it/resources`，测试通过 classpath 复制到临时目录，不引用开发机路径、系统属性或环境变量。
