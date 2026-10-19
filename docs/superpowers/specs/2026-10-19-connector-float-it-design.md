# Connector Floating-Point Integration Test Design

## Goal

为当前 `tapdata-connectors` 仓库中、且在用户提供的内部数据源清单里存在可用服务的连接器补齐真实服务集成测试；对已有 IT 的连接器跳过完整套件，但对当前分支修改的浮点类型映射增加单元覆盖。测试必须按连接器实际注册的能力执行，并至少覆盖单精度和双精度浮点场景。

## Scope decision

表格服务与当前仓库模块的交集如下：

| Connector | Service in table | Existing IT | This change |
|---|---:|---:|---|
| `mariadb-connector` | MariaDB 13306 | No | Add IT + FLOAT/DOUBLE round trip |
| `redis-connector` | Redis 6379 | No | Add IT for fixed-schema read/write/stream capabilities; add mapping assertions for FLOAT/DOUBLE |
| `tdengine-connector` | TDengine 6041 | No | Add IT + FLOAT/DOUBLE round trip |
| `vastbase-connector` | VastBase 5434 | No | Add IT + REAL/DOUBLE PRECISION round trip |
| `rocketmq-connector` | RocketMQ 9876 | No | Add topic/write/read/stream IT for JSON numeric payloads |
| `kafka-connector` | Kafka 9092/9094 | No | Add topic/write/read/stream IT for JSON numeric payloads |
| `mysql-connector` | MySQL 23306/33306 | Yes | Skip |
| `postgres-connector` | PostgreSQL 15432/25432/35432 | Yes | Skip |
| `tidb-connector` | TiDB 4000 | Yes | Skip complete IT; add spec mapping unit tests |
| `clickhouse-connector` | ClickHouse service | Yes | Skip complete IT; add spec mapping unit tests |
| `mongodb-connector` | MongoDB 27017/37017 | Yes | Skip |
| `oceanbase-mysql-connector` | no matching table service | Yes | Skip |

The table also contains Oracle, MSSQL, Sybase, Kingbase, DB2/AS400, SFTP, S3 and Kunlun records, but there is no corresponding connector module in this checkout. They are recorded as unavailable rather than represented by a fake test. SaaS and JS connectors are excluded as requested.

## Test architecture

Each new IT module uses the MySQL IT conventions where they apply:

1. A module-local `*ConnectorIT` creates the real connector, `TapConnectorContext`, `ConnectorFunctions`, `TapCodecsRegistry` and `TestStateMap`.
2. Connection configuration is loaded through the shared connection-config loader, so table-derived defaults can be overridden by `connector.it.*` properties or `CONNECTOR_IT_*` variables without changing test code.
3. Every test uses a unique topic/table/key prefix and cleans it up in `finally`/`@AfterEach`.
4. IT test classes are wired through `src/it/java`, `src/it/resources` and Failsafe, and remain skipped unless `-DskipITs=false` is supplied.
5. No test asserts capabilities that the connector does not register. DDL, CDC and target behaviors are only tested when the registered function and the service configuration make the behavior reachable.

The six connectors are deliberately not forced into one common base class:

- JDBC connectors use SQL setup and real JDBC assertions. Their type tables contain a native single-precision and double-precision column and assert schema type plus read/write value preservation.
- Redis uses its fixed `redis_key`/`redis_value` schema and its configured value mode. It tests the connector's actual string/RDB behavior; the `float` and `double` entries in `spec_redis.json` are asserted separately as type mappings rather than pretending Redis stores SQL columns.
- RocketMQ and Kafka use unique topics. Their payloads are JSON records, so the tests assert numeric values and message offsets according to the connector's `NUMBER`/JSON capability, not a fabricated relational FLOAT/DOUBLE DDL.

## Floating-point assertions

The minimum required cases are:

- MariaDB: `FLOAT` and `DOUBLE` columns, including schema mapping and a non-integral value such as `3.1415926`.
- TDengine: `FLOAT` and `DOUBLE` columns, including batch read and target write values.
- VastBase: `REAL` and `DOUBLE PRECISION` columns, including schema mapping and batch round trip.
- ClickHouse: `Float32` maps to `TapFloat`; `Float64` maps to `TapDouble`; the parameterized `Float32(precision,scale)` mapping is also checked.
- TiDB: `float(precision,scale)` maps to `TapFloat` and `double` maps to `TapDouble`.
- Redis: the `float`/`double` spec entries resolve to the corresponding Tap types; no unsupported SQL round trip is asserted.

If an external service cannot be reached, the affected IT is not silently converted into a passing test. The mapping unit test remains runnable, and the final verification report records the exact unavailable service and the command/result.

## Configuration and safety

- Prefer the internal host/port from the provided table. If only a reachable public endpoint is present in the table, use that endpoint as the default and keep the same override mechanism.
- Do not print passwords or put them into assertions. Connection logs print only connector name, host and port.
- Existing repository IT configuration conventions are preserved; no unrelated credentials or production configuration are changed.

## Acceptance criteria

1. Every in-scope connector has either an existing IT, a discoverable new `*ConnectorIT` class, or a module-local floating-point/payload unit test.
2. Each IT class covers only registered, reachable capabilities and cleans up resources.
3. JDBC ITs contain explicit single/double floating-point scenarios; all other in-scope specs have corresponding `TapFloat`/`TapDouble` mapping assertions.
4. Unit tests for all new mapping assertions fail when the expected Tap type or expression mapping is removed.
5. Unit tests and Maven test compilation pass for all changed modules.
6. Each reachable service IT is run explicitly with `-DskipITs=false`; inaccessible services are reported with their fallback unit-test result rather than claimed as passed.
7. `git diff --check` is clean and no existing IT module is rewritten unnecessarily.

## Full repository re-audit (2026-10-19)

The first pass was too narrow. The repository-wide scan found 30 connector
specifications containing `TapFloat` or `TapDouble` mappings, plus message
connectors whose real payload capability is numeric JSON rather than a
relational floating-point column.

### Coverage decision

| Group | Connectors | Decision |
|---|---|---|
| Existing IT, do not duplicate | MySQL, PostgreSQL, TiDB, ClickHouse, MongoDB, OceanBase MySQL, Excel, RisingWave | Keep existing IT; no duplicate suite |
| IT already added in this change | MariaDB, TDengine, VastBase | Keep and run when the configured service is reachable |
| Table service present but unavailable from this environment | Doris, Huawei Cloud GaussDB, ActiveMQ, Redis, RocketMQ, Kafka | Add/retain capability-specific unit fallback; only add full IT where the endpoint can actually be exercised |
| No table service or no usable repository service | Custom, Databend, DWS, Elasticsearch, Greenplum, HBase, Highgo, Kafka Avro, Kafka Enhanced, OpenGauss, SelectDB, StarRocks | Add spec-driven `TapFloat`/`TapDouble` conversion unit tests |
| Already has floating-point unit coverage | Paimon Plus, ClickHouse, TiDB, MariaDB, TDengine, VastBase, Redis, Kafka, RocketMQ | Retain and include in final aggregate verification; StarRocks receives the missing float/double assertions |
| SaaS/cloud/JS scope excluded by request | BigQuery, Snowflake, TableStore, Zoho Desk, QuickAPI and JS connectors | Do not add this round's tests |
| Service listed but module absent | DM, DB2/AS400, MSSQL, Oracle, Sybase, Kingbase, Hive, SFTP and related table-only records | No repository connector exists; no test can be added here |

The table recheck also found Doris on ports 9030/29030, GaussDB on 5432 and
ActiveMQ with only its management port 8161. The available port does not
provide a verified connector endpoint in this environment, so these are not
claimed as passing live ITs. Redis, RocketMQ and Kafka were already probed in
the first pass and remain unit-fallback cases after their connection/auth/topic
operations failed.

### Expanded acceptance criteria

1. Every non-excluded repository connector whose specification maps a native
   single- or double-precision type has either an existing IT, a new live IT,
   or a module-local conversion unit test.
2. Message/key-value connectors test their actual JSON numeric payload path;
   they do not invent SQL columns that the connector does not support.
3. The final ledger names every candidate connector and records why it was
   tested, skipped, or covered by fallback unit tests.
