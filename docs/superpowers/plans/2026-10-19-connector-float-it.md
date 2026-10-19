# Connector Floating-Point Integration Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add capability-scoped real-service IT coverage and floating-point mapping tests for the in-scope connectors in the current repository.

**Architecture:** Keep the existing MySQL-style connector context and Failsafe lifecycle for database connectors, while using small module-local IT harnesses for Redis and message brokers. Use spec-driven unit tests for ClickHouse/TiDB and as fallback evidence whenever an external service is unavailable.

**Tech Stack:** Java 8 source compatibility, JUnit 5, Maven Failsafe, `tapdata-connector-it`, `TapConnectorContext`, JDBC, Redis/Jedis, Kafka and RocketMQ client APIs.

**Spec:** `docs/superpowers/specs/2026-10-19-connector-float-it-design.md`

## Global Constraints

- Existing IT modules remain unchanged unless a mapping test is needed for the current branch's ClickHouse/TiDB spec changes.
- Ignore SaaS and JS connectors, and do not create tests for connector modules absent from this checkout.
- IT tests are skipped by default and run only with `-DskipITs=false`.
- Use table-provided internal endpoints first, then the table's reachable public endpoint; credentials must be overrideable and must not be logged.
- No IT test may assume a capability absent from `ConnectorFunctions` or from the actual connector implementation.
- Follow TDD: write each new assertion first, run it red, then add the minimum wiring/implementation and run it green.

## Review Focus

- Floating-point precision can be lost during JDBC conversion: Task 3 and Task 4 assert both schema Tap type and non-integral values.
- Redis values are strings even though its spec has float/double mappings: Task 5 tests mapping separately and avoids a false SQL-column assumption.
- Message connectors use JSON/NUMBER rather than relational FLOAT/DOUBLE: Tasks 6 and 7 assert payload values and offsets only.
- IT capability drift can make a generic suite test unsupported operations: every IT task gates calls on the connector's registered functions.
- External service failure can be mistaken for a passing build: Task 8 records each command's actual result and runs fallback unit tests where needed.

---

### Task 1: Establish the execution ledger and baseline

**Files:**
- Create: `.superpowers/sdd/2026-10-19-connector-float-it/progress.md`
- Read: the spec and this plan

- [ ] **Step 1: Create the plan ledger**

The first line must identify this plan. Record that the current feature branch is already isolated from `develop`, and record the shared interface ruling: module-local IT classes consume `tapdata-connector-it` only for context/codec helpers; no shared production helper is introduced.

- [ ] **Step 2: Run the baseline unit compile for the six new IT candidates**

Run:

```bash
mvn -pl connectors/mariadb-connector,connectors/redis-connector,connectors/tdengine-connector,connectors/vastbase-connector,connectors/rocketmq-connector,connectors/kafka-connector -am test-compile -DskipTests
```

Expected: the baseline result is captured; any pre-existing dependency/build failure is recorded before changes.

### Task 2: Add failing spec mapping tests

**Files:**
- Create: `connectors/clickhouse-connector/src/test/java/io/tapdata/connector/clickhouse/ClickhouseFloatingPointMappingTest.java`
- Create: `connectors/tidb-connector/src/test/java/io/tapdata/connector/tidb/TidbFloatingPointMappingTest.java`
- Create: `connectors/redis-connector/src/test/java/io/tapdata/connector/redis/RedisFloatingPointMappingTest.java`
- Create: `connectors/mariadb-connector/src/test/java/io/tapdata/connector/mariadb/MariadbFloatingPointMappingTest.java`
- Create: `connectors/tdengine-connector/src/test/java/io/tapdata/connector/tdengine/TDengineFloatingPointMappingTest.java`
- Create: `connectors/vastbase-connector/src/test/java/io/tapdata/connector/postgres/VastbaseFloatingPointMappingTest.java`

- [ ] **Step 1: Write the red tests**

Load each module's specification resource, build the `DefaultExpressionMatchingMap`, run `TableFieldTypesGeneratorImpl.autoFill`, and assert `TapFloat`/`TapDouble` for the exact native expressions listed in the spec. Also assert ClickHouse `Float32(6,2)` and the MySQL-family parameterized forms where present.

- [ ] **Step 2: Run only these tests**

Run the six module test selectors. Expected: at least the tests that exercise the branch's existing incorrect/ambiguous mapping behavior fail with an assertion or missing-expression failure; if a test passes immediately, keep it as a regression assertion and record that the current implementation already satisfies it.

- [ ] **Step 3: Implement only test resource parsing/helpers**

Keep mapping tests test-only. Do not change production mappings in this task; the current branch's spec changes are the behavior under test.

- [ ] **Step 4: Run the mapping tests green**

Run the same selectors and confirm all mapping assertions pass.

### Task 3: MariaDB IT and Maven wiring

**Files:**
- Modify: `connectors/mariadb-connector/pom.xml`
- Create: `connectors/mariadb-connector/src/it/java/io/tapdata/connector/mariadb/MariadbConnectorIT.java`
- Create: `connectors/mariadb-connector/src/it/resources/config/mariadb-connection.json`

- [ ] **Step 1: Write the failing IT assertions**

Create a unique MariaDB table with integer, `FLOAT`, `DOUBLE`, decimal and text fields. Assert discovery reports both floating fields, batch read preserves their non-integral values, and connector write/read can insert a second row. Add CDC/DDL only if the created context reports the corresponding functions and the service config enables them.

- [ ] **Step 2: Run the class before wiring**

Run `mvn -pl connectors/mariadb-connector -am -DskipITs=false -Dit.test=MariadbConnectorIT verify`. Expected: the new IT is not yet discoverable or fails at the intentionally incomplete harness, proving the test command exercises the intended module.

- [ ] **Step 3: Add minimal Failsafe/build-helper/test dependency wiring**

Match the existing MySQL POM's `tapdata-connector-it`, `src/it` source/resource registration, `skipITs` property and Failsafe execution. Do not alter Surefire behavior.

- [ ] **Step 4: Implement the real connector context and cleanup**

Instantiate `MariadbConnector`, load the table-derived config with override support, register capabilities, call lifecycle methods, and use JDBC only for test table setup/cleanup. Keep secrets out of log output.

- [ ] **Step 5: Run MariaDB unit and IT tests green**

Run the mapping test, `MariadbConnectorIT`, and module `verify`; record the actual service result.

### Task 4: TDengine and VastBase JDBC IT

**Files:**
- Modify: `connectors/tdengine-connector/pom.xml`
- Modify: `connectors/vastbase-connector/pom.xml`
- Create: `connectors/tdengine-connector/src/it/java/io/tapdata/connector/tdengine/TDengineConnectorIT.java`
- Create: `connectors/tdengine-connector/src/it/resources/config/tdengine-connection.json`
- Create: `connectors/vastbase-connector/src/it/java/io/tapdata/connector/postgres/VastbaseConnectorIT.java`
- Create: `connectors/vastbase-connector/src/it/resources/config/vastbase-connection.json`

- [ ] **Step 1: Write red JDBC capability tests**

For TDengine, use its registered schema/batch count/batch read/write capabilities and native `FLOAT`/`DOUBLE` columns. For VastBase, use `REAL`/`DOUBLE PRECISION` and the connector's PostgreSQL-compatible read/write capabilities. Assert schema mapping and non-integral round-trip values.

- [ ] **Step 2: Run red selectors**

Run each selector with `-DskipITs=false` before POM wiring; capture discovery or compile failure as the red result.

- [ ] **Step 3: Add minimal POM wiring and contexts**

Use the same Failsafe conventions as Task 3, with each module's exact spec resource and connector class. Enable only the functions actually registered by the connector.

- [ ] **Step 4: Run both module suites**

Run mapping tests, individual IT selectors and module `verify` commands; if either endpoint is unavailable, retain the mapping result and record the IT as unavailable with its error.

### Task 5: Redis capability-scoped IT

**Files:**
- Modify: `connectors/redis-connector/pom.xml`
- Create: `connectors/redis-connector/src/it/java/io/tapdata/connector/redis/RedisConnectorIT.java`
- Create: `connectors/redis-connector/src/it/resources/config/redis-connection.json`

- [ ] **Step 1: Write red fixed-schema tests**

Use the connector's discovered `tapdata` table. Verify schema names `redis_key` and `redis_value`, registered batch/write/stream functions, write a uniquely prefixed value, and verify it through the connector's actual batch/RDB path. Add a bounded stream assertion only if the configured mode exposes a resumable stream.

- [ ] **Step 2: Run the red selector**

Run the selector before Failsafe wiring and capture the expected discovery failure.

- [ ] **Step 3: Add test lifecycle wiring**

Configure the real Redis endpoint, call `onStart`/`onStop`, register codecs and state map, and remove only the test keys during cleanup.

- [ ] **Step 4: Run Redis IT and mapping tests**

Use bounded waits and report service reachability separately from assertion failures.

### Task 6: RocketMQ IT

**Files:**
- Modify: `connectors/rocketmq-connector/pom.xml`
- Create: `connectors/rocketmq-connector/src/it/java/io/tapdata/connector/rocketmq/RocketmqConnectorIT.java`
- Create: `connectors/rocketmq-connector/src/it/resources/config/rocketmq-connection.json`

- [ ] **Step 1: Write red topic capability tests**

Create a unique topic, discover it, write insert events containing integer and non-integral JSON number fields, read the produced events through batch/stream APIs, and assert the returned offset/event values. Do not assert relational schema or DDL.

- [ ] **Step 2: Run red selector and add Failsafe wiring**

Run the selector before wiring, then add only the standard test source/resource and Failsafe pieces.

- [ ] **Step 3: Implement lifecycle, unique topic and bounded consumer cleanup**

Ensure producer/consumer threads and connector service are closed in all paths; do not reuse pre-existing topics.

- [ ] **Step 4: Run RocketMQ IT**

Run the focused selector and module verify, recording endpoint availability and message assertions.

### Task 7: Kafka IT

**Files:**
- Modify: `connectors/kafka-connector/pom.xml`
- Create: `connectors/kafka-connector/src/it/java/io/tapdata/connector/kafka/KafkaConnectorIT.java`
- Create: `connectors/kafka-connector/src/it/resources/config/kafka-connection.json`

- [ ] **Step 1: Write red topic/JSON numeric tests**

Use the regular Kafka connector configuration from the table service, create a unique topic, write a JSON record containing integer and decimal numeric values, read it back through batch/stream, and assert event payload and offset. If the configured service has no schema registry, do not activate Avro-only paths.

- [ ] **Step 2: Run red selector and wire Failsafe**

Confirm the selector is discovered only after the standard POM wiring is added.

- [ ] **Step 3: Implement context and bounded cleanup**

Use the connector's actual `KafkaConfig` shape and close the producer/consumer/admin resources via `onStop`/finally.

- [ ] **Step 4: Run Kafka IT**

Run focused IT and module verify; retain a unit-only fallback result if the broker endpoint is unavailable.

### Task 8: Full verification and coverage ledger

**Files:**
- Modify: `.superpowers/sdd/connector-float-it/progress.md`
- Modify: `docs/superpowers/specs/2026-10-19-connector-float-it-design.md` only if a verified scope ruling changes

- [ ] **Step 1: Run changed-module unit tests**

Run the six new mapping suites plus ClickHouse/TiDB mapping suites and the existing relevant tests.

- [ ] **Step 2: Run all reachable IT selectors**

Run each of the six `*ConnectorIT` classes with `-DskipITs=false`, no blanket skip. Read each Failsafe report and classify it as passed, unavailable, or failed assertion.

- [ ] **Step 3: Run module-level Maven verification**

Run `verify` for all changed modules with ITs disabled and then the focused IT modules with ITs enabled where services are reachable.

- [ ] **Step 4: Inspect diff and final coverage matrix**

Run `git diff --check`, review all added configs for secrets/logging, verify no existing IT was rewritten, and write the final per-connector status into the ledger.

- [ ] **Step 5: Perform final self-review**

Review the branch against the design, especially capability gating, float/double assertions, resource cleanup, and unavailable-service reporting. Do not claim completion until fresh verification output supports it.

---

## Scope expansion after repository-wide re-audit

The original six-candidate plan was incomplete. The following tasks extend it
to every non-excluded connector module whose specification contains a
`TapFloat`/`TapDouble` mapping and which does not already have equivalent IT or
floating-point coverage.

### Task 9: Add spec-driven floating-point mapping coverage for non-service modules

**Files:**
- Create module-local `*FloatingPointMappingTest.java` under:
  `custom-connector`, `databend-connector`, `doris-connector`, `dws-connector`,
  `elasticsearch-connector`, `greenplum-connector`, `hbase-connector`,
  `highgo-connector`, `huawei-cloud-gaussdb-connector`, `kafka-avro-connector`,
  `kafka-enhanced-connector`, `opengauss-connector`, `selectdb-connector` and
  `starrocks-connector`.

- [ ] **Step 1: Add one test for each native single-precision mapping**

Load the module's own specification, build its `DefaultExpressionMatchingMap`,
run `TableFieldTypesGeneratorImpl.autoFill`, and assert `TapFloat` for the
exact native declaration (`Float(8,2)` for Databend's parameterized pattern,
or the exact `real`/`float`/`FLOAT` expression from each specification).

- [ ] **Step 2: Add one test for each native double-precision mapping**

Use the exact specification expression (`Double`, `double`, `double precision`
or `DOUBLE`) and assert `TapDouble`. Use the same resource parser as the
module's existing mapping tests when one exists.

- [ ] **Step 3: Run each module's focused tests**

Run the selectors with `-Dsurefire.failIfNoSpecifiedTests=false` and
`-DfailIfNoTests=false`; every new test must compile and pass.

### Task 10: Cover table-present but non-reachable message/key-value connectors

**Files:**
- Create `connectors/activemq-connector/src/test/java/io/tapdata/connector/activemq/ActivemqFloatingPointTest.java`
- Add any missing JSON payload unit test for the table-present connector while
  preserving the existing Redis/RocketMQ/Kafka tests.

- [ ] **Step 1: Test actual payload capability**

Round-trip a JSON map containing a single-precision and double-precision Java
number through the connector's `JsonParser` path. Do not assert SQL schema for
ActiveMQ, Redis, RocketMQ or Kafka.

- [ ] **Step 2: Re-run the aggregate message/key-value tests**

Confirm all payload tests pass, and record the unavailable live endpoints in
the ledger instead of marking them as live IT successes.

### Task 11: Re-audit and final verification

- [ ] **Step 1: Generate the connector/spec/IT/unit coverage matrix**

Cross-check every `connectors/*-connector` module against its spec, existing
`*IT.java` files, new mapping tests and the SaaS/JS exclusions.

- [ ] **Step 2: Run all changed-module unit tests and test compilation**

Aggregate the JUnit XML reports and require zero failures and zero errors.

- [ ] **Step 3: Run every live IT that has a verified reachable service**

Do not classify connection failures as assertion failures; preserve the exact
fallback unit-test result and endpoint classification in the ledger.

- [ ] **Step 4: Run `git diff --check` and review the final matrix**

Ensure no connector with a non-excluded floating-point spec is left without an
existing IT, new IT, or unit fallback.
