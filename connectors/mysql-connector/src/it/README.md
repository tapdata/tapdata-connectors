# MySQL connector integration coverage

Updated: 2026-09-14

`MySQLConnectorIT` invokes the connector API against a real MySQL database. It combines the shared
`ConnectorIT` suite with MySQL-specific regressions derived from `t-layer3-test`, plus opt-in TPCC and
performance suites.

## Twelve acceptance categories

| # | Category | MySQL coverage |
|---|---|---|
| 1 | Basic full source | Shared `discoverSchema`, `batchCount`, and `batchRead` tests |
| 2 | Basic incremental source | Shared incremental test plus `should_stream_insert_update_and_delete` |
| 3 | TPCC full source | `TpccConnectorIT.should_read_tpcc_schema_and_counts` and `should_batch_read_all_tpcc_source_rows` |
| 4 | TPCC incremental source | TPCC workload CDC and saved-offset resume tests |
| 5 | Basic target | Shared create/write/update/delete/clear/drop tests |
| 6 | TPCC target | TPCC tables recreated and populated through connector target APIs |
| 7 | Four DDL cases | Shared target add/drop/rename/alter tests plus `should_stream_supported_source_ddl_events` |
| 8 | Offset reset/resume | Shared offset tests plus `should_resume_from_saved_offset` |
| 9 | Full types source | `should_round_trip_full_types_and_one_megabyte_fields` covers temporal, unsigned, JSON, text, and binary types |
| 10 | Full types target | The same test writes a codec-wrapped row and reads it back through `batchRead` |
| 11 | Transactions | Commit, rollback, savepoint, uncommitted visibility, and opt-in long transaction tests |
| 12 | Large fields | 1 MiB `LONGTEXT` and `LONGBLOB` values pass full read, target write, and CDC |

## Layer3 regressions moved down

The connector-level suite intentionally covers scenarios that do not require an engine pipeline:

- `item_3_1_1.py`, `item_3_1_2261.py`, and `item_3_2255.py`: full/CDC types, datetime, and unsigned bigint.
- `item_3_3_1.py` and `item_3_3_2.py`: TPCC full and incremental behavior.
- `item_3_6_1.py` through `item_3_6_4.py`, plus `item_3_12631.py`: source and target DDL behavior.
- `item_3_9334.py`: no-primary-key CDC rows containing null fields.
- `item_3_5488.py`, `item_3_5949.py`, and `item_3_5950.py`: index and constraint metadata through the shared suite.

Cross-connector mapping, engine expressions, task scheduling, and target-specific behavior remain in
`t-layer3-test`; they are not equivalent to connector API integration tests.

## Running

Install the shared test framework first:

```bash
cd /home/tapdata-it
mvn -DskipTests install
```

Run the default suite (TPCC, performance, and long transactions excluded):

```bash
cd /home/tapdata-connectors
mvn -pl connectors/mysql-connector -am verify -DskipITs=false
```

Run opt-in suites:

```bash
mvn -pl connectors/mysql-connector -am verify -Ptpcc-only
mvn -pl connectors/mysql-connector -am verify -Pperformance-only
mvn -pl connectors/mysql-connector -am verify -Plong-transaction-only
```

Connection values can be overridden with `connector.it.*` system properties or `CONNECTOR_IT_*`
environment variables. TPCC uses BenchmarkSQL from `TPCC_BENCHMARK_HOME` and writes generated
properties/logs under `TPCC_WORK_DIR`.

## DBForge configuration source

The default source remains `src/it/resources/config/mysql-connection.json`. To acquire a
`mysql/dedicated/single` lease from DBForge instead, select `dbforge` explicitly:

```bash
CONNECTOR_IT_CONFIG_SOURCE=dbforge \
CONNECTOR_IT_DBFORGE_URL=http://<dbforge-host>:<port> \
CONNECTOR_IT_DBFORGE_TOKEN="$DBFORGE_TOKEN" \
CONNECTOR_IT_DBFORGE_TTL_MINUTES=60 \
mvn -pl connectors/mysql-connector -DskipITs=false -Djacoco.skip=true \
  -Dit.test=MySQLConnectorIT verify
```

The equivalent Maven properties are `connector.it.config.source`,
`connector.it.dbforge.url`, `connector.it.dbforge.token`, and
`connector.it.dbforge.ttl.minutes`. The lease is acquired once for the test class and released
in `@AfterAll`; TTL remains the recovery path for an abnormal test-process exit. DBForge errors
fail the test and never fall back silently to the JSON configuration.
