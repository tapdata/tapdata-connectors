# ClickHouse connector integration tests

Updated: 2026-09-21

`ClickhouseConnectorIT` extends the shared `TpccConnectorIT` suite and runs the registered source and target capabilities against a real ClickHouse database. It covers the supported portions of the 12 GA categories plus all 3 performance cases. Unsupported connector APIs are skipped by the shared capability checks.

Install the shared test framework, then run the default suite:

```bash
cd /home/tapdata-it
mvn -DskipTests install

cd /home/tapdata-connectors
mvn -pl connectors/clickhouse-connector -am verify -DskipITs=false
```

Run the opt-in long transaction cases with `-Plong-transaction-only`; they are skipped when the connector does not register transaction capabilities.

Run the Layer3-derived suites independently:

```bash
mvn -pl connectors/clickhouse-connector -am verify -Ptpcc-only
mvn -pl connectors/clickhouse-connector -am verify -Pperformance-only
```

The 5 TPCC tests are registered but abort as skipped because the bundled BenchmarkSQL workload requires transactional update semantics that ClickHouse and its connector do not expose. The 3 performance tests use a native `MergeTree` table.

Use DBForge instead of the checked-in JSON configuration:

```bash
CONNECTOR_IT_CONFIG_SOURCE=dbforge \
CONNECTOR_IT_DBFORGE_URL=http://dbforge:8080 \
CONNECTOR_IT_DBFORGE_TOKEN="$DBFORGE_TOKEN" \
mvn -pl connectors/clickhouse-connector -am verify -DskipITs=false
```

The client requests `clickhouse/dedicated/single`. DBForge must have the ClickHouse runtime
driver enabled; it provisions `clickhouse/clickhouse-server:23.7` as an isolated temporary
instance. DBForge failures intentionally fail the test and never silently fall back to JSON.

The default connection file is `src/it/resources/config/clickhouse-connection.json`. Override values with `connector.it.*` system properties or `CONNECTOR_IT_*` environment variables.
