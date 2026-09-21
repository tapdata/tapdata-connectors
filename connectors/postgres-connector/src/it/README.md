# PostgreSQL connector integration tests

Updated: 2026-09-21

`PostgresConnectorIT` extends the shared `TpccConnectorIT` suite and runs connector capabilities against a real PostgreSQL database. It covers the 12 GA categories in `CONNECTOR_TEST_STANDARD.md`, the 5 BenchmarkSQL TPCC cases, and the 3 performance cases. Unsupported connector APIs are skipped by the shared capability checks.

Install the shared test framework, then run the default suite:

```bash
cd /home/tapdata-it
mvn -DskipTests install

cd /home/tapdata-connectors
mvn -pl connectors/postgres-connector -am verify -DskipITs=false
```

Run the opt-in long transaction cases with `-Plong-transaction-only`.

Run the Layer3-derived suites independently:

```bash
mvn -pl connectors/postgres-connector -am verify -Ptpcc-only
mvn -pl connectors/postgres-connector -am verify -Pperformance-only
```

The TPCC adapter uses `/home/t-layer3-test/auto_test/benchmarksql` by default. Override it with `TPCC_BENCHMARK_HOME` or `-Dtpcc.benchmarkHome=...`.

Use DBForge instead of the checked-in JSON configuration:

```bash
CONNECTOR_IT_CONFIG_SOURCE=dbforge \
CONNECTOR_IT_DBFORGE_URL=http://dbforge:8080 \
CONNECTOR_IT_DBFORGE_TOKEN="$DBFORGE_TOKEN" \
mvn -pl connectors/postgres-connector -am verify -DskipITs=false
```

The requested lease is `postgresql/dedicated/single`. DBForge runs CDC with the connector's physical replication slot mode, so CDC and offset cases remain enabled even when logical WAL is not configured.

The default connection file is `src/it/resources/config/postgres-connection.json`. Override values with `connector.it.*` system properties or `CONNECTOR_IT_*` environment variables. Set `logPluginName` to `physical` to run CDC through a physical replication slot without requiring logical WAL.
