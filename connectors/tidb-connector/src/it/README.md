# TiDB connector integration tests

Updated: 2026-09-21

`TidbConnectorIT` extends the shared `TpccConnectorIT` suite and runs connector capabilities against a real TiDB cluster. It covers the 12 GA categories, the 5 BenchmarkSQL TPCC cases, and the 3 performance cases. Unsupported connector APIs are skipped by the shared capability checks.

Install the shared test framework, then run the default suite:

```bash
cd /home/tapdata-it
mvn -DskipTests install

cd /home/tapdata-connectors
mvn -pl connectors/tidb-connector -am verify -DskipITs=false
```

Run the opt-in long transaction cases with `-Plong-transaction-only`.

Run the Layer3-derived suites independently:

```bash
mvn -pl connectors/tidb-connector -am verify -Ptpcc-only
mvn -pl connectors/tidb-connector -am verify -Pperformance-only
```

The TPCC adapter reuses the MySQL protocol support in `/home/t-layer3-test/auto_test/benchmarksql`. Override the asset root with `TPCC_BENCHMARK_HOME` or `-Dtpcc.benchmarkHome=...`.

Use DBForge instead of the checked-in JSON configuration:

```bash
CONNECTOR_IT_CONFIG_SOURCE=dbforge \
CONNECTOR_IT_DBFORGE_URL=http://dbforge:8080 \
CONNECTOR_IT_DBFORGE_TOKEN="$DBFORGE_TOKEN" \
mvn -pl connectors/tidb-connector -am verify -DskipITs=false
```

The requested lease is `tidb/dedicated/single`. DBForge supplies the SQL, PD, and TiKV endpoints, so `enableIncrement` remains enabled and GA, TPCC, and performance CDC cases run against the leased cluster.

The default connection file is `src/it/resources/config/tidb-connection.json`. Override values with `connector.it.*` system properties or `CONNECTOR_IT_*` environment variables. Incremental cases additionally require a reachable PD endpoint and TiCDC prerequisites; set `enableIncrement` accordingly.
