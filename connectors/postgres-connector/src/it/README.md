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

The default `PostgresConnectorIT` requests `postgresql/dedicated/single` and runs CDC using `pgoutput` with the DDL trigger enabled. Run the rapid DDL/DML regression independently with:

```bash
CONNECTOR_IT_CONFIG_SOURCE=dbforge \
CONNECTOR_IT_DBFORGE_URL=http://dbforge:8080 \
CONNECTOR_IT_DBFORGE_TOKEN="$DBFORGE_TOKEN" \
mvn -pl connectors/postgres-connector -am verify -DskipITs=false \
  -Dit.test='PostgresConnectorIT#should_stream_read_ddl_dml_drop_ddl_dml_sequence'
```

`PostgresEfmTdeConnectorIT` requests `postgresql/dedicated/efm-tde`: a three-node EPAS EFM cluster with `wal_level=replica`, encrypted WAL, and a DbForge endpoint routed only to a standby. It selects physical-slot CDC, enables standby checks, and reads the encrypted TDE key file through the standby connection for WAL decoding. Run the complete inherited integration suite against this topology with:

```bash
CONNECTOR_IT_CONFIG_SOURCE=dbforge \
CONNECTOR_IT_DBFORGE_URL=http://dbforge:8080 \
CONNECTOR_IT_DBFORGE_TOKEN="$DBFORGE_TOKEN" \
mvn -pl connectors/postgres-connector -am verify -DskipITs=false \
  -Dit.test='PostgresEfmTdeConnectorIT'
```

The EFM class requires DbForge mode. With the default JSON configuration it is disabled at the class level, with an explicit environment prerequisite reason; it must not fall back to the single-node endpoint. A JSON-only CI run therefore validates only the ordinary suite, not EFM. To validate both topologies, configure the DbForge variables above and run without an `it.test` filter.

The EFM class excludes test methods that write to its read-only standby: all TPCC and performance cases, non-CDC cases requiring a verifier-backed data fixture, and cases exercising target-side write/DDL capabilities. CDC streamRead scenarios remain eligible because their source changes go through the primary verifier. This is based on where a test writes, not on `@UnderTest("streamRead")` alone. To run the DDL/DML interleaving regression last, run the full class excluding that method, then run the method separately as the final command.

Both classes run the shared CDC test independently and each acquires/releases its own DBForge lease. The EFM case needs a database account allowed to read `pg_encryption/key.bin`; DBForge's `tde` lease secret supplies its unwrap password. The key and password are held only in the in-memory test connection config and are never logged. Set `CONNECTOR_IT_POSTGRES_WAL_TDE_KEY` only if the key file cannot be read from the standby; set `CONNECTOR_IT_POSTGRES_WAL_TDE_KEY_PASSWORD` only if the `tde` secret is not present in the lease response.

The default connection file is `src/it/resources/config/postgres-connection.json`. Override values with `connector.it.*` system properties or `CONNECTOR_IT_*` environment variables. Set `connector.it.cdc.mode=postgres-physical` to select physical-slot CDC for the single-node topology; the EFM test selects it by default.

The checked-in JSON default targets the shared Kubernetes PostgreSQL instance `edb-postgres-tde128` in namespace `tapdata-db` through `edb-postgres-tde128.tapdata-db.svc.cluster.local:5433`. It uses database `root`, user `root`, and password `tapdata`. This hostname is reachable from the Kubernetes GitHub runner; do not replace it with `127.0.0.1` unless PostgreSQL runs in the same Pod.
