# Source DDL capture integration tests

This module runs the shared `ConnectorIT` composite ADD and ADD → DML → DROP → DML regressions through derived connectors. MySQL and PostgreSQL keep their existing suites; TiDB and OceanBase MySQL enable the same regressions in their existing IT classes.

| Test classes | Connector implementation | DBForge engine |
| --- | --- | --- |
| `MariadbDdlIT` | MariaDB | `mariadb12` |
| `AliyunRdsMariadbDdlIT`, `TencentDbMariadbDdlIT` | Cloud MariaDB derivatives | `mariadb12` |
| `MySQLPxcDdlIT` | MySQL PXC | `mysql` |
| `AliyunRdsMySQLDdlIT`, `AwsRdsMySQLDdlIT`, `PolarDbMySQLDdlIT` | Cloud MySQL derivatives | `mysql` |
| `AliyunRdsPostgresDdlIT`, `AliyunAdbPostgresDdlIT`, `TencentDbPostgresDdlIT`, `PolarDbPostgresDdlIT` | PostgreSQL derivatives | `postgresql` |
| `KafkaEnhancedDdlIT`, `KafkaAvroDdlIT` | Registry-backed Avro schema evolution | Real Kafka + Schema Registry |

DBForge runs for derived connectors validate their registered CDC implementation on the indicated base engine. They do **not** certify cloud-product behavior, PXC cluster topology, or vendor-specific replication restrictions. Use the explicit JSON configuration mode against an actual product for that acceptance run.

The JDBC tests require valid `TapNewFieldEvent` instances for both added columns, table identity, column types, following row values, and ADD → insert-with-column → DROP → insert-without-column delivery order. DDL statements execute through the JDBC verifier; DML does not wait for CDC between the ADD and DROP steps. Kafka has no SQL ALTER TABLE: its tests publish consecutive baseline/added/dropped Avro schemas on one partition and verify the equivalent ordered DDL/DML events. Each test creates a unique table/topic and cleans it up.

Plain Kafka does not emit column DDL on source reads. OpenGauss, Greenplum and Aliyun ADB MySQL do not register `streamRead`; their target-side DDL handlers do not constitute source DDL capture. HighGo/Vastbase/GaussDB do not expose the PostgreSQL DDL-audit setup used by these tests. These connectors are not included merely because they can apply DDL as targets.

## Run

Use JDK 17 and install the shared `tapdata-connector-it:1.0-SNAPSHOT` framework containing `sourceCompositeAddColumnsStatement` (tapdata-it PR #16/#17). Build from the repository root so local connector/core changes take precedence over published SNAPSHOTs:

```bash
mvn -pl connectors/source-ddl-it -am install -DskipTests -DskipITs=true

export CONNECTOR_IT_CONFIG_SOURCE=dbforge
export CONNECTOR_IT_DBFORGE_URL=http://dbforge:8080
export CONNECTOR_IT_DBFORGE_TOKEN="$DBFORGE_TOKEN"
export CONNECTOR_IT_KAFKA_BROKERS=broker:9092
export CONNECTOR_IT_KAFKA_REGISTRY=http://schema-registry:8081
mvn -f connectors/source-ddl-it/pom.xml verify -DskipITs=false
```

Only the two DDL regressions run by default. To select one connector:

```bash
mvn -f connectors/source-ddl-it/pom.xml verify -DskipITs=false \
  '-Dit.test=MariadbDdlIT#should_stream_composite_add_columns_ddl+should_stream_read_ddl_dml_drop_ddl_dml_sequence'
```

For an actual cloud/PXC endpoint, select its test class, set `CONNECTOR_IT_CONFIG_SOURCE=json`, and pass `-Dconnector.it.ddl.config=/path/to/connection.json`. The JDBC configuration needs `host`, `port`, `database`, `user`, and `password`. PostgreSQL uses schema `public`, `pgoutput`, a unique replication slot, and DDL-audit triggers; use an account with the requisite replication/event-trigger privileges. No cloud credentials are bundled. The Kafka settings are only required when selecting the Kafka classes.

The dedicated TiDB and OceanBase MySQL classes can be run with:

```bash
mvn -f connectors/tidb-connector/pom.xml test-compile failsafe:integration-test failsafe:verify \
  -DskipITs=false '-Dit.test=TidbConnectorIT#should_stream_composite_add_columns_ddl+should_stream_read_ddl_dml_drop_ddl_dml_sequence'
mvn -f connectors/oceanbase-mysql-connector/pom.xml test-compile failsafe:integration-test failsafe:verify \
  -DskipITs=false '-Dit.test=OceanbaseConnectorIT#should_stream_composite_add_columns_ddl+should_stream_read_ddl_dml_drop_ddl_dml_sequence'
```
