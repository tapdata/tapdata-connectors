# Source DDL IT shared support

This module contains the shared JDBC-family and Kafka schema-evolution test bases only. Concrete `*DdlIT` suites live in each owning connector module under `src/it/java`; each suite runs through that connector's Maven Failsafe lifecycle.

| Connector module | IT class |
|---|---|
| `mariadb-connector` | `MariadbDdlIT` |
| `aliyun-rds-mariadb-connector` | `AliyunRdsMariadbDdlIT` |
| `tencent-db-mariadb-connector` | `TencentDbMariadbDdlIT` |
| `mysql-pxc-connector` | `MySQLPxcDdlIT` |
| `aliyun-rds-mysql-connector` | `AliyunRdsMySQLDdlIT` |
| `aws-rds-mysql-connector` | `AwsRdsMySQLDdlIT` |
| `polar-db-mysql-connector` | `PolarDbMySQLDdlIT` |
| `aliyun-adb-postgres-connector` | `AliyunAdbPostgresDdlIT` |
| `aliyun-rds-postgres-connector` | `AliyunRdsPostgresDdlIT` |
| `tencent-db-postgres-connector` | `TencentDbPostgresDdlIT` |
| `polar-db-postgres-connector` | `PolarDbPostgresDdlIT` |
| `kafka-enhanced-connector` | `KafkaEnhancedDdlIT` |
| `kafka-avro-connector` | `KafkaAvroDdlIT` |

The JDBC suites run three source CDC scenarios by default: composite two-column ADD,
ADD / DML / DROP / DML without waiting between source operations, and ADD / RENAME / DROP
followed by DML. The module-local `it.test` property limits these DDL-only fixtures to
those scenarios; pass `-Dit.test=...` to explicitly select a scenario. Kafka suites
retain their two schema-evolution scenarios.

Build the target connector and its dependencies first, then run its own Failsafe suite
(the same lifecycle used by CI). For example, with the DBForge environment configured:

```bash
mvn -pl connectors/mariadb-connector -am install -DskipTests -DskipITs=true
mvn -f connectors/mariadb-connector/pom.xml test-compile failsafe:integration-test failsafe:verify -DskipITs=false

mvn -pl connectors/aliyun-rds-postgres-connector -am install -DskipTests -DskipITs=true
mvn -f connectors/aliyun-rds-postgres-connector/pom.xml test-compile failsafe:integration-test failsafe:verify -DskipITs=false
```

JDBC suites use their owning connector's explicit IT configuration when DBForge is not selected (`config/<connector-module>.json` by default), or acquire a DBForge lease when configured. Kafka suites also accept DBForge leases; the Kafka lease includes a Confluent-compatible Schema Registry sidecar and returns broker and registry endpoints. When DBForge is not selected, Kafka suites use `CONNECTOR_IT_KAFKA_BROKERS` and `CONNECTOR_IT_KAFKA_REGISTRY`.

To run any JDBC or Kafka DDL suite with DBForge, set `CONNECTOR_IT_CONFIG_SOURCE=dbforge`, `CONNECTOR_IT_DBFORGE_URL`, and `CONNECTOR_IT_DBFORGE_TOKEN`. Use `CONNECTOR_IT_DBFORGE_ENDPOINT_SCOPE=internal` when the test runner is in the cluster; otherwise use `external` (the default). DbForge-backed managed-service connector tests exercise the connector against its compatible engine (for example, an Aliyun PostgreSQL connector against PostgreSQL). They do not claim to emulate provider-only behavior or validate a live cloud service.
