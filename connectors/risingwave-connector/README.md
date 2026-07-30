# RisingWave Connector

This target connector writes TapData snapshot and CDC events to RisingWave. It cannot be used as a
TapData source.

## Write modes

| Mode | Use it for | Requirements |
|---|---|---|
| WebSocket streaming | Keyed inserts, updates, and deletes. Default and recommended. | RisingWave 3.0+ and a primary key |
| WebSocket JSONB append-only | Keyless insert streams such as Kafka events | RisingWave 3.0+; inserts only |
| JDBC | Compatibility fallback | PostgreSQL-compatible SQL endpoint |

WebSocket streaming normally provides the best throughput and latency. JDBC is used in every mode
for connection checks, metadata, DDL, and schema and privilege validation.

## Build

The connector requires Java 11 or later.

From the repository root:

```bash
mvn -pl connectors/risingwave-connector -am clean package
```

For a faster connector-only build:

```bash
mvn -f connectors/risingwave-connector/pom.xml clean package
```

The connector JAR is written to:

```text
connectors/risingwave-connector/target/risingwave-connector-v1.0-SNAPSHOT.jar
```

## Install

Get the Access Code from **TapData Settings -> Access Code**, then register the JAR with the
`pdk-deploy.jar` included in the TapData installation:

```bash
java -jar /path/to/pdk-deploy.jar register \
  -t http://localhost:3030 \
  -a <access-code> \
  connectors/risingwave-connector/target/risingwave-connector-v1.0-SNAPSHOT.jar
```

Restart TapData after registration. The RisingWave connector then appears under
**Connections -> Create Connection**.

## Local connection example

When TapData runs in Docker and RisingWave runs on the Docker host:

```text
Host: host.docker.internal
Port: 4566
Database: dev
Schema: public
User: root
Write Mode: WebSocket streaming
Ingest Endpoint: <blank>
SSL Mode: prefer or disable
```

Do not use `localhost` to reach the Docker host. Leaving **Ingest Endpoint** blank uses
`ws://<Host>:4560`.

Click **Test** before saving. WebSocket modes check the SQL connection, RisingWave version, schema,
DDL and write privileges, WebSocket endpoint, signed initialization when configured, and a durable
RisingWave ACK.

## Create a task

Use **Data Replication -> Create Task** for snapshot and CDC pipelines. In WebSocket streaming
mode, select the real source primary key as the update condition.

Verify the target directly:

```sql
select count(*) from public.orders;

select count(*) as rows,
       count(distinct id) as distinct_ids
from public.orders;
```

For a keyed task, test one insert, update, delete, and primary-key change at the source.

## Source requirements

| Source | Requirement |
|---|---|
| PostgreSQL | Row images must reconstruct the complete target row |
| MySQL | `binlog_row_image=FULL` |
| MongoDB | TapData Update Field Completion, `enableFillingModifiedData=true` |
| Kafka | Use JSONB append-only for keyless JSON events |
| SQL Server | Enable database and table Change Tracking |
| Oracle | Use LogMiner with `autoLog=false` and primary-key supplemental logging |

## Important behavior

- WebSocket streaming creates webhook-backed tables when needed and requires a primary key.
- Inserts and updates are complete-row upserts. Primary-key changes are delete-old plus upsert-new.
- Updates and deletes without the old primary-key identity fail.
- Automatic relational schema evolution is not supported. Unknown fields fail.
- JSONB append-only creates one `data JSONB` column and accepts inserts only.
- JSONB delivery is at-least-once. An ambiguous ACK loss and retry can append a duplicate.
- A single serialized WebSocket record larger than 8 MiB fails explicitly.
- JDBC is a compatibility mode and normally has lower throughput than WebSocket streaming.

## Optional webhook secret

Set **Webhook Secret** and **RisingWave Secret Name** to validate signed WebSocket initialization.
The connector creates or reuses the RisingWave Secret and references it from table DDL. The secret
value is not embedded in `SHOW CREATE TABLE`.

User-facing connection help is available in
[`risingwave_en_US.md`](src/main/resources/docs/risingwave_en_US.md) and
[`risingwave_zh_CN.md`](src/main/resources/docs/risingwave_zh_CN.md).
