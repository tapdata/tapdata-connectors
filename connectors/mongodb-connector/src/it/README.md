# MongoDB connector integration coverage

Updated: 2026-09-14

`MongoDBConnectorIT` runs the connector API against a real replica-set MongoDB database. It combines
the shared `ConnectorIT` suite with MongoDB regressions derived from `t-layer3-test`, plus opt-in TPCC
and performance suites.

## Twelve acceptance categories

| # | Category | MongoDB coverage |
|---|---|---|
| 1 | Basic full source | Shared `discoverSchema`, `batchCount`, `batchRead`, partition, filter, and min/max tests |
| 2 | Basic incremental source | Shared CDC plus nested insert/update/delete and Mongo array-operator regression |
| 3 | TPCC full source | Ten TPCC-named collections are discovered, counted, and fully read |
| 4 | TPCC incremental source | Transactional multi-collection workload CDC and saved-offset resume |
| 5 | Basic target | Shared collection create/write/update/delete/clear/drop tests |
| 6 | TPCC target | Ten collections are recreated and populated through connector target APIs |
| 7 | Four DDL cases | MongoDB is schema-free; collection create/drop are tested, while sparse-field add/remove/type evolution is covered through schema discovery and CDC |
| 8 | Offset reset/resume | Shared timestamp-offset test plus `should_resume_from_saved_offset` |
| 9 | Full types source | BSON scalar/special types, nested documents, mixed arrays, and dates are batch-read and streamed |
| 10 | Full types target | The same values are codec-wrapped, written through `writeRecord`, and read back |
| 11 | Transactions | Target transaction APIs, committed/aborted source transactions, TPCC transactions, and opt-in 2,000-document transaction |
| 12 | Large fields | 1 MiB binary and greater-than-1 MiB text values pass batch read, target write, and CDC |

## Layer3 regressions moved down

- `item_4_1_1.py` through `item_4_1_7.py`, `item_4_2_2.py`, and `item_4_2_5.py`: cross-version CDC mutations using `$set`, `$unset`, `$addToSet`, and `$pop`.
- `item_4_1_8.py` and `MongoFullType_tester.py`: MongoDB common/BSON types; connector IT adds nested values and 1 MiB fields.
- `item_4_2511.py`, `item_4_2709.py`, and `item_4_6_2.py`: embedded documents, arrays, and sparse field add/remove evolution.
- `item_4_3_1.py` and `item_4_10710.py`: repeated insert/update/delete and delete-event behavior.
- `item_4_8_1.py` through `item_4_8_7.py`: high-volume full+CDC and concurrent-write coverage; the connector performance suite owns raw API throughput.
- `item_4_12_1.py` through `item_4_12_4.py`, `item_4_12303.py` through `item_4_12306.py`, and `item_4_12520.py`: exactly-once remains at engine level because it depends on task replay and engine caches.

Cross-connector mapping, Union/JS transforms, task lifecycle, exactly-once engine caches, and API-server
projection behavior remain in `t-layer3-test`; they are not connector API integration tests.

## Running

```bash
cd /home/tapdata-it
mvn -DskipTests install

cd /home/tapdata-connectors
mvn -pl connectors/mongodb-connector -am verify -DskipITs=false
mvn -pl connectors/mongodb-connector -am verify -Ptpcc-only
mvn -pl connectors/mongodb-connector -am verify -Pperformance-only
mvn -pl connectors/mongodb-connector -am verify -Plong-transaction-only
```

The test database must be a replica set because change streams and multi-document transactions are
required. Connection values can be overridden with `connector.it.*` system properties or
`CONNECTOR_IT_*` environment variables. Performance volume and thresholds use the shared
`performance.*`/`PERFORMANCE_*` settings; TPCC smoke/full sizing uses `tpcc.*`/`TPCC_*`.
