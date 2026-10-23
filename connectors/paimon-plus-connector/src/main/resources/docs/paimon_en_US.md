# Apache Paimon Connector

## Overview

Apache Paimon is a streaming data lake platform that supports high-speed data ingestion, change data capture and efficient data query. This connector allows Tapdata to write data to Paimon tables.

## Supported Operations

- **Connection Test**: Verify warehouse accessibility and write permissions
- **Schema Discovery**: Load table definitions from Paimon
- **Create Table**: Create new Paimon tables
- **Drop Table**: Delete Paimon tables
- **Clear Table**: Remove all data from a table
- **Write Records**: Insert, update, and delete records

## Configuration

### Warehouse Path
The root path where Paimon stores data. This can be:
- Local file system: `/path/to/warehouse`
- HDFS: Will be constructed as `hdfs://host:port/path`
- S3: `s3://bucket/path`
- OSS: `oss://bucket/path`

### Storage Type
Select the storage backend for Paimon:
- **Local**: Local file system (for testing)
- **HDFS**: Hadoop Distributed File System
- **S3**: Amazon S3 or S3-compatible storage
- **OSS**: Aliyun Object Storage Service

### S3 Configuration
When using S3 storage:
- **S3 Endpoint**: S3 service endpoint (e.g., https://s3.amazonaws.com)
- **S3 Access Key**: AWS access key ID
- **S3 Secret Key**: AWS secret access key
- **S3 Region**: AWS region (e.g., us-east-1)

### HDFS Configuration
When using HDFS storage:
- **HDFS Host**: NameNode hostname
- **HDFS Port**: NameNode port (default: 9000)
- **HDFS User**: User for HDFS operations (default: hadoop)

### OSS Configuration
When using OSS storage:
- **OSS Endpoint**: OSS service endpoint (e.g., https://oss-cn-hangzhou.aliyuncs.com)
- **OSS Access Key**: Aliyun access key ID
- **OSS Secret Key**: Aliyun access key secret

### Database Name
The Paimon database name (default: default)

## Data Type Mapping

| Paimon Type | Tapdata Type |
|-------------|--------------|
| BOOLEAN | TapBoolean |
| TINYINT | TapNumber |
| SMALLINT | TapNumber |
| INT | TapNumber |
| BIGINT | TapNumber |
| FLOAT | TapNumber |
| DOUBLE | TapNumber |
| DECIMAL | TapNumber |
| CHAR | TapString |
| VARCHAR | TapString |
| STRING | TapString |
| BINARY | TapBinary |
| VARBINARY | TapBinary |
| DATE | TapDate |
| TIME(0-3) | TapTime |
| TIMESTAMP | TapDateTime |
| TIMESTAMP_LTZ | TapDateTime |
| STRING | TapArray (stored as JSON string) |
| STRING | TapMap (stored as JSON string) |
| STRING | TapRow/TapRaw (stored as JSON string) |

`INT` and `INTEGER` both create Paimon `INT`. Bare `DECIMAL` uses `DECIMAL(38,10)`. Bare
`TIME` uses `TIME(3)`; only precision 0-3 is supported, and sub-millisecond input is rejected.

DECIMAL values must fit the physical column's precision and scale exactly. Precision overflow or a scale reduction that would change the value fails the write; the connector does not apply `HALF_UP` rounding. For `DECIMAL(5,2)`, `1.235` is rejected, while `1.2300` is accepted as `1.23` because removing trailing zeros preserves its value.

Existing target columns are not migrated: legacy STRING columns remain writable according to their
physical type, while native Paimon ARRAY/MAP/ROW/MULTISET/VARIANT targets are rejected. Complex CDC
values must be stored as JSON in STRING columns.

## Limitations

1. **Read Operations**: CDC and streaming reads are not supported. Read-only ad-hoc queries are available through `executeQuery` (see below).
2. **Indexes**: Paimon doesn't support traditional indexes. Only primary keys are supported.
3. **Schema Changes**: Dynamic schema changes during runtime are not supported.

## Best Practices

1. **Primary Keys**: Always define primary keys for tables to enable efficient updates and deletes.
2. **Batch Size**: Use appropriate batch sizes for better write performance.
3. **Storage Selection**: Choose the appropriate storage backend based on your deployment environment.
4. **Partitioning**: Consider using Paimon's partitioning features for large tables.

## Troubleshooting

### Connection Issues
- Verify warehouse path is accessible
- Check storage credentials are correct
- Ensure network connectivity to storage backend

### Write Failures
- Verify write permissions on warehouse path
- Check table schema matches source data
- Review Paimon logs for detailed error messages

### Performance Issues
- Increase batch size for better throughput
- Use appropriate storage backend for your workload
- Consider enabling Paimon's compaction features

## References

- [Apache Paimon Documentation](https://paimon.apache.org/)
- [Paimon GitHub Repository](https://github.com/apache/paimon)

## Custom read-only SQL (`executeQuery`)

Supports one table: column projection, `CAST(timestamp AS DATE)`, `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `GROUP BY`, and `ORDER BY`. Use identical explicit aliases on both validation endpoints. JOIN, UNION, HAVING, DISTINCT, arbitrary expressions, and mutations are rejected. WHERE predicates are pushed into Paimon; only required columns are read. Aggregation scans matching records and keeps state per group, so large scans and high-cardinality groups still cost time and memory.

Projection, aggregation and sorted queries allow non-key filters and full scans by default. Set connection `queryAllowFullScan=false` (or command boolean `allowFullScan=false`) to opt into the partition/leading-primary-key filter requirement. Command options take precedence; direct/wrapped `COUNT(1)` remains exempt, while the gate also applies to historical unsorted `SELECT *`. For `AND`, any qualifying condition is enough; for `OR`, every branch must constrain an eligible key. Same-key equality alternatives can be rewritten as `IN`. Positive `IS NULL` on a key qualifies; `NOT` and negated forms do not. Use `allowFullScan=true` only when a full scan is intended. A supported filter is pushed down, but SQL syntax alone does not guarantee file pruning.

The 1000-row inspection limit belongs to the engine, not the connector. It counts result rows across callbacks and fails on row 1001, discarding all partial results. `executeCommand` uses its existing `batchSize` and result callback to stream unordered projections; a consumer exception stops reading and closes resources. Aggregation and sorting must finish scanning before final rows are delivered; input rows are not capped at 1000. Ordered queries buffer result rows in memory up to `maxBufferedRows`; group-by aggregation keeps one state per group up to `maxGroups`, and a grouped query with `ORDER BY` materializes one row per group, so it is bounded by both `maxGroups` and `maxBufferedRows`. Direct command callers can receive more than 1000 result rows. Streaming callers must discard already received batches if a later callback reports an error.

Pass these options alongside `sql` in the command's `params`:

| Parameter | Default | Meaning |
| --- | --- | --- |
| `queryTimeoutSeconds` | 0 | Whole seconds, 0 or absent disables the deadline; range 0..2147483647 (JDBC-compatible upper bound) |
| `maxScanRows` | 10000000 for new queries | At most N rows are processed; another reader row fails the command. Not physical file rows scanned |
| `maxGroups` | 100000 for new queries | Maximum groups held in memory |
| `maxResultRows` | No default inspection limit | Maximum returned rows; exceeding it fails the entire command |
| `maxBufferedRows` | 100000 | `ORDER BY` must hold every result row until the scan ends, so more than N buffered rows fails the command; aggregate queries with `ORDER BY` buffer one row per group and are bounded by this too; unordered projections stream by `batchSize` and are not bounded by this |
| `allowFullScan` | Connection setting, true when absent | Boolean or `"true"`/`"false"` string; permit queries without the key predicates above |

Historical `SELECT *` and direct/wrapped `COUNT(1)` have no new default scan/result caps; explicitly supplied limits still apply. Row/group limits must be positive whole numbers; negative, fractional and overflowing parameters fail. A limit failure fails the command; streaming callers must discard any previously delivered batches. Timeout checks run during catalog access, planning, reading, sorting and result construction. This is a **cooperative deadline**, and cannot interrupt blocked Paimon/filesystem I/O; configure storage I/O timeouts separately. A key predicate permits pruning but does not guarantee it. Benchmark representative data and table layouts before using large queries.

### SQL boundaries and return values

`SELECT *`, scalar projections and aliases, `COUNT(*)`, `COUNT(1)`, `COUNT(column)`, `SUM`, `AVG`, `MIN`, `MAX`, multi-column `GROUP BY`, and `ORDER BY` selected expressions, aliases or ordinals are supported. `CAST(column AS DATE)` accepts DATE or TIMESTAMP without time zone; it uses the calendar date without an implicit zone conversion. `DATE(column)` is not supported. ASC defaults to NULLS FIRST and DESC to NULLS LAST; explicit NULLS FIRST/LAST are supported. Unquoted ORDER BY aliases ignore case; quoted aliases match exactly. The existing JSqlParser 4.4 grammar rejects doubled double quotes inside identifiers; such names cannot currently be queried.

WHERE supports column/literal comparisons (in either operand order), BETWEEN, IN, IS NULL, limited LIKE, AND/OR/NOT. Integer columns accept only whole-number literals inside the column range; fractional or out-of-range literals fail with an explicit `literal ... is not representable as ...` error instead of being truncated or reported as a syntax error. General subqueries, WITH, windows, arithmetic, pagination, table aliases and qualified table names are rejected. ARRAY/MAP/MULTISET/ROW columns cannot be selected or used in expressions; explicitly select scalar columns on tables containing them.

Empty input returns COUNT=0 and NULL for other global aggregates; grouped queries return no rows. SUM/AVG use BigDecimal for all numeric input, AVG rounds with DECIMAL128. SUM/AVG reject NaN and either infinity with an explicit data error and no partial result. MIN/MAX and ORDER BY use -Infinity < finite numbers < +Infinity < NaN; signed zeros compare equal. `GROUP BY` also treats signed zeros as one group and puts every NaN in one group. COUNT and plain projection permit these floating values.

Direct and legacy wrapped `COUNT(1)` return a scalar `Long`; all other queries return `List<Map<String,Object>>`. SQL NULL becomes Java null. Scalar columns and MIN/MAX keep the connector's existing Java types; CAST expressions have the types below:

| SQL value / expression | Java value |
| --- | --- |
| BOOLEAN; TINYINT / SMALLINT / INT / BIGINT | Boolean; Byte / Short / Integer / Long |
| FLOAT / DOUBLE / DECIMAL | Float / Double / BigDecimal |
| CHAR / VARCHAR; BINARY / VARBINARY | String; byte[] |
| DATE column | java.sql.Date, epoch-day multiplied by 86400000 milliseconds (historical behavior; textual display depends on JVM zone) |
| TIME column | LocalDateTime anchored at 1970-01-01 |
| TIMESTAMP without time zone | LocalDateTime |
| TIMESTAMP with local time zone | ZonedDateTime in UTC |
| CAST(column AS DATE) | LocalDate |
| COUNT; SUM / AVG | Long; BigDecimal |
| MIN / MAX | Same Java type as the argument column (or LocalDate for CAST AS DATE) |

The query deadline and elapsed time start after lifecycle admission; catalog access, planning, reading, sorting and result construction are included. Sorting cancellation discards its private buffer without publishing partially sorted data.

Use the same aliases in the SQL Server source query, for example:

```sql
SELECT CAST(CreationTime AS DATE) AS creation_date,
       COUNT(*) AS row_count,
       SUM(TotalValues) AS total_values,
       SUM(PointsAfterRedemption) AS points_after_redemption
FROM DRSRedemptions
WHERE TransactionDatetime BETWEEN '2025-01-01 00:00:00' AND '2025-07-01 00:00:00'
  AND op <> 'd'
GROUP BY CAST(CreationTime AS DATE)
ORDER BY CAST(CreationTime AS DATE)
```
