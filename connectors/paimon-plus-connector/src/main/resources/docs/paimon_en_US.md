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

1. **Read Operations**: This connector currently only supports write operations. Read and CDC operations are not supported.
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

Supports one table: column projection, `CAST(timestamp AS DATE)` / `DATE(timestamp)`, `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `GROUP BY`, and `ORDER BY`. Use identical explicit aliases on both validation endpoints. JOIN, UNION, HAVING, DISTINCT, arbitrary expressions, and mutations are rejected. WHERE predicates are pushed into Paimon; only required columns are read. Aggregation scans matching records and keeps state per group, so large scans and high-cardinality groups still cost time and memory.

New projection, aggregation and sorted queries require comparison, BETWEEN or IN predicates on a partition key or leading primary key. For an existing connection without a suitable key filter, edit the connection and enable `queryAllowFullScan`, or explicitly pass `params.allowFullScan=true` per command. Command options take precedence. This gate applies to newly supported SQL; historical unsorted `SELECT *` and direct/wrapped `COUNT(1)` remain allowed without key filters.

Pass these options alongside `sql` in the command's `params`:

| Parameter | Default | Meaning |
| --- | --- | --- |
| `queryTimeoutSeconds` | 0 | Whole seconds, 0 or absent disables the deadline; range 0..2147483647 (JDBC-compatible upper bound) |
| `maxScanRows` | 10000000 for new queries | Rows delivered by the reader, not physical file rows scanned |
| `maxGroups` | 100000 for new queries | Maximum groups held in memory |
| `maxResultRows` | 1000 for new queries | Maximum returned rows; exceeding it fails the entire command |
| `allowFullScan` | Connection setting, false when absent | Permit new queries without the key predicates above |

Historical `SELECT *` and direct/wrapped `COUNT(1)` have no new default scan/result caps; explicitly supplied limits still apply. Row/group limits must be positive whole numbers; negative, fractional and overflowing parameters fail. A limit failure returns an error without partial results. Timeout checks run during catalog access, planning, reading, sorting and result construction. This is a **cooperative deadline**, and cannot interrupt blocked Paimon/filesystem I/O; configure storage I/O timeouts separately. A key predicate permits pruning but does not guarantee it. Benchmark representative data and table layouts before using large queries.
