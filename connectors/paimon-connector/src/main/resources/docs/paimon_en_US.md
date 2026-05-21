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
| TIMESTAMP | TapDateTime |
| TIMESTAMP_LTZ | TapDateTime |
| ARRAY | TapArray (stored as JSON string) |
| MAP | TapMap (stored as JSON string) |
| ROW | TapMap (stored as JSON string) |

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

