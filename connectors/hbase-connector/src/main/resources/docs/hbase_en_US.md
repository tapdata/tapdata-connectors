# HBase Connector

HBase is a distributed, scalable, big data store modeled after Google's Bigtable. This connector enables Tapdata to read from and write to Apache HBase.

## Prerequisites

- HBase 2.x cluster with ZooKeeper quorum accessible
- Network connectivity between Tapdata agent and HBase/ZooKeeper

## Connection Configuration

| Parameter | Description |
|---|---|
| ZooKeeper Quorum | ZooKeeper cluster addresses, format: `host1:2181,host2:2181,host3:2181` |
| ZooKeeper ZNode Parent | Root ZNode for HBase in ZooKeeper, default: `/hbase` |
| Username | Optional, for Kerberos authentication |
| Password | Optional, for Kerberos authentication |

## Data Model

- HBase tables are mapped to Tapdata tables
- Each row key is mapped to a `row_key` field (primary key)
- Each column family is mapped to a field containing a JSON object of `qualifier: value` pairs

## Limitations

- Incremental sync (CDC) is not supported in this version
- All values are treated as strings
