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
| Username | Optional, Hadoop user for simple authentication |
| Password | Optional, reserved for future Kerberos support |

## Data Model

- HBase tables are mapped to Tapdata tables
- Each row key is mapped to a `row_key` field (primary key)
- Source fields are stored as qualifiers within a configurable column family (default: `cf`), following HBase best practice of 2–3 column families per table
- When reading tables with multiple column families, each additional CF is represented as a field containing a JSON object of `qualifier: value` pairs

## Supported Operations

| Operation | Support |
|---|---|
| Batch Read | Yes, with checkpoint/resume support |
| Batch Count | Yes |
| Write (Insert/Update/Delete) | Yes |
| Create Table | Yes, creates with a single configurable column family |
| Drop Table | Yes |
| Incremental Sync (CDC) | Not supported in this version |

## Limitations

- Incremental sync (CDC) is not supported in this version
- All values are treated as strings; binary, numeric, and temporal types are serialized to string form
- Kerberos authentication is not yet supported
- No query-by-advance-filter support
