# OceanBase MySQL source DDL integration tests

`OceanbaseConnectorIT` enables the shared composite ADD, ADD → DML → DROP → DML, and ADD → rename → DROP followed by DML regressions. The composite case checks both new-field events and a subsequent row containing both added fields; the interleaving case inserts without waiting for CDC between DDL steps and verifies delivery order and field values.

The native reader advances its private decoding schema from ordered DDL events, so these tests do not require the engine to refresh the shared table map.

Use JDK 17 and the shared framework containing the source DDL hooks (tapdata-it PR #16/#17). Connection settings come from `src/it/resources/config/oceanbase-mysql-connection.json`, explicit `connector.it.*` overrides, or a DBForge lease. Native CDC requires a compatible libobcdc/server pair and reachable observer SQL/RPC endpoints. See [the source DDL test matrix](../../../source-ddl-it/README.md) for the targeted command.
