## **Connection Configuration Help**
### Database version 
  HuaWei Open GaussDB Standby 8.1 postgres version 9.2
### **1. Necessary inspections**

1. Enable logical replication

- The logical log is currently extracted from DN. If logical replication is performed, SSL connection should be used, so it is necessary to ensure that the GUC parameter ssl on the corresponding DN is set to on

2. Set GUC parameters

- Set the GUC parameter wal_level to logical

- Set GUC parameters max_replication_slots >= Required number of physical flow replication slots+backup slots+logical replication slots for each node
    The default value for the database is 20. If you need to set it, it is recommended to set the reference value to+1 for the number of task tasks using this connection as the source
    
3. Before using CDC, you need to configure your user machine (the machine where the Agent is currently deployed) in pg_hba.conf of each DN node:
```text
    # Prerequisite: Add the JDBC user machine IP to the database whitelist, Add the following content to pg_hba.conf, and then restart the database:
    # Assuming the JDBC user IP is 10.10.10.10
    host all all 10.10.10.10/32 sha256
    host replication all 10.10.10.10/32 sha256
```
After configuration, the database needs to be restarted

### Data source parameters
1. Database IP
2. Port
3. Database Name
4. Schema Name
5. Database Username
6. Database Password
7. Logic replicate IP, The IP of the main DN
8. Logical replication port, usually the port of the main DN+1, which defaults to 8001
9. Log plugin, using mppdb_decoding by default
10. Timezone
    
### About CDC Logic replicate
1. DDL statement decoding is not supported, and executing specific DDL statements (such as regular table truncate or partitioned table exchange) may cause decoding data loss.
2. Decoding of column storage and data page copying is not supported.
3. The size of a single tuple should not exceed 1GB. Considering that the decoding result may be larger than the inserted data, it is recommended that the size of a single tuple should not exceed 500MB
4. The data types that GaussDB supports decoding are:
```text
    INTEGER、BIGINT、SMALLINT、TINYINT、SERIAL、SMALLSERIAL、BIGSERIAL、
    FLOAT、DOUBLE PRECISION、
    DATE、TIME[WITHOUT TIME ZONE]、TIMESTAMP[WITHOUT TIME ZONE]、
    CHAR(n)、VARCHAR(n)、TEXT。
```
5. Copying interval partition tables is not supported.
6. Global temporary tables are not supported.
7. After executing a DDL statement in a transaction, the DDL statement and subsequent statements will not be decoded.
8. To parse the UPDATE and Delete statements of a certain Astrore table, it is necessary to configure the REPLICA Identity property for this table. When this table does not have a primary key, it needs to be configured as Full

### Exception Handling
1. Incremental start failed
    - Copying slot connection failed with the following error:
      ```text
      com.huawei.opengauss.jdbc.util.PSQLException: 
        [192.168.*.*:5***5/1.9*.1*2.1*2:8001] 
        FATAL: no pg_hba.conf entry for replication connection from host "1*.1**.2*5.1*0",
        user "r**t", SSL off
      ```
      
      Reason for error: Access the whitelist list of machines that are not on the DN node
      
      Solution:
        Configure your user machine (the machine where the Agent is currently deployed) in the pg_hba.conf of each DN node:
        ```text
        # Add the JDBC user machine IP to the database whitelist, Add the following content to pg_hba.conf
        # Assuming the JDBC user IP is 10.10.10.10
          host all all 10.10.10.10/32 sha256
          host replication all 10.10.10.10/32 sha256
        # Then restart the database
        ```