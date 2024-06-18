## **Connection Configuration Help**

### **1. TiDB installation instructions**

Please follow the instructions below to ensure that TiDB database is successfully.as well as successful deployment of TiKV service and PD service

### **2. Supported versions**

 - TiDB version above 5.4.0 and below 8.0.0 support CDC
 
 - If TiDB version 8.0.0 or above needs to support CDC:
    
    1. Please go to **https://tiup-mirrors.pingcap.com/cdc-v${ti-db-version}-linux-${system-architecture}.tar.gz** , Download the corresponding version of the incremental startup tool
       
        - ${ti-db-version}: The version corresponding to TiDB, for example: 8.0.0
        
        - ${system-architecture}: Corresponding operating system architecture, such as amd64 or arm64
    
    2. After downloading and decompressing, name it **cdc** and place it in the **{tapData_dir}/run-resource/ti-db/tool** environment directory
    
**{tapDatadir}/run-resource/ti-db/tool/cdc** Must have read, write, and execute permissions

### **3. Prerequisites (as source)**
3.1 Example of configuration connection
3.1.1 Incremental configuration is not enabled
```
PdServer address: xxxx:xxxx
Database address: xxxx
Port: xxxx
Database name: xxxx
Account: xxxx
Password: xxxx
```
3.1.2 Only the following configurations need to be added to enable the increment
```
Kafka address: xxxx:xxxx
Kafka Subject: xxxx
Ticdc address: xxxx:xxxx
```

Assign select permission to a database
```
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'user' IDENTIFIED BY 'password';
```
Permissions for global
```
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```
###  **4. Prerequisites (as targets)**
Assign all permissions to a database
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'user' IDENTIFIED BY 'password';
```
Permissions for global
```
GRANT PROCESS ON *.* TO 'user' IDENTIFIED BY 'password';
```

### **5.Attention

1. TiDB needs to be deployed in the TapData intranet (same network segment) environment

2. TiCDC only replicates tables with at least one valid primary key index. A valid index is defined as follows:
﻿
    - A primary key (PRIMARY KEY) is a valid index
    
    - A unique index (UNIQUE INDEX) is valid if every column of the index is explicitly defined as non-nullable (NOT NULL) and the index does not have a virtual generated column (VIRTUAL GENERATED COLUMNS)
