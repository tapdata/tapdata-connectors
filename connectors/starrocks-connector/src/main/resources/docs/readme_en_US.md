## **Connection Configuration Help**
### **1. StarRocks Installation Instructions**
Please follow the instructions below to ensure that the StarRocks database is successfully added and used in TapData.

### **2. Supported Versions**
StarRocks 3+

### **3. Prerequisites (As a Source)**
#### **3.1 Creating a StarRocks Account**
``
// Create user
create user 'username'@'localhost' identified with mysql_native_password by 'password';
``

#### **3.2 Granting Permissions to the Account**
Grant SELECT privileges to the database that needs to be synchronized:
``
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'username' IDENTIFIED BY 'password';
``
Grant global privileges:
``
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'username' IDENTIFIED BY 'password';
``

### **4. Prerequisites (As a Target)**
Grant all privileges to a specific database:
``
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'username' IDENTIFIED BY 'password';
``
Grant global privileges:
``
GRANT PROCESS ON *.* TO 'tapdata' IDENTIFIED BY 'password';
``

### **5. Table Creation Notes**
``
Since StarRocks writes as a target using the Stream Load method, here are some considerations for table creation:
``

#### **5.1 Primary**
``
The source table has a primary key, and when data involves insert/update/delete operations, this is the recommended table model. Data with the same primary key will only exist in one copy, optimized for queries.
``

#### **5.2 Unique**
``
Similar to the Primary table, but optimized for writes. If the scenario involves significantly more writes than queries, this table model can be used.
``

#### **5.3 Aggregate**
``
The Aggregate model creates tables with aggregation. It supports updating data to non-NULL values but does not support deletion.
``

#### **5.4 Duplicate**
``
Only supports insertion, not updates or deletions. Suitable for full-load tasks where the target is configured to clear the target table data.
```
