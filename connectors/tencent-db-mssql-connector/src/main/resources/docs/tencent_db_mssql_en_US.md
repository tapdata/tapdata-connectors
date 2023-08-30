## **Connection configuration help**

### **1. SQL SERVER Installation Notes**

Please follow the instructions below to ensure that the SQLServer database is successfully added and used in Tapdata. By default, SQL Server incremental replication is not enabled. In order to perform change data capture on SQL Server, the incremental replication function must be explicitly enabled by the administrator in advance.

> **WARN**：<br>
> You must log in to SQLServer Management Studio or sqlcmd as a member of sysadmin.
> Incremental replication is a feature supported by SQLServer 2008 and later versions.
> Make sure that the SQL Agent task is started (in the lower left corner of SQLServer Management Studio)

### **2. Supported version**
SQL Server 2005、2008、2008 R2、2012、2014、2016、2017

### **3. Prerequisites**
#### **3.1 Open Sql Server database proxy service**
- **Find the mssql-conf tool**
```
find / -name mssql-conf
```
- **Turn on proxy service**
```
mssql-conf set sqlagent.enabled true
```

#### **3.2 Enable incremental database replication**
- **Enable incremental replication for the database**<br>
```
use [database name]
go
EXEC sys.sp_cdc_enable_db
go
```
Where `[database name]` is the database to enable incremental replication.<br>
- **Check whether the database is enabled for incremental replication**<br>
```
SELECT [name], database_id, is_cdc_enabled
FROM sys.databases
WHERE [name] = N'[database name]'
go
```
Where `[database name]` is the database you want to copy.<br>

#### **3.3 Table opens incremental replication**
-**Enable incremental replication**
```
use<database name>
go
EXEC sys.sp_cdc_enable_table
@source_schema = N'[Schema]',
@source_name = N'[Table]',
@role_name = N'[Role]'
go
```
instruction:
- `<Schema>` such as `dbo`.
- `<Table>` is the name of the data table (no schema).
- `<Role>` is the role that can access the changed data. If you don't want to use a gated role, please set it to `NULL`.
> **Note**:
>If "\" is specified when incremental replication is enabled, you must ensure that the database user name provided to Tapdata has the appropriate role so that Tapdata can access the incremental replication table.
-Check if incremental replication is enabled for the table<br>
```
use <database name>
go
SELECT [name],is_tracked_by_cdc
FROM sys.tables
WHERE [name] = N'[table]'
go
```
- **CDC table after executing DDL**<br>
  If the CDC table adds, deletes, or changes the field of the DDL operation, you must perform the following operations, otherwise during the incremental synchronization process, there may be data out of synchronization or error reporting.<br>
  **- CDC of this table needs to be disabled**<br>
```use<database name>
go
EXEC sys.sp_cdc_disable_table
@source_schema = N'[Schema]',
@source_name = N'[Table]',
@capture_instance = N'[Schema_Table]'
go
// capture_instance is generally spliced ​​in the format of schema_table, you can use the following command to query the actual value
exec sys.sp_cdc_help_change_data_capture
@source_schema = N'[Schema]',
@source_name = N'[Table]';
```
**- Re-enable CDC**<br>
```
use<database name>
go
EXEC sys.sp_cdc_enable_table
@source_schema = N'[Schema]',
@source_name = N'[Table]',
@role_name = N'[Role]'
go
```

#### **CDC version 3.4 lower than 2008**
MSSQL has provided CDC support since SQLServer 2008. For earlier versions, the "**custom sql**" function must be used to simulate change data capture. When copying data from the old version, the following points need to be considered:<br>
- The source table must have a change tracking column, such as `LAST_UPDATED_TIME`, which is updated every time a record is inserted or updated. <br>
- In the task setting interface<br>
>Make sure to select only **INITIAL_SYNC** because **CDC** is not supported<br>
>Set "**Repeat custom SQL**" to **True**. This will result in repeated execution of custom SQL. <br>
- Provide appropriate custom SQL on mapping design<br>








