## **连接配置帮助**
### **1. SQL SERVER安装说明**
请遵循以下说明以确保在 Tapdata 中成功添加和使用SQLServer数据库。默认情况下，未启用 SQLServer 增量复制。为了在 SQLServer上执行更改数据捕获，必须事先由管理员明确启用增量复制功能。
> **注意**：<br>
> 您必须以 sysadmin 的成员身份登录到 SQLServer Management Studio 或 sqlcmd。
> 增量复制是 SQLServer 2008 及更高版本支持的功能。
> 确保 SQL 代理 任务是启动状态（在 SQLServer Management Studio 里面左下角)
### **2. 支持版本**
SQL Server 2005、2008、2008 R2、2012、2014、2016、2017、2019
### **3. 先决条件**
#### **3.1 开启 Sql Server 数据库代理服务**
- **查找 mssql-conf 工具**
```
find / -name mssql-conf
```
- **开启代理服务**
```
mssql-conf set sqlagent.enabled true
```

#### **3.2 启用数据库增量复制**
- **数据库启用增量复制**<br>
```
use [数据库名称]
go
EXEC sys.sp_cdc_enable_db
go
```
其中 `[数据库名称]` 是要启用增量复制的数据库。<br>
- **检查数据库是否启用增量复制**<br>
```
SELECT [name], database_id, is_cdc_enabled
FROM sys.databases
WHERE [name] = N'[数据库名称]'
go
```
其中 `[数据库名称]` 是您要复制的数据库。<br>

#### **3.3 表开启增量复制**
- **启用增量复制**
```
use<数据库名称>
go
EXEC sys.sp_cdc_enable_table
@source_schema = N'[Schema]',
@source_name = N'[Table]',
@role_name = N'[Role]'
go
```
说明：
- `<Schema>` 如` dbo`。
- `<Table>` 是数据表的名称(没有 schema )。
- `<Role> `是可以访问更改数据的角色。如果您不想使用选通角色，请将其设置为`NULL`。
> **注意**：
>如果在启用增量复制时指定了 "\"，则必须确保提供给 Tapdata 的数据库用户名具有适当的角色，以便 Tapdata 可以访问增量复制表。
- 检查是否为表启用了增量复制<br>
```
use <数据库名称>
go
SELECT [name],is_tracked_by_cdc
FROM sys.tables
WHERE [name] = N'[table]'
go
```
- **CDC的表执行DDL后**<br>
  如果CDC的表对字段进行了增、删、改的DDL操作，则必须进行如下操作，否则在增量同步过程中，可能会出现数据不同步或者报错的情况<br>
  **- 需要disable该表的CDC**<br>
```use<数据库名称>
go
EXEC sys.sp_cdc_disable_table
@source_schema = N'[Schema]',
@source_name = N'[Table]',
@capture_instance = N'[Schema_Table]'
go
// capture_instance一般为schema_table的格式拼接而成，可以通过以下命令，查询实际的值
exec sys.sp_cdc_help_change_data_capture
@source_schema = N'[Schema]',
@source_name = N'[Table]';
```
**- 重新enable开启CDC**<br>
```
use<数据库名称>
go
EXEC sys.sp_cdc_enable_table
@source_schema = N'[Schema]',
@source_name = N'[Table]',
@role_name = N'[Role]'
go
```

#### **3.4 版本低于 2008 的 CDC**
MSSQL 从 SQLServer 2008 开始提供 CDC 支持。对于较早的版本，必须使用 “**custom sql**” 功能来模拟更改数据捕获。在从旧版本复制数据时，需要考虑以下几点：<br>
- 源表必须有一个更改跟踪列，比如 `LAST_UPDATED_TIME`，它在每次插入或更新记录时都会更新。<br>
- 在任务设置界面<br>
>确保只选择** INITIAL_SYNC** ，因为不支持** CDC**<br>
>将 “**重复运行自定义 SQL**” 设置为 **True**。这将导致重复执行定制 SQL。<br>
- 在映射设计上提供适当的自定义 SQL<br>








