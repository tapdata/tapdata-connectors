## **連接配置幫助**
### **1. SQL SERVER安裝說明**
請遵循以下說明以確保在 Tapdata 中成功添加和使用SQLServer數據庫。默認情況下，未啟用 SQLServer 增量複製。為了在 SQLServer上執行更改數據捕獲，必須事先由管理員明確啟用增量複製功能。
> **注意**：<br>
> 您必須以 sysadmin 的成員身份登錄到 SQLServer Management Studio 或 sqlcmd。
> 增量複製是 SQLServer 2008 及更高版本支持的功能。
> 確保 SQL 代理 任務是啟動狀態（在 SQLServer Management Studio 裡面左下角)
### **2. 支持版本**
SQL Server 2005、2008、2008 R2、2012、2014、2016、2017、2019
### **3. 先決條件**
#### **3.1 開啟 Sql Server 數據庫代理服務**
- **查找 mssql-conf 工具**
```
find / -name mssql-conf
```
- **開啟代理服務**
```
mssql-conf set sqlagent.enabled true
```

#### **3.2 啟用數據庫增量複製**
- **數據庫啟用增量複製**<br>
```
use [數據庫名稱]
go
EXEC sys.sp_cdc_enable_db
go
```
其中 `[數據庫名稱]` 是要啟用增量複製的數據庫。 <br>
- **檢查數據庫是否啟用增量複製**<br>
```
SELECT [name], database_id, is_cdc_enabled
FROM sys.databases
WHERE [name] = N'[數據庫名稱]'
go
```
其中 `[數據庫名稱]` 是您要復制的數據庫。 <br>

#### **3.3 表開啟增量複製**
- **啟用增量複製**
```
use<數據庫名稱>
go
EXEC sys.sp_cdc_enable_table
@source_schema = N'[Schema]',
@source_name = N'[Table]',
@role_name = N'[Role]'
go
```
說明：
- `<Schema>` 如` dbo`。
- `<Table>` 是數據表的名稱(沒有 schema )。
- `<Role> `是可以訪問更改數據的角色。如果您不想使用選通角色，請將其設置為`NULL`。
> **注意**：
>如果在啟用增量複製時指定了 "\"，則必須確保提供給 Tapdata 的數據庫用戶名具有適當的角色，以便 Tapdata 可以訪問增量複製表。
- 檢查是否為表啟用了增量複製<br>
```
use <數據庫名稱>
go
SELECT [name],is_tracked_by_cdc
FROM sys.tables
WHERE [name] = N'[table]'
go
```
- **CDC的表執行DDL後**<br>
  如果CDC的表對字段進行了增、刪、改的DDL操作，則必須進行如下操作，否則在增量同步過程中，可能會出現數據不同步或者報錯的情況<br>
  **- 需要disable該表的CDC**<br>
```use<數據庫名稱>
go
EXEC sys.sp_cdc_disable_table
@source_schema = N'[Schema]',
@source_name = N'[Table]',
@capture_instance = N'[Schema_Table]'
go
// capture_instance一般為schema_table的格式拼接而成，可以通過以下命令，查詢實際的值
exec sys.sp_cdc_help_change_data_capture
@source_schema = N'[Schema]',
@source_name = N'[Table]';
```
**- 重新enable開啟CDC**<br>
```
use<數據庫名稱>
go
EXEC sys.sp_cdc_enable_table
@source_schema = N'[Schema]',
@source_name = N'[Table]',
@role_name = N'[Role]'
go
```

#### **3.4 版本低於 2008 的 CDC**
MSSQL 從 SQLServer 2008 開始提供 CDC 支持。對於較早的版本，必須使用 “**custom sql**” 功能來模擬更改數據捕獲。在從舊版本複制數據時，需要考慮以下幾點：<br>
- 源表必須有一個更改跟踪列，比如 `LAST_UPDATED_TIME`，它在每次插入或更新記錄時都會更新。 <br>
- 在任務設置界面<br>
>確保只選擇** INITIAL_SYNC** ，因為不支持** CDC**<br>
>將 “**重複運行自定義 SQL**” 設置為 **True**。這將導致重複執行定制 SQL。 <br>
- 在映射設計上提供適當的自定義 SQL<br>
