## **連接配寘幫助**
### **1. VASTBASE安裝說明**
請遵循以下說明以確保在Tapdata中成功添加和使用VASTBASE資料庫。
### **2. 支持版本**
VASTBASE-G100版本
### **3. CDC原理和支持**
#### **3.1 CDC原理**
VASTBASE的邏輯解碼功能與Postgres相同，它是一種機制，允許選取提交到事務日誌中的更改，並通過輸出挿件以用戶友好的管道處理這些更改。

#### **3.2 CDC支持**
- **邏輯解碼**（Logical Decoding）：用於從WAL日誌中解析邏輯變更事件
- **複製協定**（Replication Protocol）：提供了消費者實时訂閱（甚至同步訂閱）資料庫變更的機制
- **快照匯出**（export snapshot）：允許匯出資料庫的一致性快照（pg_export_snapshot）
- **複製槽**（Replication Slot）：用於保存消費者偏移量，跟踪訂閱者進度。
  所以，根據以上，我們需要安裝邏輯解碼器，現有提供的解碼器如下拉清單中所示

### **4. 先決條件**
#### **4.1修改REPLICA IDENTITY**
該内容决定了當數據發生`UPDATE，DELETE`時，日誌記錄的欄位
- **DEFAULT** -更新和删除將包含primary key列的現前值
- **NOTHING** -更新和删除將不包含任何先前值
- **FULL** -更新和删除將包含所有列的先前值
- **INDEX index name** -更新和删除事件將包含名為index name的索引定義中包含的列的先前值
  如果有多錶合併同步的場景，則Tapdata需要調整該内容為FULL
  示例
```
alter table '[schema]'.' [table name]' REPLICA IDENTITY FULL`
```

#### **4.2挿件安裝**
（現時VASTBASE自帶wal2json挿件）

#### **4.3許可權**
##### **4.3.1作為源**
- **初始化**<br>
```
GRANT SELECT ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
- **增量**<br>
  用戶需要有replication login許可權，如果不需要日誌增量功能，則可以不設定replication許可權
```
CREATE ROLE <rolename> REPLICATION LOGIN;
CREATE USER <username> ROLE <rolename> PASSWORD '<password>';
// or
CREATE USER <username> WITH REPLICATION LOGIN PASSWORD '<password>';
```
設定檔pg_hba.conf需要添加如下內容：<br>
```
pg_hba.conf
local   replication     <youruser>                     trust
host    replication     <youruser>  0.0.0.0/32         md5
host    replication     <youruser>  ::1/128            trust
```

##### **4.3.2作為目標**
```
GRANT INSERT,UPDATE,DELETE,TRUNCATE
ON ALL TABLES IN SCHEMA <schemaname> TO <username>;
```
> **注意**：以上只是基本許可權的設定，實際場景可能更加複雜

##### **4.4測試日誌挿件**
> **注意**：以下操作建議在POC環境進行
>連接vastbase資料庫，切換至需要同步的資料庫，創建一張測試錶
```
--假設需要同步的資料庫為vastbase，模型為public
\c vastbase

create table public.test_decode
（
uid    integer not null
constraint users_pk
primary key,
name varchar（50），
age    integer,
score  decimal
）
```
可以根據自己情况創建一張測試錶<br>
-創建slot連接，以wal2json挿件為例
```
select * from pg_create_logical_replication_slot（'slot_test'，'wal2json'）
```
-創建成功後，對測試錶插入一條數據<br>
-監聽日誌，查看返回結果，是否有剛才插入操作的資訊<br>
```
select * from pg_logical_slot_peek_changes（'slot_test'，null，null）
```
-成功後，銷毀slot連接，删除測試錶<br>
```
select * from pg_drop_replication_slot（'slot_test'）
drop table public.test_decode
```
#### **4.5異常處理**
- **Slot清理**<br>
  如果tapdata由於不可控异常（斷電、行程崩潰等），導致cdc中斷，會導致slot連接無法正確從pg主節點删除，將一直佔用一個slot連接名額，需手動登入主節點，進行删除
  査詢slot資訊
```
//查看是否有slot_name以tapdata_cdc_開頭的資訊
TABLE pg_replication_slots;
```
- **删除slot節點**<br>
```
select * from pg_drop_replication_slot（'tapdata'）；
```
- **删除操作**<br>
  在使用wal2json挿件解碼時，如果源錶沒有主鍵，則無法實現增量同步的删除操作

#### **4.6使用最後更新時間戳記的管道進行增量同步**
##### **4.6.1名詞解釋**
**schema**：中文為模型，pgsql一共有3級目錄，庫->模型->錶，以下命令中<schema>字元，需要填入錶所在的模型名稱
##### **4.6.2預先準備（該步驟只需要操作一次）**
- **創建公共函數**
  在資料庫中，執行以下命令
```
CREATE OR REPLACE FUNCTION <schema>.update_lastmodified_column（）
RETURNS TRIGGER language plpgsql AS $$
BEGIN
NEW.last_update = now（）；
RETURN NEW;
END;
$$；
```
- **創建欄位和trigger**
> **注意**：以下操作，每張錶需要執行一次
假設需要新增last update的錶名為mytable
- **創建last_update欄位**
```
alter table <schema>.mytable add column last_udpate timestamp default now（）；
```
- **創建trigger**
```
create trigger trg_uptime before update on <schema>.mytable for each row execute procedure
update_lastmodified_column（）；
```
### **5. 全類型欄位支持**
- smallint
- integer
- bigint
- numeric
- real
- double precision
- character
- varchar
- text
- bytea
- bit
- bit varying
- boolean
- date
- interval
- timestamp
- timestamp with time zone
- point
- line
- lseg
- box
- path
- polygon
- circle
- cidr
- inet
- macaddr
- uuid
- xml
- json
- tsvector（增量不支持不報錯）
- tsquery（增量不支持不報錯）
- oid
- regproc（增量不支持不報錯）
- regprocedure（增量不支持不報錯）
- regoper（增量不支持不報錯）
- regoperator（增量不支持不報錯）
- regclass（增量不支持不報錯）
- regtype（增量不支持不報錯）
- regconfig（增量不支持不報錯）
- regdictionary（增量不支持不報錯）
