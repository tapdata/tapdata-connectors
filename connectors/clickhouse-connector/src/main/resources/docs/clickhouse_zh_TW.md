## **連接配置幫助**
### ClickHouse安裝說明
請遵循以下說明以確保在 Tapdata 中成功添加和使用ClickHouse數據庫。
### 支持版本
Clickhouse 20.x, 21.x 22.x, 23.x ,24.x
### 功能限制
- 目前clickhouse 作爲源時，僅支持輪訓字段增量同步方式
### 先決條件
#### 作爲源
1. 登錄clickhouse 數據庫，執行下述格式的命令，創建用於數據同步/開發任務的賬號。
```sql
CREATE user tapdata IDENTIFIED WITH plaintext_password BY 'mypassword'
```
2. 爲剛創建的用戶授予權限
```sql
grant select on default.* to user
```
- user爲複製/轉換用戶的用戶名
- password爲用戶的密碼
#### 作爲目標
- 方式一：
  以最高權限用戶(default)進行創建用於數據複製/轉換的用戶
  - 在user.xml <quota>default</quota>下面添加如下配置，否者無法創建用戶
  ``` xml
  <access_management>1</access_management>
  <named_collection_control>1</named_collection_control>
  <show_named_collections>1</show_named_collections>
  <show_named_collections_secrets>1</show_named_collections_secrets>
  ```
  - 創建用於數據複製、轉換的用戶
  ```sql
  CREATE USER user IDENTIFIED WITH plaintext_password BY 'password';
  ```
  - 爲創建的用戶授予權限
  ```sql
  grant create,alter,drop,select,insert on default.* to user
  ```
  - user爲複製/轉換用戶的用戶名
  - password爲用戶的密碼
- 方式二： 使用具有創建用戶權限並且可以授予寫權限的用戶創建用於數據/轉換的用戶
  - 創建具有創建用戶權限並且可以授予寫權限的用戶
  ``` sql
  CREATE USER user IDENTIFIED WITH plaintext_password BY 'password'
  ```
  - 授予這個用戶創建用戶的權限
  ```sql
  GRANT CREATE USER ON *.* TO adminUser
  ```
  - 授予這個用戶授予寫權限，並且允許它可以將這些權限授予給其他用戶
  ```sql
  grant create,alter,drop,select,insert,delete on default.* to adminUser with grant option
  ```
  - 使用具有創建用戶、授予寫權限的用戶登錄ClickHouse
  - 創建用於進行數據複製/轉換的用戶
  ```sql
  CREATE USER user IDENTIFIED WITH plaintext_password BY 'password';
  ```
  - 授予這個用戶create,alter,drop,select,insert,delete權限
  ```sql
  grant create,alter,drop,select,insert,delete on default.* to user
  ```
  - 其中adminUser 和 user 分別爲新創建的授權賬號名與用於數據複製/轉換的用戶名
  - password 爲用戶密碼
### 支持數據類型
- FixedString、String、UUID、Int8、UInt8、Int16、UInt16、Int32、UInt32、Int64、UInt64、Int128、UInt128、Int256、UInt256、Float32、Float64、Decimal、Date、Date32、DateTime、DateTime64、Enum8、Enum16、Array、Tuple
### 使用幫助
- 當ClickHouse作爲目標時，節點配置中--&gt;高級配置--&gt;數據源專屬配置--&gt;合併分區間隔(分鐘)配置選項可以配置ClickHouse的Optimize Table的間隔，您可以根據業務需求自定義Optimize Table間隔。
### 性能測試
- 環境說明
  - TapData 版本: v3.7.0, 其中 16GB 分配給引擎, 8GB 分配給管理端; 元數據庫通過 --wiredTigerCacheSizeGB 限定內存, 4GB 分配給元數據庫
  - ClickHouse 數據庫: ecs.u1-c1m2.2xlarge 機型, 8C 16G, 100GB ESSD 磁盤,版本爲24.5.3.5
  ##### 測試結果
  1. ClickHouse 全量寫入 : 將 10,000,000 1KB 數據從 Dummy 數據庫 同步到 ClickHouse,平均RPS 爲250K
  2. ClickHouse 全量讀取 RPS: 將 10,000,000 1KB 數據從 ClickHouse 同步到 Dummy 數據庫，平均RPS 爲130K