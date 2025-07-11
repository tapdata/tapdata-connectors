## **連接配置幫助**
### **1. StarRocks 安裝說明**
請遵循以下說明，以確保在 TapData 中成功添加和使用 StarRocks 數據庫。

### **2. 支持版本**
StarRocks 3+

### **3. 先決條件（作為來源）**
#### **3.1 創建 StarRocks 賬號**
``
// 創建用戶
create user 'username'@'localhost' identified with mysql_native_password by 'password';
``

#### **3.2 給賬號授權**
對於需要同步的數據庫授予 SELECT 權限：
``
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'username' IDENTIFIED BY 'password';
``
全局權限授予：
``
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'username' IDENTIFIED BY 'password';
``

### **4. 先決條件（作為目標）**
對於某個數據庫授予全部權限：
``
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'username' IDENTIFIED BY 'password';
``
對於全局權限：
``
GRANT PROCESS ON *.* TO 'tapdata' IDENTIFIED BY 'password';
``

### **5. 建表注意事項**
``
由於 StarRocks 作為目標寫入採用 Stream Load 方式，以下是建表時的一些注意事項：
``

#### **5.1 Primary**
``
源表有主鍵，且數據存在插入/更新/刪除時建議採用的表模型，相同主鍵的數據將只存在一份，針對查詢進行了優化。
``

#### **5.2 Unique**
``
與 Primary 表類似，針對寫入進行了優化，如果場景中寫入遠高於查詢，可以使用此表模型。
``

#### **5.3 Aggregate**
``
Aggregate 以聚合模型建表，支持將數據更新為非 NULL 值，且不支持刪除。
``

#### **5.4 Duplicate**
``
僅支持插入，不支持更新和刪除，適合用於全量任務，且目標配置為清空目標表數據。
```
