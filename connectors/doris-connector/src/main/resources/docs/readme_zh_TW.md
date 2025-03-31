## **連接配寘幫助**
### **1. Doris安裝說明**
請遵循以下說明以確保在Tapdata中成功添加和使用Doris資料庫。
### **2. 支持版本**
Doris 1.x、Doris 2.x
### **3. 先決條件**
#### **3.1創建Doris帳號**
```
//創建用戶
create user 'username'@'localhost' identified with mysql_native_password by 'password'；
//修改密碼
alter user 'username'@'localhost' identified with mysql_native_password by 'password'；
```
#### **3.2給tapdata帳號授權**
對於某個資料庫賦於select許可權
```
GRANT SELECT，SHOW VIEW，CREATE ROUTINE，LOCK TABLES ON <DATABASE_NAME>.< TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password'；
```
對於全域的許可權
```
GRANT RELOAD，SHOW DATABASES，REPLICATION SLAVE，REPLICATION CLIENT ON *.* TO 'tapdata' IDENTIFIED BY 'password'；
```
#### **3.3約束說明**
```
當從Doris同步到其他異構資料庫時，如果源Doris存在錶級聯設定，因該級聯觸發產生的數據更新和删除不會傳遞到目標。 如需要在目標端構建級聯處理能力，可以視目標情况，通過觸發器等手段來實現該類型的資料同步。
```
### **4. 先決條件（作為目標）**
對於某個資料庫賦於全部許可權
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.< TABLE_NAME> TO 'tapdata' IDENTIFIED BY 'password'；
```
對於全域的許可權
```
GRANT PROCESS ON *.* TO 'tapdata' IDENTIFIED BY 'password'；
```
### **5. 建錶注意事項**
```
由於Doris作為目標寫入均是由Stream Load管道進行，以下是建錶時的一些注意事項
```
#### **5.1 Duplicate**
```
Duplicate管道建錶表示可重複的模式，在非追加寫入模式下，默認以更新條件欄位為排序鍵，追加寫入模式無更新條件欄位時，手動配寘

該模式下更新事件將會新增一條記錄，不會覆蓋原有記錄，删除事件則會删除所有滿足條件的相同記錄，囙此推薦在追加寫入模式下使用
```
#### **5.2 Aggregate**
```
Aggregate管道建錶表示聚合模式，默認以更新條件欄位為聚合鍵，非聚合鍵使用Replace If Not Null

該模式下大量的更新事件效能更優，缺陷是無法將Set Null的事件生效，另外删除事件是不受支持的。 囙此推薦無物理删除場景且更新事件無Set Null場景和源端數據欄位不全情况使用
```
#### **5.3 Unique**
```
Unique管道建錶表示唯一鍵模式，默認以更新條件欄位為唯一鍵，使用管道與絕大多數關係資料來源一致

如果大量的更新事件，且更新的欄位不停變化地情况下，推薦源端使用全欄位補齊的管道，以保證可觀的效能
```
### **6. 常見錯誤**
Unknown error 1044
如果許可權已經grant了，但是通過tapdata還是無法通過測試連接，可以通過下麵的步驟檢查並修復
```
SELECT host，user，Grant_ priv，Super_ priv FROM Doris.user where user='username'；
//查看Grant_ priv欄位的值是否為Y
//如果不是，則執行以下命令
UPDATE Doris.user SET Grant_ priv='Y' WHERE user='username'；
FLUSH PRIVILEGES；
```
