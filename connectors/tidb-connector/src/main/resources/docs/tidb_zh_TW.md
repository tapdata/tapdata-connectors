## **連接配置幫助**

### **1. TiDB 安裝説明**

請遵循以下説明以確保在 TapData 中成功添加和使用 TiDB數據庫以以及成功部署TiKV服務以及以及PD服務

### **2. 支持版本**

 - TiDB 6.0.x～8.1.x （arm/amd系統架構環境下）支持CDC
 
**{tapData_dir}/run-resource/ti-db/tool/cdc** 需要具備可讀可寫可執行許可權

### **3. 先決條件（作为源）**

3.1配置連接示例

3.1.1未開啓增量配置
```
PdServer 地址：xxxx:xxxx
數據庫地址：xxxx
端口：xxxx
數據庫名稱：xxxx
账号：xxxx
密码：xxxx
```
3.1.2開啓增量配置只需要追加以下配置
```
kafka地址：xxxx:xxxx
kafka主題：xxxx
ticdc地址：xxxx:xxxx

```

对于某个數據庫赋于select權限
```
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'user' IDENTIFIED BY 'password';
```
对于全局權限
```
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```
###  **4. 先决条件（作为目标）**
对于某个數據庫赋于全部權限
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'user' IDENTIFIED BY 'password';
```
对于全局的權限
```
GRANT PROCESS ON *.* TO 'user' IDENTIFIED BY 'password';
```

### **5.注意事項

1. TiDB需部署在TapData內網（同網段）環境下

2. TiCDC只複製至少有一個主鍵或有效索引的，有效索引定義如下：

    - 主鍵（primary key）是一個有效的索引
    
    - 如果索引的每一列都明確定義為不可為NULL（NOT NULL），並且索引沒有虛擬生成列（虛擬生成列），則唯一索引（unique index）是有效的
