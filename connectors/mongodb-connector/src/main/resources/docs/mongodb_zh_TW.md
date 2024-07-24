## **連接配置幫助**
MongoDB 是壹個流行的、開源 NoSQL 數據庫，以靈活/可擴展的方式存儲和檢索數據。完成 Agent 部署後，您可以跟隨本文教程在 TapData 中添加 MongoDB 數據源，後續可將其作爲源或目標庫來構建數據管道。
> **注意**：MongoDB 作爲源端連接時，必須是副本集。
## 支持版本
MongoDB 3.6+
>**注意**：<br>
>由于 Tapdata 數據同步目前是基于 MongoDB 的 Change Stream 支持對多表合並的操作，而 MongoDB 官方是從 4.0 版本開始支持 Change Stream 的，因此，請盡量保證源端數據庫和目標端數據庫都是 4.0 及以上版本。
## 功能限制
* 源端 MongoDB 支持副本集和分片集群。
* 支持MongoDB分片集群從節點全量和增量同步
##  准備工作
### 作爲源庫
1. 保障源庫的架構爲副本集或分片集群，如果爲單節點架構，您可以將其配置爲單成員的副本集以開啓 Oplog。
   具體操作，見[如何將單節點轉爲副本集](https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/)。
2. 配置充足的 Oplog 存儲空間，至少需要容納 24 小時的 Oplog。
   具體操作，見[修改 Oplog 大小](https://docs.mongodb.com/manual/tutorial/change-oplog-size/)。
3. 根據權限管控需求選擇下述步驟，創建用于數據同步/開發任務的賬號並授予權限。
> **注意**：由于分片服務器不會向 config 數據庫獲取用戶權限，因此，當源庫爲分片集群架構時，您需要在每個分片的主節點上創建相應的用戶並授予權限。
* 授予指定庫（以 **demodata** 庫爲例）的讀權限
  ```bash
  use admin
  db.createUser(
    {
      user: "tapdata",
      pwd: "my_password",
      roles: [
         { role: "read", db: "demodata" },
         { role: "read", db: "local" },
         { role: "read", db: "config" },
         { role: "clusterMonitor", db: "admin" },
      ]
    }
  )
  ```

* 授予所有庫的讀權限。

     ```bash
     use admin
     db.createUser(
       {
         user: "tapdata",
         pwd: "my_password",
         roles: [
            { role: "readAnyDatabase", db: "admin" },
            { role: "clusterMonitor", db: "admin" },
         ]
       }
      )
     ```
> **注意**：只有 MongoDB 版本 3.2 需要 local 數據庫的讀取權限。
4. 在設置 MongoDB URI 時，推薦將寫關注級別設置爲大多數，即 `w=majority`，否則可能因 Primary 節點異常宕機導致的數據丟失文檔。

5. 源庫爲集群架構時，爲提升數據同步性能，TapData 將會爲每個分片創建壹個線程並讀取數據，在配置數據同步/開發任務前，您還需要執行下述操作。

  * 關閉源庫的均衡器（Balancer），避免塊遷移對數據壹致性的影響。具體操作，見[如何停止平衡器](https://docs.mongodb.com/manual/reference/method/sh.stopBalancer/)。
  * 清除源庫中，因塊遷移失敗而産生的孤立文檔，避免 _id 沖突。具體操作，見[如何清理孤立文檔](https://docs.mongodb.com/manual/reference/command/cleanupOrphaned/)。

> **重要事項**<br>
> 1.對于集群分片，您必須在每個分片主節點上創建適當的用戶權限。 這是由于MongoDB的安全架構設計。
> 當登錄到每個單獨的分片時，分片服務器不會向config數據庫獲取用戶權限。 相反，它將使用其本地用戶數據庫進行身份驗證和授權。<br>
> 2.對于MongoDB 3.x 以下版本請確保Tapdata服務與MongoDB各節點通信正常。
### 作爲目標庫

授予指定庫（以 **demodata** 庫爲例）的寫權限，並授予 **clusterMonitor** 角色以供數據驗證使用，示例如下：

```bash
use admin
db.createUser(
  {
    user: "tapdata",
    pwd: "my_password",
    roles: [
       { role: "readWrite", db: "demodata" },
       { role: "clusterMonitor", db: "admin" },
       { role: "read",db: "local"}
    ]
  }
)
```
> **注意**：只有 MongoDB 版本 3.2 需要 local 數據庫的讀取權限。
### MongoDB TLS/SSL配置
- **啓用TLS/SSL**<br>
  請在左側配置頁的 “使用TLS/SSL連接”中選擇“是”項進行配置<br>
- **設置MongoDB PemKeyFile**<br>
  點擊“選擇文件”，選擇證書文件，若證書文件有密碼保護，則在“私鑰密碼”中填入密碼<br>
- **設置CAFile**<br>
  請在左側配置頁的 “驗證服務器證書”中選擇“是”<br>
  然後在下方的“認證授權”中點擊“選擇文件”<br>
### MongoDB 性能測試
配置:ecs.u1-c1m2.2xlarge 機型, 8C 16G, 100GB ESSD 磁盤

|  寫入方式   | RPS      |
| -------- |------------|
| 全量寫入  |    95K     |
| 混合寫入  |    2.5k    |


| 讀取方式 | RPS      |
|------|------------|
| 全量讀取 |    50k     |
| 增量讀取 |    14k    |
