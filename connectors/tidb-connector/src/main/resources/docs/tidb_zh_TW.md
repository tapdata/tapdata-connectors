# TiDB
TiDB 是 PingCAP 設計、研發的開源分佈式關係型資料庫，是一款同時支援線上交易處理與線上分析處理的融合型分佈式資料庫產品。完成 Agent 部署後，您可以跟隨本文教程在 TapData 中新增 TiDB 資料源，後續可將其作為源或目標庫來構建資料管道。

## 支援版本

* **全量資料同步**：所有版本
* **增量資料同步**：6.0.0 ～ 8.1.9

## 支援同步的操作

- DML：INSERT、UPDATE、DELETE
- DDL：ADD COLUMN、CHANGE COLUMN、AlTER COLUMN、DROP COLUMN

## 資料類型

&nbsp;&nbsp;**全量**支援的資料類型同步包含以下：

&nbsp;&nbsp;&nbsp;&nbsp;***char***, varchar, ***tinytext***, text, ***mediumtext***, longtext, ***json***, binary, ***varbinary***, tinyblob, ***blob***, mediumblob, ***longblob***, bit, ***tinyint***, tinyint unsigned, ***smallint***, smallint unsigned, ***mediumint***, mediumint unsigned, ***int***, int unsigned, ***bigint***, bigint unsigned, ***decimal***, decimal unsigned, ***float***, double, ***double unsigned***, date, ***time***, datetime, ***timestamp***, year, ***enum***, set, ***INTEGER***, BOOLEAN, ***TEXT***

&nbsp;&nbsp;**增量**支援的資料類型同步包含以下：

&nbsp;&nbsp;&nbsp;&nbsp;Boolean, ***Float***, Double, ***Decimal***, Char, ***Varchar***, Binary, ***Varbinary***, Tinytext, ***Text***, Mediumtext, ***Longtext***, Tinyblob, ***Blob***, CLOB, ***Bit***, Mediumblob, ***Longblob***, Date, ***Datetime***, Timestamp, ***Time***, Year, ***Enum***, Set, ***Bit***, JSON, ***tinyinttinyint unsigned***, smallint, ***smallint unsigned***, mediumint, ***mediumint unsigned***, int, ***int unsigned***, bigint, ***bigint unsigned***, DECIMAL, ***INTEGER***, REAL

> [!注意事項]
>
> 暫不支援圖片類型

## 注意事項

- TapData引擎與您的 TiDB 資料庫實例應處於同網段下或者能之間連接通，例如：內網環境

- 將 TiDB 作為源以實現增量資料同步場景時，您還需要檢查下述資訊：待同步的表需具备

  - 待同步的表需具備主鍵或唯一索引，其中唯一索引所屬列的值不可為 NULL 且不能為虛擬列。
  
  - 為避免 TiCDC 的垃圾回收影響事務或增量資料資訊提取，推薦執行命令 ```SET GLOBAL tidb_gc_life_time= '24h'``` 將其設定為 24 小時或者一個符合您業務需求的時間長度，設定一個較長的時間將允許您讀取更久以前的增量資料，該屬性的 TiDB 資料庫預設屬性值為：```10m0s```
  
  - TiDB 連接器支援 CDC 的運行環境：TapData 引擎需要部署運行在 **arm 或 amd** 系統架構時。

## 增量同步原理

為進一步簡化使用流程，TapData 的 TiDB 連接器整合了 [TiFlow 組件](https://github.com/pingcap/tiflow/releases/tag/v8.1.0) （8.1.0 版本），可基於資料變更日誌解析為有序的行級變更資料。更多原理及概念介紹，見 [TiCDC 概述](https://docs.pingcap.com/zh/tidb/stable/ticdc-overview) 。

## <span id="prerequisite">準備工作</span>

1. 登錄 TiDB 資料庫，執行下述格式的命令，創建用於資料同步/開發任務的帳號。

   ```sql
   CREATE USER 'username'@'host' IDENTIFIED BY 'password';
   ```

   * **username**：用戶名。
   * **host**：允許該帳號登錄的主機，百分號（%）表示允許任意主機。
   * **password**：密碼。

   示例：創建一個名為 username 的帳號，允許從任意主機登錄。

   ```sql
   CREATE USER 'username'@'%' IDENTIFIED BY 'your_password';
   ```

2. 為剛創建的帳號授予權限。

   * 作為源庫

     ```sql
     -- 全量 + 增量同步所需權限如下
     GRANT SELECT ON *.* TO 'username' IDENTIFIED BY 'password';
     ```

   * 作為目標庫

     ```sql
     -- 授予指定庫權限
     GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, CREATE, DROP ON database_name.* TO 'username';
     
     -- 授予所有庫權限
     GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, CREATE, DROP ON *.* TO 'username';
     ```

* **database_name**：資料庫<span id="ticdc">名稱</span>。
* **username**：用戶名。



## 新增資料源

1. 登錄 TapData 平台。

2. 在左側導覽列，單擊**連接管理**。

3. 單擊頁面右側的**創建**。

4. 在彈出的對話框中，搜尋並選擇 **TiDB**。

5. 在跳轉到的頁面，根據下述說明填寫 TiDB 的連接資訊。

   * **連接資訊設定**
      * **連接名稱**：填寫具有業務意義的獨有名稱。
      * **連接類型**：支援將 TiDB 資料庫作為源或目標。
      * **PD Server 地址**：填寫 PDServer 的連接地址和端口，預設端口號為 **2379**，本參數僅在作為源庫且需要增量資料同步時填寫。
      * **資料庫地址**：資料庫連接地址。
      * **端口**：資料庫的服務端口，默認為 **4000**。
      * **資料庫名稱**：資料庫名稱（區分大小寫），即一個連接對應一個資料庫，如有多個資料庫則需創建多個資料連接。
      * **帳號**、**密碼**：資料庫的帳號和密碼，帳號的創建和授權方法，見 [準備工作](#prerequisite) 。
      * **TiKV 端口**：作為 TiDB 的存儲層，提供了資料持久化、讀寫服務和統計資訊資料記錄功能，預設端口為 **20160**，本參數僅在作為源庫且需要增量資料同步時填寫。
      
   * **高級設定**
      * **其他連接串參數**：額外的連接參數，默認為空。
      * **時間類型的時區**：默認為資料庫所用的時區，您也可以根據業務需求手動指定。
      * **共享挖掘**：挖掘源庫的增量日誌，可為多個任務共享源庫的增量日誌，避免重複讀取，從而最大程度上減輕增量同步對源庫的壓力，開啟該功能後還需要選擇一個外存用來存儲增量日誌資訊，本參數僅在作為源庫時需填寫。
      * **包含表**：默認為**全部**，您也可以選擇自定義並填寫包含的表， 多個表之間用英文逗號（,）分隔。
      * **排除表**：打開該開關後，可以設定要排除的表， 多個表之間用英文逗號（,）分隔。
      * **Agent 設定**：默認為**平臺自動分配**，您也可以手動指定。
      * **模型加載時間**：當資料源中模型數量小於 10,000 時，每小時刷新一次模型資訊；如果模型資料超過 10,000，則每天按照您指定的時間刷新模型資訊。
      * **開啟心跳表**：當連接類型選擇為 **源頭和目標**、**源頭** 時，支援打開該開關，由 Tapdata 在源庫中創建一個名為 **_tapdata_heartbeat_table** 的心跳表並每隔 10 秒更新一次其中的資料（資料庫帳號需具備相關權限），用於資料源連接與任務的健康度監測。
      
   * **SSL 設定**：選擇是否開啟 SSL 連接資料源，可進一步提升資料安全性，開啟該功能後還需要上傳 CA 文件、客戶端證書、密鑰填寫客戶端密碼。更多介紹，見[生成自簽名證書](https://docs.pingcap.com/zh/tidb/stable/generate-self-signed-certificates)。

6. 單擊**連接測試**，測試通過後單擊**保存**。

   > 如提示連接測試失敗，請根據頁面提示進行修復。

  

## 常見問題

- **問**：資料同步需要保障哪些端口的通信？

  ***答***：全量同步需要保障 TiDB 集群的資料庫端口與 TapData 間的通信，如需增量資料同步，還需要開放下述端口：

  * **2379** 端口：2379 為 PD Server 的預設端口，用於 PD 與 TiKV、TiDB 之間的通信以及對外提供 API 介面。TiKV 和 TiDB 會通過這個端口從 PD 獲取集群的配置信息和調度命令，如有另外指定端口號您需要開放此端口號。

  - **20160** 端口：20160 為 TiKV Server 的預設端口，用於 TiKV 對外提供存儲服務，包括處理 TiDB 的 SQL 請求、讀寫資料以及和其他 TiKV 節點之間的內部通信（如 Raft 協議的消息），如有另外指定端口號您需要開放此端口號。

- **問**：TapData 對 TiDB 的部署架構有要求嗎？

  ***答***：TiDB 的單機或集群部署架構均可得到支援。

- **問**：如果我的 TiDB 版本不在 6.0.0 ～ 8.1.9 範圍內，需要執行增量資料同步，應該怎麼辦？

  ***答***：TapData 的 TiDB 連接器整合了 TiCDC，可基於資料變更日誌解析為有序的行級變更資料。如果您的資料庫在支援版本以外，您可以前往 [Github: tiflow](https://github.com/pingcap/tiflow/releases) 下載支援對應版本的 Tiflow 組件，然後跟隨下述步驟自行編譯 cdc 工具：

  > 擴展其他版本的 Tiflow 組件可能帶來不確定因素或影響正在運行的任務，請謹慎操作。

  1. 將下載後的文件解壓，然後進入解壓後的目錄執行 `make` 命令進行編譯。

  2. 找到生成的 **cdc** 二進制文件，將其放置在 TapData 引擎所屬機器的 **{tapData-dir}/run-resource/ti-db/tool** 目錄下（如有則替換）。

  3. 通過 chmod 命令，為該目錄下的文件授予可讀可寫可執行權限。

<div style="text-align: center;width: 100%;height: 1px;background-color: rgba(203,203,203,0.9);"></div>
<div style="opacity:0.95;font-size: 14px;text-align:center;"><font style="color: gray">TapData</font> & <font style="color:gray;">TiDB</font></div>
<div style="opacity:0.6;font-size: 10px;text-align:center;">基於CDC的即時資料平臺，用於異構資料庫複製、即時資料集成或構建實时資料倉庫</div>
<div style="opacity:0;font-size: 10px;text-align:center;">TiDB connector doc design by Gavin</div>
