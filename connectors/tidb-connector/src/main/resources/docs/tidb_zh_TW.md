
<div style="text-align: right; font-weight: normal;color: gray;"><img style="width: 20px;height:20px;object-fit: contain;flex: 1;" src="https://tapdata.io/assets-global.website-files.com/61acd54a60fc632b98cbac1a/620d4543dd51ea0fcb7b237c_Favicon2.png" alt="TiCDC的架构" /> & <img src="https://img1.tidb.net/favicons/favicon-32x32.png" style="width: 20px;height:20px;object-fit: contain;flex: 1;"/></div>

## **連接配寘幫助**

&nbsp;&nbsp; 請遵循以下說明以確保在TapData中成功添加和使用TiDB資料庫，並且成功部署TiKV服務以及TiPD服務
    
### ***1 同步類型***

&nbsp;&nbsp; TiDB資料來源現時覆蓋多種資料同步場景，支持同步類型有以下四種類型：

 - **僅全量（源）**：全量資料同步，庫級/錶級數據遷移
 - **僅增量（源）**：增量資料同步，DML/DDL同步
 - **全量+增量（源）**：全量資料同步，庫級/錶級數據遷移，增量資料同步，DML/DDL同步
 - **數據寫入（目標）**：創建錶，删除錶，數據寫入（插入/修改/删除），DDL同步

#### 1.1 **作為源**

  - **資料來源連接内容配寘**

    | 内容名稱  | 是否必填   | 内容說明                                                 |
    | :-------------: | :--------------: | :----------------------------------------------------------- |
    | PdServer 地址 | 增量場景下必填 | PD-Server的IP和埠，需要指定對應的協定 <br>如http://{IP:埠}或者https://{IP:埠}<br>IP為您TiDB資料庫實例所在服務器的IP，默認的PD Server埠：2379<br>囙此您需要保證此埠處於開放狀態 <br>例如：http://127.0.0.1:2379 |
    | 資料庫地址 | 必填         | 您TiDB資料庫實例所在服務器的IP地址 <br>例如：127.0.0.1 |
    | 埠         | 必填           | 您TiDB資料庫實例所在服務器的資料庫埠 <br>TiDB默認埠：4000 <br>囙此您需要保證此埠處於開放狀態 |
    | 資料庫名稱 | 必填         | 資料庫名稱，區分大小寫 <br>通過show databases命令列出TiDB所有資料庫 |
    | 帳號        | 非必填      | 您的TiDB資料庫用戶帳號                          |
    | 密碼        | 非必填      | 您的TiDB資料庫使用者密碼                         |
    | TiKV埠 | 增量場景下必填 | TiKV默認埠是20160 <br>該埠是TiDB資料庫實例用於TiKV對外提供存儲服務 <br>囙此您需要保證在資料庫實例的服務上此埠處於開放狀態 |

  - **資料庫許可權檢查**

    - 許可權限制：

      &nbsp;&nbsp;為了保障您的資料庫系統安全及其操作完整性，TiDB連接器作為源主要依賴於以下資料庫許可權，您需要保證您提供的資料庫用戶帳號至少能够對相應的資料庫下的相應錶具備以下完整許可權：
      
      - ***SELECT***：允許用戶從錶中讀取數據的用戶許可權。
      
    - 許可權授權：如果您的用戶不具備以上許可權，您可以在DBA用戶下參攷下列操作進行用戶授權
    
      ```sql
          GRANT 
              SELECT
          ON <DATABASE_NAME>.<TABLE_NAME> 
          TO 'user' IDENTIFIED BY 'password';
      ```
  - **影響增量的内容**
     - 為避免TiCDC的垃圾回收影響事務或增量數據資訊選取，推薦執行命令 ```SET GLOBAL tidb_gc_life_time= '24h'``` 將其設定為24小時或者一個符合您業務需求的時間長度，設定一個較長的時間將允許您讀取更久以前的增量數據，該内容的TiDB資料庫默認屬性值為：```10m0s``` 。


#### 1.2 **作為目標**

  - **資料來源連接内容配寘**

    |  内容名稱  | 是否必填 | 内容說明                                                     |
    | :--------: | :------: | :----------------------------------------------------------- |
    | 資料庫地址 |   必填   | 您TiDB資料庫實例所在服務器的IP<br>例如：127.0.0.1            |
    |     埠     |   必填   | 您TiDB資料庫實例所在服務器的埠 <br>TiDB默認埠: 4000 <br>囙此您需要保證此埠處於開放狀態 |
    | 資料庫名稱 |   必填   | 資料庫名稱，區分大小寫 <br>可以通過 show databases 命令列出TiDB所有資料庫 |
    |    帳號    |  非必填  | 您的TiDB資料庫用戶帳號                                       |
    |    密碼    |  非必填  | 您的TiDB資料庫使用者密碼                                     |

  - **資料庫許可權檢查**

    - 許可權限制：

      &nbsp; 為了保障您的資料庫系統安全及其操作完整性，TiDB連接器作為目標主要依賴於以下資料庫許可權，您需要保證您提供的資料庫用戶帳號對於相應的資料庫下的相應錶至少能够完整具備以下許可權：
    
      - ***CREATE***：允許用戶從錶中讀取數據的用戶許可權的用戶許可權，如果不具備此許可權則無法完成自動建錶的操作。
      - ***DROP***：允許用戶删除資料庫或錶的用戶許可權，如果不具備此許可權則無法完成自動删除錶的操作。
      - ***ALTER***：允許用戶修改錶結構的用戶許可權，如添加、删除列，如果不具備此許可權則無法完成相應的DDL同步。
      - ***INDEX***：允許用戶創建或删除索引的用戶許可權，如果不具備此許可權則無法完成自動索引創建。
      - ***INSERT***：允許用戶向錶中插入新數據的用戶許可權，如果不具備此許可權則相應的插入操作將出現異常。
      - ***UPDATE***：允許用戶更新錶中的現有數據的用戶許可權，如果不具備此許可權則相應的修改操作將出現異常。
      - ***DELETE***：允許用戶删除錶中的數據的用戶許可權，如果不具備此許可權則相應的删除操作將出現異常。
    
    - 許可權授權：如果您的用戶不具備以上許可權，您可以在DBA用戶下參攷下列操作進行用戶授權
    
      ```sql
          GRANT 
            CREATE, DROP, ALTER, INDEX, INSERT, UPDATE, DELETE
          ON <DATABASE_NAME>.<TABLE_NAME> 
          TO 'user' IDENTIFIED BY 'password';
      ```

### ***2 系統架構***

&nbsp;&nbsp; 現時支持的TiDB實例的部署架構主要有以下兩種，請確認您的TiDB部署模式是否符合場景需求：

#### ***2.1 TiDB單節點***：

&nbsp;&nbsp; TiDB單節點是一個部署在單個服務器上的TiDB實例。 它包含所有關鍵組件（TiDB Server、TiKV Server和PD Server），適用於開發和測試環境，但不具備分散式資料庫的高可用性和擴展性特性。

#### ***2.2 TiDB集羣***：

&nbsp;&nbsp; TiDB集羣是一個分散式資料庫系統，由TiDB Server（處理SQL査詢）、TiKV Server（存儲數據）和PD Server（管理中繼資料和調度）組成，實現高可用性、可擴展性和强一致性。

### ***3 版本支持***

#### ***3.1 僅全量資料同步場景***：

&nbsp;&nbsp; 任意版本的TiDB實例均支持同步全量數據

#### ***3.2 包含增量階段資料同步的場景***：

&nbsp;&nbsp; 增量資料同步主要依賴於TiCDC實現，詳情可參閱 [TiCDC官方文档](https://docs.pingcap.com/tidb/stable/ticdc-overview) ，資料來源默認使用 [Tiflow8.1.0](https://github.com/pingcap/tiflow/releases/tag/v8.1.0) 組件用於支持TiDB資料來源進行CDC增量數據獲取，囙此您創建TiDB連接需要支持CDC增量數據變更應該滿足以下全部內容：

  - **運行環境**：TapData引擎需要部署運行在 **arm 或 amd** 系統架構環境下
  - **版本核實**：默認支持的TiDB資料庫版本在 **6.0.X ～ 8.1.X** 之間（包含6.0.0以及8.1.9）

  - **埠開放**：TiServer需要開放多個埠與TiKV進行通信，請保證部署TapData引擎的服務器能正常訪問資料庫服務器中的以下埠：
    - **2379** 埠：該埠主要用於PD與TiKV、TiDB之間的通信以及對外提供API介面。 TiKV和TiDB會通過這個埠從PD獲取集羣的配寘資訊和調度命令。
    - **20162** 埠：該埠用於TiKV對外提供存儲服務，包括處理TiDB的SQL請求、讀寫數據以及和其他TiKV節點之間的內部通信（如Raft協定的消息）。
  - **網絡檢查**：TapData引擎與您的TiDB資料庫實例應處於同網段下或者能之間連接通，例如：內網環境
  - **組件許可權**：進入增量後，TapData會執行Tiflow組件啟動TiServer後臺進程，囙此您需要確保Tiflow組件資源具備作業系統下的 **可讀可寫可執行 **許可權（增量啟動後Tiflow組件存在與**{tapData-dir}/run-resource/ti-db/tool**目錄下名稱為 **cdc**）
  - **庫錶檢查**：TiCDC只複製至少有一個主鍵有效索引的錶。 有效索引定義如下：
    - 主鍵（primary key）是一個有效的索引
    - 如果索引的每一列都明確定義為不可為NULL（NOT NULL），並且索引沒有虛擬生成列（虛擬生成列），則唯一索引（unique index）是有效的

#### ***3.3 原理與擴展***：

&nbsp;&nbsp; 增量進行時會開啟相應的TiCDC Server後臺進程，後臺進程由Tiflow組件支持，若您有其他版本TiDB資料庫的CDC使用需求，您可以前往 [Github: tiflow](https://github.com/pingcap/tiflow/releases) 官方提供的Tiflow組件庫下載支持您TiDB資料庫版本的Tiflow組件，用於支持您的CDC數據讀取，請將下載後的Tiflow組件庫資源在當前作業系統下進行編譯後命名成 **cdc** ，並放置於磁片目錄 **{tapData-dir}/run-resource/ti-db/tool** 下（如目錄下cdc檔案已存在則替換此檔案），以下是擴展Tiflow資源配置步驟：

1. 下載您的tiflow資源後解壓到你的服務器檔案系統，假如解壓到了***/opt/gocode/src*** 目錄下，則這個目錄下存在一個 ***/tiflow*** 目錄
2. 進入到資源目錄（ ***/opt/gocode/src/tiflow*** ）下，執行 ```make``` 命令
3. 在 ***/opt/gocode/src/tiflow*** 目錄下找到生成 ***cdc*** 二進位檔案
4. 將生成的 ***cdc*** 放置於磁片目錄 ***{tapData-dir}/run-resource/ti-db/tool*** 下
5. 值得注意的是：擴展其他版本的Tiflow挿件可能會帶來不確定因素或者可能影響您正在運行或運行過的歷史任務，請謹慎操作哦

&nbsp;&nbsp; TiCDC Server的讀取TiKV增量數據的系統架構，資料來源: [PingCAP](https://docs.pingcap.com/tidb/stable/ticdc-overview)

  <div style="text-align: center;"><img src="https://download.pingcap.com/images/docs/ticdc/cdc-architecture.png" alt="TiCDC的架构" /></div>

  <div style="width:100%;opacity:0.95;font-size: 14px;text-align:center;">圖1.2-1 TiCDC的架構</div> 

### ***4 同步效能***   

&nbsp;&nbsp; TiDB資料來源的讀取效能，測試環境 (MacOS M1 Pro | 8C 16G)

&nbsp;&nbsp; 測試場景： TiDB -> Dummy

&nbsp;&nbsp;全量數據：3000W， 增量壓測：500w，6~8k/s

|  | 全量階段（源） | 增量階段（源） |
| :----------:| :----------: | :----------: |
| QPS峰值 | 19.5w/s | 10k/s |
| QPS最小值 | 6.2w/s | 1.8k/s |
| QPS均值 | 10w/s | 8k/s |
| 延遲峰值 | - | 3s 812ms |
| 延遲最小值 | - | 3s |
| 延遲平均值 | - | 4s94ms |

<span style="color:gray;opacity:0.8;font-size: 8px;padding:0;margin:0;">以上數據决於測試環境且受多種因素影響僅做參攷，以實際運行為主</span>

&nbsp;&nbsp; TiDB資料來源的寫入效能，測試環境 (MacOS M1 | 8C 16G) 

&nbsp;&nbsp; 測試場景：Dummy -> TiDB

|           | 僅插入（目標） | 僅修改（目標） | 僅删除（目標） | 混合寫入（目標） |
| :---------: | :--------------: | :--------------: | :--------------: | :----------------: |
| QPS峰值 | 12k/s<br>14k/s<br>16k/s<br>15.6k/s | 3.71k/s<br>5.39k/s<br>7.378k/s<br>7.82k/s |        4.18k/s<br>6.4k/s<br>8.48k/s<br>8.6k/s        |        1.94k/s<br>3.17k/s<br>4.88k/s<br>5.9k/s        |
| QPS最小值 | 9.8k/s<br>9.9k/s<br>10k/s<br>7.8k/s | 2.66k/s<br>3.69k/s<br>6.15k/s <br>5.42k/s |        3.65k/s<br>5.64k/s<br>7.03k/s<br>7.8k/s        |        1.31k/s<br>2.59k/s<br>3.36k/s<br>5.25k/s        |
| QPS均值   | 11k/s<br>12k/s<br>14k/s<br>10.8k/s | 3.10k/s<br>3.12k/s<br>7.1k/s<br>6.66k/s |      4.0k/s<br>5.98k/s<br>8.01k/s<br>8.42k/s      |        1.79k/s<br>2.98k/s<br>3.52k/s<br>5.43k/s        |

<div style="width: 100%;height: 100%;margin-bottom: 10px;">
    <div style="text-align: right;padding-right: 5px;padding-top: 10px;width: 100%;padding-bottom: 10px;">
        <div style="display: flex;padding-left: 90%;width: 100%;"><div style="width: 16px;height: 8px;background-color: #5470c6;margin-right: 5px;margin-bottom: 2px;"></div><font style="text-align: left;font-size: 7px;color: gray;line-height: 1;">單行緒</font></div>
        <div style="display: flex;padding-left: 90%;width: 100%;"><div style="width: 16px;height: 8px;background-color: #3ba272;margin-right: 5px;margin-bottom: 2px;"></div><font style="text-align: left;font-size: 7px;color: gray;line-height: 1;">多行緒 2T</font></div>
        <div style="display: flex;padding-left: 90%;width: 100%;"><div style="width: 16px;height: 8px;background-color: #fc8452;margin-right: 5px;margin-bottom: 2px;"></div><font style="text-align: left;font-size: 7px;color: gray;line-height: 1;">多行緒 4T</font></div>
        <div style="display: flex;padding-left: 90%;width: 100%;"><div style="width: 16px;height: 8px;background-color: #ee6666;margin-right: 5px;margin-bottom: 2px;"></div><font style="text-align: left;font-size: 7px;color: gray;line-height: 1;">多行緒 8T</font></div>
    </div>
    <div style="display: flex;width: 100%;height: 100%;">
        <div class="x-axis" style="padding-top: 20px;display: flex;flex-direction: column;justify-content: space-between;width:auto;height: 100%;">
             <div style="padding-left:5px;padding-right:5px;margin-top: 10px;font-size: 14px;">插入</div>
             <div style="padding-left:5px;padding-right:5px;margin-top: 45px;font-size: 14px;">修改</div>
             <div style="padding-left:5px;padding-right:5px;margin-top: 45px;font-size: 14px;">删除</div>
             <div style="padding-left:5px;padding-right:5px;margin-top: 50px;font-size: 14px;">混合</div>
        </div> 
        <div style="text-align: center;width: 100%;">
            <div class="chart-container" style="height: 100%;width: 100%;border-left: 1px solid gray;border-bottom: 1px solid gray;padding: 0;padding-top: 20px;">
                <div class="bar-group" style="margin-bottom: 30px;">
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 42%;margin-bottom: 5px;background-color: #5470c6;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">1.2w/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 56%;margin-bottom: 5px;background-color: #3ba272;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">1.4w/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 64%;margin-bottom: 5px;background-color: #fc8452;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">1.6w/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 62.4%;margin-bottom: 5px;background-color: #ee6666;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">1.56w/s</font></div>
                </div>
                <div class="bar-group" style="margin-bottom: 30px;">
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 14.84%;margin-bottom: 5px;background-color: #5470c6;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">3.71k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 21.56%;margin-bottom: 5px;background-color: #3ba272;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">5.39k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 29.512%;margin-bottom: 5px;background-color: #fc8452;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">7.378k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 31.28%;margin-bottom: 5px;background-color: #ee6666;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">7.82k/s</font></div>
                </div>
                <div class="bar-group" style="margin-bottom: 30px;">
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 16.72%;margin-bottom: 5px;background-color: #5470c6;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">4.18k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 25.6%;margin-bottom: 5px;background-color: #3ba272;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">6.4k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 33.92%;margin-bottom: 5px;background-color: #fc8452;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">8.48k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 34.4%;margin-bottom: 5px;background-color: #ee6666;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">8.6k/s</font></div>
                </div>
                <div class="bar-group" style="margin-bottom: 5px;">
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 7.76%;margin-bottom: 5px;background-color: #5470c6;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">1.94k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 12.68%;margin-bottom: 5px;background-color: #3ba272;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">3.17k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 19.52%;margin-bottom: 5px;background-color: #fc8452;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">4.88k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 23.6%;margin-bottom: 5px;background-color: #ee6666;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">5.9k/s</font></div>
                </div>
              </div>
            <div class="x-axis" style="display: flex;justify-content: space-around;margin-top: 2px;font-size: 6px;color: gray;">
                <div style="width: 5%;text-align: right;">1k</div>
                <div style="width: 5%;text-align: right;">2k</div>
                <div style="width: 5%;text-align: right;">3k</div>
                <div style="width: 5%;text-align: right;">4k</div>
                <div style="width: 5%;text-align: right;">5k</div>
                <div style="width: 5%;text-align: right;">6k</div>
                <div style="width: 5%;text-align: right;">7k</div>
                <div style="width: 5%;text-align: right;">8k</div>
                <div style="width: 5%;text-align: right;">9k</div>
                <div style="font-weight: bold;text-align: right;font-size: 8px;margin-top: 0;width: 5%">1w</div>
                <div style="width: 5%;text-align: right;">11k</div>
                <div style="width: 5%;text-align: right;">12k</div>
                <div style="width: 5%;text-align: right;">13k</div>
                <div style="width: 5%;text-align: right;">14k</div>
                <div style="width: 5%;text-align: right;">15k</div>
                <div style="width: 5%;text-align: right;">16k</div>
                <div style="width: 5%;text-align: right;">17k</div>
                <div style="width: 5%;text-align: right;">18k</div>
                <div style="width: 5%;text-align: right;">19k</div>
                <div style="font-weight: bold;font-size: 8px;margin-top: 0;width: 5%;text-align: right;">2w</div>
                <div style="width: 5%;text-align: right;">21k</div>
                <div style="width: 5%;text-align: right;">22k</div>
                <div style="width: 5%;text-align: right;">23k</div>
                <div style="width: 5%;text-align: right;">24k</div>
                <div style="width: 5%;text-align: right;">25k</div>
            </div> 
        </div>
    </div>
</div>



<span style="color:gray;opacity:0.8;font-size: 8px;padding:0;margin:0;">以上數據决於測試環境且受多種因素影響僅做參攷，以實際運行為主</span>

### ***5 資料類型***

&nbsp;&nbsp;**全量**支持的資料類型同步包含以下：

&nbsp;&nbsp;&nbsp;&nbsp;***char***, varchar, ***tinytext***, text, ***mediumtext***, longtext, ***json***, binary, ***varbinary***, tinyblob, ***blob***, mediumblob, ***longblob***, bit, ***tinyint***, tinyint unsigned, ***smallint***, smallint unsigned, ***mediumint***, mediumint unsigned, ***int***, int unsigned, ***bigint***, bigint unsigned, ***decimal***, decimal unsigned, ***float***, double, ***double unsigned***, date, ***time***, datetime, ***timestamp***, year, ***enum***, set, ***INTEGER***, BOOLEAN, ***TEXT***

&nbsp;&nbsp;**增量**支持的資料類型同步包含以下：

&nbsp;&nbsp;&nbsp;&nbsp;Boolean, ***Float***, Double, ***Decimal***, Char, ***Varchar***, Binary, ***Varbinary***, Tinytext, ***Text***, Mediumtext, ***Longtext***, Tinyblob, ***Blob***, CLOB, ***Bit***, Mediumblob, ***Longblob***, Date, ***Datetime***, Timestamp, ***Time***, Year, ***Enum***, Set, ***Bit***, JSON, ***tinyinttinyint unsigned***, smallint, ***smallint unsigned***, mediumint, ***mediumint unsigned***, int, ***int unsigned***, bigint, ***bigint unsigned***, DECIMAL, ***INTEGER***, REAL

> [!注意事項]
>
> 暫不支持圖片類型

### ***6 對源庫的壓力或影響***

&nbsp;&nbsp;測試環境 （MacOS M1 | 8C 16G）

|                    | CPU佔用 | 記憶體消耗 |    網絡消耗     |
| :----------------: | :-----: | :--------: | :-------------: |
| TiDB測試前機器狀態 | 20%~25% | 12GB~13GB  | 100KB/s~300KB/s |
| 全量測試後機器狀態 | 45%~55% |  14.17GB   |  25MB/s~65MB/s  |
| 增量測試後機器狀態 | 50%~55% | 14GB~15GB  |  10MB/s~12MB/s  |
| 數據寫入後機器狀態 | 50%~55% |  14.14GB   |  10MB/s~20MB/s  |

<span style="color:gray;opacity:0.8;font-size: 8px;padding:0;margin:0;">以上數據决於測試環境且受多種因素影響僅做參攷，以實際運行為主</span>



<div style="text-align: center;width: 100%;height: 1px;background-color: rgba(203,203,203,0.9);"></div>
<div style="opacity:0.95;font-size: 14px;text-align:center;"><font style="color: gray">TapData</font> & <font style="color:gray;">TiDB</font></div>
<div style="opacity:0.6;font-size: 10px;text-align:center;">基於CDC的即時資料平臺，用於異構資料庫複製、即時資料集成或構建實时資料倉庫</div>
<div style="opacity:0;font-size: 10px;text-align:center;">TiDB connector doc design by Gavin</div>
