# Apache Paimon 連接器

## 概述

Apache Paimon 是一個流式數據湖平台，支持高速數據攝取、變更數據捕獲和高效數據查詢。此連接器允許 Tapdata 將數據寫入 Paimon 表。

## 支持的操作

- **連接測試**: 驗證倉庫可訪問性和寫入權限
- **模型加載**: 從 Paimon 加載表定義
- **創建表**: 創建新的 Paimon 表
- **刪除表**: 刪除 Paimon 表
- **清空表**: 刪除表中的所有數據
- **寫入記錄**: 插入、更新和刪除記錄

## 配置說明

### 倉庫路徑
Paimon 存儲數據的根路徑。可以是:
- 本地文件系統: `/path/to/warehouse`
- HDFS: 將構造為 `hdfs://host:port/path`
- S3: `s3://bucket/path`
- OSS: `oss://bucket/path`

### 存儲類型
選擇 Paimon 的存儲後端:
- **Local**: 本地文件系統（用於測試）
- **HDFS**: Hadoop 分佈式文件系統
- **S3**: Amazon S3 或 S3 兼容存儲
- **OSS**: 阿里雲對象存儲服務

### S3 配置
使用 S3 存儲時:
- **S3 端點**: S3 服務端點（例如：https://s3.amazonaws.com）
- **S3 訪問密鑰**: AWS 訪問密鑰 ID
- **S3 密鑰**: AWS 密鑰
- **S3 區域**: AWS 區域（例如：us-east-1）

### HDFS 配置
使用 HDFS 存儲時:
- **HDFS 主機**: NameNode 主機名
- **HDFS 端口**: NameNode 端口（默認：9000）
- **HDFS 用戶**: HDFS 操作用戶（默認：hadoop）

### OSS 配置
使用 OSS 存儲時:
- **OSS 端點**: OSS 服務端點（例如：https://oss-cn-hangzhou.aliyuncs.com）
- **OSS 訪問密鑰**: 阿里雲訪問密鑰 ID
- **OSS 密鑰**: 阿里雲訪問密鑰

### 數據庫名稱
Paimon 數據庫名稱（默認：default）

### 本地臨時目錄與 Spill 回收

連接器在 `diskTmpDir` 下按以下結構存放 Paimon 的本地 Spill 與索引文件，每個寫入表各有獨立的租約目錄：

```text
<diskTmpDir>/tapdata-paimon-v2/<進程標識>/<租約>/owner.lock
<diskTmpDir>/tapdata-paimon-v2/<進程標識>/<租約>/io/paimon-io-<UUID>/
```

- **存活判定：** 只以作業系統文件鎖判斷目錄是否仍在使用，不按修改時間推斷。運行中任務的目錄不會被其他任務或其他引擎刪除。
- **正常停止：** 任務正常停止後，其目錄全部刪除，不留空目錄。
- **異常退出：** 進程崩潰（OOM、`kill -9`）後遺留的目錄，由同一台機器上下一次啟動 Paimon 任務的引擎回收（包括重啟後的同一引擎）；每次最多回收 64 個，其餘留到下一次。
- **部署建議：** 每個引擎進程使用獨佔的 `diskTmpDir`。Kubernetes 使用每個 Pod 獨立的 emptyDir 或獨立的 subPath，Docker 使用獨立的 volume。
- **網路或 FUSE 文件系統：** `diskTmpDir` 位於 NFS、CIFS、FUSE、Ceph 等文件系統時，跨主機文件鎖不可靠，連接器不自動回收遺留目錄，只記錄告警。請改用本地磁碟，或按下方步驟手動清理。
- **升級：** 升級連接器後請重啟引擎，避免同一進程內新舊版本並存。舊版本在 `diskTmpDir` 根下創建的 `paimon-io-*` 目錄與 `.paimon-io-*.tapdata-owner.lock` 文件不會被新版本打開或刪除。確認使用該目錄的所有引擎都已升級並重啟後，可以一次性刪除：

  ```sh
  # 先列出，確認沒有任何舊版本引擎仍在使用該目錄後再刪除
  find <diskTmpDir> -maxdepth 1 \( -name 'paimon-io-*' -o -name '.paimon-io-*.tapdata-owner.lock' \) -print
  find <diskTmpDir> -maxdepth 1 \( -name 'paimon-io-*' -o -name '.paimon-io-*.tapdata-owner.lock' \) -exec rm -rf {} +
  ```

- **排查：** 租約的創建、退役、保留、回收與跳過事件寫入引擎日誌，logger 名為 `io.tapdata.connector.paimon.spill`；任務日誌只記錄摘要。

## 數據類型映射

| Paimon 類型 | Tapdata 類型 |
|-------------|--------------|
| BOOLEAN | TapBoolean |
| TINYINT | TapNumber |
| SMALLINT | TapNumber |
| INT | TapNumber |
| BIGINT | TapNumber |
| FLOAT | TapNumber |
| DOUBLE | TapNumber |
| DECIMAL | TapNumber |
| CHAR | TapString |
| VARCHAR | TapString |
| STRING | TapString |
| BINARY | TapBinary |
| VARBINARY | TapBinary |
| DATE | TapDate |
| TIME(0-3) | TapTime |
| TIMESTAMP | TapDateTime |
| TIMESTAMP_LTZ | TapDateTime |
| STRING | TapArray (以 JSON 字符串存儲) |
| STRING | TapMap (以 JSON 字符串存儲) |
| STRING | TapRow/TapRaw (以 JSON 字符串存儲) |

`INT` 和 `INTEGER` 都建立 Paimon `INT`。不帶參數的 `DECIMAL` 使用 `DECIMAL(38,10)`；
不帶參數的 `TIME` 使用 `TIME(3)`，僅支援 0-3 位精度，包含亞毫秒精度的輸入會被拒絕。

DECIMAL 值必須精確符合實體欄位的 precision 和 scale。精度溢位或縮減小數位數會改變數值時，寫入失敗；連接器不會執行 `HALF_UP` 捨入。例如，寫入 `DECIMAL(5,2)` 時，`1.235` 會被拒絕，`1.2300` 則可作為 `1.23` 寫入，因為去除尾隨零不會改變數值。

連接器不會自動遷移既有目標欄位：歷史 STRING 欄位仍按實際物理類型寫入，原生 Paimon
ARRAY/MAP/ROW/MULTISET/VARIANT 目標欄位會被拒絕。複雜 CDC 值必須以 JSON 寫入 STRING 欄位。

## 限制

1. **讀取操作**: 不支援 CDC 與串流讀取；唯讀臨時查詢請使用 `executeQuery`（見下文）。
2. **索引**: Paimon 不支持傳統索引。僅支持主鍵。
3. **模型變更**: 不支持運行時動態模型變更。

## 最佳實踐

1. **主鍵**: 始終為表定義主鍵以實現高效的更新和刪除。
2. **批量大小**: 使用適當的批量大小以獲得更好的寫入性能。
3. **存儲選擇**: 根據部署環境選擇適當的存儲後端。
4. **分區**: 對於大表，考慮使用 Paimon 的分區功能。

## 故障排除

### 連接問題
- 驗證倉庫路徑可訪問
- 檢查存儲憑據是否正確
- 確保與存儲後端的網絡連接

### 寫入失敗
- 驗證倉庫路徑的寫入權限
- 檢查表模型是否與源數據匹配
- 查看 Paimon 日誌以獲取詳細錯誤信息

### 性能問題
- 增加批量大小以提高吞吐量
- 為工作負載使用適當的存儲後端
- 考慮啟用 Paimon 的壓縮功能

## 參考資料

- [Apache Paimon 文檔](https://paimon.apache.org/)
- [Paimon GitHub 倉庫](https://github.com/apache/paimon)

## 自訂唯讀 SQL（`executeQuery`）

支援單表投影、`CAST(timestamp AS DATE)`、`COUNT/SUM/MIN/MAX/AVG`、`GROUP BY` 與 `ORDER BY`。校驗兩端請使用相同的明確別名。不支援 JOIN、UNION、HAVING、DISTINCT、任意運算式或資料修改。WHERE 下推至 Paimon，並只讀取必要欄位；聚合仍須掃描匹配資料並按組儲存狀態。

投影、聚合及排序查詢預設允許非鍵篩選與全表掃描。需要限制時，可設定連線 `queryAllowFullScan=false` 或命令布林值 `allowFullScan=false`，啟用分區鍵或前導主鍵篩選要求。命令參數優先；直接／包裝 `COUNT(1)` 不受該鍵篩選要求影響；該限制也適用於歷史未排序 `SELECT *`。`AND` 任一條件符合即可；`OR` 的每個分支都必須限制合格鍵。相同鍵的多個等值條件可改寫為 `IN`。鍵欄位的正向 `IS NULL` 可用於鍵篩選；`NOT` 及否定形式不符合。確實要全表掃描時才明確設為 `true`。支援的 WHERE 會下推，但篩選語法本身不保證檔案裁剪。

自訂校驗的 1000 行上限由引擎負責，跨回呼累計查詢結果行數，在第 1001 行報錯並丟棄全部部分結果。`executeCommand` 使用既有 `batchSize` 與結果回呼分批回傳未排序投影；回呼拋出例外會停止讀取並關閉資源。聚合及排序須完成掃描後才能回傳最終結果，不會將聚合輸入明細限制為 1000 行。帶 `ORDER BY` 的查詢會將結果行快取在記憶體中，上限為 `maxBufferedRows`；聚合按組儲存狀態，上限為 `maxGroups`；帶 `ORDER BY` 的聚合查詢會將每組物化為一行後排序，同時受兩個上限約束。直接呼叫連接器可回傳超過 1000 行；串流呼叫方若收到後續錯誤，必須丟棄先前收到的批次。

以下參數與 `sql` 一起放在命令的 `params`：

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| `queryTimeoutSeconds` | 0 | 整數秒，省略或 0 表示無期限；範圍 0..2147483647，與 JDBC 上界一致 |
| `maxScanRows` | 新增查詢 10000000 | 最多處理 N 行，若 reader 還有下一行則命令失敗；並非底層檔案實體掃描行數 |
| `maxGroups` | 新增查詢 100000 | 記憶體內最大聚合組數 |
| `maxResultRows` | 預設不設校驗行數上限 | 最大結果行數，超過即整次失敗 |
| `maxBufferedRows` | 100000 | 帶 `ORDER BY` 的查詢必須快取全部結果行才能排序，快取超過 N 行即整次失敗；帶 `ORDER BY` 的聚合查詢同樣按組快取並受此限制；未排序投影按 `batchSize` 串流回傳，不受此限制 |
| `allowFullScan` | 連線設定，缺省 true | 布林值或 `"true"`/`"false"` 字串；是否允許查詢沒有上述鍵篩選 |

行數／組數參數須為正整數；負數、小數及溢位值會報錯。限制觸發即整次命令失敗，呼叫方應丟棄已收到的批次。超時在 catalog、規劃、讀取、排序及結果階段檢查，屬於**協作式期限**，無法中斷已阻塞的 Paimon／檔案系統 I/O，須另外設定儲存端 I/O 超時。有鍵篩選不保證實際裁剪；大表及高基數分組仍須以代表性資料評估效能與記憶體。

### SQL 邊界與回傳類型

支援 `SELECT *`、標量欄位投影與別名、`COUNT(*)`、`COUNT(1)`、`COUNT(column)`、`SUM`、`AVG`、`MIN`、`MAX`、多欄位 GROUP BY，以及依已選擇的表達式、別名或序號 ORDER BY。`CAST(column AS DATE)` 僅接受 DATE 或無時區 TIMESTAMP，直接取日曆日期，不隱式轉換時區；不支援 DATE(column)。ASC 預設 NULLS FIRST，DESC 預設 NULLS LAST，也可明確指定。排序需將結果行快取在記憶體，超過 `maxBufferedRows` 即整次失敗；整型欄位只接受欄位範圍內的整數字面量，小數或超範圍字面量會以 `literal ... is not representable as ...` 明確報錯，不會靜默截斷，也不會誤報為語法錯誤。ORDER BY 未加引號的別名忽略大小寫，帶引號的別名精確匹配。目前 JSqlParser 4.4 不接受識別字內部雙寫雙引號的語法。

WHERE 支援欄位與常數比較（欄位可在任一側）、BETWEEN、IN、IS NULL、有限 LIKE、AND/OR/NOT。不支援通用子查詢、WITH、視窗函數、算術、分頁、表別名或帶資料庫/schema 前綴的表名。不可選擇或在表達式中引用 ARRAY/MAP/MULTISET/ROW 欄位，含複雜欄位的表請明確列出標量欄位。

空輸入的全域 COUNT 為 0，其他聚合為 NULL；分組查詢回傳空結果。SUM/AVG 對所有數值類型使用 BigDecimal，AVG 按 DECIMAL128 捨入。SUM/AVG 遇到 NaN 或正負無窮時明確報資料錯誤，不回傳部分結果。MIN/MAX 與 ORDER BY 的順序為 -Infinity < 有限數值 < +Infinity < NaN；正負零相等。`GROUP BY` 同樣將正負零歸入同一分組，所有 NaN 也歸入同一分組。COUNT 及一般投影允許上述浮點特殊值。

直接及歷史包裝 COUNT(1) 回傳單個 Long，其他查詢回傳 `List<Map<String,Object>>`；SQL NULL 對應 Java null。一般欄位與 MIN/MAX 保持既有類型：

| SQL 值或表達式 | Java 值 |
| --- | --- |
| BOOLEAN；TINYINT / SMALLINT / INT / BIGINT | Boolean；Byte / Short / Integer / Long |
| FLOAT / DOUBLE / DECIMAL | Float / Double / BigDecimal |
| CHAR / VARCHAR；BINARY / VARBINARY | String；byte[] |
| DATE 欄位 | java.sql.Date，毫秒值為 epoch-day × 86400000（歷史行為；文字顯示取決於 JVM 時區） |
| TIME 欄位 | 錨定 1970-01-01 的 LocalDateTime |
| 無時區 TIMESTAMP | LocalDateTime |
| 本地時區 TIMESTAMP | UTC ZonedDateTime |
| CAST(column AS DATE) | LocalDate |
| COUNT；SUM / AVG | Long；BigDecimal |
| MIN / MAX | 與參數欄位類型相同；CAST AS DATE 參數回傳 LocalDate |

截止時間與耗時在生命週期准入完成後開始計算，包含 catalog、規劃、讀取、排序及結果建構。排序取消會丟棄內部緩衝，不發布部分排序結果。

SQL Server 來源端使用相同別名，例如：

```sql
SELECT CAST(CreationTime AS DATE) AS creation_date,
       COUNT(*) AS row_count,
       SUM(TotalValues) AS total_values,
       SUM(PointsAfterRedemption) AS points_after_redemption
FROM DRSRedemptions
WHERE TransactionDatetime BETWEEN '2025-01-01 00:00:00' AND '2025-07-01 00:00:00'
  AND op <> 'd'
GROUP BY CAST(CreationTime AS DATE)
ORDER BY CAST(CreationTime AS DATE)
```


### 本機暫存目錄與升級

建议每個引擎使用獨佔、持久且容量充足的本地暫存目錄。設定 `diskTmpDir` 時，同時檢查
`fs.s3a.buffer.dir` 和 `fs.s3a.bucket.<bucket>.buffer.dir` 覆盖值。初始化寫入探針只验证
當時能寫入一字节，不预留上传块，也不保证執行期间空間始终充足。

升級後必须重启引擎：旧任務的 ClassLoader 不遵守新增的回收同步协议。
`systemd-tmpfiles` 对 `/tmp` 的清理以及容器卷刪除，都可能越过应用檔案鎖刪除活跃目錄。
请在主机或容器中設定暫存目錄的保留规则。NFS/FUSE 等不在准入白名单内的檔案系統不自动回收。

旧平铺 `paimon-io-*` 目錄只告警、不自动刪除，升級不会自动释放這些空間。
先停止所有使用這些根目錄的引擎和任務，再用
`find <root> -maxdepth 1 -type d -name 'paimon-io-*'` 列出候选，
用 `du -sh -- <specific-directory>` 檢查体积，僅逐個刪除已确认不再使用的目錄。
引擎執行時不得刪除 `tapdata-paimon-v2` 或其他仍在使用的暫存目錄。

啟動最多等待回收 200ms；超時後背景扫描继续持锁完成，忙時不积压後续扫描。
回收 WARN/ERROR 同時轉發到有界异步任務日誌；日誌後端阻塞或佇列饱和仍可能丢失事件，
关闭摘要中的 `droppedLogs` 可辅助观察。容量或挂載问题需使用引擎账号，在相同挂載命名空間下
结合 `df -hT`、`df -i`、`findmnt -T <directory>`、`namei -l <directory>` 檢查。
