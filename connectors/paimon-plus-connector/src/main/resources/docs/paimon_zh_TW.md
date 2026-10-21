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

1. **讀取操作**: 此連接器目前僅支持寫入操作。不支持讀取和 CDC 操作。
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

自訂校驗的 1000 行上限由引擎負責，跨回呼累計查詢結果行數，在第 1001 行報錯並丟棄全部部分結果。`executeCommand` 使用既有 `batchSize` 與結果回呼分批回傳未排序投影；回呼拋出例外會停止讀取並關閉資源。聚合及排序須完成掃描後才能回傳最終結果，不會將聚合輸入明細限制為 1000 行。帶 `ORDER BY` 的查詢會將結果行快取在記憶體中，上限為 `maxBufferedRows`；聚合按組儲存狀態，上限為 `maxGroups`。直接呼叫連接器可回傳超過 1000 行；串流呼叫方若收到後續錯誤，必須丟棄先前收到的批次。

以下參數與 `sql` 一起放在命令的 `params`：

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| `queryTimeoutSeconds` | 0 | 整數秒，省略或 0 表示無期限；範圍 0..2147483647，與 JDBC 上界一致 |
| `maxScanRows` | 新增查詢 10000000 | 最多處理 N 行，若 reader 還有下一行則命令失敗；並非底層檔案實體掃描行數 |
| `maxGroups` | 新增查詢 100000 | 記憶體內最大聚合組數 |
| `maxResultRows` | 預設不設校驗行數上限 | 最大結果行數，超過即整次失敗 |
| `maxBufferedRows` | 100000 | 帶 `ORDER BY` 的查詢必須快取全部結果行才能排序，快取超過 N 行即整次失敗；未排序投影按 `batchSize` 串流回傳，不受此限制 |
| `allowFullScan` | 連線設定，缺省 true | 布林值或 `"true"`/`"false"` 字串；是否允許查詢沒有上述鍵篩選 |

行數／組數參數須為正整數；負數、小數及溢位值會報錯。限制觸發即整次命令失敗，呼叫方應丟棄已收到的批次。超時在 catalog、規劃、讀取、排序及結果階段檢查，屬於**協作式期限**，無法中斷已阻塞的 Paimon／檔案系統 I/O，須另外設定儲存端 I/O 超時。有鍵篩選不保證實際裁剪；大表及高基數分組仍須以代表性資料評估效能與記憶體。

### SQL 邊界與回傳類型

支援 `SELECT *`、標量欄位投影與別名、`COUNT(*)`、`COUNT(1)`、`COUNT(column)`、`SUM`、`AVG`、`MIN`、`MAX`、多欄位 GROUP BY，以及依已選擇的表達式、別名或序號 ORDER BY。`CAST(column AS DATE)` 僅接受 DATE 或無時區 TIMESTAMP，直接取日曆日期，不隱式轉換時區；不支援 DATE(column)。ASC 預設 NULLS FIRST，DESC 預設 NULLS LAST，也可明確指定。排序需將結果行快取在記憶體，超過 `maxBufferedRows` 即整次失敗。ORDER BY 未加引號的別名忽略大小寫，帶引號的別名精確匹配。目前 JSqlParser 4.4 不接受識別字內部雙寫雙引號的語法。

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
