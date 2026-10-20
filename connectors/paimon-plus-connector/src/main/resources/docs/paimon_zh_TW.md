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

支援單表投影、`CAST(timestamp AS DATE)` / `DATE(timestamp)`、`COUNT/SUM/MIN/MAX/AVG`、`GROUP BY` 與 `ORDER BY`。校驗兩端請使用相同的明確別名。不支援 JOIN、UNION、HAVING、DISTINCT、任意運算式或資料修改。WHERE 下推至 Paimon，並只讀取必要欄位；聚合仍須掃描匹配資料並按組儲存狀態。

新增投影、聚合及排序查詢預設要求分區鍵或前導主鍵的比較、BETWEEN 或 IN 篩選。舊連線若需在沒有此類篩選時使用新增查詢，請編輯連線開啟 `queryAllowFullScan`，或在個別命令傳入 `params.allowFullScan=true`。命令參數優先。歷史未排序的 `SELECT *` 及直接／包裝 `COUNT(1)` 仍允許無鍵篩選，預設不增加掃描／結果上限，明確傳入的限制仍生效。

以下參數與 `sql` 一起放在命令的 `params`：

| 參數 | 預設值 | 說明 |
| --- | --- | --- |
| `queryTimeoutSeconds` | 0 | 整數秒，省略或 0 表示無期限；範圍 0..2147483647，與 JDBC 上界一致 |
| `maxScanRows` | 新增查詢 10000000 | reader 交付行數，並非底層實際掃描的檔案行數 |
| `maxGroups` | 新增查詢 100000 | 記憶體內最大聚合組數 |
| `maxResultRows` | 新增查詢 1000 | 最大結果行數，超過即整次失敗 |
| `allowFullScan` | 連線設定，缺省 false | 是否允許新增查詢沒有上述鍵篩選 |

行數／組數參數須為正整數；負數、小數及溢位值會報錯。限制觸發即整次命令失敗，不回傳部分結果。超時在 catalog、規劃、讀取、排序及結果階段檢查，屬於**協作式期限**，無法中斷已阻塞的 Paimon／檔案系統 I/O，須另外設定儲存端 I/O 超時。有鍵篩選不保證實際裁剪；大表及高基數分組仍須以代表性資料評估效能與記憶體。
