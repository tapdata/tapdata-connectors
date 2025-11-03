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
| TIMESTAMP | TapDateTime |
| TIMESTAMP_LTZ | TapDateTime |
| ARRAY | TapArray (以 JSON 字符串存儲) |
| MAP | TapMap (以 JSON 字符串存儲) |
| ROW | TapMap (以 JSON 字符串存儲) |

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

