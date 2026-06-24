# HBase 連接器

HBase 是一個分佈式、可擴展的大數據存儲，基於 Google Bigtable 模型。此連接器使 Tapdata 能夠讀寫 Apache HBase。

## 前置條件

- HBase 2.x 集群，ZooKeeper 可訪問
- Tapdata 代理與 HBase/ZooKeeper 之間網絡互通

## 連接配置

| 參數 | 說明 |
|---|---|
| ZooKeeper 集群地址 | ZooKeeper 地址，格式：`host1:2181,host2:2181,host3:2181` |
| ZooKeeper ZNode 父路徑 | HBase 在 ZooKeeper 中的根節點，預設：`/hbase` |
| 使用者名稱 | 可選，Hadoop Simple 認證用戶 |
| 密碼 | 可選，為未來 Kerberos 支援預留 |

## 數據模型

- HBase 表映射為 Tapdata 表
- 行鍵映射為 `row_key` 字段（主鍵）
- 源字段作為 qualifier 存儲在可配置的列族中（預設：`cf`），遵循 HBase 每個表 2–3 個列族的最佳實踐
- 讀取多列族表時，額外的列族以 JSON 字段呈現（`qualifier: value` 鍵值對）

## 支援的操作

| 操作 | 支援情況 |
|---|---|
| 批量讀取 | 支援，含斷點續讀 |
| 批量計數 | 支援 |
| 寫入（增/改/刪） | 支援 |
| 創建表 | 支援，使用單個可配置列族 |
| 刪除表 | 支援 |
| 增量同步 (CDC) | 當前版本不支援 |

## 限制

- 當前版本不支援增量同步（CDC）
- 所有值均以字符串形式處理；二進制、數值、時間類型會被序列化為字符串
- 暫不支援 Kerberos 認證
- 不支援高級查詢過濾
