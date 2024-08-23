# **1. 支援版本**

- Scala：`2.12`
- kafka：`2.0` ~ `2.5`

# **2. 支援的資料類型**

- 对象：對應 `TapMap`
- 数组：對應 `TapArray`
- 數字：對應 `TapNumber`
- 布林：對應 `TapBoolean`
- 字元：對應 `TapString`

# **3. 功能限制/注意事項/能力邊界**

- 僅支援 `JSON Object` 字串的訊息格式 (如 `{"id":1, "name": "張三"}`)
- 如果選擇「忽略非JSON物件格式訊息」或「忽略推播訊息異常」，則仍會記錄這些訊息的 `offset`，即是後續不會推送這些訊息，存在資料遺失風險
- 訊息推播實作為 `At least once`，對應的消費端要做好冪等運算
- 訊息壓縮類型：預設 `gzip`，大流量訊息開啟壓縮可以提高傳輸效率
- `Kafka` 本身沒有 `DML`、`DDL` 區分：
- 場景 `Oracle` >> `Kafka` >> `MySQL`，支援 `Oracle` 的 `DDL`、`DML` 同步
- 暫不支援由其它產品同步到 `Kafka` 的 `DDL` 同步場景

# **4. ACK 確認機制**

- 不確認（`0`）：訊息推送後退出，不會檢查 kafka 是否已經正確寫入
- 僅寫入 Master 分區（`1`）：訊息推送後，會保證主分區接收並寫入
- 寫入大多數 ISR 分區（`-1`，預設選項）：訊息推送後，會保證資料被超過一半的分區接收並寫入
- 寫入所有 ISR 分區（`all`）：訊息推送後，會確保資料被所有分區接收並寫入

# **5. 認證方式**

## **5.1 使用者密碼**

- 不填時，為無認證模式，使用 `org.apache.kafka.common.security.plain.PlainLoginModule` 配置
- 填寫時，會使用 `org.apache.kafka.common.security.scram.ScramLoginModule` 來配置

## **5.2 Kerberos**

1. 取得 Kerberos Ticket
    - 可透過 `kinit -kt <keytab-file> <principal>`、`kadmin.local -q "ktadd -norandkey -k <keytab-file> <principal>"` 取得
        - `keytab-file`：為檔案名
        - `principal`：為主體配置名
        - `-norandkey` 不變更密碼
    - 可透過 `klist`、`kadmin.local -q "list_principals"` 查看配置
2. 根據 Ticket 資訊配置
    - 金鑰表示文件，如：`kdc.keytab`
    - 設定文件，如：`kdc.conf`
    - 主體配置，如：`kafka/kkz.com@EXAMPLE.COM`
    - 服務名，如：`kafka`

### **5.2.1 常見問題**

1. 提示網域未配置，如：`kkz.com`
    - 登入 `FE` 所在機器
    - 修改 `/etc/hosts` 設定：
    ```shell
    cat >> /etc/hosts << EOF
    192.168.1.2 kkz.com
    EOF
    ```
