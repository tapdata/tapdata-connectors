## **連接配置幫助**

### **1. Hudi 安裝説明**

請遵循以下説明以確保在 Tapdata 中成功添加和使用 Hudi 數據庫。

### **2. 限制説明**

Tapdata 系統當前版本 Hudi 僅支持作爲目標。

### **3. 支持版本**

Hudi0.11.0

### **4. 配置説明**

#### 數據源配置示例

*   集群地址
    *   ip\:port
*   數據庫
    *   test\_tapdata
*   Kerberos認證
    *   密鑰表示文件
        *   上傳user.keytab文件
    *   配置文件
        *   上傳krb5.conf文件
    *   Hive主體配置
        *   spark2x/hadoop.<hadoop.com@HADOOP.COM> (對應principal的值)
*   賬戶
    *   test\_tapdata
*   密碼
*   連接參數
    *   ;sasl.qop=auth-conf;auth=KERBEROS

### **5. 連接測試項**

- 檢測 host/IP 和 port
- 檢查數據庫名稱
- 檢查賬號和密碼
- 檢查寫權限