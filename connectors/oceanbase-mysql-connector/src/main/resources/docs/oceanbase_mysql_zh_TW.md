## **連接配寘幫助**

### **1. OceanBase連接器說明**
```
OceanBase連接器默認是開源MySQL模式，如果使用的是Oracle企業版模式，需要使用閉源連接器

該模式下，OceanBase連接器高度相容MySQL，支持MySQL的絕大部分功能，對於源讀取和目標寫入相關授權要求完全可以參考MySQL的相關文檔
```
### **2. 支持版本**
OceanBase 4.0+

### **3. CDC先決條件**

OceanBase的CDC前置要求與MySQL相同
-開啟binlog
- binlog_format：必須設定為row或者ROW
- binlog_row_image：必須設定為full
- 安裝ObLogProxy服務

### **4. ObLogProxy**
```
OBLogProxy是OceanBase的增量日誌代理服務，它可以與OceanBase建立連接並進行增量日誌讀取，為下游服務提供了變更數據捕獲（CDC）的能力。
```

#### **4.1 ObLogProxy安裝**
```
官網下載OceanBase日誌代理服務包（OBLogProxy）

https://www.oceanbase.com/softwarecenter

rpm -i oblogproxy-{version}.{arch}.rpm

項目安裝默認為/usr/local/oblogproxy
```

#### **4.2 ObLogProxy配寘**
OBLogProxy的設定檔默認放在conf/conf.json
```
"ob_sys_username": ""
"ob_sys_password": ""
```
找到上述配寘項修改為OceanBase加密後的用戶名和密碼
- 特別注意
```
OBLogProxy需要配寘用戶的用戶名和密碼，用戶必須是OceanBase的sys租戶的用戶才能連接。

此處的用戶名不應包含集羣名稱或租戶名稱，且必須具備sys租戶下OceanBase資料庫的讀許可權。
```
加密管道如下：
```
./bin/logproxy -x username
./bin/logproxy -x password
```

#### **4.3 ObLogProxy啟動**
```
cd /usr/local/oblogproxy

./run.sh start
```