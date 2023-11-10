## **连接配置帮助**

### **1. Hudi 安装说明**

请遵循以下说明以确保在 Tapdata 中成功添加和使用 Hudi 数据库。

### **2. 限制说明**

Tapdata 系统当前版本 Hudi 仅支持作为目标。

### **3. 支持版本**

Hudi0.11.0

### **4. 配置说明**

#### 数据源配置示例

*   集群地址
    *   ip\:port
*   数据库
    *   test\_tapdata
*   Kerberos认证
    *   密钥表示文件
        *   上传user.keytab文件
    *   配置文件
        *   上传krb5.conf文件
    *   Hive主体配置
        *   spark2x/hadoop.<hadoop.com@HADOOP.COM> (对应principal的值)
*   账户
    *   test\_tapdata
*   密码
*   连接参数
    *   ;sasl.qop=auth-conf;auth=KERBEROS

### **5. 连接测试项**

- 检测 host/IP 和 port
- 检查数据库名称
- 检查账号和密码
- 检查写权限
