## **连接配置帮助**

### **1. Hudi 安装说明**

请遵循以下说明以确保在 Tapdata 中成功添加和使用 Hudi 数据库。

### **2. 限制说明**

1. Tapdata 系统当前版本 Hudi 仅支持作为目标。

2. 环境配置要求

   - 计算引擎的机器上应具备Hadoop的环境变量，Hadoop版本与您的服务端安装的Hadoop应保持一致
    
     ```
     执行以下命令检查您的机器是否具备此条件：
       Windows环境下
            按住 win+R， 
            输入 cmd 或 prowershell 后
            在命令窗口输入 hadoop -version 检查是否具备此条件
       Linux或Mac环境下，
            打开终端，
            在命令窗口输入 hadoop -version 检查是否具备此条件
     ```

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
*   服务端hadoop的配置文件：core-site.xml， 一般在你服务端Hadoop安装目录下etc/Hadoop目录下
*   服务端hdfs的配置文件：hdfs-site.xml， 一般在你服务端Hadoop安装目录下etc/Hadoop目录下
*   服务端hive的配置文件：hive-site.xml， 一般在你服务端Hive安装目录的配置文件目录下
*   连接参数
    *   ;sasl.qop=auth-conf;auth=KERBEROS

### **5. 连接测试项**

- 检测 host/IP 和 port
- 检查数据库名称
- 检查账号和密码
- 检查写权限
