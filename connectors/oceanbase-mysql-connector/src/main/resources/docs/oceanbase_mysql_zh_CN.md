## **连接配置帮助**

### **1. OceanBase 连接器说明**
```
OceanBase连接器默认是开源MySQL模式，如果使用的是Oracle企业版模式，需要使用闭源连接器

该模式下，OceanBase连接器高度兼容MySQL，支持MySQL的绝大部分功能，对于源读取和目标写入相关授权要求完全可以参考MySQL的相关文档
```
### **2. 支持版本**
OceanBase 4.0+

### **3. CDC先决条件**

OceanBase的CDC前置要求与MySQL相同
- 开启binlog
- binlog_format：必须设置为 row 或者 ROW 
- binlog_row_image：必须设置为 full
- 安装ObLogProxy服务

### **4. ObLogProxy**
```
OBLogProxy 是 OceanBase 的增量日志代理服务，它可以与 OceanBase 建立连接并进行增量日志读取，为下游服务提供了变更数据捕获（CDC）的能力。
```

#### **4.1 ObLogProxy 安装**
```
官网下载 OceanBase 日志代理服务包 (OBLogProxy)

https://www.oceanbase.com/softwarecenter

rpm -i oblogproxy-{version}.{arch}.rpm

项目安装默认为 /usr/local/oblogproxy
```

#### **4.2 ObLogProxy 配置**
OBLogProxy 的配置文件默认放在 conf/conf.json
```
"ob_sys_username": ""
"ob_sys_password": ""
```
找到上述配置项修改为 OceanBase 加密后的用户名和密码
- 特别注意
```
OBLogProxy 需要配置用户的用户名和密码，用户必须是 OceanBase 的 sys 租户的用户才能连接。

此处的用户名不应包含集群名称或租户名称，且必须具备 sys 租户下 OceanBase 数据库的读权限。
```
加密方式如下：
```
./bin/logproxy -x username
./bin/logproxy -x password
```

#### **4.3 ObLogProxy 启动**
```
cd /usr/local/oblogproxy

./run.sh start
```