## **Connection Configuration Help**

### **1. OceanBase connector description**
```
The OceanBase connector defaults to open source MySQL mode. If using Oracle Enterprise mode, a closed source connector is required

In this mode, the OceanBase connector is highly compatible with MySQL and supports most of its functions. For authorization requirements related to source read and target write, you can refer to the relevant documentation of MySQL
```
### **2. Supported versions**
OceanBase 4.0+

### **3. CDC prerequisites**

The CDC pre requirements for OceanBase are the same as MySQL
- Enable binlog
- Binlog_format: must be set to row or ROW
- Binlog_row_image: must be set to full
- Install ObLogProxy service

### **4. ObLogProxy**
```
OBLogProxy is the incremental log proxy service of OceanBase, which can establish a connection with OceanBase and perform incremental log reads, providing downstream services with the ability to capture change data (CDC).
```

#### **4.1 ObLogProxy installation**
```
Download OceanBase Log Proxy Service Pack (OBLogProxy) from the official website

https://www.oceanbase.com/softwarecenter

rpm -i oblogproxy-{version}.{arch}.rpm

The project installation defaults to/usr/local/oblogproxy
```

#### **4.2 ObLogProxy Configuration**
The configuration file for OBLogProxy is placed by default in conf/conf.json
```
"Ob_sys_username": "
"Ob_sys_password": "
```
Find the configuration item above and modify it to the username and password encrypted with OceanBase
- Special attention
```
OBLogProxy requires configuring the user's username and password, and the user must be a sys tenant of OceanBase in order to connect.

The username here should not include the cluster name or tenant name, and must have read access to the OceanBase database under the sys tenant.
```
The encryption method is as follows:
```
./bin/logproxy - x username
./bin/logproxy - x password
```

#### **4.3 ObLogProxy startup**
```
cd /usr/local/oblogproxy

./run.sh start
```