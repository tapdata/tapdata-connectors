## **连接配置帮助**
### **1. TiDB 安装说明**

请遵循以下说明以确保在 TapData 中成功添加和使用 TiDB 数据库以及成功部署TiKV服务以及以及PD服务

### **2. 支持版本**

 - TiDB 5.4.0以上版本 8.0.0以下版本 默认支持CDC 
 
 - TiDB 8.0.0以上版本如需要支持CDC:
    
    1. 请前往 ***https://tiup-mirrors.pingcap.com/cdc-v${ti-db-version}-linux-${system-architecture}.tar.gz*** ，下载对应版本的增量启动工具
    
        - ${ti-db-version}: TiDB对应的版本，例如：8.0.0
    
        - ${system-architecture}: 对应的操作系统架构，例如：amd64 或者 arm64
    
    2. 下载后解压后命名成**cdc**，放置在 **{tapData_dir}/run-resource/ti-db/tool** 环境目录下

**{tapData_dir}/run-resource/ti-db/tool/cdc** 需要具备可读可写可执行权限

### **3. 先决条件（作为源）**
3.1配置连接示例

3.1.1未开启增量配置
```
PdServer 地址：xxxx:xxxx
数据库地址：xxxx
端口：xxxx
数据库名称：xxxx
账号：xxxx
密码：xxxx
```

对于某个数据库赋于select权限
```
GRANT SELECT, SHOW VIEW, CREATE ROUTINE, LOCK TABLES ON <DATABASE_NAME>.<TABLE_NAME> TO 'user' IDENTIFIED BY 'password';
```
对于全局的权限
```
GRANT RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```
###  **4. 先决条件（作为目标）**
对于某个数据库赋于全部权限
```
GRANT ALL PRIVILEGES ON <DATABASE_NAME>.<TABLE_NAME> TO 'user' IDENTIFIED BY 'password';
```
对于全局的权限
```
GRANT PROCESS ON *.* TO 'user' IDENTIFIED BY 'password';
```

### **5. 注意事项

1. TiDB需部署在TapData内网（同网段）环境下

2. TiCDC只复制至少有一个主键有效索引的表。有效索引定义如下：

    - 主键（primary key）是一个有效的索引
    - 如果索引的每一列都明确定义为不可为NULL（NOT NULL），并且索引没有虚拟生成列（虚拟生成列），则唯一索引（unique index）是有效的
