## **连接配置帮助**
### 数据库版本 
  HuaWei Open GaussDB 主备8.1 postgres版本9.2
### **1. 必要的检查**
1. 开启逻辑复制

- 逻辑日志目前从DN中抽取，如果进行逻辑复制，应使用SSL连接，因此需要保证相应DN上的GUC参数ssl设置为on

2. 设置GUC参数

- 设置GUC参数wal_level为logical

- 设置GUC参数max_replication_slots >= 每个节点所需的（物理流复制槽数+备份槽数+逻辑复制槽数）
    資料庫預設值為20，如需設定，建议参考值设置为使用此连接作为源的的任务任务数+1
    
3. 使用CDC前需要在各DN节点的 pg_hba.conf 中配置你的用户机器（当前部署Agent的机器）：
```text
    # 前提条件:添加JDBC用户机器IP到数据库白名单里，在pg_hba.conf添加以下内容，然后重启数据库即可:
    # 假设JDBC用户IP为10.10.10.10
    host all all 10.10.10.10/32 sha256
    host replication all 10.10.10.10/32 sha256
```
配置完后需重启数据库

### 数据源参数
1. 数据库IP
2. 端口
3. 数据库名称
4. Schema名称
5. 数据库登录用户名
6. 数据库登录密码
7. 逻辑复制IP，主DN的IP
8. 逻辑复制端口，通常是主DN的端口+1，即默认为8001
9. 日志插件，默认使用mppdb_decoding
10. 时区

    
    
### 关于CDC逻辑复制
1. 不支持DDL语句解码，在执行特定的DDL语句（如普通表truncate或分区表exchange）时，可能造成解码数据丢失。
2. 不支持列存、数据页复制的解码。
3. 单条元组大小不超过1GB，考虑解码结果可能大于插入数据，因此建议单条元组大小不超过500MB
4. GaussDB支持解码的数据类型为：
```text
    INTEGER、BIGINT、SMALLINT、TINYINT、SERIAL、SMALLSERIAL、BIGSERIAL、
    FLOAT、DOUBLE PRECISION、
    DATE、TIME[WITHOUT TIME ZONE]、TIMESTAMP[WITHOUT TIME ZONE]、
    CHAR(n)、VARCHAR(n)、TEXT。
```
5. 不支持interval partition表复制。
6. 不支持全局临时表。
7. 在事务中执行DDL语句后，该DDL语句与之后的语句不会被解码。
8. 为解析某个astore表的UPDATE和DELETE语句，需为此表配置REPLICA IDENITY属性，在此表无主键时需要配置为FULL

### 异常处理
1. 增量启动失败
    - 复制槽连接失败，相关错误如下：
      ```text
      com.huawei.opengauss.jdbc.util.PSQLException: 
        [192.168.*.*:5***5/1.9*.1*2.1*2:8001] 
        FATAL: no pg_hba.conf entry for replication connection from host "1*.1**.2*5.1*0",
        user "r**t", SSL off
      ```
      
      错误原因： 访问机器不在DN节点的白名单列表
      
      解决方法：
        在各DN节点的 pg_hba.conf 中配置你的用户机器（当前部署Agent的机器）：
        ```text
        # 添加JDBC用户机器IP到数据库白名单里，在pg_hba.conf添加以下内容
        # 假设JDBC用户IP为10.10.10.10
          host all all 10.10.10.10/32 sha256
          host replication all 10.10.10.10/32 sha256
        # 然后重启数据库即可
        ```