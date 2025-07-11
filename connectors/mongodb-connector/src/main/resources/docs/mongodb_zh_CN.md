## **连接配置帮助**
MongoDB 是一个流行的、开源 NoSQL 数据库，以灵活/可扩展的方式存储和检索数据。完成 Agent 部署后，您可以跟随本文教程在 TapData 中添加 MongoDB 数据源，后续可将其作为源或目标库来构建数据管道。
> **注意**：MongoDB 作为源端连接时，必须是副本集。
## 支持版本
MongoDB 3.6+
>**注意**：<br>
>由于 Tapdata 数据同步目前是基于 MongoDB 的 Change Stream 支持对多表合并的操作，而 MongoDB 官方是从 4.0 版本开始支持 Change Stream 的，因此，请尽量保证源端数据库和目标端数据库都是 4.0 及以上版本。
## 功能限制
* 源端 MongoDB 支持副本集和分片集群
* 支持MongoDB分片集群从节点全量和增量同步
##  准备工作
### 作为源库
1. 保障源库的架构为副本集或分片集群，如果为单节点架构，您可以将其配置为单成员的副本集以开启 Oplog。
   具体操作，见[如何将单节点转为副本集](https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/)。
2. 配置充足的 Oplog 存储空间，至少需要容纳 24 小时的 Oplog。
   具体操作，见[修改 Oplog 大小](https://docs.mongodb.com/manual/tutorial/change-oplog-size/)。
3. 根据权限管控需求选择下述步骤，创建用于数据同步/开发任务的账号并授予权限。
> **注意**：由于分片服务器不会向 config 数据库获取用户权限，因此，当源库为分片集群架构时，您需要在每个分片的主节点上创建相应的用户并授予权限。
   * 授予指定库（以 **demodata** 库为例）的读权限
     ```bash
     use admin
     db.createUser(
       {
         user: "tapdata",
         pwd: "my_password",
         roles: [
            { role: "read", db: "demodata" },
            { role: "read", db: "local" },
            { role: "read", db: "config" },
            { role: "clusterMonitor", db: "admin" },
         ]
       }
     )
     ```

   * 授予所有库的读权限。

     ```bash
     use admin
     db.createUser(
       {
         user: "tapdata",
         pwd: "my_password",
         roles: [
            { role: "readAnyDatabase", db: "admin" },
            { role: "clusterMonitor", db: "admin" },
         ]
       }
      )
     ```
> **注意**：只有 MongoDB 版本 3.2 需要 local 数据库的读取权限。
4. 在设置 MongoDB URI 时，推荐将写关注级别设置为大多数，即 `w=majority`，否则可能因 Primary 节点异常宕机导致的数据丢失文档。

5. 源库为集群架构时，为提升数据同步性能，TapData 将会为每个分片创建一个线程并读取数据，在配置数据同步/开发任务前，您还需要执行下述操作。

   * 关闭源库的均衡器（Balancer），避免块迁移对数据一致性的影响。具体操作，见[如何停止平衡器](https://docs.mongodb.com/manual/reference/method/sh.stopBalancer/)。
   * 清除源库中，因块迁移失败而产生的孤立文档，避免 _id 冲突。具体操作，见[如何清理孤立文档](https://docs.mongodb.com/manual/reference/command/cleanupOrphaned/)。

> **重要事项**<br>
> 1.对于集群分片，您必须在每个分片主节点上创建适当的用户权限。 这是由于MongoDB的安全架构设计。
> 当登录到每个单独的分片时，分片服务器不会向config数据库获取用户权限。 相反，它将使用其本地用户数据库进行身份验证和授权。<br>
> 2.对于MongoDB 3.x 以下版本请确保Tapdata服务与MongoDB各节点通信正常。
### 作为目标库

授予指定库（以 **demodata** 库为例）的写权限，并授予 **clusterMonitor** 角色以供数据验证使用，示例如下：

```bash
use admin
db.createUser(
  {
    user: "tapdata",
    pwd: "my_password",
    roles: [
       { role: "readWrite", db: "demodata" },
       { role: "clusterMonitor", db: "admin" },
       { role: "read",db: "local"}
    ]
  }
)
```
> **注意**：只有 MongoDB 版本 3.2 需要 local 数据库的读取权限。
### MongoDB TLS/SSL配置
- **启用TLS/SSL**<br>
请在左侧配置页的 “使用TLS/SSL连接”中选择“是”项进行配置<br>
- **设置MongoDB PemKeyFile**<br>
点击“选择文件”，选择证书文件，若证书文件有密码保护，则在“私钥密码”中填入密码<br>
- **设置CAFile**<br>
请在左侧配置页的 “验证服务器证书”中选择“是”<br>
然后在下方的“认证授权”中点击“选择文件”<br>
### MongoDB 性能测试
配置:ecs.u1-c1m2.2xlarge 机型, 8C 16G, 100GB ESSD 磁盘

|  写入方式   | RPS      |
| -------- |------------|
| 全量写入  |    95K     |
| 混合写入  |    2.5k    |


| 读取方式 | RPS      |
|------|------------|
| 全量读取 |    50k     |
| 增量读取 |    14k    |
