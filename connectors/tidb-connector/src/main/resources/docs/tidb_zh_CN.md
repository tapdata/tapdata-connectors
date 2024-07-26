# TiDB
TiDB 是 PingCAP 设计、研发的开源分布式关系型数据库，是一款同时支持在线事务处理与在线分析处理的融合型分布式数据库产品。完成 Agent 部署后，您可以跟随本文教程在 TapData 中添加 TiDB 数据源，后续可将其作为**源**或**目标库**来构建数据管道。

## 支持版本

* **全量数据同步**：所有版本
* **增量数据同步**：6.0.0 ～ 8.1.9

## 支持同步的操作

- DML：INSERT、UPDATE、DELETE
- DDL：ADD COLUMN、CHANGE COLUMN、AlTER COLUMN、DROP COLUMN

## 数据类型

&nbsp;&nbsp;**全量**支持的数据类型同步包含以下：

&nbsp;&nbsp;&nbsp;&nbsp;***char***, varchar, ***tinytext***, text, ***mediumtext***, longtext, ***json***, binary, ***varbinary***, tinyblob, ***blob***, mediumblob, ***longblob***, bit, ***tinyint***, tinyint unsigned, ***smallint***, smallint unsigned, ***mediumint***, mediumint unsigned, ***int***, int unsigned, ***bigint***, bigint unsigned, ***decimal***, decimal unsigned, ***float***, double, ***double unsigned***, date, ***time***, datetime, ***timestamp***, year, ***enum***, set, ***INTEGER***, BOOLEAN, ***TEXT***

&nbsp;&nbsp;**增量**支持的数据类型同步包含以下：

&nbsp;&nbsp;&nbsp;&nbsp;Boolean, ***Float***, Double, ***Decimal***, Char, ***Varchar***, Binary, ***Varbinary***, Tinytext, ***Text***, Mediumtext, ***Longtext***, Tinyblob, ***Blob***, CLOB, ***Bit***, Mediumblob, ***Longblob***, Date, ***Datetime***, Timestamp, ***Time***, Year, ***Enum***, Set, ***Bit***, JSON, ***tinyinttinyint unsigned***, smallint, ***smallint unsigned***, mediumint, ***mediumint unsigned***, int, ***int unsigned***, bigint, ***bigint unsigned***, DECIMAL, ***INTEGER***, REAL

> [!注意事项]
>
> 暂不支持图片类型

## 注意事项

- TapData引擎与您的TiDB数据库实例应处于同网段下或者能之间连接通，例如：内网环境
- 将 TiDB 作为源以实现增量数据同步场景时，您还需要检查下述信息：

  - 待同步的表需具备主键或唯一索引，其中唯一索引所属列的值不可为 NULL 且不能为虚拟列。
  - 为避免 TiCDC 的垃圾回收影响事务或增量数据信息提取，推荐执行命令 ```SET GLOBAL tidb_gc_life_time= '24h'``` 将其设置为 24 小时或者一个符合您业务需求的时间长度，设置一个较长的时间将允许您读取更久以前的增量数据，该属性的TiDB数据库默认属性值为：```10m0s```
  - TiDB连接器支持CDC的运行环境：TapData 引擎需要部署运行在 **arm 或 amd** 系统架构时。

## 增量同步原理

为进一步简化使用流程，TapData 的 TiDB 连接器集成了 [TiFlow 组件](https://github.com/pingcap/tiflow/releases/tag/v8.1.0) （8.1.0 版本），可基于数据变更日志解析为有序的行级变更数据。更多原理及概念介绍，见 [TiCDC 概述](https://docs.pingcap.com/zh/tidb/stable/ticdc-overview) 。

## <span id="prerequisite">准备工作</span>

1. 登录 TiDB 数据库，执行下述格式的命令，创建用于数据同步/开发任务的账号。

   ```sql
   CREATE USER 'username'@'host' IDENTIFIED BY 'password';
   ```

   * **username**：用户名。
   * **host**：允许该账号登录的主机，百分号（%）表示允许任意主机。
   * **password**：密码。

   示例：创建一个名为 username 的账号，允许从任意主机登录。

   ```sql
   CREATE USER 'username'@'%' IDENTIFIED BY 'your_password';
   ```

2. 为刚创建的账号授予权限。

   * 作为源库

     ```sql
     -- 全量 + 增量同步所需权限如下
     GRANT SELECT ON *.* TO 'username' IDENTIFIED BY 'password';
     ```

   * 作为目标库

     ```sql
     -- 授予指定库权限
     GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, CREATE, DROP ON database_name.* TO 'username';
     
     -- 授予所有库权限
     GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, CREATE, DROP ON *.* TO 'username';
     ```

* **database_name**：数据库<span id="ticdc">名称</span>。
* **username**：用户名。



## 添加数据源

1. 登录 TapData 平台。

2. 在左侧导航栏，单击**连接管理**。

3. 单击页面右侧的**创建**。

4. 在弹出的对话框中，搜索并选择 **TiDB**。

5. 在跳转到的页面，根据下述说明填写 TiDB 的连接信息。

   * **连接信息设置**
      * **连接名称**：填写具有业务意义的独有名称。
      * **连接类型**：支持将 TiDB 数据库作为源或目标。
      * **PD Server 地址**：填写 PDServer 的连接地址和端口，默认端口号为 **2379**，本参数仅在作为源库且需要增量数据同步时填写。
      * **数据库地址**：数据库连接地址。
      * **端口**：数据库的服务端口，默认为 **4000**。
      * **数据库名称**：数据库名称（区分大小写），即一个连接对应一个数据库，如有多个数据库则需创建多个数据连接。
      * **账号**、**密码**：数据库的账号和密码，账号的创建和授权方法，见 [准备工作](#prerequisite) 。
      * **TiKV 端口**：作为 TiDB 的存储层，提供了数据持久化、读写服务和统计信息数据记录功能，默认端口为 **20160**， 本参数仅在作为源库且需要增量数据同步时填写。
      
   * **高级设置**
      * **其他连接串参数**：额外的连接参数，默认为空。
      * **时间类型的时区**：默认为数据库所用的时区，您也可以根据业务需求手动指定。
      * **共享挖掘**：挖掘源库的增量日志，可为多个任务共享源库的增量日志，避免重复读取，从而最大程度上减轻增量同步对源库的压力，开启该功能后还需要选择一个外存用来存储增量日志信息，本参数仅在作为源库时需填写。
      * **包含表**：默认为**全部**，您也可以选择自定义并填写包含的表，多个表之间用英文逗号（,）分隔。
      * **排除表**：打开该开关后，可以设定要排除的表，多个表之间用英文逗号（,）分隔。
      * **Agent 设置**：默认为**平台自动分配**，您也可以手动指定。
      * **模型加载时间**：当数据源中模型数量小于 10,000 时，每小时刷新一次模型信息；如果模型数据超过 10,000，则每天按照您指定的时间刷新模型信息。
      * **开启心跳表**：当连接类型选择为**源头和目标**、**源头**时，支持打开该开关，由 Tapdata 在源库中创建一个名为 **_tapdata_heartbeat_table** 的心跳表并每隔 10 秒更新一次其中的数据（数据库账号需具备相关权限），用于数据源连接与任务的健康度监测。
      
   * **SSL 设置**：选择是否开启 SSL 连接数据源，可进一步提升数据安全性，开启该功能后还需要上传 CA 文件、客户端证书、密钥填写客户端密码。更多介绍，见[生成自签名证书](https://docs.pingcap.com/zh/tidb/stable/generate-self-signed-certificates)。

6. 单击**连接测试**，测试通过后单击**保存**。

   > 如提示连接测试失败，请根据页面提示进行修复。

  

## 常见问题

- **问**：数据同步需要保障哪些端口的通信？

  ***答***：全量同步需要保障 TiDB 集群的数据库端口与 TapData 间的通信，如需增量数据同步，还需要开放下述端口：

  * **2379** 端口：2379为PD Server的默认端口，用于 PD 与 TiKV、TiDB 之间的通信以及对外提供 API 接口。TiKV 和 TiDB 会通过这个端口从 PD 获取集群的配置信息和调度命令，如有另外指定端口号您需要开放此端口号。

  - **20160** 端口：20160为TiKV Server的默认端口，用于 TiKV 对外提供存储服务，包括处理 TiDB 的 SQL 请求、读写数据以及和其他 TiKV 节点之间的内部通信（如 Raft 协议的消息），如有另外指定端口号您需要开放此端口号。

- **问**：TapData 对 TiDB 的部署架构有要求吗？

  ***答***：TiDB 的单机或集群部署架构均可得到支持。

- **问**：如果我的 TiDB 版本不在 6.0.0 ～ 8.1.9 范围内，需要执行增量数据同步，应该怎么办？

  ***答***：TapData 的 TiDB 连接器集成了 TiCDC，可基于数据变更日志解析为有序的行级变更数据。如果您的数据库在支持版本以外，您可以前往 [Github: tiflow](https://github.com/pingcap/tiflow/releases) 下载支持对应版本的 Tiflow 组件，然后跟随下述步骤自行编译 cdc 工具：

  > 扩展其他版本的 Tiflow 组件可能带来不确定因素或影响正在运行的任务，请谨慎操作。

  1. 将下载后的文件解压，然后进入解压后的目录执行 `make` 命令进行编译。

  2. 找到生成的 **cdc** 二进制文件，将其放置在 TapData 引擎所属机器的 **{tapData-dir}/run-resource/ti-db/tool** 目录下（如有则替换）。

  3. 通过 chmod 命令，为该目录下的文件授予可读可写可执行权限。
 
 
 
 
<div style="text-align: center;width: 100%;height: 1px;background-color: rgba(203,203,203,0.9);"></div>
<div style="opacity:0.95;font-size: 14px;text-align:center;"><font style="color: gray">TapData</font> & <font style="color:gray;">TiDB</font></div>
<div style="opacity:0.6;font-size: 10px;text-align:center;">基于CDC的实时数据平台，用于异构数据库复制、实时数据集成或构建实时数据仓库</div>
<div style="opacity:0;font-size: 10px;text-align:center;">TiDB connector doc design by Gavin</div>

     