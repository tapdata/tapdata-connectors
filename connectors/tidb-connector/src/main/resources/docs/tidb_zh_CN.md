
<div style="text-align: right; font-weight: normal;color: gray;"><img style="width: 20px;height:20px;object-fit: contain;flex: 1;" src="https://tapdata.io/assets-global.website-files.com/61acd54a60fc632b98cbac1a/620d4543dd51ea0fcb7b237c_Favicon2.png" alt="TiCDC的架构" /> & <img src="https://img1.tidb.net/favicons/favicon-32x32.png" style="width: 20px;height:20px;object-fit: contain;flex: 1;"/></div>

## **连接配置帮助**

&nbsp;&nbsp; 请遵循以下说明以确保在 TapData 中成功添加和使用TiDB数据库，并且成功部署TiKV服务以及TiPD服务
    
### ***1 同步类型***

&nbsp;&nbsp; TiDB数据源目前覆盖多种数据同步场景，支持同步类型有以下四种类型：

 - **仅全量（源）**：全量数据同步，库级/表级数据迁移
 - **仅增量（源）**：增量数据同步，DML/DDL同步
 - **全量+增量（源）**：全量数据同步，库级/表级数据迁移，增量数据同步，DML/DDL同步
 - **数据写入（目标）**：创建表，删除表，数据写入（插入/修改/删除），DDL同步

#### 1.1 **作为源**

  - **数据源连接属性配置**

    | 属性名称      | 是否必填       | 属性说明                                                     |
    | :-------------: | :--------------: | :----------------------------------------------------------- |
    | PdServer 地址 | 增量场景下必填 | PD-Server的IP和端口，需要指定对应的协议 <br>如 http://{IP:端口} 或者 https://{IP:端口} <br>IP为您TiDB数据库实例所在服务器的IP，默认的PD Server端口：2379 <br>因此您需要保证此端口处于开放状态 <br>例如：http://127.0.0.1:2379 |
    | 数据库地址    | 必填           | 您TiDB数据库实例所在服务器的IP地址 <br>例如：127.0.0.1       |
    | 端口          | 必填           | 您TiDB数据库实例所在服务器的数据库端口 <br>TiDB默认端口: 4000 <br>因此您需要保证此端口处于开放状态 |
    | 数据库名称    | 必填           | 数据库名称，区分大小写 <br>通过 show databases 命令列出TiDB所有数据库 |
    | 账号          | 非必填         | 您的TiDB数据库用户账号                                       |
    | 密码          | 非必填         | 您的TiDB数据库用户密码                                       |
    | TiKV端口      | 增量场景下必填 | TiKV 默认端口是 20160 <br>该端口是TiDB数据库实例用于TiKV对外提供存储服务 <br>因此您需要保证在数据库实例的服务上此端口处于开放状态 |

  - **数据库权限检查**

    - 权限限制：

      &nbsp;&nbsp;为了保障您的数据库系统安全及其操作完整性，TiDB连接器作为源主要依赖于以下数据库权限，您需要保证您提供的数据库用户账号至少能够对相应的数据库下的相应表具备以下完整权限：
      
      - ***SELECT***：允许用户从表中读取数据的用户权限。
      
    - 权限授权： 如果您的用户不具备以上权限，您可以在DBA用户下参考下列操作进行用户授权
    
      ```sql
          GRANT 
              SELECT
          ON <DATABASE_NAME>.<TABLE_NAME> 
          TO 'user' IDENTIFIED BY 'password';
      ```
  - **增量影响属性**
     - 为避免 TiCDC 的垃圾回收影响事务或增量数据信息提取，推荐执行命令 ```SET GLOBAL tidb_gc_life_time= '24h'``` 将其设置为 24 小时或者一个符合您业务需求的时间长度，设置一个较长的时间将允许您读取更久以前的增量数据，该属性的TiDB数据库默认属性值为：```10m0s``` 。

#### 1.2 **作为目标**

  - **数据源连接属性配置**

    |  属性名称  | 是否必填 |                           属性说明                           |
    | :--------: | :------: | :---------------------------------------------------------- |
    | 数据库地址 |   必填   |      您TiDB数据库实例所在服务器的IP <br>例如：127.0.0.1      |
    |    端口    |   必填   | 您TiDB数据库实例所在服务器的端口 <br>TiDB默认端口: 4000 <br>因此您需要保证此端口处于开放状态 |
    | 数据库名称 |   必填   | 数据库名称，区分大小写 <br>可以通过 show databases 命令列出TiDB所有数据库 |
    |    账号    |  非必填  |                    您的TiDB数据库用户账号                    |
    |    密码    |  非必填  |                    您的TiDB数据库用户密码                    |

  - **数据库权限检查**

    - 权限限制：

      &nbsp; &nbsp;为了保障您的数据库系统安全及其操作完整性，TiDB连接器作为目标主要依赖于以下数据库权限，您需要保证您提供的数据库用户账号对于相应的数据库下的相应表至少能够完整具备以下权限：
    
      - ***CREATE***：允许用户从表中读取数据的用户权限的用户权限，如果不具备此权限则无法完成自动建表的操作。
      - ***DROP***：允许用户删除数据库或表的用户权限，如果不具备此权限则无法完成自动删除表的操作。
      - ***ALTER***：允许用户修改表结构的用户权限，如添加、删除列，如果不具备此权限则无法完成相应的DDL同步。
      - ***INDEX***：允许用户创建或删除索引的用户权限，如果不具备此权限则无法完成自动索引创建。
      - ***INSERT***：允许用户向表中插入新数据的用户权限，如果不具备此权限则相应的插入操作将出现异常。
      - ***UPDATE***：允许用户更新表中的现有数据的用户权限，如果不具备此权限则相应的修改操作将出现异常。
      - ***DELETE***：允许用户删除表中的数据的用户权限，如果不具备此权限则相应的删除操作将出现异常。
    
    - 权限授权： 如果您的用户不具备以上权限，您可以在DBA用户下参考下列操作进行用户授权
    
      ```sql
          GRANT 
            CREATE, DROP, ALTER, INDEX, INSERT, UPDATE, DELETE
          ON <DATABASE_NAME>.<TABLE_NAME> 
          TO 'user' IDENTIFIED BY 'password';
      ```

### ***2 系统架构***

&nbsp;&nbsp; 目前支持的TiDB实例的部署架构主要有以下两种，请确认您的TiDB部署模式是否符合场景需求：

#### ***2.1 TiDB单节点***：

&nbsp;&nbsp; TiDB 单节点是一个部署在单个服务器上的 TiDB 实例。它包含所有关键组件（TiDB Server、TiKV Server 和 PD Server），适用于开发和测试环境，但不具备分布式数据库的高可用性和扩展性特性。

#### ***2.2 TiDB集群***：

&nbsp;&nbsp; TiDB 集群是一个分布式数据库系统，由 TiDB Server（处理 SQL 查询）、TiKV Server（存储数据）和 PD Server（管理元数据和调度）组成，实现高可用性、可扩展性和强一致性。

### ***3 版本支持***

#### ***3.1 仅全量数据同步场景***：

&nbsp;&nbsp; 任意版本的TiDB实例均支持同步全量数据

#### ***3.2 包含增量阶段数据同步的场景***：

&nbsp;&nbsp; 增量数据同步主要依赖于TiCDC实现，详情可参阅 [TiCDC官方文档](https://docs.pingcap.com/tidb/stable/ticdc-overview) ，数据源默认使用 [Tiflow8.1.0](https://github.com/pingcap/tiflow/releases/tag/v8.1.0) 组件用于支持TiDB数据源进行CDC增量数据获取，因此您创建TiDB连接需要支持CDC增量数据变更应该满足以下全部内容：

  - **运行环境**：TapData引擎需要部署运行在 **arm 或 amd** 系统架构环境下 
  - **版本核实**：默认支持的TiDB数据库版本在 **6.0.X ～ 8.1.X** 之间（包含6.0.0以及8.1.9）

  - **端口开放**：TiServer需要开放多个端口与TiKV进行通信，请保证部署TapData引擎的服务器能正常访问数据库服务器中的以下端口：
    - **2379** 端口：该端口主要用于 PD 与 TiKV、TiDB 之间的通信以及对外提供 API 接口。TiKV 和 TiDB 会通过这个端口从 PD 获取集群的配置信息和调度命令。
    - **20162** 端口：该端口用于 TiKV 对外提供存储服务，包括处理 TiDB 的 SQL 请求、读写数据以及和其他 TiKV 节点之间的内部通信（如 Raft 协议的消息）。
  - **网络检查**：TapData引擎与您的TiDB数据库实例应处于同网段下或者能之间连接通，例如：内网环境
  - **组件权限**：进入增量后，TapData会执行Tiflow组件启动TiServer后台进程，因此您需要确保Tiflow组件资源具备操作系统下的 **可读可写可执行 **权限（增量启动后Tiflow组件存在与**{tapData-dir}/run-resource/ti-db/tool**目录下名称为 **cdc**）
  - **库表检查**：TiCDC只复制至少有一个主键有效索引的表。有效索引定义如下：
    - 主键（primary key）是一个有效的索引
    - 如果索引的每一列都明确定义为不可为NULL（NOT NULL），并且索引没有虚拟生成列（虚拟生成列），则唯一索引（unique index）是有效的

#### ***3.3 原理与扩展***：

&nbsp;&nbsp; 增量进行时会开启相应的TiCDC Server后台进程，后台进程由Tiflow组件支持，若您有其他版本TiDB数据库的CDC使用需求，您可以前往 [Github: tiflow](https://github.com/pingcap/tiflow/releases) 官方提供的Tiflow组件库下载支持您TiDB数据库版本的Tiflow组件，用于支持您的CDC数据读取，请将下载后的Tiflow组件库资源在当前操作系统下进行编译后命名成 **cdc** ，并放置于磁盘目录 **{tapData-dir}/run-resource/ti-db/tool** 下（如目录下cdc文件已存在则替换此文件），以下是扩展Tiflow资源配置步骤：

1. 下载您的tiflow资源后解压到你的服务器文件系统，假如解压到了 ***/opt/gocode/src*** 目录下，则这个目录下存在一个 ***/tiflow*** 目录
2. 进入到资源目录（ ***/opt/gocode/src/tiflow*** ）下，执行 ```make``` 命令
3. 在 ***/opt/gocode/src/tiflow*** 目录下找到生成 ***cdc*** 二进制文件
4. 将生成的 ***cdc*** 放置于磁盘目录 ***{tapData-dir}/run-resource/ti-db/tool*** 下
5. 值得注意的是：扩展其他版本的Tiflow插件可能会带来不确定因素或者可能影响您正在运行或运行过的历史任务，请谨慎操作哦

&nbsp;&nbsp; TiCDC Server的读取TiKV增量数据的系统架构，资料来源: [PingCAP](https://docs.pingcap.com/tidb/stable/ticdc-overview)

  <div style="text-align: center;"><img src="https://download.pingcap.com/images/docs/ticdc/cdc-architecture.png" alt="TiCDC的架构" /></div>

  <div style="width:100%;opacity:0.95;font-size: 14px;text-align:center;">图3.3-1 TiCDC的架构</div> 

### ***4 同步性能***   

&nbsp;&nbsp; TiDB数据源的读取性能，测试环境 (MacOS M1 Pro | 8C 16G)

&nbsp;&nbsp; 测试场景： TiDB -> Dummy

&nbsp;&nbsp;全量数据：3000W， 增量压测：500w，6~8k/s

|  | 全量阶段（源） | 增量阶段（源） |
| :----------:| :----------: | :----------: |
| QPS峰值 | 19.5w/s | 10k/s |
| QPS最小值 | 6.2w/s | 1.8k/s |
| QPS均值 | 10w/s | 8k/s |
| 延迟峰值 | - | 3s 812ms |
| 延迟最小值 | - | 3s |
| 延迟均值 | - | 4s94ms |

<span style="color:gray;opacity:0.8;font-size: 8px;padding:0;margin:0;">以上数据决于测试环境且受多种因素影响仅做参考，以实际运行为主</span>

&nbsp;&nbsp; TiDB数据源的写入性能，测试环境 (MacOS M1 | 8C 16G) 

&nbsp;&nbsp; 测试场景：Dummy -> TiDB

|           | 仅插入（目标） | 仅修改（目标） | 仅删除（目标） | 混合写入（目标） |
| :---------: | :--------------: | :--------------: | :--------------: | :----------------: |
| QPS峰值   | 12k/s<br>14k/s<br>16k/s<br>15.6k/s | 3.71k/s<br>5.39k/s<br>7.378k/s<br>7.82k/s |        4.18k/s<br>6.4k/s<br>8.48k/s<br>8.6k/s        |        1.94k/s<br>3.17k/s<br>4.88k/s<br>5.9k/s        |
| QPS最小值 | 9.8k/s<br>9.9k/s<br>10k/s<br>7.8k/s | 2.66k/s<br>3.69k/s<br>6.15k/s <br>5.42k/s |        3.65k/s<br>5.64k/s<br>7.03k/s<br>7.8k/s        |        1.31k/s<br>2.59k/s<br>3.36k/s<br>5.25k/s        |
| QPS均值   | 11k/s<br>12k/s<br>14k/s<br>10.8k/s | 3.10k/s<br>3.12k/s<br>7.1k/s<br>6.66k/s |      4.0k/s<br>5.98k/s<br>8.01k/s<br>8.42k/s      |        1.79k/s<br>2.98k/s<br>3.52k/s<br>5.43k/s        |

<div style="width: 100%;height: 100%;margin-bottom: 10px;">
    <div style="text-align: right;padding-right: 5px;padding-top: 10px;width: 100%;padding-bottom: 10px;">
        <div style="display: flex;padding-left: 90%;width: 100%;"><div style="width: 16px;height: 8px;background-color: #5470c6;margin-right: 5px;margin-bottom: 2px;"></div><font style="text-align: left;font-size: 7px;color: gray;line-height: 1;">单线程</font></div>
        <div style="display: flex;padding-left: 90%;width: 100%;"><div style="width: 16px;height: 8px;background-color: #3ba272;margin-right: 5px;margin-bottom: 2px;"></div><font style="text-align: left;font-size: 7px;color: gray;line-height: 1;">多线程 2T</font></div>
        <div style="display: flex;padding-left: 90%;width: 100%;"><div style="width: 16px;height: 8px;background-color: #fc8452;margin-right: 5px;margin-bottom: 2px;"></div><font style="text-align: left;font-size: 7px;color: gray;line-height: 1;">多线程 4T</font></div>
        <div style="display: flex;padding-left: 90%;width: 100%;"><div style="width: 16px;height: 8px;background-color: #ee6666;margin-right: 5px;margin-bottom: 2px;"></div><font style="text-align: left;font-size: 7px;color: gray;line-height: 1;">多线程 8T</font></div>
    </div>
    <div style="display: flex;width: 100%;height: 100%;">
        <div class="x-axis" style="padding-top: 20px;display: flex;flex-direction: column;justify-content: space-between;width:auto;height: 100%;">
             <div style="padding-left:5px;padding-right:5px;margin-top: 10px;font-size: 14px;">插入</div>
             <div style="padding-left:5px;padding-right:5px;margin-top: 45px;font-size: 14px;">修改</div>
             <div style="padding-left:5px;padding-right:5px;margin-top: 45px;font-size: 14px;">删除</div>
             <div style="padding-left:5px;padding-right:5px;margin-top: 50px;font-size: 14px;">混合</div>
        </div> 
        <div style="text-align: center;width: 100%;">
            <div class="chart-container" style="height: 100%;width: 100%;border-left: 1px solid gray;border-bottom: 1px solid gray;padding: 0;padding-top: 20px;">
                <div class="bar-group" style="margin-bottom: 30px;">
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 42%;margin-bottom: 5px;background-color: #5470c6;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">1.2w/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 56%;margin-bottom: 5px;background-color: #3ba272;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">1.4w/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 64%;margin-bottom: 5px;background-color: #fc8452;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">1.6w/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 62.4%;margin-bottom: 5px;background-color: #ee6666;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">1.56w/s</font></div>
                </div>
                <div class="bar-group" style="margin-bottom: 30px;">
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 14.84%;margin-bottom: 5px;background-color: #5470c6;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">3.71k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 21.56%;margin-bottom: 5px;background-color: #3ba272;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">5.39k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 29.512%;margin-bottom: 5px;background-color: #fc8452;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">7.378k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 31.28%;margin-bottom: 5px;background-color: #ee6666;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">7.82k/s</font></div>
                </div>
                <div class="bar-group" style="margin-bottom: 30px;">
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 16.72%;margin-bottom: 5px;background-color: #5470c6;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">4.18k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 25.6%;margin-bottom: 5px;background-color: #3ba272;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">6.4k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 33.92%;margin-bottom: 5px;background-color: #fc8452;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">8.48k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 34.4%;margin-bottom: 5px;background-color: #ee6666;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">8.6k/s</font></div>
                </div>
                <div class="bar-group" style="margin-bottom: 5px;">
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 7.76%;margin-bottom: 5px;background-color: #5470c6;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">1.94k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 12.68%;margin-bottom: 5px;background-color: #3ba272;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">3.17k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 19.52%;margin-bottom: 5px;background-color: #fc8452;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">4.88k/s</font></div>
                    <div style="display: flex;width: 100%;"><div style="height: 10px;width: 23.6%;margin-bottom: 5px;background-color: #ee6666;"></div><font style="font-size: 12px;color: gray;margin-left: 0.5%;line-height: 1;">5.9k/s</font></div>
                </div>
              </div>
            <div class="x-axis" style="display: flex;justify-content: space-around;margin-top: 2px;font-size: 6px;color: gray;">
                <div style="width: 5%;text-align: right;">1k</div>
                <div style="width: 5%;text-align: right;">2k</div>
                <div style="width: 5%;text-align: right;">3k</div>
                <div style="width: 5%;text-align: right;">4k</div>
                <div style="width: 5%;text-align: right;">5k</div>
                <div style="width: 5%;text-align: right;">6k</div>
                <div style="width: 5%;text-align: right;">7k</div>
                <div style="width: 5%;text-align: right;">8k</div>
                <div style="width: 5%;text-align: right;">9k</div>
                <div style="font-weight: bold;text-align: right;font-size: 8px;margin-top: 0;width: 5%">1w</div>
                <div style="width: 5%;text-align: right;">11k</div>
                <div style="width: 5%;text-align: right;">12k</div>
                <div style="width: 5%;text-align: right;">13k</div>
                <div style="width: 5%;text-align: right;">14k</div>
                <div style="width: 5%;text-align: right;">15k</div>
                <div style="width: 5%;text-align: right;">16k</div>
                <div style="width: 5%;text-align: right;">17k</div>
                <div style="width: 5%;text-align: right;">18k</div>
                <div style="width: 5%;text-align: right;">19k</div>
                <div style="font-weight: bold;font-size: 8px;margin-top: 0;width: 5%;text-align: right;">2w</div>
                <div style="width: 5%;text-align: right;">21k</div>
                <div style="width: 5%;text-align: right;">22k</div>
                <div style="width: 5%;text-align: right;">23k</div>
                <div style="width: 5%;text-align: right;">24k</div>
                <div style="width: 5%;text-align: right;">25k</div>
            </div> 
        </div>
    </div>
</div>


<span style="color:gray;opacity:0.8;font-size: 8px;padding:0;margin:0;">以上数据取决于测试环境且受多种因素影响仅做参考，以实际运行为主</span>

### ***5 数据类型***

&nbsp;&nbsp;**全量**支持的数据类型同步包含以下：

&nbsp;&nbsp;&nbsp;&nbsp;***char***, varchar, ***tinytext***, text, ***mediumtext***, longtext, ***json***, binary, ***varbinary***, tinyblob, ***blob***, mediumblob, ***longblob***, bit, ***tinyint***, tinyint unsigned, ***smallint***, smallint unsigned, ***mediumint***, mediumint unsigned, ***int***, int unsigned, ***bigint***, bigint unsigned, ***decimal***, decimal unsigned, ***float***, double, ***double unsigned***, date, ***time***, datetime, ***timestamp***, year, ***enum***, set, ***INTEGER***, BOOLEAN, ***TEXT***

&nbsp;&nbsp;**增量**支持的数据类型同步包含以下：

&nbsp;&nbsp;&nbsp;&nbsp;Boolean, ***Float***, Double, ***Decimal***, Char, ***Varchar***, Binary, ***Varbinary***, Tinytext, ***Text***, Mediumtext, ***Longtext***, Tinyblob, ***Blob***, CLOB, ***Bit***, Mediumblob, ***Longblob***, Date, ***Datetime***, Timestamp, ***Time***, Year, ***Enum***, Set, ***Bit***, JSON, ***tinyinttinyint unsigned***, smallint, ***smallint unsigned***, mediumint, ***mediumint unsigned***, int, ***int unsigned***, bigint, ***bigint unsigned***, DECIMAL, ***INTEGER***, REAL

> [!注意事项]
>
> 暂不支持图片类型

### ***6 对源库的压力或影响***

&nbsp;&nbsp;测试环境 （MacOS M1 | 8C 16G）

|                    | CPU占用 | 内存消耗  |    网络消耗     |
| :----------------: | :-----: | :-------: | :-------------: |
| TiDB测试前机器状态 | 20%~25% | 12GB~13GB | 100KB/s~300KB/s |
| 全量测试后机器状态 | 45%~55% |  14.17GB  |  25MB/s~65MB/s  |
| 增量测试后机器状态 | 50%~55% | 14GB~15GB |  10MB/s~12MB/s  |
| 数据写入后机器状态 | 50%~55% |  14.14GB  |  10MB/s~20MB/s  |

<span style="color:gray;opacity:0.8;font-size: 8px;padding:0;margin:0;">以上数据取决于测试环境且受多种因素影响仅做参考，以实际运行为主</span>



<div style="text-align: center;width: 100%;height: 1px;background-color: rgba(203,203,203,0.9);"></div>
<div style="opacity:0.95;font-size: 14px;text-align:center;"><font style="color: gray">TapData</font> & <font style="color:gray;">TiDB</font></div>
<div style="opacity:0.6;font-size: 10px;text-align:center;">基于CDC的实时数据平台，用于异构数据库复制、实时数据集成或构建实时数据仓库</div>
<div style="opacity:0;font-size: 10px;text-align:center;">TiDB connector doc design by Gavin</div>
