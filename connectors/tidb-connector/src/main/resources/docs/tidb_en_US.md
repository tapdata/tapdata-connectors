# TiDB
TiDB is an open-source distributed relational database designed and developed by PingCAP. It is a converged distributed database product that supports both online transaction processing and online analytical processing. After completing the Agent deployment, you can follow the tutorial in this article to add TiDB data sources in TapData, which can be used as **source** or **target library** to build data pipelines in the future
## Supported versions

* **Full data synchronization**: All versions
* **Incremental data synchronization**：6.0.0 ～ 8.1.9

## Support synchronous operations

- DML：INSERT、UPDATE、DELETE
- DDL：ADD COLUMN、CHANGE COLUMN、AlTER COLUMN、DROP COLUMN

## Data type

&nbsp;&nbsp;**Full data synchronization**The supported data types for synchronization include the following:

&nbsp;&nbsp;&nbsp;&nbsp;***char***, varchar, ***tinytext***, text, ***mediumtext***, longtext, ***json***, binary, ***varbinary***, tinyblob, ***blob***, mediumblob, ***longblob***, bit, ***tinyint***, tinyint unsigned, ***smallint***, smallint unsigned, ***mediumint***, mediumint unsigned, ***int***, int unsigned, ***bigint***, bigint unsigned, ***decimal***, decimal unsigned, ***float***, double, ***double unsigned***, date, ***time***, datetime, ***timestamp***, year, ***enum***, set, ***INTEGER***, BOOLEAN, ***TEXT***

&nbsp;&nbsp;**Incremental data synchronization**The supported data types for synchronization include the following:

&nbsp;&nbsp;&nbsp;&nbsp;Boolean, ***Float***, Double, ***Decimal***, Char, ***Varchar***, Binary, ***Varbinary***, Tinytext, ***Text***, Mediumtext, ***Longtext***, Tinyblob, ***Blob***, CLOB, ***Bit***, Mediumblob, ***Longblob***, Date, ***Datetime***, Timestamp, ***Time***, Year, ***Enum***, Set, ***Bit***, JSON, ***tinyinttinyint unsigned***, smallint, ***smallint unsigned***, mediumint, ***mediumint unsigned***, int, ***int unsigned***, bigint, ***bigint unsigned***, DECIMAL, ***INTEGER***, REAL

> [!matters needing attention]
>
> Image types are not currently supported

## Matters needing attention

- The TapData engine and your TiDB database instance should be in the same network segment or able to connect to each other, for example, in an internal network environment
- When using TiDB as a source to achieve incremental data synchronization scenarios, you also need to check the following information:

  - The table to be synchronized must have a primary key or a unique index, where the value of the column to which the unique index belongs cannot be NULL or a virtual column.
  - To avoid the impact of TiCDC's garbage collection on transaction or incremental data information extraction, it is recommended to execute command ```SET GLOBAL tidb_gc_life_time= '24h'``` to set it to 24 hours or a time length that meets your business needs. Setting a longer time will allow you to read incremental data from a longer period of time. The default TiDB database attribute value for this attribute is: ```10m0s```
  - The TiDB connector supports the CDC runtime environment: The TapData engine needs to be deployed and run on the **arm or amd** system architecture.
  
## Incremental synchronization principle

To further simplify the usage process, TapData's TiDB connector integrates  [Component TiFlow](https://github.com/pingcap/tiflow/releases/tag/v8.1.0) (version 8.1.0), It can be parsed into ordered row level change data based on data change logs. For more principles and concept introductions, please refer to [Overview TiCDC](https://docs.pingcap.com/zh/tidb/stable/ticdc-overview) 

## <span id="prerequisite">Preparation</span>

1. Log in to the TiDB database and execute the following command format to create an account for data synchronization/development tasks

   ```sql
   CREATE USER 'username'@'host' IDENTIFIED BY 'password';
   ```

   * **username**：user name
   * **host**：Allow the account to log in to any host, with a percentage (%) indicating permission for any host
   * **password**：password

   Example: Create an account named username that allows login from any host

   ```sql
   CREATE USER 'username'@'%' IDENTIFIED BY 'your_password';
   ```

2. Grant permissions to the newly created account

   * As a source database

     ```sql
     -- The required permissions for full and incremental synchronization are as follows
     GRANT SELECT ON *.* TO 'username' IDENTIFIED BY 'password';
     ```

   * As a target database

     ```sql
     -- Grant specified database permissions
     GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, CREATE, DROP ON database_name.* TO 'username';
     
     -- Grant all database permissions
     GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, CREATE, DROP ON *.* TO 'username';
     ```

* **database_name**：database <span id="ticdc">name</span>
* **username**：user name



## Add connector

1. Log in to the TapData platform

2. On the left navigation bar, click **Connection Management**

3. Click on **Create** on the right side of the page

4. In the pop-up dialog box, search for and select **TiDB**

5. On the redirected page, fill in the TiDB connection information according to the following instructions

   * **Connection information settings**
      * **Connection Name**: Fill in a unique name with business significance
      
      * **Connection Type**：Support using TiDB database as source or target
      
      * **PD Server Addr**：Fill in the connection address and port of PDServer. The default port number is **2379**. This parameter is only filled in when it is used as a source library and incremental data synchronization is required.
      
      * **Database address**：Database connection address.
      
      * **Port**：The default service port for the database is **4000**。
      
      * **Database Name**：Database name (case sensitive), which means one connection corresponds to one database. If there are multiple databases, multiple data connections need to be created.
      
      * **Account**/**Password**：The account and password for the database, as well as the methods for creating and authorizing the account, can be found in [Prerequisite](#prerequisite) 。
      
      * **TiKV Port**：As the storage layer of TiDB, it provides data persistence, read-write services, and statistical information data recording functions. The default port is **20160**, this parameter is only filled in when used as a source library and incremental data synchronization is required.
      
   * **Advanced setting**
   
      * **Other connection string parameters**：Extra connection parameters, default to empty
      
      * **Time zone of time type**：The default is the time zone used by the database, or you can manually specify it according to business needs
      
      * **Shared mining**：Digging incremental logs from the source library allows multiple tasks to share incremental logs from the source library, avoiding duplicate reads and minimizing the pressure of incremental synchronization on the source library. After enabling this feature, it is also necessary to select an external storage to store incremental log information. This parameter only needs to be filled in when used as the source library
      
      * **Include table**：The default is **All**. You can also choose to customize and fill in the included tables, separated by English commas (,) between multiple tables
      
      * **Exclusion Table**：After turning on this switch, you can set the tables to be excluded, and separate multiple tables with English commas (,)
      
      * **Agent Settings**：The default is **platform automatic allocation**, you can also manually specify it
      
      * **Model loading time**：When the number of models in the data source is less than 10000, refresh the model information once per hour; If the model data exceeds 10000, refresh the model information at the time you specify every day
      
      * **Activate heartbeat meter**：When the connection type is selected as **Source and Target**, **Source**, this switch can be turned on. Tapdata will create a heartbeat table named **_tapdata_heartbeat_table** in the source library and update its data every 10 seconds (the database account needs to have relevant permissions) for data source connection and task health monitoring
      
      
   * **SSL Settings**：Choosing whether to enable SSL connection to the data source can further enhance data security. After enabling this feature, you also need to upload CA files, client certificates, and fill in the client password with the key. For more information, see [Generate self signed certificate](https://docs.pingcap.com/zh/tidb/stable/generate-self-signed-certificates) 

6. Click **Connect Test**, and after passing the test, click **Save**.

   > If the connection test fails, please follow the prompts on the page to fix it.

  

## Common problem

- **Q**：Which ports need to ensure communication for data synchronization?

  ***A***：Full synchronization requires ensuring communication between the database ports of the TiDB cluster and TapData. If incremental data synchronization is required, the following ports need to be opened:

  * **2379** Port：2379 is the default port for PD Server, used for communication between PD and TiKV, TiDB, and providing API interfaces to the outside world. TiKV and TiDB will obtain cluster configuration information and scheduling commands from PD through this port. If you specify another port number, you need to open it.

  - **20160** Port：20160 is the default port for TiKV Server, used for providing storage services to external TiKV, including processing SQL requests for TiDB, reading and writing data, and internal communication with other TiKV nodes (such as Raft protocol messages). If you specify another port number, you need to open this port number.

- **Q**：Do TapData have any requirements for the deployment architecture of TiDB?

  ***A***：Both standalone and cluster deployment architectures of TiDB are supported.

- **Q**：What should I do if my TiDB version is not within the range of 6.0.0 to 8.1.9 and I need to perform incremental data synchronization?

  ***A***：TapData's TiDB connector integrates TiCDC, which can parse data change logs into ordered row level change data. If your database is outside of the supported version, you can go to [Github: tiflow](https://github.com/pingcap/tiflow/releases) to download the Tiflow component that supports the corresponding version, and then follow the steps below to compile the CDC tool yourself:

  > Expanding other versions of Tiflow components may bring uncertain factors or affect running tasks, please handle with caution.

  1. Extract the downloaded file and then enter the extracted directory to execute the `make` command for compilation.

  2. Find the generated **cdc** binary file and place it in the **{tapData-dir}/run-resource/ti-db/tool** directory of the machine to which the TapData engine belongs (replace it if available).

  3. Grant read, write, and execute permissions to files in this directory using the chmod command.
 


<div style="text-align: center;width: 100%;height: 1px;background-color: rgba(203,203,203,0.9);"></div>
<div style="opacity:0.95;font-size: 14px;text-align:center;"><font style="color: gray">TapData</font> & <font style="color:gray;">TiDB</font></div>
<div style="opacity:0.6;font-size: 10px;text-align:center;">A real-time data platform based on CDC, used for heterogeneous database replication, real-time data integration, or building real-time data warehouses</div>
<div style="opacity:0;font-size: 10px;text-align:center;">TiDB connector doc design by Gavin</div>
