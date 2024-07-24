
<div style="text-align: right; font-weight: normal;color: gray;"><img style="width: 20px;height:20px;object-fit: contain;flex: 1;" src="https://tapdata.io/assets-global.website-files.com/61acd54a60fc632b98cbac1a/620d4543dd51ea0fcb7b237c_Favicon2.png" alt="TiCDC的架构" /> & <img src="https://img1.tidb.net/favicons/favicon-32x32.png" style="width: 20px;height:20px;object-fit: contain;flex: 1;"/></div>

## **Connection Configuration Help**

&nbsp;&nbsp;&nbsp;Please follow the following instructions to ensure successful addition and use of TiDB database in TapData, as well as successful deployment of TiKV and TiPD services
    
### ***1 Synchronization type***

&nbsp;&nbsp; The TiDB data source currently covers multiple data synchronization scenarios and supports four types of synchronization:

 - **Only full quantity (Source)**: Full data synchronization, database/table level data migration
 - **Only incremental (Source)**: Incremental data synchronization, DMZ/DDL synchronization
 - **Full quantity + incremental (Source)**: Full data synchronization, database/table level data migration, incremental data synchronization, DMZ/DDL synchronization
 - **Data writing (Target)**: Create tables, delete tables, write data (insert/modify/delete), synchronize DDL

#### 1.1 **As Source**

  - **Data source connection attribute configuration**

    | Attribute Name | Is it mandatory | Attribute Description                                |
    | :-------------: | :--------------: | :----------------------------------------------------------- |
    | PdServer Adds | Required in incremental scenarios | The IP and port of PD Server need to be specified with the corresponding protocol <br>Such as http://{IP: port} or https://{IP: port}<br>The IP is the IP of the server where your TiDB database instance is located, default PD Server port: 2379 <br>Therefore, you need to ensure that this port is in an open state<br>Such as: http://127.0.0.1:2379 |
    | Database address | Required   | The IP address of the server where your TiDB database instance is located <br>Such as: 127.0.0.1 |
    | Port       | Required   | The database port of the server where your TiDB database instance is located <br>TiDB default port: 4000 <br>Therefore, you need to ensure that this port is in an open state |
    | Database Name | Required   | Database name, case sensitive <br>List all TiDB databases using the 'show databases' command |
    | Username  | Not Required | Your TiDB database user account        |
    | Password   | Not Required | Your TiDB database user password       |
    | TiKV Port  | Required in incremental scenarios | The default port for TiKV is 20160 <br>This port is a TiDB database instance used for TiKV to provide external storage services <br>Therefore, you need to ensure that this port is open on the database instance's service |

  - **Database permission check**

    - Permission restrictions:

      &nbsp;&nbsp;In order to ensure the security and operational integrity of your database system, the TiDB connector as a source mainly relies on the following database permissions. You need to ensure that the database user account you provide has at least the following complete permissions for the corresponding tables in the corresponding database:
      
      - ***SELECT***：Allow user permissions to read data from the table.
      
    - Permission authorization:  If your user does not have the above permissions, you can refer to the following operations for user authorization under DBA user
    
      ```sql
          GRANT 
              SELECT
          ON <DATABASE_NAME>.<TABLE_NAME> 
          TO 'user' IDENTIFIED BY 'password';
      ```
  - **Incremental impact content**
     - To avoid the impact of TiCDC's garbage collection on transaction or incremental data information extraction, it is recommended to execute command ```SET GLOBAL tidb_gc_life_time= '24h'``` to set it to 24 hours or a time length that meets your business needs. Setting a longer time will allow you to read incremental data from a longer period of time. The default attribute value for this attribute in the TiDB database is: ```10m0s``` 


#### 1.2 **As Target**

  - **Data source connection attribute configuration**

    | Attribute Name | Is it mandatory | Attribute Description                                        |
    | :------------: | :-------------: | :----------------------------------------------------------- |
    | Database Attr  |    Required     | The IP address of the server where your TiDB database instance is located <br>Such as: 127.0.0.1 |
    |      Port      |    Required     | The port of the server where your TiDB database instance is located <br>TiDB default port: 4000 <br>Therefore, you need to ensure that this port is in an open state |
    | Database Name  |    Required     | Database name, case sensitive <br>You can list all TiDB databases using the 'show databases' command |
    |    Username    |  Not Required   | Your TiDB database user account                              |
    |    Password    |  Not Required   | Your TiDB database user password                             |

  - **Database permission check**

    - Permission restrictions:

      &nbsp; In order to ensure the security and operational integrity of your database system, TiDB connector mainly relies on the following database permissions as the target. You need to ensure that the database user account you provide has at least the following permissions for the corresponding tables in the corresponding database:
    
      - ***CREATE***：The user permission that allows users to read data from a table. Without this permission, automatic table creation cannot be completed.
      - ***DROP***：Allow users to delete database or table permissions. Without this permission, automatic table deletion cannot be completed.
      - ***ALTER***：Allow users to modify table structures with user permissions, such as adding and deleting columns. Without this permission, corresponding DDL synchronization cannot be completed.
      - ***INDEX***：Allow users to create or delete indexes. Without this permission, automatic index creation cannot be completed.
      - ***INSERT***：Allow users to insert new data into the table with user permissions. Without this permission, the corresponding insertion operation will encounter exceptions.
      - ***UPDATE***：Allow users to update existing data in the table with user permissions. Without this permission, the corresponding modification operation will encounter exceptions.
      - ***DELETE***：Allow users to delete data in the table with user permissions. If they do not have this permission, the corresponding deletion operation will be abnormal.
    
    - Permission authorization: if your user does not have the above permissions, you can refer to the following operations for user authorization under DBA user
    
      ```sql
          GRANT 
            CREATE, DROP, ALTER, INDEX, INSERT, UPDATE, DELETE
          ON <DATABASE_NAME>.<TABLE_NAME> 
          TO 'user' IDENTIFIED BY 'password';
      ```

### ***2 System Architecture***

&nbsp;&nbsp; There are currently two main deployment architectures supported for TiDB instances. Please confirm if your TiDB deployment mode meets the requirements of the scenario:

#### ***2.1 TiDB Single node***：

&nbsp;&nbsp; TiDB single node is a TiDB instance deployed on a single server. It contains all key components (TiDB Server, TiKV Server, and PD Server), suitable for development and testing environments, but does not have the high availability and scalability features of distributed databases.

#### ***2.2 TiDB Colony***：

&nbsp;&nbsp; TiDB cluster is a distributed database system consisting of TiDB Server (processing SQL queries), TiKV Server (storing data), and PD Server (managing metadata and scheduling), achieving high availability, scalability, and strong consistency.

### ***3 Version support***

#### ***3.1 Only in the scenario of full data synchronization***:

&nbsp;&nbsp; Any version of TiDB instance supports synchronizing full data

#### ***3.2 Scenarios involving incremental phase data synchronization***:

&nbsp;&nbsp; ncremental data synchronization mainly relies on TiCDC implementation. For details, please refer to [TiCDC Official documents](https://docs.pingcap.com/tidb/stable/ticdc-overview) , The connector defaults to using [Tiflow8.1.0](https://github.com/pingcap/tiflow/releases/tag/v8.1.0) ,  this component is used to support CDC incremental data acquisition from TiDB data sources. Therefore, when creating a TiDB connection, you need to support CDC incremental data changes, which should meet all of the following requirements:

  - **Operating environment**: The TapData engine needs to be deployed and run on **arm or amd**  the system architecture environment
  - **Version verification**: The default supported TiDB database versions are between **6.0.X** and **8.1.X** (including 6.0.0 and 8.1.9)

  - **Port open**: TiServer needs to open multiple ports to communicate with TiKV. Please ensure that the server deployed with TapData engine can access the following ports in the database server normally:
    - Port **2379** : This port is mainly used for communication between PD and TiKV, TiDB, as well as providing API interfaces to the outside world. TiKV and TiDB will obtain cluster configuration information and scheduling commands from PD through this port.
    - Port **20162** : This port is used for TiKV to provide storage services externally, including processing SQL requests from TiDB, reading and writing data, and internal communication with other TiKV nodes (such as Raft protocol messages).
  - **Network Check**：The TapData engine and your TiDB database instance should be in the same network segment or able to connect to each other, for example, in an internal network environment
  - **Component permissions**: After entering the increment, TapData will execute the Tiflow component to start the TiServer background process, so you need to ensure that the Tiflow component resources have **readable, writable, and executable ** permissions under the operating system (after incremental startup, the Tiflow component exists in the **{tapData-dir}/run-resource/ti-db/tool** directory under the name **cdc** )
  - **Inventory check**: TiCDC only copies tables that have at least one valid primary key index. The definition of effective index is as follows:
    - The primary key is a valid index
    - If each column of an index is explicitly defined as NOT NULL and there are no virtual generated columns in the index, then a unique index is valid

#### ***3.3 Principles and extensions***：

&nbsp;&nbsp; When the increment is in progress, the corresponding TiCDC Server background process will be opened, which is supported by the Tiflow component. If you have CDC usage requirements for other versions of TiDB databases, you can go to the Tiflow component library provided by [Github: tiflow](https://github.com/pingcap/tiflow/releases)  to download the Tiflow component that supports your TiDB database version to support your CDC data reading. Please compile the downloaded Tiflow component library resources in the current operating system, name them as **cdc**, and place them in disk directory **{tapData-dir}/run-resource/ti-db/tool**  (if the cdc file already exists in the directory, replace this file). The following are the steps to expand Tiflow resource configuration:

1. After downloading your tiflow resources, unzip them to your server file system. If you unzip them to the ***/opt/ocode/src** directory, there will be a ***/tiflow*** directory in this directory
2. Go to the resource directory (***/opt/gocode/src/Tiflow***) and execute the ```make``` command
3. Find the generated ***cdc*** binary file in the ***/opt/ocode/src/tflow*** directory
4. Place the generated ***cdc*** in the disk directory ***{tapData-dir}/run-resource/ti-db/tool*** 
5. It is worth noting that extending other versions of Tiflow plugins may bring uncertain factors or may affect the historical tasks you are currently running or have run. Please handle with caution

&nbsp;&nbsp; System architecture for reading TiKV incremental data from TiCDC Server, source of information: [PingCAP](https://docs.pingcap.com/tidb/stable/ticdc-overview)

  <div style="text-align: center;"><img src="https://download.pingcap.com/images/docs/ticdc/cdc-architecture.png" alt="TiCDC的架构" /></div>

  <div style="width:100%;opacity:0.95;font-size: 14px;text-align:center;">Chart 3.3-1 Architecture of TiPDC</div> 

### ***4 Synchronization performance***   

&nbsp;&nbsp;Reading performance of TiDB data source, testing environment (MacOS M1 Pro | 8C 16G)

&nbsp;&nbsp; Test scenario: TiDB -> Dummy

&nbsp;&nbsp;Full data: 3000W,  Incremental pressure measurement: 500w, 6~8k/s

|  | Full stage (source) | Incremental Stage (Source) |
| :----------:| :----------: | :----------: |
| Max value of QPS | 19.5w/s | 10k/s |
| Min value of QPS | 6.2w/s | 1.8k/s |
| Average value of QPS | 10w/s | 8k/s |
| Delay peak | - | 3s 812ms |
| Minimum delay value | - | 3s |
| Delay mean | - | 4s94ms |

<span style="color:gray;opacity:0.8;font-size: 8px;padding:0;margin:0;">The above data is based on the testing environment and is subject to various factors for reference only. It is mainly based on actual operation</span>

&nbsp;&nbsp; Write performance of TiDB data source, testing environment (MacOS M1 | 8C 16G) 

&nbsp;&nbsp; Test scenario: Dummy -> TiDB

|           | Insert only (target) | Only modify (target) | Only delete (target) | Mixed Write (Target) |
| :---------: | :--------------: | :--------------: | :--------------: | :----------------: |
| QPS peak value | 12k/s<br>14k/s<br>16k/s<br>15.6k/s | 3.71k/s<br>5.39k/s<br>7.378k/s<br>7.82k/s |        4.18k/s<br>6.4k/s<br>8.48k/s<br>8.6k/s        |        1.94k/s<br>3.17k/s<br>4.88k/s<br>5.9k/s        |
| QPS minimum value | 9.8k/s<br>9.9k/s<br>10k/s<br>7.8k/s | 2.66k/s<br>3.69k/s<br>6.15k/s <br>5.42k/s |        3.65k/s<br>5.64k/s<br>7.03k/s<br>7.8k/s        |        1.31k/s<br>2.59k/s<br>3.36k/s<br>5.25k/s        |
| QPS mean | 11k/s<br>12k/s<br>14k/s<br>10.8k/s | 3.10k/s<br>3.12k/s<br>7.1k/s<br>6.66k/s |      4.0k/s<br>5.98k/s<br>8.01k/s<br>8.42k/s      |        1.79k/s<br>2.98k/s<br>3.52k/s<br>5.43k/s        |

<div style="width: 100%;height: 100%;margin-bottom: 10px;">
    <div style="text-align: right;padding-right: 5px;padding-top: 10px;width: 100%;padding-bottom: 10px;">
        <div style="display: flex;padding-left: 90%;width: 100%;"><div style="width: 16px;height: 8px;background-color: #5470c6;margin-right: 5px;margin-bottom: 2px;"></div><font style="text-align: left;font-size: 7px;color: gray;line-height: 1;">Single threaded</font></div>
        <div style="display: flex;padding-left: 90%;width: 100%;"><div style="width: 16px;height: 8px;background-color: #3ba272;margin-right: 5px;margin-bottom: 2px;"></div><font style="text-align: left;font-size: 7px;color: gray;line-height: 1;">Multi threading 2T</font></div>
        <div style="display: flex;padding-left: 90%;width: 100%;"><div style="width: 16px;height: 8px;background-color: #fc8452;margin-right: 5px;margin-bottom: 2px;"></div><font style="text-align: left;font-size: 7px;color: gray;line-height: 1;">Multi threading 4T</font></div>
        <div style="display: flex;padding-left: 90%;width: 100%;"><div style="width: 16px;height: 8px;background-color: #ee6666;margin-right: 5px;margin-bottom: 2px;"></div><font style="text-align: left;font-size: 7px;color: gray;line-height: 1;">Multi threading 8T</font></div>
    </div>
    <div style="display: flex;width: 100%;height: 100%;">
        <div class="x-axis" style="padding-top: 20px;display: flex;flex-direction: column;justify-content: space-between;width:auto;height: 100%;">
             <div style="padding-left:5px;padding-right:5px;margin-top: 10px;font-size: 14px;">Insert Op</div>
             <div style="padding-left:5px;padding-right:5px;margin-top: 45px;font-size: 14px;">Update Op</div>
             <div style="padding-left:5px;padding-right:5px;margin-top: 45px;font-size: 14px;">Del Op</div>
             <div style="padding-left:5px;padding-right:5px;margin-top: 50px;font-size: 14px;">Admix Op</div>
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




<span style="color:gray;opacity:0.8;font-size: 8px;padding:0;margin:0;">The above data depends on the testing environment and is subject to various factors for reference only. It is mainly based on actual operation</span>

### ***5 Data type**

&nbsp;&nbsp;The **fully** supported data type synchronization includes the following:

&nbsp;&nbsp;&nbsp;&nbsp;***char***, varchar, ***tinytext***, text, ***mediumtext***, longtext, ***json***, binary, ***varbinary***, tinyblob, ***blob***, mediumblob, ***longblob***, bit, ***tinyint***, tinyint unsigned, ***smallint***, smallint unsigned, ***mediumint***, mediumint unsigned, ***int***, int unsigned, ***bigint***, bigint unsigned, ***decimal***, decimal unsigned, ***float***, double, ***double unsigned***, date, ***time***, datetime, ***timestamp***, year, ***enum***, set, ***INTEGER***, BOOLEAN, ***TEXT***

&nbsp;&nbsp;**Incremental** supports synchronization of the following data types:

&nbsp;&nbsp;&nbsp;&nbsp;Boolean, ***Float***, Double, ***Decimal***, Char, ***Varchar***, Binary, ***Varbinary***, Tinytext, ***Text***, Mediumtext, ***Longtext***, Tinyblob, ***Blob***, CLOB, ***Bit***, Mediumblob, ***Longblob***, Date, ***Datetime***, Timestamp, ***Time***, Year, ***Enum***, Set, ***Bit***, JSON, ***tinyinttinyint unsigned***, smallint, ***smallint unsigned***, mediumint, ***mediumint unsigned***, int, ***int unsigned***, bigint, ***bigint unsigned***, DECIMAL, ***INTEGER***, REAL

> [!Matters needing attention]
>
> Image types are not currently supported

### ***6 Pressure or impact on the source library***

&nbsp;&nbsp;Testing environment （MacOS M1 | 8C 16G）

|                                          | CPU usage | Memory consumption | Network consumption |
| :--------------------------------------: | :-------: | :----------------: | :-----------------: |
|    Machine status before TiDB testing    |  20%~25%  |     12GB~13GB      |   100KB/s~300KB/s   |
|    Machine status after full testing     |  45%~55%  |      14.17GB       |    25MB/s~65MB/s    |
| Machine status after incremental testing |  50%~55%  |     14GB~15GB      |    10MB/s~12MB/s    |
|    Machine status after data writing     |  50%~55%  |      14.14GB       |    10MB/s~20MB/s    |

<span style="color:gray;opacity:0.8;font-size: 8px;padding:0;margin:0;">The above data depends on the testing environment and is subject to various factors for reference only. It is mainly based on actual operation</span>



<div style="text-align: center;width: 100%;height: 1px;background-color: rgba(203,203,203,0.9);"></div>
<div style="opacity:0.95;font-size: 14px;text-align:center;"><font style="color: gray">TapData</font> & <font style="color:gray;">TiDB</font></div>
<div style="opacity:0.6;font-size: 10px;text-align:center;">A real-time data platform based on CDC, used for heterogeneous database replication, real-time data integration, or building real-time data warehouses</div>
<div style="opacity:0;font-size: 10px;text-align:center;">TiDB connector doc design by Gavin</div>
