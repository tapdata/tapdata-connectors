## **连接配置帮助**
### ClickHouse安装说明
请遵循以下说明以确保在 Tapdata 中成功添加和使用ClickHouse数据库。
### 支持版本
Clickhouse 20.x, 21.x，22.x,23.x
### 功能限制
- 目前clickhouse 作为源时，仅支持轮训字段增量同步方式
### 先决条件
#### 作为源
- 登录clickhouse 数据库，执行下述格式的命令，创建用于数据同步/开发任务的账号。
   ```sql
    CREATE user tapdata IDENTIFIED WITH plaintext_password BY 'mypassword'
   ```
- 为创建的用户授予权限
   ```sql
    grant select on default.* to user 
   ```
- user为复制/转换用户的用户名
- password为用户的密码
#### 作为目标
- 方式一：
以最高权限用户(default)进行创建用于数据复制/转换的用户
- 在user.xml `<`quota`>`default`<`/quota`>` 添加如下配置，否者无法创建用户
``` xml
<access_management>1</access_management>
<named_collection_control>1</named_collection_control>
<show_named_collections>1</show_named_collections>
<show_named_collections_secrets>1</show_named_collections_secrets>
```
- 创建用于数据复制、转换的用户
   ```sql
   CREATE USER user IDENTIFIED WITH plaintext_password BY 'password';
    ```
- 为创建的用户授予权限
   ```sql
    grant create,alter,drop,select,insert on default.* to user
   ```
    - user为复制/转换用户的用户名
    - password为用户的密码
- 方式二： 使用具有创建用户权限并且可以授予写权限的用户创建用于数据/转换的用户
    <br>
  - 创建具有创建用户权限并且可以授予写权限的用户
    ``` sql
      CREATE USER user IDENTIFIED WITH plaintext_password BY 'password'
    ```
  - 授予这个用户创建用户的权限
    ```sql
      GRANT CREATE USER ON *.* TO adminUser 
    ```
  - 授予这个用户授予写权限，并且允许它可以将这些权限授予给其他用户
    ```sql
      grant create,alter,drop,select,insert,delete on default.* to adminUser with grant option 
    ```
  - 使用具有创建用户、授予写权限的用户登录ClickHouse
  - 创建用于进行数据复制/转换的用户
    ```sql
      CREATE USER user IDENTIFIED WITH plaintext_password BY 'password';
    ```
  - 授予这个用户create,alter,drop,select,insert,delete权限
    ```sql
      grant create,alter,drop,select,insert,delete on default.* to user 
    ```
    - 其中adminUser 和 user 分别为新创建的授权账号名与用于数据复制/转换的用户名
    - password 为用户密码
#### 注意事项
- ClickHouse不支持binary相关的字段类型，如果您的源表中有相关类型的字段，目标会将binary相关字段转成Base64字符串写入
#### 使用帮助
- 当ClickHouse作为目标时，节点配置中-->高级配置-->数据源专属配置-->合并分区间隔(分钟)配置选项可以配置ClickHouse的Optimize Table的间隔，您可以根据业务需求自定义Optimize Table间隔。
#### 性能测试
- 环境说明
  - 本次测试中，Tapdata的部署环境为12C96G，ClickHouse使用docker 部署，分配的资源为8C48G
  - 测试任务的源使用Tapdata 的模拟数据源模拟1000w数据对ClickHouse进行写入。目标开启4线程并发写入，每批次写入条数为20000
  ##### 测试结果
    | 同步写入条数   | 同步耗时 |   平均QPS |
    | :------------- | :----------: | ------------: |
    | 1000w |   9s   | 11w/s |
    | 5000w        |    6min24s     |         13w/s |