## Connection configuration help 
### ClickHouse Installation Instructions
Follow these instructions to ensure that the ClickHouse database is added and used successfully in Tapdata.
### Supported versions
Clickhouse 20.x, 21.x，22.x,23.x
### Functional limitations
- Currently clickhouse only supports incremental field synchronization
### Prerequisites
#### as source
Log in to the clickhouse database and run the following command format to create an account for your sync/dev tasks.
```sql
CREATE user tapdata IDENTIFIED WITH plaintext_password BY 'mypassword'
```
2. Grant permissions to the newly created user
```sql
grant select on default.* to user
```
-user is the username of the copied/converted user
-password is the user's password
#### as a target
- Way one:
The user created as the highest privilege user (default) for data replication/transformation
1. In user.xml '&lt; `quota`&gt; `default`&lt; `/quota`&gt; 'If you don't, you won't be able to create a user
``` xml
<access_management>1</access_management>
<named_collection_control>1</named_collection_control>
<show_named_collections>1</show_named_collections>
<show_named_collections_secrets>1</show_named_collections_secrets>
```
2. Create users for data replication and transformation
```sql
CREATE USER user IDENTIFIED WITH plaintext_password BY 'password';
```
- Way two: Create a user for data/transformation using a user who has created user permission and can grant write permission
  - Create a user that has created user permission and can grant write permission
    ``` sql
    CREATE USER user IDENTIFIED WITH plaintext_password BY 'password'
    ```
  - Give this user permission to create a user
    ```sql
    GRANT CREATE USER ON *.* TO adminUser
    ```
  - Grant write permissions to this user, and allow it to grant these permissions to other users
    ```sql
    grant create,alter,drop,select,insert,delete on default.* to adminUser with grant option
    ```
  - Log in to ClickHouse as a user with create user, grant write permission
  - Create a user to do data replication/transformation
    ```sql
    CREATE USER user IDENTIFIED WITH plaintext_password BY 'password';
    ```
  - granted to the users to create, alter, drop, the select, insert, delete permissions
    ```sql
    grant create,alter,drop,select,insert,delete on default.* to user
    ```
  - Where adminUser and user are the name of the newly created authorized account and the username used for data replication/conversion, respectively
  -password is the user's password
#### Notes
- ClickHouse does not support a binary-dependent field type. If you have a field of that type in your source table, the target will convert the binary-dependent field to a Base64 string
#### Help
- When ClickHouse is the target, the node configuration is --&gt; Advanced configuration --&gt; Data source specific configuration -&gt; The Merge Partition Interval (minutes) configuration option allows you to configure ClickHouse's Optimize Table interval, which you can customize according to your business needs.
#### Performance testing
- Environment description
  -In this test, the deployment environment of Tapdata is 12C96G, and ClickHouse is deployed using docker with 8C48G allocated resources
  -The source of the test task uses Tapdata's mock data source to simulate 1000w data writes to ClickHouse. The target starts 4-threaded concurrent writes with batches of 20,000
  ##### Test results
  | 同步写入条数   | 同步耗时 |   平均QPS |
      | :------------- | :----------: | ------------: |
  | 1000w |   9s   | 11w/s |
  | 5000w        |    6min24s     |         13w/s |