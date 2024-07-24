## **Connection configuration help**
MongoDB is a popular, open source NoSQL database that stores and retrieves data in a flexible/scalable way. After completing the Agent deployment, you can follow the tutorial in this article to add the MongoDB data source in TapData, which can later be used as a source or target library to build a data pipeline.
> **TIP**: When MongoDB is connected as the source, it must be a replica set.
## Supported version
MongoDB 3.6+
>**TIP**:<br>
>Since Tapdata data synchronization is currently based on MongoDB's Change Stream, which supports multi-table merging, and MongoDB officially supports Change Stream from version 4.0, please try to ensure that the source and target databases are both version 4.0 and above.
## Functional limitations
* The source MongoDB supports replica sets and sharded clusters.
* Supports full and incremental synchronization of slave nodes in MongoDB sharded clusters
##  Preparation
### As a Source Database
1. Make sure that the schema of the source database is a replica set or a sharding cluster. If it is standalone, you can configure it as a single-member replica set to open Oplog. For more information, see [Convert a Standalone to a Replica Set](https://docs.mongodb.com/manual/tutorial/convert-standalone-to-replica-set/).
2. To ensure sufficient storage space for the Oplog, it is important to configure it to accommodate at least 24 hours' worth of data. For detailed instructions, see [Change the Size of the Oplog](https://docs.mongodb.com/manual/tutorial/change-oplog-size/).
3. To create an account and grant permissions according to permission management requirements, follow the necessary steps.
> **TIP**：In shard cluster architectures, the shard server is unable to retrieve user permissions from the config database. Therefore, it is necessary to create corresponding users and grant permissions on the master nodes of each shard.
 * Grant read role to specified database (e.g. demodata)
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

    * Grant read role to all databases.

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
> **TIP**: Only when using MongoDB version 3.2, it is necessary to grant the read role to the local database.
4. When configuring the MongoDB URI, it is advisable to set the write concern to majority (w=majority) to mitigate the risk of data loss in the event of a primary node downtime.

5. create a thread for each shard and read the data. Before configuring data synchronization/development tasks, you also need to perform the following operations.

    * Turn off the Balancer to avoid the impact of chunk migration on data consistency. For more information, see [Stop the Balancer](https://docs.mongodb.com/manual/reference/method/sh.stopBalancer/).
    * Clears the orphaned documents due to failed chunk migration to avoid _id conflicts. For more information, see [Clean Up Orphaned Documents](https://docs.mongodb.com/manual/reference/command/cleanupOrphaned/).

> **IMPORTANT**<br>
> 1. For cluster sharding, you must create appropriate user permissions on each shard master node. This is due to MongoDB's security architecture design.
  When logging into each individual shard, the shard server does not obtain user permissions from the config database. Instead, it will use its local user database for authentication and authorization. <br>
> 2. For versions below MongoDB 3.x, please ensure that the Tapdata service communicates normally with each MongoDB node.
### As a Target Database

Grant write role to specified database (e.g. demodata) and clusterMonitor role for data validation, e.g.:

```bash
useadmin
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
> **TIP**: Only when using MongoDB version 3.2, it is necessary to grant the read role to the local database.
### MongoDB TLS/SSL configuration**
- **Enable TLS/SSL**<br>
  Please select "Yes" in "Connect using TLS/SSL" on the left configuration page to configure<br>
- **Set MongoDB PemKeyFile**<br>
  Click "Select File" and select the certificate file. If the certificate file is password protected, fill in the password in "Private Key Password"<br>
- **Set CAFile**<br>
  Please select "Yes" in "Validate server certificate" on the left configuration page<br>
  Then click "Select File" in "Authentication and Authorization" below<br>
### MongoDB performance test
Configuration: ecs.u1-c1m2.2xlarge model, 8C 16G, 100GB ESSD disk

| Writing method | RPS |
|-------- |------------|
| Full write | 95K |
| Mixed writing | 2.5k |


| Reading method | RPS |
|------|------------|
| Full read | 50k |
| Incremental read | 14k |