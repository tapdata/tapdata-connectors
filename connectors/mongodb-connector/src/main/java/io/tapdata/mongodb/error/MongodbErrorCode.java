package io.tapdata.mongodb.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;
import io.tapdata.exception.TapExLevel;
import io.tapdata.exception.TapExType;

@TapExClass(
        code = 37,
        module = "mongodb-connector",
        describe = "MongoDB Error Code",
        prefix = "MONGODB")
public interface MongodbErrorCode {

    @TapExCode(
            describe = "The single document size limit for MongoDB is 16MB, which is a hard limit that cannot be changed. If the data you attempt to insert or update exceeds this limit, MongoDB will throw this error.",
            describeCN = "MongoDB 的单个文档大小限制是 16MB，这是一个硬性限制，不能更改。如果你尝试插入或更新的数据超过了这个限制，MongoDB 将会抛出这个错误。",
            solution = "1. Optimize data structure: delete unnecessary fields, compress variable-length fields (such as strings), and use more space-saving data types.\n" +
                    "2. Data sharding: If the size of a single document must exceed 16MB, consider splitting large documents into multiple small documents or using data sharding technology.\n" +
                    "3. Use GridFS: If you are storing large files, you can use MongoDB's GridFS mechanism to store large files.",
            solutionCN = "1. 优化数据结构：删除不必要的字段，压缩可变长度字段（如字符串），使用更节省空间的数据类型。\n" +
                    "2. 数据分片：如果单个文档的大小必须超过 16MB，考虑将大文档拆分成多个小文档，或者使用数据分片技术。\n" +
                    "3. 使用 GridFS：如果存储的是大型文件，可以使用 MongoDB 的 GridFS 机制来存储大文件。",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME,
            seeAlso = {"https://docs.mongodb.com/manual/reference/limits/"}
    )
    String EXCEEDS_16M_LIMIT = "370001";

    @TapExCode(
            describe = "The _id field in MongoDB is the default primary key and cannot be modified. The _id value must be unique within a collection and is immutable. If you try to modify the _id field, MongoDB will throw this error.",
            describeCN = "MongoDB 中的 _id 字段是默认的主键，并且它是不可以被修改的。_id 值在一个集合中必须是唯一的，并且是不可变的。\n" +
                    "如果你尝试修改 _id 字段，MongoDB 将会抛出这个错误",
            solution = "If you need to modify the _id field, you can delete the original document and insert a new document with the modified _id value.",
            solutionCN = "如果你需要修改 _id 字段，可以删除原始文档，然后插入一个新的文档，新文档的 _id 值是修改后的值。",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME,
            seeAlso = {"https://docs.mongodb.com/manual/reference/method/db.collection.update/#update-parameter"}
    )
    String MODIFY_ON_ID = "370002";

    @TapExCode(
            describe = "Incremental dependency on MongoDB's changeStream feature, which requires the support of a replica set or sharded cluster, as it relies on replication to keep track of real-time changes in data. If the MongoDB instance is not configured as a replica set, the changeStream feature cannot be used.",
            describeCN = "增量依赖MongoDB的changeStream功能，它需要复制集或分片集群的支持，因为它依赖于复制来保持数据变化的实时监控。如果MongoDB实例没有配置成副本集，那么无法使用changeStream功能。",
            solution = "1. If your application scenario allows, you can configure the existing MongoDB instance as a replica set. This can be done by:\n" +
                    "Ensure you have at least three nodes to form a replica set.\n" +
                    "Start these nodes using mongod and specify the --replSet parameter and different ports.\n" +
                    "Connect to one of the nodes using mongo.\n" +
                    "Initialize the replica set by executing rs.initiate().\n" +
                    "Add other nodes to the replica set using rs.add().\n" +
                    "2. If configuring a replica set is not feasible, then you need to change the application logic to not use changeStream, but to poll the database periodically to detect changes. This approach may have latency and is not suitable for real-time response scenarios.\n" +
                    "3. Another option is to migrate the existing single server instance to a replica set and then use changeStream.\n" +
                    "4. If your application scenario allows, you can also consider upgrading to a MongoDB sharded cluster and using changeStream. Sharded clusters provide higher levels of replication and scalability, but this also requires you to redesign the deployment of the database and application logic.",
            solutionCN = "1. 如果你的应用场景允许，可以将现有的MongoDB实例配置为副本集。这可以通过以下步骤完成：\n" +
                    "确保你有至少三个节点来组成一个副本集。\n" +
                    "使用mongod启动这些节点，并且指定--replSet参数以及不同的端口。\n" +
                    "使用mongo连接到其中一个节点。\n" +
                    "通过执行rs.initiate()来初始化副本集。\n" +
                    "添加其他节点到副本集中，使用rs.add()。\n" +
                    "2. 如果配置副本集不可行，那么你需要将应用逻辑更改为不使用changeStream，而是定期轮询数据库来检测更改。这种方法可能会有延迟，不适合需要实时响应的应用场景。\n" +
                    "3. 另一个选择是将现有的单个服务器实例迁移到一个复制集中，然后再使用changeStream。\n" +
                    "4. 如果你的应用场景允许，也可以考虑升级到MongoDB的分片集群，并使用changeStream。分片集群提供了更高级别的复制和扩展性，但这同样需要你重新设计数据库的部署和应用逻辑。",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME,
            seeAlso = {"https://docs.mongodb.com/manual/changeStreams/", "https://docs.mongodb.com/manual/tutorial/deploy-replica-set/"}
    )
    String NO_REPLICA_SET = "370003";
}
