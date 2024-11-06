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
            describe = "The MongoDB connection URI is invalid. The connection URI must be in the correct format and contain the necessary connection information. For more information, see the connection page example.",
            describeCN = "MongoDB 连接 URI 无效。连接 URI 必须是正确的格式，并且包含必要的连接信息，详细信息参考连接页面样例",
            solution = "1. Check the connection URI format: The connection URI must be in the correct format, refer to the connection page example\n" +
                    "2. Check the connection information: Ensure that the connection URI contains the necessary connection information, such as username, password, host, port, and database name.\n" +
                    "3. Check if the username and password in the URI contain special characters: If the username and password contain special characters, URL encoding is required.",
            solutionCN = "1. 检查连接 URI 格式：连接 URI 必须是正确的格式，参考连接页面样例\n" +
                    "2. 检查连接信息：确保连接 URI 包含必要的连接信息，如用户名、密码、主机、端口和数据库名称。\n" +
                    "3. 检查 URI 中用户名密码是否含特殊字符：如果用户名密码中含有特殊字符，需要进行 URL 编码。",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME,
            seeAlso = {"https://docs.mongodb.com/manual/reference/connection-string/"}
    )
    String INVALID_URI = "370001";

    @TapExCode(
            describe = "This error is usually due to an incorrect username or password, or the MONGO URI does not specify authSource for admin authentication.",
            describeCN = "这个错误通常是用户名或密码不正确，也可能是 MONGO URI 没有指定 authSource 为 admin 身份认证",
            solution = "Ensure that the username and password are correct, or try adding ?authSource=admin or &authSource=admin to the URI connection string.",
            solutionCN = "确保用户名和密码无误，或尝试 URI 连接串添加 ?authSource=admin 或 &authSource=admin",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME,
            dynamicDescription = "Username: {}",
            dynamicDescriptionCN = "用户：{}",
            seeAlso = {"https://docs.mongodb.com/manual/reference/connection-string/"}
    )
    String AUTH_FAIL = "370002";

    @TapExCode(
            describe = "The user does not have the necessary permissions to perform the operation. This error is usually caused by insufficient user permissions.",
            describeCN = "用户没有执行操作所需的权限。这个错误通常是由于用户权限不足引起的。",
            solution = "1. Check user permissions: Ensure that the user has the necessary permissions to perform the operation.\n" +
                    "2. Grant user permissions: If the user does not have the necessary permissions, you can grant the user the necessary permissions.\n" +
                    "3. Use the correct user: Ensure that the user you are using has the necessary permissions to perform the operation.",
            solutionCN = "1. 检查用户权限：确保用户有执行操作所需的权限。\n" +
                    "2. 授予用户权限：如果用户没有执行操作所需的权限，可以授予用户所需的权限。\n" +
                    "3. 使用正确的用户：确保你使用的用户有执行操作所需的权限。",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME,
            seeAlso = {"https://docs.mongodb.com/manual/reference/privilege-actions/"}
    )
    String READ_PRIVILEGES_MISSING = "370003";

    @TapExCode(
            describe = "The user does not have the necessary permissions to perform write operations on the database. This error is usually caused by insufficient user permissions.",
            describeCN = "用户对库没有执行写操作所需的权限。这个错误通常是由于用户权限不足引起的。",
            solution = "1. Check user permissions: Ensure that the user has the necessary permissions to perform the operation.\n" +
                    "2. Grant user permissions: If the user does not have the necessary permissions, you can grant the user the necessary permissions.\n" +
                    "3. Use the correct user: Ensure that the user you are using has the necessary permissions to perform the operation.",
            solutionCN = "检查用户权限：确保用户有执行写操作所需的权限。\n" +
                    "<pre><code>use admin\n" +
                    "db.createUser(\n" +
                    "  {\n" +
                    "    user: \"tapdata\",\n" +
                    "    pwd: \"my_password\",\n" +
                    "    roles: [\n" +
                    "       { role: \"readWrite\", db: \"demodata\" },\n" +
                    "       { role: \"clusterMonitor\", db: \"admin\" },\n" +
                    "    ]\n" +
                    "  }\n" +
                    ")</code></pre>",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME,
            seeAlso = {"https://docs.mongodb.com/manual/reference/privilege-actions/"}
    )
    String WRITE_PRIVILEGES_MISSING = "370004";

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
    String EXCEEDS_16M_LIMIT = "370005";

    @TapExCode(
            describe = "The _id field in MongoDB is the default primary key and cannot be modified. The _id value must be unique within a collection and is immutable. If you try to modify the _id field, MongoDB will throw this error.",
            describeCN = "MongoDB 中的 _id 字段是默认的主键，并且它是不可以被修改的。_id 值在一个集合中必须是唯一的，并且是不可变的。\n" +
                    "如果你尝试修改 _id 字段，MongoDB 将会抛出这个错误",
            solution = "If you need to modify the _id field, you can delete the original document and insert a new document with the modified _id value.",
            solutionCN = "如果你需要修改 _id 字段，可以删除原始文档，然后插入一个新的文档，新文档的 _id 值是修改后的值。",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME,
            dynamicDescription = "Error event: {}",
            dynamicDescriptionCN = "错误事件：{}",
            seeAlso = {"https://docs.mongodb.com/manual/reference/method/db.collection.update/#update-parameter"}
    )
    String MODIFY_ON_ID = "370006";

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
    String NO_REPLICA_SET = "370007";
}
