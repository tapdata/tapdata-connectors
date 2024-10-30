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
            solutionCN = "1.优化数据结构：删除不必要的字段，压缩可变长度字段（如字符串），使用更节省空间的数据类型。\n" +
                    "2.数据分片：如果单个文档的大小必须超过 16MB，考虑将大文档拆分成多个小文档，或者使用数据分片技术。\n" +
                    "3.使用 GridFS：如果存储的是大型文件，可以使用 MongoDB 的 GridFS 机制来存储大文件。",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME,
            seeAlso = {"https://docs.mongodb.com/manual/reference/limits/"}
    )
    String EXCEEDS_16M_LIMIT = "370001";
}
