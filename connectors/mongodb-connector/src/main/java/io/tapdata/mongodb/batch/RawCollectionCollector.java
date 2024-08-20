package io.tapdata.mongodb.batch;

import com.mongodb.client.MongoCollection;
import org.bson.RawBsonDocument;

public interface RawCollectionCollector {
    MongoCollection<RawBsonDocument> collectRawCollection(String tableId);
}
