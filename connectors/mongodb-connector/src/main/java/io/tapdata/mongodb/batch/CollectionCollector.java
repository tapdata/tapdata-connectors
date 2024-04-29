package io.tapdata.mongodb.batch;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

public interface CollectionCollector {
    MongoCollection<Document> collectCollection(String tableId);
}
