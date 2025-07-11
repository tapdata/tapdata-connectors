package io.tapdata.mongodb;

import com.mongodb.MongoNamespace;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.internal.MongoClientImpl;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class MongodbUtilTest {
    @Test
    void test_getServerTime(){
        MongoClientImpl mongoClient = mock(MongoClientImpl.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        when(mongoClient.getDatabase("test")).thenReturn(mongoDatabase);
        when(mongoDatabase.runCommand(eq(new BsonDocument("serverStatus", new BsonInt32(1))))).thenReturn(new Document("localTime",new Date()));
        Assertions.assertNotNull(MongodbUtil.getServerTime(mongoClient, "test"));
    }
    @Nested
    class sampleDataRowTest{
        @Test
        void test(){
            MongoCollection collection = mock(MongoCollection.class);
            Consumer<BsonDocument> callback = mock(Consumer.class);
            List<Document> pipeline = new ArrayList<>();
            pipeline.add(new Document("$sample", new Document("size", 1)));
            AggregateIterable<BsonDocument> aggregateIterable = mock(AggregateIterable.class);
            when(aggregateIterable.allowDiskUse(true)).thenReturn(aggregateIterable);
            MongoCursor<BsonDocument> mongoCursor = mock(MongoCursor.class);
            when(mongoCursor.hasNext()).thenReturn(true).thenReturn(false);
            when(mongoCursor.next()).thenReturn(new BsonDocument());
            when(aggregateIterable.iterator()).thenReturn(mongoCursor);
            when(collection.aggregate(eq(pipeline), eq(BsonDocument.class))).thenReturn(aggregateIterable);
            MongoNamespace mongoNamespace = mock(MongoNamespace.class);
            when(collection.getNamespace()).thenReturn(mongoNamespace);
            when(mongoNamespace.getFullName()).thenReturn("test.test");
            MongodbUtil.sampleDataRow(collection, 1, callback);
            verify(callback,times(2)).accept(any());
        }
        @Test
        void test_load_oplog(){
            MongoCollection collection = mock(MongoCollection.class);
            Consumer<BsonDocument> callback = mock(Consumer.class);
            List<Document> pipeline = new ArrayList<>();
            pipeline.add(new Document("$sample", new Document("size", 1)));
            AggregateIterable<BsonDocument> aggregateIterable = mock(AggregateIterable.class);
            when(aggregateIterable.allowDiskUse(true)).thenReturn(aggregateIterable);
            MongoCursor<BsonDocument> mongoCursor = mock(MongoCursor.class);
            when(mongoCursor.hasNext()).thenReturn(true).thenReturn(false);
            when(mongoCursor.next()).thenReturn(new BsonDocument());
            when(aggregateIterable.iterator()).thenReturn(mongoCursor);
            when(collection.aggregate(eq(pipeline), eq(BsonDocument.class))).thenReturn(aggregateIterable);
            MongoNamespace mongoNamespace = mock(MongoNamespace.class);
            when(collection.getNamespace()).thenReturn(mongoNamespace);
            when(mongoNamespace.getFullName()).thenReturn("local.oplog.rs");
            MongodbUtil.sampleDataRow(collection, 1, callback);
            verify(callback,times(1)).accept(any());
        }
    }
}
