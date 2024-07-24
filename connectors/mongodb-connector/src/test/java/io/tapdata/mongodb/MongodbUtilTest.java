package io.tapdata.mongodb;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.internal.MongoClientImpl;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MongodbUtilTest {
    @Test
    void test_getServerTime(){
        MongoClientImpl mongoClient = mock(MongoClientImpl.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        when(mongoClient.getDatabase("test")).thenReturn(mongoDatabase);
        when(mongoDatabase.runCommand(eq(new BsonDocument("serverStatus", new BsonInt32(1))))).thenReturn(new Document("localTime",new Date()));
        Assertions.assertNotNull(MongodbUtil.getServerTime(mongoClient, "test"));
    }
}
