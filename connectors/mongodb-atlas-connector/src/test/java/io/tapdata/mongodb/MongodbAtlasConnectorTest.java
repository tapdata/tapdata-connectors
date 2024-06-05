package io.tapdata.mongodb;

import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.client.*;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MongodbAtlasConnectorTest {

    @Nested
    class testGetTableCount{
        MongodbAtlasConnector mongodbAtlasConnector = new MongodbAtlasConnector();
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);

        MongoIterable<String> result;

        @BeforeEach
        void before(){
            ReflectionTestUtils.setField(mongodbAtlasConnector, "canListCollections", true);
            ReflectionTestUtils.setField(mongodbAtlasConnector,"mongoDatabase",mongoDatabase);
            result =  new MongoIterable<String>() {
                @Override
                public MongoCursor<String> iterator() {
                    return mock(MongoCursor.class);
                }

                @Override
                public MongoCursor<String> cursor() {
                    return null;
                }

                @Override
                public String first() {
                    return null;
                }

                @Override
                public <U> MongoIterable<U> map(Function<String, U> function) {
                    return null;
                }

                @Override
                public <A extends Collection<? super String>> A into(A objects) {
                    return null;
                }

                @Override
                public MongoIterable<String> batchSize(int i) {
                    return null;
                }
            };
        }
        @Test
        void testCanListCollection() throws Throwable {
            ReflectionTestUtils.setField(mongodbAtlasConnector, "canListCollections", true);
            when(mongoDatabase.listCollectionNames()).thenReturn(result);
            Assertions.assertEquals(0,mongodbAtlasConnector.tableCount(mock(TapConnectionContext.class)));
        }

        @Test
        void testCannotListCollection() throws Throwable {
            ReflectionTestUtils.setField(mongodbAtlasConnector, "canListCollections", false);
            Document result = Document.parse("{\"cursor\": {\"id\": 0, \"ns\": \"tapdata.$cmd.listCollections\", " +
                    "\"firstBatch\": [{\"name\": \"tapdata\", \"type\": \"collection\"}, {\"name\": \"tapdata_2\", \"type\": \"collection\"}, {\"name\": \"tapdata_3\", \"type\": \"collection\"}]}}");
            when(mongoDatabase.runCommand(any())).thenReturn(result);
            MongoCollection<Document> collection = mock(MongoCollection.class);
            when(mongoDatabase.getCollection(any())).thenReturn(collection);
            when(collection.find()).thenReturn(mock(FindIterable.class));
            Assertions.assertEquals(3,mongodbAtlasConnector.tableCount(mock(TapConnectionContext.class)));
        }

        @Test
        void testCannotListCollectionOneNoPermission() throws Throwable {
            ReflectionTestUtils.setField(mongodbAtlasConnector, "canListCollections", false);
            Document result = Document.parse("{\"cursor\": {\"id\": 0, \"ns\": \"tapdata.$cmd.listCollections\", " +
                    "\"firstBatch\": [{\"name\": \"tapdata\", \"type\": \"collection\"}, {\"name\": \"tapdata_2\", \"type\": \"collection\"}, {\"name\": \"tapdata_3\", \"type\": \"collection\"}]}}");
            when(mongoDatabase.runCommand(any())).thenReturn(result);
            MongoCollection<Document> collection = mock(MongoCollection.class);
            when(mongoDatabase.getCollection(any())).thenReturn(collection);
            when(collection.find()).thenReturn(mock(FindIterable.class)).thenReturn(mock(FindIterable.class)).thenThrow(new MongoClientException(""));
            Assertions.assertEquals(2,mongodbAtlasConnector.tableCount(mock(TapConnectionContext.class)));
        }

        @Test
        void testError() {
            ReflectionTestUtils.setField(mongodbAtlasConnector, "canListCollections", false);
            ReflectionTestUtils.setField(mongodbAtlasConnector, "exceptionCollector", mock(MongodbExceptionCollector.class));
            when(mongoDatabase.runCommand(any())).thenThrow(new MongoClientException(""));
            assertThrows(MongoClientException.class,()->mongodbAtlasConnector.tableCount(mock(TapConnectionContext.class)));
        }
    }
    @Nested
    class testGetTableNames {
        MongodbAtlasConnector mongodbAtlasConnector = new MongodbAtlasConnector();
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        MongoClient mongoClient = mock(MongoClient.class);
        ListCollectionsIterable<Document> listCollectionsIterable;

        @BeforeEach
        void before() {
            ReflectionTestUtils.setField(mongodbAtlasConnector, "canListCollections", true);
            ReflectionTestUtils.setField(mongodbAtlasConnector,"mongoDatabase",mongoDatabase);
            ReflectionTestUtils.setField(mongodbAtlasConnector,"mongoClient",mongoClient);
            listCollectionsIterable = new ListCollectionsIterable<Document>() {
                @Override
                public ListCollectionsIterable<Document> filter(Bson bson) {
                    return null;
                }

                @Override
                public ListCollectionsIterable<Document> maxTime(long l, TimeUnit timeUnit) {
                    return null;
                }

                @Override
                public ListCollectionsIterable<Document> batchSize(int i) {
                    return null;
                }

                @Override
                public ListCollectionsIterable<Document> comment(String s) {
                    return null;
                }

                @Override
                public ListCollectionsIterable<Document> comment(BsonValue bsonValue) {
                    return null;
                }

                @Override
                public MongoCursor<Document> iterator() {
                    return mock(MongoCursor.class);
                }

                @Override
                public MongoCursor<Document> cursor() {
                    return null;
                }

                @Override
                public Document first() {
                    return null;
                }

                @Override
                public <U> MongoIterable<U> map(Function<Document, U> function) {
                    return null;
                }

                @Override
                public <A extends Collection<? super Document>> A into(A objects) {
                    return null;
                }
            };
        }

        @Test
        void testCanListCollection() throws Throwable {
            MongodbConfig mongodbConfig = new MongodbConfig();
            mongodbConfig.setDatabase("test");
            ReflectionTestUtils.setField(mongodbAtlasConnector, "canListCollections", true);
            ReflectionTestUtils.setField(mongodbAtlasConnector, "mongoConfig",mongodbConfig);
            when(mongoClient.getDatabase(any())).thenReturn(mongoDatabase);
            when(mongoDatabase.listCollections()).thenReturn(listCollectionsIterable);
            List<String> result = new ArrayList<>();
            mongodbAtlasConnector.getTableNames(mock(TapConnectionContext.class),10, result::addAll);
            Assertions.assertEquals(0,result.size());
        }

        @Test
        void testCannotListCollection() throws Throwable {
            MongodbConfig mongodbConfig = new MongodbConfig();
            mongodbConfig.setDatabase("test");
            ReflectionTestUtils.setField(mongodbAtlasConnector, "canListCollections", false);
            ReflectionTestUtils.setField(mongodbAtlasConnector, "mongoConfig",mongodbConfig);
            ReflectionTestUtils.setField(mongodbAtlasConnector, "canListCollections", false);
            Document document = Document.parse("{\"cursor\": {\"id\": 0, \"ns\": \"tapdata.$cmd.listCollections\", " +
                    "\"firstBatch\": [{\"name\": \"tapdata\", \"type\": \"collection\"}, {\"name\": \"tapdata_2\", \"type\": \"view\"}, {\"name\": \"system.test\", \"type\": \"collection\"}]}}");
            when(mongoDatabase.runCommand(any())).thenReturn(document);
            List<String> result = new ArrayList<>();
            MongoCollection<Document> collection = mock(MongoCollection.class);
            when(mongoDatabase.getCollection(any())).thenReturn(collection);
            when(collection.find()).thenReturn(mock(FindIterable.class));
            mongodbAtlasConnector.getTableNames(mock(TapConnectionContext.class),10, result::addAll);
            Assertions.assertEquals(1,result.size());
        }

        @Test
        void testError() throws Throwable {
            MongodbConfig mongodbConfig = new MongodbConfig();
            mongodbConfig.setDatabase("test");
            ReflectionTestUtils.setField(mongodbAtlasConnector, "exceptionCollector", mock(MongodbExceptionCollector.class));
            ReflectionTestUtils.setField(mongodbAtlasConnector, "canListCollections", false);
            ReflectionTestUtils.setField(mongodbAtlasConnector, "mongoConfig",mongodbConfig);
            ReflectionTestUtils.setField(mongodbAtlasConnector, "canListCollections", false);
            Document document = Document.parse("{\"cursor\": {\"id\": 0, \"ns\": \"tapdata.$cmd.listCollections\", " +
                    "\"firstBatch\": [{\"name\": \"tapdata\", \"type\": \"collection\"}, {\"name\": \"tapdata_2\", \"type\": \"view\"}, {\"name\": \"system.test\", \"type\": \"collection\"}]}}");
            when(mongoDatabase.runCommand(any())).thenThrow(new MongoClientException(""));
            List<String> result = new ArrayList<>();
            assertThrows(MongoClientException.class,()->mongodbAtlasConnector.getTableNames(mock(TapConnectionContext.class),10, result::addAll));
            Assertions.assertEquals(0,result.size());
        }
    }
    @Nested
    class TestDiscoverSchema{

    }
}
