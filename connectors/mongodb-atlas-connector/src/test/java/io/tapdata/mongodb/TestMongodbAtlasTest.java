package io.tapdata.mongodb;

import com.mongodb.MongoClientException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapdata.mongodb.atlasTest.MongodbAtlasTest;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestMongodbAtlasTest {
    MongoClient mongoClient = mock(MongoClient.class);
    MongodbAtlasTest mongodbAtlasTest = new MongodbAtlasTest(mock(MongodbConfig.class), (item -> {}), mongoClient, new ConnectionOptions());
    @Nested
    class testConnect{
        @Test
        void testPass(){
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            when(mongoClient.getDatabase(any())).thenReturn(mongoDatabase);
            when(mongoDatabase.runCommand(any())).thenReturn(new Document("cursor","test"));
            Assertions.assertTrue(mongodbAtlasTest.testConnect());
        }

        @Test
        void testFail(){
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            when(mongoClient.getDatabase(any())).thenReturn(mongoDatabase);
            when(mongoDatabase.runCommand(any())).thenThrow(new MongoClientException(""));
            Assertions.assertFalse(mongodbAtlasTest.testConnect());
        }
    }
    @Nested
    class testStreamRead{
        @Test
        void testPass(){
            try(MockedStatic<MongodbUtil> mongodbUtilMockedStatic = Mockito.mockStatic(MongodbUtil.class)){
                HashMap<String,String> map = new HashMap<>();
                map.put("test","test");
                mongodbUtilMockedStatic.when(()->MongodbUtil.nodesURI(any(),any())).thenReturn(map);
                Assertions.assertTrue(mongodbAtlasTest.testStreamRead());
            }
        }

        @Test
        void testFail(){
            try(MockedStatic<MongodbUtil> mongodbUtilMockedStatic = Mockito.mockStatic(MongodbUtil.class)){
                HashMap<String,String> map = new HashMap<>();
                mongodbUtilMockedStatic.when(()->MongodbUtil.nodesURI(any(),any())).thenReturn(map);
                Assertions.assertFalse(mongodbAtlasTest.testStreamRead());
            }
        }

        @Test
        void testError(){
            try(MockedStatic<MongodbUtil> mongodbUtilMockedStatic = Mockito.mockStatic(MongodbUtil.class)){
                HashMap<String,String> map = new HashMap<>();
                mongodbUtilMockedStatic.when(()->MongodbUtil.nodesURI(any(),any())).thenThrow(new MongoClientException(""));
                Assertions.assertFalse(mongodbAtlasTest.testStreamRead());
            }
        }
    }
    @Nested
    class testReadPrivilege{


        @Test
        void testPassByOpenAuth(){
            MongodbConfig mongodbConfig = new MongodbConfig();
            mongodbConfig.setUri("mongodb://localhost:27017/test");
            MongodbAtlasTest mongodbAtlasTest = new MongodbAtlasTest(mongodbConfig, (item -> {}), mongoClient, new ConnectionOptions());
            Assertions.assertTrue(mongodbAtlasTest.testReadPrivilege());
        }

        @Test
        void testPass(){
            MongodbConfig mongodbConfig = new MongodbConfig();
            mongodbConfig.setUri("mongodb://test:test@localhost:27017/tapdata");
            MongodbAtlasTest mongodbAtlasTest = new MongodbAtlasTest(mongodbConfig, (item -> {}), mongoClient, new ConnectionOptions());
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            when(mongoClient.getDatabase(any())).thenReturn(mongoDatabase);
            Document document = Document.parse("{\"authInfo\": {\"authenticatedUsers\": [{\"user\": \"test2\", \"db\": \"admin\"}], " +
                    "\"authenticatedUserRoles\": [{\"role\": \"read\", \"db\": \"tapdata\"}, {\"role\": \"read\", \"db\": \"local\"}], " +
                    "\"authenticatedUserPrivileges\": [{\"resource\": {\"db\": \"tapdata\", \"collection\": \"\"}, " +
                    "\"actions\": [\"changeStream\", \"collStats\", \"dbHash\", \"dbStats\", \"find\", \"killCursors\", \"listCollections\", \"listIndexes\", \"listSearchIndexes\", \"planCacheRead\"]}" +
                    ", {\"resource\": {\"db\": \"tapdata\", \"collection\": \"system.js\"}, " +
                    "\"actions\": [\"changeStream\", \"collStats\", \"dbHash\", \"dbStats\", \"find\", \"killCursors\", \"listCollections\", \"listIndexes\", \"listSearchIndexes\", \"planCacheRead\"]}, " +
                    "{\"resource\": {\"db\": \"local\", \"collection\": \"\"}, \"actions\": [\"changeStream\", \"collStats\", \"dbHash\", \"dbStats\", \"find\", \"killCursors\", \"listCollections\", \"listIndexes\", \"listSearchIndexes\", \"planCacheRead\"]}, " +
                    "{\"resource\": {\"db\": \"local\", \"collection\": \"system.js\"}, \"actions\": [\"changeStream\", \"collStats\", \"dbHash\", \"dbStats\", \"find\", \"killCursors\", \"listCollections\", \"listIndexes\", \"listSearchIndexes\", \"planCacheRead\"]}]}, \"ok\": 1}");
            when(mongoDatabase.runCommand(any())).thenReturn(document).thenReturn(new Document("setName","test"));
            Assertions.assertTrue(mongodbAtlasTest.testReadPrivilege());
        }

        @Test
        void testFail(){
            MongodbConfig mongodbConfig = new MongodbConfig();
            mongodbConfig.setUri("mongodb://test:test@localhost:27017/test");
            MongodbAtlasTest mongodbAtlasTest = new MongodbAtlasTest(mongodbConfig, (item -> {}), mongoClient, new ConnectionOptions());
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            when(mongoClient.getDatabase(any())).thenReturn(mongoDatabase);
            Document document = Document.parse("{\"authInfo\": {\"authenticatedUsers\": [{\"user\": \"test2\", \"db\": \"admin\"}], " +
                    "\"authenticatedUserRoles\": [{\"role\": \"read\", \"db\": \"tapdata\"}], " +
                    "\"authenticatedUserPrivileges\": [{\"resource\": {\"db\": \"tapdata\", \"collection\": \"\"}, " +
                    "\"actions\": [\"changeStream\", \"collStats\", \"dbHash\", \"dbStats\", \"find\", \"killCursors\", \"listCollections\", \"listIndexes\", \"listSearchIndexes\", \"planCacheRead\"]}, " +
                    "{\"resource\": {\"db\": \"tapdata\", \"collection\": \"system.js\"}, \"actions\": [\"changeStream\", \"collStats\", \"dbHash\", \"dbStats\", \"find\", \"killCursors\", \"listCollections\", \"listIndexes\", \"listSearchIndexes\", \"planCacheRead\"]}]}, \"ok\": 1}");
            when(mongoDatabase.runCommand(any())).thenReturn(document).thenReturn(new Document("msg","isdbgrid"));
            Assertions.assertFalse(mongodbAtlasTest.testReadPrivilege());
        }

        @Test
        void testFailMsgIsNull(){
            MongodbConfig mongodbConfig = new MongodbConfig();
            mongodbConfig.setUri("mongodb://test:test@localhost:27017/test");
            MongodbAtlasTest mongodbAtlasTest = new MongodbAtlasTest(mongodbConfig, (item -> {}), mongoClient, new ConnectionOptions());
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            when(mongoClient.getDatabase(any())).thenReturn(mongoDatabase);
            Document document = Document.parse("{\"authInfo\": {\"authenticatedUsers\": [{\"user\": \"test2\", \"db\": \"admin\"}], " +
                    "\"authenticatedUserRoles\": [{\"role\": \"read\", \"db\": \"tapdata\"}], " +
                    "\"authenticatedUserPrivileges\": [{\"resource\": {\"db\": \"tapdata\", \"collection\": \"\"}, " +
                    "\"actions\": [\"changeStream\", \"collStats\", \"dbHash\", \"dbStats\", \"find\", \"killCursors\", \"listCollections\", \"listIndexes\", \"listSearchIndexes\", \"planCacheRead\"]}, " +
                    "{\"resource\": {\"db\": \"tapdata\", \"collection\": \"system.js\"}, \"actions\": [\"changeStream\", \"collStats\", \"dbHash\", \"dbStats\", \"find\", \"killCursors\", \"listCollections\", \"listIndexes\", \"listSearchIndexes\", \"planCacheRead\"]}]}, \"ok\": 1}");
            when(mongoDatabase.runCommand(any())).thenReturn(document);
            Assertions.assertFalse(mongodbAtlasTest.testReadPrivilege());
        }



        @Test
        void testFailUserPrivilegesIsNull(){
            MongodbConfig mongodbConfig = new MongodbConfig();
            mongodbConfig.setUri("mongodb://test:test@localhost:27017/test");
            MongodbAtlasTest mongodbAtlasTest = new MongodbAtlasTest(mongodbConfig, (item -> {}), mongoClient, new ConnectionOptions());
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            when(mongoClient.getDatabase(any())).thenReturn(mongoDatabase);
            Document document = Document.parse("{\"authInfo\": {\"authenticatedUsers\": [{\"user\": \"test2\", \"db\": \"admin\"}], " +
                    "\"authenticatedUserRoles\": [{\"role\": \"read\", \"db\": \"tapdata\"}]" +
                    "}, \"ok\": 1}");
            when(mongoDatabase.runCommand(any())).thenReturn(document).thenReturn(new Document("setName","test"));
            Assertions.assertFalse(mongodbAtlasTest.testReadPrivilege());
        }

        @Test
        void testFailUserPrivilegesIsMismatch(){
            MongodbConfig mongodbConfig = new MongodbConfig();
            mongodbConfig.setUri("mongodb://test:test@localhost:27017/test");
            MongodbAtlasTest mongodbAtlasTest = new MongodbAtlasTest(mongodbConfig, (item -> {}), mongoClient, new ConnectionOptions());
            MongoDatabase mongoDatabase = mock(MongoDatabase.class);
            when(mongoClient.getDatabase(any())).thenReturn(mongoDatabase);
            Document document = Document.parse("{\"authInfo\": {\"authenticatedUsers\": [{\"user\": \"test2\", \"db\": \"admin\"}], " +
                    "\"authenticatedUserRoles\": [{\"role\": \"read\", \"db\": \"tapdata\"}], " +
                    "\"authenticatedUserPrivileges\": [{\"resource\": {\"db\": \"tapdata\", \"collection\": \"\"}, " +
                    "\"actions\": [\"changeStream\", \"collStats\", \"dbHash\", \"dbStats\", \"find\", \"killCursors\", \"listCollections\", \"listIndexes\", \"listSearchIndexes\", \"planCacheRead\"]}, " +
                    "{\"resource\": {\"db\": \"tapdata\", \"collection\": \"system.js\"}, \"actions\": [\"changeStream\", \"collStats\", \"dbHash\", \"dbStats\", \"find\", \"killCursors\", \"listCollections\", \"listIndexes\", \"listSearchIndexes\", \"planCacheRead\"]}]}, \"ok\": 1}");
            when(mongoDatabase.runCommand(any())).thenReturn(document).thenReturn(new Document("setName","test"));
            Assertions.assertFalse(mongodbAtlasTest.testReadPrivilege());
        }
    }
}
