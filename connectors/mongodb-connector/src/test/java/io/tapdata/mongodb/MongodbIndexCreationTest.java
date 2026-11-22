package io.tapdata.mongodb;

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.CreateIndexCommitQuorum;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.*;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.bson.Document;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MongodbIndexCreationTest {
    private MongodbConnector connector;
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    private TapConnectorContext context;
    private TapTable table;
    private TapCreateIndexEvent event;
    private MongodbExceptionCollector errors;

    @BeforeEach
    void setUp() {
        connector = mock(MongodbConnector.class, CALLS_REAL_METHODS);
        database = mock(MongoDatabase.class);
        collection = mock(MongoCollection.class);
        context = mock(TapConnectorContext.class);
        table = mock(TapTable.class);
        event = mock(TapCreateIndexEvent.class);
        errors = mock(MongodbExceptionCollector.class);
        MongodbConfig config = new MongodbConfig();
        config.setDatabase("test");
        ReflectionTestUtils.setField(connector, "mongoDatabase", database);
        ReflectionTestUtils.setField(connector, "mongoConfig", config);
        ReflectionTestUtils.setField(connector, "mongoClient", mock(MongoClient.class));
        ReflectionTestUtils.setField(connector, "exceptionCollector", errors);
        when(table.getName()).thenReturn("records");
        when(database.getCollection("records")).thenReturn(collection);
        when(context.getLog()).thenReturn(mock(Log.class));
    }

    @Test
    void absentAndEmptyIndexesDoNotContactTheDatabase() {
        when(event.getIndexList()).thenReturn(null, Collections.emptyList());
        connector.createIndex(context, table, event);
        connector.createIndex(context, table, event);
        verifyNoInteractions(database);
    }

    @Test
    void immutableIdIndexIsSkipped() {
        when(event.getIndexList()).thenReturn(Collections.singletonList(index("_id_", "_id")));
        connector.createIndex(context, table, event);
        verifyNoInteractions(collection);
    }

    @Test
    void cumulativeModelsRetainNamesDirectionsAndUniqueness() {
        TapIndex first = index("by_a", "a");
        first.unique(true);
        TapIndex second = index("by_b", "b");
        second.unique(false);
        second.getIndexFields().get(0).fieldAsc(false);
        List<List<IndexModel>> submissions = new ArrayList<>();
        when(collection.createIndexes(anyList())).thenAnswer(call -> {
            submissions.add(new ArrayList<>(call.getArgument(0)));
            return Collections.emptyList();
        });
        when(event.getIndexList()).thenReturn(Arrays.asList(first, second));
        connector.createIndex(context, table, event);
        assertEquals(1, submissions.get(0).size());
        assertEquals(2, submissions.get(1).size());
        IndexModel a = submissions.get(1).get(0);
        IndexModel b = submissions.get(1).get(1);
        assertEquals(new Document("a", 1), a.getKeys());
        assertEquals(new Document("b", -1), b.getKeys());
        assertEquals("by_a", a.getOptions().getName());
        assertTrue(a.getOptions().isUnique());
        assertFalse(b.getOptions().isUnique());
    }

    @ParameterizedTest
    @ValueSource(ints = {85, 86})
    void existingIndexConflictsDoNotPreventFollowingIndexes(int code) {
        MongoCommandException failure = commandFailure(code, code == 85 ? "IndexOptionsConflict" : "IndexKeySpecsConflict");
        doThrow(failure).doReturn(Collections.emptyList()).when(collection).createIndexes(anyList());
        when(event.getIndexList()).thenReturn(Arrays.asList(index("by_a", "a"), index("by_b", "b")));
        assertDoesNotThrow(() -> connector.createIndex(context, table, event));
        verify(collection, times(2)).createIndexes(anyList());
        verifyNoInteractions(errors);
    }

    @Test
    void unrelatedCommandFailuresArePropagated() {
        MongoCommandException failure = commandFailure(85, "AnotherFailure");
        when(event.getIndexList()).thenReturn(Collections.singletonList(index("by_a", "a")));
        when(collection.createIndexes(anyList())).thenThrow(failure);
        assertSame(failure, assertThrows(MongoCommandException.class,
                () -> connector.createIndex(context, table, event)));
        verify(errors).throwWriteExIfNeed(null, failure);
    }

    @Test
    void emptyFieldsKeepTheExistingSubmissionBehavior() {
        TapIndex empty = new TapIndex();
        empty.setIndexFields(Collections.emptyList());
        when(event.getIndexList()).thenReturn(Collections.singletonList(empty));
        connector.createIndex(context, table, event);
        verify(collection).createIndexes(Collections.emptyList());
    }

    @Test
    void supportedServersReceiveMajorityCommitQuorum() {
        TapIndex unnamed = index(null, "a");
        when(event.getIndexList()).thenReturn(Collections.singletonList(unnamed));
        try (MockedStatic<MongodbUtil> utilities = mockStatic(MongodbUtil.class)) {
            utilities.when(() -> MongodbUtil.getVersionString(any(), eq("test"))).thenReturn("4.4.0");
            connector.createIndex(context, table, event);
            verify(collection).createIndexes(anyList(), argThat(options ->
                    CreateIndexCommitQuorum.MAJORITY.equals(options.getCommitQuorum())));
        }
    }

    private MongoCommandException commandFailure(int code, String name) {
        return new MongoCommandException(new BsonDocument("code", new BsonInt32(code))
                .append("codeName", new BsonString(name)).append("errmsg", new BsonString("index failure")),
                new ServerAddress("127.0.0.1", 27017));
    }

    private TapIndex index(String name, String field) {
        TapIndex index = new TapIndex();
        index.setName(name);
        index.setIndexFields(Collections.singletonList(new TapIndexField().name(field).fieldAsc(true)));
        return index;
    }
}
