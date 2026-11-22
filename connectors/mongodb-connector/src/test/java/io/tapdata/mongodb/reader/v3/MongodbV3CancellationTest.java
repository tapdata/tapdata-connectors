package io.tapdata.mongodb.reader.v3;

import com.mongodb.MongoInterruptedException;
import com.mongodb.client.*;
import io.tapdata.mongodb.util.IntervalReport;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.bson.*;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MongodbV3CancellationTest {
    @Test
    void canceledConsumerReturnsWithoutLosingInterrupt() throws Exception {
        MongodbV3StreamReader reader = runningReader();
        StreamReadConsumer consumer = mock(StreamReadConsumer.class);
        Thread.currentThread().interrupt();
        try {
            reader.read(mock(TapConnectorContext.class), Collections.emptyList(), null, 10, consumer);
            assertTrue(Thread.currentThread().isInterrupted());
            verifyNoInteractions(consumer);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void canceledShutdownDoesNotSpinWaitingForWorkerTermination() {
        MongodbV3StreamReader reader = runningReader();
        ThreadPoolExecutor pool = mock(ThreadPoolExecutor.class);
        ReflectionTestUtils.setField(reader, "replicaSetReadThreadPool", pool);
        Thread.currentThread().interrupt();
        try {
            reader.onDestroy();
            assertTrue(Thread.currentThread().isInterrupted());
            verify(pool).shutdownNow();
            verify(pool, times(1)).isTerminated();
            assertFalse(running(reader).get());
        } finally {
            Thread.interrupted();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void oplogCancellationClosesCursorAndStopsWorker(boolean queueInterrupted) throws Exception {
        MongodbV3StreamReader reader = runningReader();
        reader.namespaces.add("test.orders");
        ReflectionTestUtils.setField(reader, "offset", Collections.singletonMap("rs", new MongoV3StreamOffset(1, 0)));
        ReflectionTestUtils.setField(reader, "tapEventQueue", new LinkedBlockingDeque<TapEventOffset>(1));
        MongoClient client = mock(MongoClient.class);
        MongoDatabase database = mock(MongoDatabase.class);
        MongoCollection<Document> collection = mock(MongoCollection.class);
        FindIterable<Document> iterable = mock(FindIterable.class, RETURNS_SELF);
        MongoCursor<Document> first = mock(MongoCursor.class);
        MongoCursor<Document> ongoing = mock(MongoCursor.class);
        when(client.getDatabase("local")).thenReturn(database);
        when(database.getCollection("oplog.rs")).thenReturn(collection);
        when(collection.find(any(Bson.class))).thenReturn(iterable);
        when(iterable.iterator()).thenReturn(first, ongoing);
        when(first.hasNext()).thenReturn(true);
        when(first.next()).thenReturn(new Document("ts", new BsonTimestamp(1, 0)));
        if (queueInterrupted) {
            when(ongoing.hasNext()).thenReturn(true);
            when(ongoing.next()).thenReturn(new Document("ns", "test.orders").append("op", "i")
                    .append("o", new Document("_id", 1)).append("ts", new BsonTimestamp(2, 0)));
        } else {
            when(ongoing.hasNext()).thenThrow(new MongoInterruptedException("cursor canceled", new InterruptedException()));
        }
        try (MockedStatic<MongoClients> clients = mockStatic(MongoClients.class)) {
            clients.when(() -> MongoClients.create(anyString())).thenReturn(client);
            if (queueInterrupted) Thread.currentThread().interrupt();
            ReflectionTestUtils.invokeMethod(reader, "readFromOplog", mock(TapConnectorContext.class), "rs",
                    mock(IntervalReport.class), "mongodb://localhost/test", 10, mock(StreamReadConsumer.class));
            assertEquals(queueInterrupted, Thread.currentThread().isInterrupted());
            assertFalse(running(reader).get());
            verify(first).close();
            verify(ongoing).close();
            verify(client).close();
        } finally {
            Thread.interrupted();
        }
    }

    private MongodbV3StreamReader runningReader() {
        MongodbV3StreamReader reader = new MongodbV3StreamReader();
        running(reader).set(true);
        return reader;
    }

    private AtomicBoolean running(MongodbV3StreamReader reader) {
        return (AtomicBoolean) ReflectionTestUtils.getField(reader, "running");
    }
}
