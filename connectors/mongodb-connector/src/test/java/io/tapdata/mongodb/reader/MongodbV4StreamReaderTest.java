package io.tapdata.mongodb.reader;

import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.ReadConcern;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.model.changestream.FullDocument;
import io.tapdata.mongodb.entity.MongoCdcOffset;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import java.util.Date;
import java.util.stream.Stream;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapdata.exception.TapCodeException;
import io.tapdata.mongodb.error.MongodbErrorCode;
import io.tapdata.common.concurrent.SimpleConcurrentProcessorImpl;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.mongodb.entity.MongodbConfig;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-05-08 17:53
 **/
@DisplayName("Class MongodbV4StreamReader Test")
class MongodbV4StreamReaderTest {
    @Test
    void shouldStartAfterExactCapturedTimestamp() {
        MongoDatabase database = mock(MongoDatabase.class);
        MongodbV4StreamReader reader = new MongodbV4StreamReader();
        ReflectionTestUtils.setField(reader, "mongoDatabase", database);
        BsonTimestamp captured = new BsonTimestamp(1784769971, 1016);
        when(database.runCommand(new Document("isMaster", 1))).thenReturn(
                new Document("$clusterTime", new Document("clusterTime", captured)));

        BsonTimestamp start = (BsonTimestamp) reader.streamOffset(null);
        Assertions.assertEquals(new BsonTimestamp(captured.getTime(), 1017), start);
        // Old inserts and the captured boundary must not be replayed, while the
        // immediately following operation in the same second must remain visible.
        for (int inc = 1006; inc <= 1016; inc++) {
            Assertions.assertTrue(new BsonTimestamp(captured.getTime(), inc).compareTo(start) < 0);
        }
        Assertions.assertEquals(0, new BsonTimestamp(captured.getTime(), 1017).compareTo(start));
        Assertions.assertEquals(start, MongoCdcOffset.fromOffset(new MongoCdcOffset(null, start).toOffset()).getCdcOffset());
    }

    @Test
    void shouldCarryIncrementOverflowToNextSecond() {
        MongoDatabase database = mock(MongoDatabase.class);
        MongodbV4StreamReader reader = new MongodbV4StreamReader();
        ReflectionTestUtils.setField(reader, "mongoDatabase", database);
        when(database.runCommand(any())).thenReturn(new Document("$clusterTime",
                new Document("clusterTime", new BsonTimestamp(1784769971, -1))));
        Assertions.assertEquals(new BsonTimestamp(1784769972, 0), reader.streamOffset(null));
    }

    static Stream<Arguments> serverTimeFallbacks() {
        return Stream.of(
                Arguments.of(new Document("localTime", new Date(1784769971123L)), 1784769971),
                Arguments.of(new Document("localTime", new Date(1000L)).append("$clusterTime",
                        new Document("clusterTime", new Date(1784769971123L))), 1784769971),
                Arguments.of(new Document(), 0));
    }

    @ParameterizedTest
    @MethodSource("serverTimeFallbacks")
    void shouldPreserveServerTimeFallback(Document response, int expected) {
        MongoDatabase database = mock(MongoDatabase.class);
        MongodbV4StreamReader reader = new MongodbV4StreamReader();
        ReflectionTestUtils.setField(reader, "mongoDatabase", database);
        when(database.runCommand(any())).thenReturn(response);
        Assertions.assertEquals(expected, reader.streamOffset(null));
        verify(database, times(1)).runCommand(new Document("isMaster", 1));
    }

    @Test
    void shouldPreserveExplicitStartTime() {
        MongoDatabase database = mock(MongoDatabase.class);
        MongodbV4StreamReader reader = new MongodbV4StreamReader();
        ReflectionTestUtils.setField(reader, "mongoDatabase", database);
        Assertions.assertEquals(1784769971, reader.streamOffset(1784769971123L));
        verifyNoInteractions(database);
    }

    static Stream<Arguments> readOffsets() {
        return Stream.of(
                Arguments.of(new BsonTimestamp(1784769971, 1017)),
                Arguments.of(1784769971),
                Arguments.of(new BsonDocument("_data", new BsonString("resume-token"))),
                Arguments.of((Object) null));
    }

    @ParameterizedTest
    @MethodSource("readOffsets")
    @SuppressWarnings("unchecked")
    void shouldApplyCompatibleReadOffset(Object offset) throws Exception {
        MongoDatabase database = mock(MongoDatabase.class);
        ChangeStreamIterable<RawBsonDocument> stream = mock(ChangeStreamIterable.class, RETURNS_SELF);
        MongodbV4StreamReader reader = new MongodbV4StreamReader();
        AtomicBoolean running = new AtomicBoolean(true);
        ReflectionTestUtils.setField(reader, "mongoDatabase", database);
        ReflectionTestUtils.setField(reader, "mongodbConfig", new MongodbConfig());
        ReflectionTestUtils.setField(reader, "running", running);
        ReflectionTestUtils.setField(reader, "concurrentProcessor", mock(SimpleConcurrentProcessorImpl.class));
        when(database.withReadConcern(ReadConcern.MAJORITY)).thenReturn(database);
        when(database.watch(anyList(), eq(RawBsonDocument.class))).thenReturn(stream);
        // Stop at cursor creation, after all resume options have been applied.
        when(stream.cursor()).thenAnswer(invocation -> {
            running.set(false);
            throw new MongoInterruptedException("Stopped for test", new InterruptedException());
        });
        reader.read(mock(TapConnectorContext.class), Arrays.asList("test"), offset, 10,
                mock(StreamReadConsumer.class));

        if (offset instanceof BsonTimestamp) {
            verify(stream).startAtOperationTime((BsonTimestamp) offset);
            verify(stream, never()).startAfter(any());
        } else if (offset instanceof Integer) {
            verify(stream).startAtOperationTime(new BsonTimestamp((Integer) offset, 0));
            verify(stream, never()).startAfter(any());
        } else if (offset instanceof BsonDocument) {
            verify(stream).startAfter((BsonDocument) offset);
            verify(stream, never()).startAtOperationTime(any());
        } else {
            verify(stream, never()).startAfter(any());
            verify(stream, never()).startAtOperationTime(any());
        }
        verify(stream).fullDocument(FullDocument.DEFAULT);
    }

	@Test
	void testOnDestroy() {
		MongoClient mongoClient = mock(MongoClient.class);
		AtomicBoolean running = new AtomicBoolean(true);
		MongodbV4StreamReader mongodbV4StreamReader = new MongodbV4StreamReader();
		ReflectionTestUtils.setField(mongodbV4StreamReader, "mongoClient", mongoClient);
		ReflectionTestUtils.setField(mongodbV4StreamReader, "running", running);

		mongodbV4StreamReader.onDestroy();

		assertFalse(((AtomicBoolean) ReflectionTestUtils.getField(mongodbV4StreamReader, "running")).get());
		verify(mongoClient, times(1)).close();
		assertNull(ReflectionTestUtils.getField(mongodbV4StreamReader, "mongoClient"));

		ReflectionTestUtils.setField(mongodbV4StreamReader, "mongoClient", null);
		running.set(true);
		ReflectionTestUtils.setField(mongodbV4StreamReader, "running", running);

		mongodbV4StreamReader.onDestroy();

		assertFalse(((AtomicBoolean) ReflectionTestUtils.getField(mongodbV4StreamReader, "running")).get());
	}

	@Test
	void testOnStartUsesDecodeConfig() {
		MongodbConfig mongodbConfig = mock(MongodbConfig.class);
		when(mongodbConfig.getUri()).thenReturn("mongodb://localhost:27017/test");
		when(mongodbConfig.getDecodeThreads()).thenReturn(2);
		when(mongodbConfig.getDecodeQueueSize()).thenReturn(4);
		MongoClient mongoClient = mock(MongoClient.class);
		MongodbV4StreamReader mongodbV4StreamReader = new MongodbV4StreamReader();
		ReflectionTestUtils.setField(mongodbV4StreamReader, "mongoClient", mongoClient);
		@SuppressWarnings("unchecked")
		SimpleConcurrentProcessorImpl<?, ?> processor = mock(SimpleConcurrentProcessorImpl.class);

		try (MockedStatic<TapExecutors> tapExecutors = mockStatic(TapExecutors.class)) {
			tapExecutors.when(() -> TapExecutors.createSimple(2, 4, "MongodbV4StreamReader-Processor")).thenReturn(processor);
			mongodbV4StreamReader.onStart(mongodbConfig);
			tapExecutors.verify(() -> TapExecutors.createSimple(2, 4, "MongodbV4StreamReader-Processor"));
		}
	}

	@Test
	void testOnStartUsesDefaultDecodeConfig() {
		MongodbConfig mongodbConfig = new MongodbConfig();
		mongodbConfig.setUri("mongodb://localhost:27017/test");
		MongoClient mongoClient = mock(MongoClient.class);
		MongodbV4StreamReader mongodbV4StreamReader = new MongodbV4StreamReader();
		ReflectionTestUtils.setField(mongodbV4StreamReader, "mongoClient", mongoClient);
		@SuppressWarnings("unchecked")
		SimpleConcurrentProcessorImpl<?, ?> processor = mock(SimpleConcurrentProcessorImpl.class);

		try (MockedStatic<TapExecutors> tapExecutors = mockStatic(TapExecutors.class)) {
			tapExecutors.when(() -> TapExecutors.createSimple(8, 32, "MongodbV4StreamReader-Processor")).thenReturn(processor);
			mongodbV4StreamReader.onStart(mongodbConfig);
			tapExecutors.verify(() -> TapExecutors.createSimple(8, 32, "MongodbV4StreamReader-Processor"));
		}
	}

    @Nested
	class OpenChangeStreamPreAndPostImagesTest{
		MongoDatabase mongoDatabase;
		MongodbV4StreamReader mongodbV4StreamReader;
		@BeforeEach
		void setUp(){
			mongoDatabase = mock(MongoDatabase.class);
			mongodbV4StreamReader = new MongodbV4StreamReader();
			ReflectionTestUtils.setField(mongodbV4StreamReader, "mongoDatabase", mongoDatabase);
			ReflectionTestUtils.setField(mongodbV4StreamReader,"isPreImage",true);
		}
		@Test
		void testOpenedSuccessfully(){
			when(mongoDatabase.runCommand(any())).thenReturn(new Document());
			mongodbV4StreamReader.openChangeStreamPreAndPostImages(Arrays.asList("test"));
			verify(mongoDatabase,times(0)).createCollection("test");
		}

		@Test
		void testTableNotExist(){
			MongoException mongoException = new MongoException(26,"Collection test does not exist");
			when(mongoDatabase.runCommand(any())).thenThrow(mongoException).thenReturn(new Document());
			mongodbV4StreamReader.openChangeStreamPreAndPostImages(Arrays.asList("test"));
			verify(mongoDatabase,times(1)).createCollection("test");
			verify(mongoDatabase,times(2)).runCommand(any());
		}

		@Test
		void testError(){
			MongoException mongoException = new MongoException(27,"error");
			when(mongoDatabase.runCommand(any())).thenThrow(mongoException);
			Assertions.assertThrows(MongoException.class,()->mongodbV4StreamReader.openChangeStreamPreAndPostImages(Arrays.asList("test")));
		}

		@Test
		void testNoCollModPrivilegeButAlreadyEnabled(){
			MongoException mongoException = new MongoException(13,"not authorized on test to execute command collMod");
			Document listCollectionsResult = new Document("cursor",
					new Document("firstBatch", Arrays.asList(new Document("options",
							new Document("changeStreamPreAndPostImages", new Document("enabled", true))))));
			when(mongoDatabase.runCommand(any())).thenThrow(mongoException).thenReturn(listCollectionsResult);
			mongodbV4StreamReader.openChangeStreamPreAndPostImages(Arrays.asList("test"));
			verify(mongoDatabase,times(0)).createCollection("test");
			verify(mongoDatabase,times(2)).runCommand(any());
		}

		@Test
		void testNoCollModPrivilegeAndNotEnabled(){
			MongoException mongoException = new MongoException(13,"not authorized on test to execute command collMod");
			when(mongoDatabase.runCommand(any())).thenThrow(mongoException).thenReturn(new Document());
			TapCodeException tapCodeException = Assertions.assertThrows(TapCodeException.class,
					()->mongodbV4StreamReader.openChangeStreamPreAndPostImages(Arrays.asList("test")));
			Assertions.assertEquals(MongodbErrorCode.PRE_POST_IMAGES_PRIVILEGE_MISSING, tapCodeException.getCode());
		}

	}
}