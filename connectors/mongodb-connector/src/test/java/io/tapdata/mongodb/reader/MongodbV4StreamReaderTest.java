package io.tapdata.mongodb.reader;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.*;
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
			Assertions.assertThrows(RuntimeException.class,()->mongodbV4StreamReader.openChangeStreamPreAndPostImages(Arrays.asList("test")));
		}

	}
}