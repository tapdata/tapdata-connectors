package io.tapdata.mongodb.reader;

import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

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
}