package io.tapdata.mongodb.entity;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("Class MongodbConfig Test")
class MongodbConfigTest {

	@Test
	void testDecodeConfigDefaults() {
		MongodbConfig mongodbConfig = new MongodbConfig();
		assertEquals(MongodbConfig.DEFAULT_DECODE_THREADS, mongodbConfig.getDecodeThreads());
		assertEquals(MongodbConfig.DEFAULT_DECODE_QUEUE_SIZE, mongodbConfig.getDecodeQueueSize());
	}

	@Test
	void testDecodeConfigCustomValues() {
		MongodbConfig mongodbConfig = new MongodbConfig();
		mongodbConfig.setDecodeThreads(2);
		mongodbConfig.setDecodeQueueSize(4);
		assertEquals(2, mongodbConfig.getDecodeThreads());
		assertEquals(4, mongodbConfig.getDecodeQueueSize());
	}

	@Test
	void testDecodeConfigInvalidValuesFallBackToDefault() {
		MongodbConfig mongodbConfig = new MongodbConfig();
		mongodbConfig.setDecodeThreads(0);
		mongodbConfig.setDecodeQueueSize(-1);
		assertEquals(MongodbConfig.DEFAULT_DECODE_THREADS, mongodbConfig.getDecodeThreads());
		assertEquals(MongodbConfig.DEFAULT_DECODE_QUEUE_SIZE, mongodbConfig.getDecodeQueueSize());
	}
}
