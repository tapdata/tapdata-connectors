package io.tapdata.mongodb.writer;

import com.mongodb.client.MongoClient;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * @author samuel
 * @Description
 * @create 2024-08-19 15:02
 **/
@DisplayName("Class MongodbWriter Test")
class MongodbWriterTest {

	private KVMap<Object> globalStateMap;
	private MongodbConfig mongodbConfig;
	private MongoClient mongoClient;
	private Log log;
	private Map<String, Set<String>> shardKeyMap;
	private MongodbWriter mongodbWriter;

	@BeforeEach
	void setUp() {
		globalStateMap = new KVMap<Object>() {
			Map<String, Object> map = new HashMap<>();
			@Override
			public Object get(String key) {
				return map.get(key);
			}

			@Override
			public void init(String mapKey, Class valueClass) {

			}

			@Override
			public void put(String key, Object o) {
				map.put(key, o);
			}

			@Override
			public Object putIfAbsent(String key, Object o) {
				return map.putIfAbsent(key, o);
			}

			@Override
			public Object remove(String key) {
				return map.remove(key);
			}

			@Override
			public void clear() {
				map.clear();
			}

			@Override
			public void reset() {
				map.clear();
			}
		};
		mongodbConfig = new MongodbConfig();
		mongodbConfig.setUri("mongodb://localhost:27017");
		mongodbConfig.setDatabase("test");
		mongoClient = mock(MongoClient.class);
		log = mock(Log.class);
		shardKeyMap = new HashMap<>();
		mongodbWriter = new MongodbWriter(globalStateMap, mongodbConfig, mongoClient, log, shardKeyMap);
	}

	@Nested
	@DisplayName("Method removeOidIfNeed test")
	class removeOidIfNeedTest {
		@Test
		@DisplayName("test pk is _id or contains _id")
		void test1() {
			List<String> pks = new ArrayList<>();
			pks.add("_id");

			List<TapRecordEvent> tapRecordEvents = new ArrayList<>();
			tapRecordEvents.add(TapInsertRecordEvent.create().init().after(new Document("_id", "1").append("title", "xxxxxxxx").append("createAt", Instant.now())));
			tapRecordEvents.add(TapInsertRecordEvent.create().init().after(new Document("_id", "2").append("title", "xxxxxxxx").append("createAt", Instant.now())));

			mongodbWriter.removeOidIfNeed(tapRecordEvents, pks);

			assertEquals(2, tapRecordEvents.size());
			assertEquals("1", ((TapInsertRecordEvent)tapRecordEvents.get(0)).getAfter().get("_id"));
			assertEquals("2", ((TapInsertRecordEvent) tapRecordEvents.get(1)).getAfter().get("_id"));

			pks.add("title");

			mongodbWriter.removeOidIfNeed(tapRecordEvents, pks);

			assertEquals(2, tapRecordEvents.size());
			assertEquals("1", ((TapInsertRecordEvent)tapRecordEvents.get(0)).getAfter().get("_id"));
			assertEquals("2", ((TapInsertRecordEvent) tapRecordEvents.get(1)).getAfter().get("_id"));
		}

		@Test
		@DisplayName("test pk not contains _id")
		void test2() {
			List<String> pks = new ArrayList<>();
			pks.add("uid");

			List<TapRecordEvent> tapRecordEvents = new ArrayList<>();
			tapRecordEvents.add(TapInsertRecordEvent.create().init().after(new Document("_id", "1").append("uid", 1).append("createAt", Instant.now())));
			tapRecordEvents.add(TapInsertRecordEvent.create().init().after(new Document("_id", "2").append("uid", 2).append("createAt", Instant.now())));
			tapRecordEvents.add(TapUpdateRecordEvent.create().init().after(new Document("_id", "3").append("uid", 3).append("createAt", Instant.now())));

			mongodbWriter.removeOidIfNeed(tapRecordEvents, pks);

			assertEquals(3, tapRecordEvents.size());
			assertFalse(((TapInsertRecordEvent)tapRecordEvents.get(0)).getAfter().containsKey("_id"));
			assertFalse(((TapInsertRecordEvent)tapRecordEvents.get(1)).getAfter().containsKey("_id"));
			assertFalse(((TapUpdateRecordEvent)tapRecordEvents.get(2)).getAfter().containsKey("_id"));
		}

		@Test
		@DisplayName("test merge info level is 1 and 2, pk not contains _id")
		void test3() {
			List<String> pks = new ArrayList<>();
			pks.add("uid");

			List<TapRecordEvent> tapRecordEvents = new ArrayList<>();
			MergeInfo mergeInfo1 = new MergeInfo();
			mergeInfo1.setLevel(1);
			TapInsertRecordEvent tapInsertRecordEvent1 = TapInsertRecordEvent.create().init().after(new Document("_id", "1").append("uid", 1).append("createAt", Instant.now()));
			tapInsertRecordEvent1.addInfo(MergeInfo.EVENT_INFO_KEY, mergeInfo1);
			tapRecordEvents.add(tapInsertRecordEvent1);
			MergeInfo mergeInfo2 = new MergeInfo();
			mergeInfo2.setLevel(2);
			TapInsertRecordEvent tapInsertRecordEvent2 = TapInsertRecordEvent.create().init().after(new Document("_id", "2").append("uid", 2).append("createAt", Instant.now()));
			tapInsertRecordEvent2.addInfo(MergeInfo.EVENT_INFO_KEY, mergeInfo2);
			tapRecordEvents.add(tapInsertRecordEvent2);

			mongodbWriter.removeOidIfNeed(tapRecordEvents, pks);

			assertEquals(2, tapRecordEvents.size());
			assertFalse(((TapInsertRecordEvent)tapRecordEvents.get(0)).getAfter().containsKey("_id"));
			assertTrue(((TapInsertRecordEvent)tapRecordEvents.get(1)).getAfter().containsKey("_id"));
		}

		@Test
		@DisplayName("test input params is null")
		void test4() {
			assertDoesNotThrow(() -> mongodbWriter.removeOidIfNeed(null, null));
			List<String> pks = new ArrayList<>();
			pks.add("uid");
			assertDoesNotThrow(() -> mongodbWriter.removeOidIfNeed(null, pks));
			List<TapRecordEvent> tapRecordEvents = new ArrayList<>();
			tapRecordEvents.add(TapInsertRecordEvent.create().init().after(new Document("_id", "1").append("title", "xxxxxxxx").append("createAt", Instant.now())));
			tapRecordEvents.add(TapInsertRecordEvent.create().init().after(new Document("_id", "2").append("title", "xxxxxxxx").append("createAt", Instant.now())));
			assertDoesNotThrow(() -> mongodbWriter.removeOidIfNeed(tapRecordEvents, null));
		}

		@Test
		@DisplayName("test after is null")
		void test5() {
			List<String> pks = new ArrayList<>();
			pks.add("uid");

			List<TapRecordEvent> tapRecordEvents = new ArrayList<>();
			tapRecordEvents.add(TapInsertRecordEvent.create().init().after(null));
			tapRecordEvents.add(TapInsertRecordEvent.create().init().after(null));

			assertDoesNotThrow(() -> mongodbWriter.removeOidIfNeed(tapRecordEvents, pks));

			assertEquals(2, tapRecordEvents.size());
			assertNull(((TapInsertRecordEvent)tapRecordEvents.get(0)).getAfter());
			assertNull(((TapInsertRecordEvent)tapRecordEvents.get(1)).getAfter());
		}
	}
}