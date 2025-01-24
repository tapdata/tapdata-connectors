package io.tapdata.mongodb.writer;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

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
	@DisplayName("Method wrapUnset test")
	class wrapUnsetTest {
		@Test
		@DisplayName("test insert event")
		void test1() {
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create()
					.after(new Document("id", 1).append("f1", 1))
					.removedFields(new ArrayList<String>() {{
						add("f2");
					}});
			Document document = mongodbWriter.wrapUnset(tapInsertRecordEvent);
			assertEquals(1, document.size());
			assertInstanceOf(Boolean.class, document.get("f2"));
			assertTrue((Boolean) document.get("f2"));
		}

		@Test
		@DisplayName("test update event")
		void test2() {
			TapUpdateRecordEvent tapUpdateRecordEvent = TapUpdateRecordEvent.create()
					.before(new Document("id", 1).append("f1", 1))
					.after(new Document("id", 1).append("f1", 2))
					.removedFields(new ArrayList<String>() {{
						add("f2");
					}});
			Document document = mongodbWriter.wrapUnset(tapUpdateRecordEvent);
			assertEquals(1, document.size());
			assertInstanceOf(Boolean.class, document.get("f2"));
			assertTrue((Boolean) document.get("f2"));
		}

		@Test
		@DisplayName("test delete event")
		void test3() {
			TapDeleteRecordEvent tapDeleteRecordEvent = TapDeleteRecordEvent.create()
					.before(new Document("id", 1));
			Document document = mongodbWriter.wrapUnset(tapDeleteRecordEvent);
			assertNull(document);
		}
	}

	@Nested
	@DisplayName("Method normalWriteMode test")
	class normalWriteModeTest {

		private TapTable tapTable;

		@BeforeEach
		void setUp() {
			tapTable = new TapTable("test");
			tapTable.putField("id", new TapField("id", "int"));
			tapTable.putField("f1", new TapField("f1", "int"));
			tapTable.putField("f2", new TapField("f2", "int"));
		}

		@Test
		@DisplayName("test insert have remove fields, insert policy is update_on_exists")
		void test1() {
			ReflectionTestUtils.setField(mongodbWriter, "insertPolicy", ConnectionOptions.DML_INSERT_POLICY_UPDATE_ON_EXISTS);
			List<String> pks = new ArrayList<>();
			pks.add("id");
			List<String> removeFields = new ArrayList<>();
			removeFields.add("f2");
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create()
					.after(new Document("id", 1).append("f1", 1))
					.removedFields(removeFields);
			WriteModel<Document> writeModel = mongodbWriter.normalWriteMode(new AtomicLong(), new AtomicLong(), new AtomicLong(), new UpdateOptions().upsert(true), tapTable, pks, tapInsertRecordEvent);
			assertInstanceOf(UpdateManyModel.class, writeModel);
			Document update = (Document) ((UpdateManyModel<Document>) writeModel).getUpdate();
			assertTrue(update.containsKey("$unset"));
		}

		@Test
		@DisplayName("test insert have remove fields, insert policy is just_insert")
		void test2() {
			ReflectionTestUtils.setField(mongodbWriter, "insertPolicy", ConnectionOptions.DML_INSERT_POLICY_JUST_INSERT);
			List<String> pks = new ArrayList<>();
			pks.add("id");
			List<String> removeFields = new ArrayList<>();
			removeFields.add("f2");
			TapInsertRecordEvent tapInsertRecordEvent = TapInsertRecordEvent.create()
					.after(new Document("id", 1).append("f1", 1))
					.removedFields(removeFields);
			WriteModel<Document> writeModel = mongodbWriter.normalWriteMode(new AtomicLong(), new AtomicLong(), new AtomicLong(), new UpdateOptions().upsert(true), tapTable, pks, tapInsertRecordEvent);
			assertInstanceOf(UpdateManyModel.class, writeModel);
			Document update = (Document) ((UpdateManyModel<Document>) writeModel).getUpdate();
			assertTrue(update.containsKey("$unset"));
		}

		@Test
		@DisplayName("test update have remove fields")
		void test3() {
			List<String> pks = new ArrayList<>();
			pks.add("id");
			List<String> removeFields = new ArrayList<>();
			removeFields.add("f2");
			TapUpdateRecordEvent tapUpdateRecordEvent = TapUpdateRecordEvent.create()
					.before(new Document("id", 1).append("f1", 1))
					.after(new Document("id", 1).append("f1", 2))
					.removedFields(removeFields);
			WriteModel<Document> writeModel = mongodbWriter.normalWriteMode(new AtomicLong(), new AtomicLong(), new AtomicLong(), new UpdateOptions().upsert(true), tapTable, pks, tapUpdateRecordEvent);
			assertInstanceOf(UpdateManyModel.class, writeModel);
			Document update = (Document) ((UpdateManyModel<Document>) writeModel).getUpdate();
			assertTrue(update.containsKey("$unset"));
		}
	}
}