package io.tapdata.mongodb.writer;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.UpdateManyModel;
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
		// 设置app_type系统属性，避免AppType初始化失败
		System.setProperty("app_type", "DAAS");

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
			assertEquals("1", ((TapInsertRecordEvent) tapRecordEvents.get(0)).getAfter().get("_id"));
			assertEquals("2", ((TapInsertRecordEvent) tapRecordEvents.get(1)).getAfter().get("_id"));

			pks.add("title");

			mongodbWriter.removeOidIfNeed(tapRecordEvents, pks);

			assertEquals(2, tapRecordEvents.size());
			assertEquals("1", ((TapInsertRecordEvent) tapRecordEvents.get(0)).getAfter().get("_id"));
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
			assertFalse(((TapInsertRecordEvent) tapRecordEvents.get(0)).getAfter().containsKey("_id"));
			assertFalse(((TapInsertRecordEvent) tapRecordEvents.get(1)).getAfter().containsKey("_id"));
			assertFalse(((TapUpdateRecordEvent) tapRecordEvents.get(2)).getAfter().containsKey("_id"));
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
			assertFalse(((TapInsertRecordEvent) tapRecordEvents.get(0)).getAfter().containsKey("_id"));
			assertTrue(((TapInsertRecordEvent) tapRecordEvents.get(1)).getAfter().containsKey("_id"));
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
			assertNull(((TapInsertRecordEvent) tapRecordEvents.get(0)).getAfter());
			assertNull(((TapInsertRecordEvent) tapRecordEvents.get(1)).getAfter());
		}
	}

	@Nested
	@DisplayName("Method wrapUnset test")
	class wrapUnsetTest {

		@Test
		@DisplayName("test TapInsertRecordEvent with removedFields and after")
		void testInsertEventWithRemovedFields() {
			// 准备测试数据
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("name", "test");
			after.put("address", "beijing");

			List<String> removedFields = new ArrayList<>();
			removedFields.add("age");
			removedFields.add("email");

			TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create()
					.after(after)
					.removedFields(removedFields);

			// 执行测试
			Document result = mongodbWriter.wrapUnset(insertEvent);

			// 验证结果
			assertNotNull(result);
			assertEquals(2, result.size());
			assertTrue(result.containsKey("age"));
			assertTrue(result.containsKey("email"));
			assertEquals(true, result.get("age"));
			assertEquals(true, result.get("email"));
		}

		@Test
		@DisplayName("test TapUpdateRecordEvent with removedFields and after")
		void testUpdateEventWithRemovedFields() {
			// 准备测试数据
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("name", "updated");

			List<String> removedFields = new ArrayList<>();
			removedFields.add("age");
			removedFields.add("phone");

			TapUpdateRecordEvent updateEvent = TapUpdateRecordEvent.create()
					.after(after)
					.removedFields(removedFields);

			// 执行测试
			Document result = mongodbWriter.wrapUnset(updateEvent);

			// 验证结果
			assertNotNull(result);
			assertEquals(2, result.size());
			assertTrue(result.containsKey("age"));
			assertTrue(result.containsKey("phone"));
		}

		@Test
		@DisplayName("test with null removedFields")
		void testNullRemovedFields() {
			// 准备测试数据
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("name", "test");

			TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create()
					.after(after)
					.removedFields(null);

			// 执行测试
			Document result = mongodbWriter.wrapUnset(insertEvent);

			// 验证结果
			assertNull(result);
		}

		@Test
		@DisplayName("test with empty removedFields")
		void testEmptyRemovedFields() {
			// 准备测试数据
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("name", "test");

			TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create()
					.after(after)
					.removedFields(new ArrayList<>());

			// 执行测试
			Document result = mongodbWriter.wrapUnset(insertEvent);

			// 验证结果
			assertNull(result);
		}

		@Test
		@DisplayName("test with null after")
		void testNullAfter() {
			// 准备测试数据
			List<String> removedFields = new ArrayList<>();
			removedFields.add("age");
			removedFields.add("email");

			TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create()
					.after(null)
					.removedFields(removedFields);

			// 执行测试
			Document result = mongodbWriter.wrapUnset(insertEvent);

			// 验证结果
			assertNull(result);
		}

		@Test
		@DisplayName("test with empty after")
		void testEmptyAfter() {
			// 准备测试数据
			List<String> removedFields = new ArrayList<>();
			removedFields.add("age");
			removedFields.add("email");

			TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create()
					.after(new HashMap<>())
					.removedFields(removedFields);

			// 执行测试
			Document result = mongodbWriter.wrapUnset(insertEvent);

			// 验证结果
			assertNull(result);
		}

		@Test
		@DisplayName("test hierarchical fields filtering - parent field exists")
		void testHierarchicalFieldsFilteringParentExists() {
			// 准备测试数据
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("name", "test");

			// removedFields包含层级字段，应该只保留最高层级的字段
			List<String> removedFields = new ArrayList<>();
			removedFields.add("user");
			removedFields.add("user.name");
			removedFields.add("user.age");
			removedFields.add("user.profile.email");

			TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create()
					.after(after)
					.removedFields(removedFields);

			// 执行测试
			Document result = mongodbWriter.wrapUnset(insertEvent);

			// 验证结果 - 应该只包含最高层级的字段 "user"
			assertNotNull(result);
			assertEquals(1, result.size());
			assertTrue(result.containsKey("user"));
			assertFalse(result.containsKey("user.name"));
			assertFalse(result.containsKey("user.age"));
			assertFalse(result.containsKey("user.profile.email"));
		}

		@Test
		@DisplayName("test hierarchical fields filtering - no parent field")
		void testHierarchicalFieldsFilteringNoParent() {
			// 准备测试数据
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("name", "test");

			// removedFields包含同级字段
			List<String> removedFields = new ArrayList<>();
			removedFields.add("user.name");
			removedFields.add("user.age");
			removedFields.add("profile.email");

			TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create()
					.after(after)
					.removedFields(removedFields);

			// 执行测试
			Document result = mongodbWriter.wrapUnset(insertEvent);

			// 验证结果 - 应该包含所有字段，因为没有父字段覆盖
			assertNotNull(result);
			assertEquals(3, result.size());
			assertTrue(result.containsKey("user.name"));
			assertTrue(result.containsKey("user.age"));
			assertTrue(result.containsKey("profile.email"));
		}

		@Test
		@DisplayName("test field exists in after - should be excluded from unset")
		void testFieldExistsInAfter() {
			// 准备测试数据
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("name", "test");
			after.put("age", 25); // age字段在after中存在

			List<String> removedFields = new ArrayList<>();
			removedFields.add("age");
			removedFields.add("email");
			removedFields.add("phone");

			TapInsertRecordEvent insertEvent = TapInsertRecordEvent.create()
					.after(after)
					.removedFields(removedFields);

			// 执行测试
			Document result = mongodbWriter.wrapUnset(insertEvent);

			// 验证结果 - age字段在after中存在，不应该被unset
			assertNotNull(result);
			assertEquals(2, result.size());
			assertFalse(result.containsKey("age")); // age不应该在unset中
			assertTrue(result.containsKey("email"));
			assertTrue(result.containsKey("phone"));
		}

		@Test
		@DisplayName("test TapDeleteRecordEvent - should return null")
		void testDeleteEvent() {
			// 准备测试数据
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("name", "test");

			TapDeleteRecordEvent deleteEvent = TapDeleteRecordEvent.create()
					.before(before);

			// 执行测试
			Document result = mongodbWriter.wrapUnset(deleteEvent);

			// 验证结果 - delete事件不支持，应该返回null
			assertNull(result);
		}

		@Test
		@DisplayName("test unsupported event type")
		void testUnsupportedEventType() {
			// 创建一个不支持的事件类型
			TapRecordEvent unsupportedEvent = new TapRecordEvent(999) {
				@Override
				public Map<String, Object> getFilter(Collection<String> primaryKeys) {
					return null;
				}
			};

			// 执行测试
			Document result = mongodbWriter.wrapUnset(unsupportedEvent);

			// 验证结果
			assertNull(result);
		}

		@Test
		@DisplayName("test single field scenarios")
		void testSingleFieldScenarios() {
			// 测试单个字段的各种情况
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);

			// 情况1：单个字段不在after中
			List<String> removedFields1 = new ArrayList<>();
			removedFields1.add("name");
			TapInsertRecordEvent event1 = TapInsertRecordEvent.create()
					.after(after)
					.removedFields(removedFields1);
			Document result1 = mongodbWriter.wrapUnset(event1);
			assertNotNull(result1);
			assertEquals(1, result1.size());
			assertTrue(result1.containsKey("name"));

			// 情况2：单个字段在after中
			after.put("name", "test");
			TapInsertRecordEvent event2 = TapInsertRecordEvent.create()
					.after(after)
					.removedFields(removedFields1);
			Document result2 = mongodbWriter.wrapUnset(event2);
			assertNotNull(result2);
			assertEquals(0, result2.size()); // name在after中，不应该unset
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
			List<WriteModel<Document>> writeModels = mongodbWriter.normalWriteMode(new AtomicLong(), new AtomicLong(), new AtomicLong(), new UpdateOptions().upsert(true), tapTable, pks, tapInsertRecordEvent);
			assertNotNull(writeModels);
			assertEquals(2, writeModels.size());
			assertInstanceOf(UpdateManyModel.class, writeModels.get(0));
			Document update = (Document) ((UpdateManyModel<Document>) (writeModels.get(0))).getUpdate();
			assertTrue(update.containsKey("$set"));
			assertInstanceOf(UpdateManyModel.class, writeModels.get(1));
			update = (Document) ((UpdateManyModel<Document>) (writeModels.get(1))).getUpdate();
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
			List<WriteModel<Document>> writeModels = mongodbWriter.normalWriteMode(new AtomicLong(), new AtomicLong(), new AtomicLong(), new UpdateOptions().upsert(true), tapTable, pks, tapInsertRecordEvent);
			assertNotNull(writeModels);
			assertEquals(2, writeModels.size());
			assertInstanceOf(UpdateManyModel.class, writeModels.get(0));
			Document update = (Document) ((UpdateManyModel<Document>) (writeModels.get(0))).getUpdate();
			assertTrue(update.containsKey("$set"));
			assertInstanceOf(UpdateManyModel.class, writeModels.get(1));
			update = (Document) ((UpdateManyModel<Document>) (writeModels.get(1))).getUpdate();
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
			List<WriteModel<Document>> writeModels = mongodbWriter.normalWriteMode(new AtomicLong(), new AtomicLong(), new AtomicLong(), new UpdateOptions().upsert(true), tapTable, pks, tapUpdateRecordEvent);
			assertNotNull(writeModels);
			assertEquals(2, writeModels.size());
			assertInstanceOf(UpdateManyModel.class, writeModels.get(0));
			Document update = (Document) ((UpdateManyModel<Document>) (writeModels.get(0))).getUpdate();
			assertTrue(update.containsKey("$set"));
			assertInstanceOf(UpdateManyModel.class, writeModels.get(1));
			update = (Document) ((UpdateManyModel<Document>) (writeModels.get(1))).getUpdate();
			assertTrue(update.containsKey("$unset"));
		}
	}
}