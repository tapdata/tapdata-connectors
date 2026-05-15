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
	@DisplayName("Method dumpBulkWriteErrorContext test")
	class dumpBulkWriteErrorContextTest {
		@Test
		@DisplayName("should write dump file with thread/database/collection/namespace and minute-level filename")
		void testDumpToFileIncludesMetadata() throws Exception {
			// Prepare
			com.mongodb.client.MongoCollection<Document> collection = org.mockito.Mockito.mock(com.mongodb.client.MongoCollection.class);
			org.mockito.Mockito.when(collection.getNamespace()).thenReturn(new com.mongodb.MongoNamespace("ut_db", "ut_col"));
			BulkWriteModel bulkWriteModel = new BulkWriteModel(false);
			// mock 100 mixed write models: insert / update / upsert / delete
			for (int i = 0; i < 100; i++) {
				if (i < 30) {
					org.bson.Document doc = new org.bson.Document("_id", i).append("k", "v" + i);
					com.mongodb.client.model.InsertOneModel<org.bson.Document> insert = new com.mongodb.client.model.InsertOneModel<>(doc);
					bulkWriteModel.addAnyOpModel(insert);
				} else if (i < 60) {
					// update without upsert
					com.mongodb.client.model.UpdateOneModel<org.bson.Document> update =
							new com.mongodb.client.model.UpdateOneModel<>(
									com.mongodb.client.model.Filters.eq("_id", i),
									new org.bson.Document("$set", new org.bson.Document("k", "upd" + i)));
					bulkWriteModel.addAnyOpModel(update);
				} else if (i < 90) {
					// update with upsert
					com.mongodb.client.model.UpdateOneModel<org.bson.Document> upsert =
							new com.mongodb.client.model.UpdateOneModel<>(
									com.mongodb.client.model.Filters.eq("_id", i),
									new org.bson.Document("$set", new org.bson.Document("k", "upsert" + i)),
									new com.mongodb.client.model.UpdateOptions().upsert(true));
					bulkWriteModel.addAnyOpModel(upsert);
				} else {
					// delete
					com.mongodb.client.model.DeleteOneModel<org.bson.Document> delete =
							new com.mongodb.client.model.DeleteOneModel<>(
									com.mongodb.client.model.Filters.eq("_id", i));
					bulkWriteModel.addAnyOpModel(delete);
				}
			}
			com.mongodb.client.model.BulkWriteOptions bulkWriteOptions = new com.mongodb.client.model.BulkWriteOptions();
			String threadName = Thread.currentThread().getName();
			String sanitizedThread = threadName.replaceAll("[^a-zA-Z0-9_.-]", "_");

				// fabricate a MongoBulkWriteException with random index errors
				java.util.Random rnd = new java.util.Random();
				java.util.List<com.mongodb.bulk.BulkWriteError> errors = new java.util.ArrayList<>();
				int opsSize = bulkWriteModel.getAllOpWriteModels() != null ? bulkWriteModel.getAllOpWriteModels().size() : 0;
				if (opsSize > 0) {
					int maxErr = Math.min(5, opsSize);
					int errCount = 1 + rnd.nextInt(maxErr);
					java.util.Set<java.lang.Integer> usedIdx = new java.util.HashSet<>();
					for (int i = 0; i < errCount; i++) {
						int idx;
						do {
							idx = rnd.nextInt(opsSize);
						} while (!usedIdx.add(idx));
						int[] codes = new int[]{28, 2, 11000};
						int code = codes[rnd.nextInt(codes.length)];
						org.bson.BsonDocument details = new org.bson.BsonDocument("reason", new org.bson.BsonString("ut-error"));
						errors.add(new com.mongodb.bulk.BulkWriteError(code, "ut error at " + idx, details, idx));
					}
				}
				com.mongodb.MongoBulkWriteException ex = org.mockito.Mockito.mock(com.mongodb.MongoBulkWriteException.class);
				org.mockito.Mockito.when(ex.getWriteErrors()).thenReturn(errors);
				org.mockito.Mockito.when(ex.getServerAddress()).thenReturn(new com.mongodb.ServerAddress("localhost", 27017));
				org.mockito.Mockito.when(ex.getWriteConcernError()).thenReturn(null);

			String pathObj = mongodbWriter.dumpBulkWriteErrorContext(ex, bulkWriteModel, bulkWriteOptions, collection);

			assertNotNull(pathObj, "dumpBulkWriteErrorContext should return file path");
			String filePath = String.valueOf(pathObj);
			java.nio.file.Path path = java.nio.file.Paths.get(filePath);
			assertTrue(java.nio.file.Files.exists(path), "dump file should exist");

			try {
				String content = new String(java.nio.file.Files.readAllBytes(path), java.nio.charset.StandardCharsets.UTF_8);
				// Verify filename pattern
				String fileName = path.getFileName().toString();
				assertTrue(fileName.startsWith("mongodb_bulk_write_error_"));
				assertTrue(fileName.endsWith("_" + sanitizedThread + ".log"));

				// Verify content contains metadata
				assertTrue(content.contains("thread=" + threadName));
				assertTrue(content.contains("database=ut_db collection=ut_col"));
				assertTrue(content.contains("namespace=ut_db.ut_col"));
			} finally {
				// Clean up test artifact file
				try {
					java.nio.file.Files.deleteIfExists(path);
				} catch (Throwable ignored) {
				}
			}
		}
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