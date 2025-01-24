package io.tapdata.mongodb.writer;

import com.mongodb.client.model.UpdateOptions;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.mongodb.entity.MergeBundle;
import io.tapdata.mongodb.entity.MergeFilter;
import io.tapdata.mongodb.entity.MergeResult;
import io.tapdata.mongodb.merge.MergeFilterManager;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.apis.entity.merge.MergeTableProperties;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

/**
 * @author samuel
 * @Description
 * @create 2024-04-23 15:36
 **/
@DisplayName("Class MongodbMergeOperate Test")
class MongodbMergeOperateTest {

	@Nested
	@DisplayName("Method appendAllParentMergeFilters Test")
	class appendAllParentMergeFiltersTest {
		@Test
		@DisplayName("Test main process")
		void testMainProcess() {
			MergeResult mergeResult = new MergeResult();
			mergeResult.setFilter(new Document("id", 1));
			MergeFilter mergeFilter = new MergeFilter(true);
			mergeFilter.addFilter(new Document("id1", 2));
			MongodbMergeOperate.appendAllParentMergeFilters(mergeResult, mergeFilter);

			assertEquals(2, mergeResult.getFilter().size());
			assertEquals(1, mergeResult.getFilter().getInteger("id"));
			assertEquals(2, mergeResult.getFilter().getInteger("id1"));
		}

		@Test
		@DisplayName("Test input merge result is null")
		void testInputMergeResultIsNull() {
			assertDoesNotThrow(() -> MongodbMergeOperate.appendAllParentMergeFilters(null, new MergeFilter(true)));
			assertDoesNotThrow(() -> MongodbMergeOperate.appendAllParentMergeFilters(new MergeResult(), new MergeFilter(true)));
		}

		@Test
		@DisplayName("Test input merge filter is null")
		void testInputMergeFilterIsNull() {
			assertDoesNotThrow(() -> MongodbMergeOperate.appendAllParentMergeFilters(new MergeResult(), null));
			MergeResult mergeResult = new MergeResult();
			mergeResult.setFilter(new Document("id", 1));
			assertDoesNotThrow(() -> MongodbMergeOperate.appendAllParentMergeFilters(mergeResult, new MergeFilter(true)));
			assertEquals(1, mergeResult.getFilter().size());
			assertEquals(1, mergeResult.getFilter().getInteger("id"));
		}

		@Test
		@DisplayName("Test filter predicate works")
		void testFilterPredicate() {
			MergeResult mergeResult = new MergeResult();
			mergeResult.setFilter(new Document("id", 1));
			MergeFilter mergeFilter = new MergeFilter(true);
			mergeFilter.addFilter(new Document("id1", 2));
			mergeFilter.addFilter(new Document("id2", 3));

			try (
					MockedStatic<MergeFilterManager> mergeFilterManagerMockedStatic = mockStatic(MergeFilterManager.class)
			) {
				mergeFilterManagerMockedStatic.when(() -> MergeFilterManager.test(any(Map.Entry.class))).thenAnswer(invocationOnMock -> {
					Object argument1 = invocationOnMock.getArgument(0);
					Map.Entry<String, Object> entry = (Map.Entry<String, Object>) argument1;
					String key = entry.getKey();
					if ("id1".equals(key)) {
						return true;
					}
					return false;
				});
				MongodbMergeOperate.appendAllParentMergeFilters(mergeResult, mergeFilter);

				assertEquals(2, mergeResult.getFilter().size());
				assertEquals(1, mergeResult.getFilter().getInteger("id"));
				assertEquals(3, mergeResult.getFilter().getInteger("id2"));
			}
		}
	}

	@Nested
	@DisplayName("Method upsertMerge Test")
	class upsertMergeTest {
		@Test
		@DisplayName("test upsert merge, op: u, before: empty, after: {id: 1, _str: 'test1'}, expect filter: {id: 1}")
		void testFilter() {
			Map<String, Object> before = new HashMap<>();
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("_str", "test1");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE, before, after);
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			Map<String, String> joinKey = new HashMap<>();
			joinKey.put("source", "id");
			joinKey.put("target", "id");
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(joinKey);
			mergeTableProperties.setJoinKeys(joinKeys);
			MergeResult mergeResult = new MergeResult();
			MongodbMergeOperate.upsertMerge(mergeBundle, mergeTableProperties, mergeResult);
			Document filter = mergeResult.getFilter();
			assertEquals(1, filter.getInteger("id"));
		}
	}

	@Nested
	@DisplayName("Method recursiveMerge test")
	class recursiveMergeTest {
		@Test
		@DisplayName("test multiple update write lookup results, expect 3 merge results")
		void test1() {
			Map<String, Object> before1 = new HashMap<>();
			before1.put("id", 1);
			before1.put("col1", "test");
			Map<String, Object> after1 = new HashMap<>();
			after1.put("id", 1);
			after1.put("col1", "test1");
			MergeTableProperties mergeTableProperties1 = new MergeTableProperties();
			mergeTableProperties1.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
			}});
			mergeTableProperties1.setMergeType(MergeTableProperties.MergeType.updateOrInsert);
			MergeBundle mergeBundle1 = new MergeBundle(MergeBundle.EventOperation.UPDATE, before1, after1);
			List<MergeResult> mergeResults = new ArrayList<>();
			List<MergeLookupResult> mergeLookupResults = new ArrayList<>();
			MergeLookupResult mergeLookupResult1 = new MergeLookupResult();
			mergeLookupResult1.setData(new HashMap<String, Object>() {{
				put("id1", 11);
				put("col1", "test");
			}});
			mergeLookupResult1.setDataExists(true);
			mergeLookupResult1.setTapTable(new TapTable("sub1"));
			MergeTableProperties mergeTableProperties2 = new MergeTableProperties();
			mergeTableProperties2.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties2.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id1");
					put("target", "id1");
				}});
			}});
			mergeTableProperties2.setTargetPath("sub1");
			mergeLookupResult1.setProperty(mergeTableProperties2);
			mergeLookupResults.add(mergeLookupResult1);
			MergeLookupResult mergeLookupResult2 = new MergeLookupResult();
			mergeLookupResult2.setData(new HashMap<String, Object>() {{
				put("id2", 111);
				put("col1", "test");
			}});
			mergeLookupResult2.setDataExists(true);
			mergeLookupResult2.setTapTable(new TapTable("sub1"));
			MergeTableProperties mergeTableProperties3 = new MergeTableProperties();
			mergeTableProperties3.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties3.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id2");
					put("target", "id2");
				}});
			}});
			mergeTableProperties3.setTargetPath("sub2");
			mergeLookupResult2.setProperty(mergeTableProperties3);
			mergeLookupResults.add(mergeLookupResult2);
			MergeFilter mergeFilter = new MergeFilter(false);
			MongodbMergeOperate.recursiveMerge(
					mergeBundle1,
					mergeTableProperties1,
					mergeResults,
					mergeLookupResults,
					null,
					null,
					mergeFilter,
					1
			);
			assertEquals(3, mergeResults.size());
		}
	}

	@Nested
	@DisplayName("Method filterSetDocByUnsetDoc test")
	class filterSetDocByUnsetDocTest {
		@Test
		@DisplayName("main process test")
		void test1() {
			Document setDoc = new Document()
					.append("id", 1)
					.append("name", "test")
					.append("desc.city", "shanghai")
					.append("sub1", new Document())
					.append("sub2", new Document("f1", "test"));
			Document unsetDoc = new Document()
					.append("desc.city", true)
					.append("sub1.f1", true)
					.append("sub2.f1", true);
			Document actual = MongodbMergeOperate.filterSetDocByUnsetDoc(setDoc, unsetDoc);
			assertSame(actual, setDoc);
			assertEquals(3, actual.size());
			assertTrue(actual.containsKey("id"));
			assertTrue(actual.containsKey("name"));
			assertTrue(actual.containsKey("sub2"));
		}

		@Test
		@DisplayName("input set doc is null")
		void test2() {
			Document actual = MongodbMergeOperate.filterSetDocByUnsetDoc(null, new Document());
			assertNull(actual);
		}

		@Test
		@DisplayName("input unset doc is null")
		void test3() {
			Document setDoc = new Document()
					.append("id", 1)
					.append("name", "test");
			Document actual = MongodbMergeOperate.filterSetDocByUnsetDoc(setDoc, null);
			assertEquals(setDoc, actual);
		}
	}

	@Nested
	@DisplayName("Method updateIntoArrayMerge test")
	class updateIntoArrayMergeTest {
		@Test
		@DisplayName("test update into array, op: u, target path: 'arr', isArray: false, joinKey: {source: 'id', target: 'id'}, arrayKeys: ['id', 'index']")
		void test1() {
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE, new HashMap<String, Object>() {{
				put("ID", 1);
				put("INDEX", 1);
				put("NAME", "test");
			}}, new HashMap<String, Object>() {{
				put("id", 2);
				put("index", 22);
				put("name", "test");
			}});
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			mergeTableProperties.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
			}});
			mergeTableProperties.setTargetPath("arr");
			mergeTableProperties.setArrayKeys(new ArrayList<String>() {{
				add("id");
				add("index");
			}});
			mergeTableProperties.setIsArray(false);
			MergeResult mergeResult = new MergeResult();
			MergeFilter mergeFilter = new MergeFilter(true);

			MongodbMergeOperate.updateIntoArrayMerge(mergeBundle, mergeTableProperties, mergeResult, mergeFilter);

			Document filter = mergeResult.getFilter();
			assertNotNull(filter);
			assertTrue(filter.containsKey("id"));
			assertEquals(2, filter.getInteger("id"));
			Document update = mergeResult.getUpdate();
			assertNotNull(update);
			assertTrue(update.containsKey("$set"));
			Document setDoc = update.get("$set", Document.class);
			assertNotNull(setDoc);
			assertEquals(3, setDoc.size());
			assertEquals(2, setDoc.getInteger("arr.$[element1].id"));
			assertEquals(22, setDoc.getInteger("arr.$[element1].index"));
			assertEquals("test", setDoc.getString("arr.$[element1].name"));
			UpdateOptions updateOptions = mergeResult.getUpdateOptions();
			assertNotNull(updateOptions);
			List<? extends Bson> arrayFilters = updateOptions.getArrayFilters();
			assertNotNull(arrayFilters);
			assertEquals(1, arrayFilters.size());
			Bson arrayFilter = arrayFilters.get(0);
			assertNotNull(arrayFilter);
			assertEquals(2, arrayFilter.toBsonDocument().getInt32("element1.id").getValue());
			assertEquals(22, arrayFilter.toBsonDocument().getInt32("element1.index").getValue());
		}
	}

	@Nested
	@DisplayName("Method updateMerge test")
	class updateMergeTest {
		@Test
		@DisplayName("test update merge, set: {\"id\": 1, \"model\": {}}, remove fields: {\"model\": 1}, set and unset both exists")
		void test1() {
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE,
					new Document("id", 1),
					new Document("id", 1).append("model", new Document()));
			mergeBundle.setRemovefields(new Document("model", 1));
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			Map<String, String> joinKey = new HashMap<>();
			joinKey.put("source", "id");
			joinKey.put("target", "id");
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(joinKey);
			mergeTableProperties.setJoinKeys(joinKeys);
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties.setTargetPath("subMap");
			MergeResult mergeResult = new MergeResult();
			MongodbMergeOperate.updateMerge(mergeBundle, mergeTableProperties, mergeResult, new HashSet<>(), new MergeFilter(true));

			Document update = mergeResult.getUpdate();
			assertEquals("{\"$unset\": {\"subMap.model\": 1}, \"$set\": {\"subMap.id\": 1}}", update.toJson());
		}

		@Test
		@DisplayName("on the basis of test1, in the update in the mergeResult passed in, add $set")
		void test2() {
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE,
					new Document("id", 1),
					new Document("id", 1).append("model", new Document()));
			mergeBundle.setRemovefields(new Document("model", 1));
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			Map<String, String> joinKey = new HashMap<>();
			joinKey.put("source", "id");
			joinKey.put("target", "id");
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(joinKey);
			mergeTableProperties.setJoinKeys(joinKeys);
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties.setTargetPath("subMap");
			MergeResult mergeResult = new MergeResult();
			mergeResult.setUpdate(new Document("$set", new Document("td", 1)));
			MongodbMergeOperate.updateMerge(mergeBundle, mergeTableProperties, mergeResult, new HashSet<>(), new MergeFilter(true));

			Document update = mergeResult.getUpdate();
			assertEquals("{\"$set\": {\"td\": 1, \"subMap.id\": 1}, \"$unset\": {\"subMap.model\": 1}}", update.toJson());
		}
	}

	@Nested
	@DisplayName("Method mergeBundle test")
	class mergeBundleTest {
		@Test
		@DisplayName("test insert event")
		void test1() {
			Document after = new Document("id", 1).append("title", "xxxxxxxx").append("lastModDate", Instant.now());
			List<String> removeFields = new ArrayList<>();
			removeFields.add("test");
			TapInsertRecordEvent insertRecordEvent = TapInsertRecordEvent.create().init().after(after).removedFields(removeFields);
			MergeBundle mergeBundle = MongodbMergeOperate.mergeBundle(insertRecordEvent);
			assertNotNull(mergeBundle);
			assertEquals(MergeBundle.EventOperation.INSERT, mergeBundle.getOperation());
			assertEquals(after, mergeBundle.getAfter());
			assertEquals(new HashMap<String, Object>() {
				{
					put("test", true);
				}
			}, mergeBundle.getRemovefields());
		}

		@Test
		@DisplayName("test update event")
		void test2() {
			Document before = new Document("id", 1).append("title", "xxxxxxxx").append("lastModDate", Instant.now()).append("test", 1);
			Document after = new Document("id", 1).append("title", "yyyyyyyy").append("lastModDate", Instant.now());
			List<String> removeFields = new ArrayList<>();
			removeFields.add("test");
			TapUpdateRecordEvent updateRecordEvent = TapUpdateRecordEvent.create().init().before(before).after(after).removedFields(removeFields);
			MergeBundle mergeBundle = MongodbMergeOperate.mergeBundle(updateRecordEvent);
			assertNotNull(mergeBundle);
			assertEquals(MergeBundle.EventOperation.UPDATE, mergeBundle.getOperation());
			assertEquals(before, mergeBundle.getBefore());
			assertEquals(after, mergeBundle.getAfter());
			assertEquals(new HashMap<String, Object>() {
				{
					put("test", true);
				}
			}, mergeBundle.getRemovefields());
		}

		@Test
		@DisplayName("test delete event")
		void test3() {
			Document before = new Document("id", 1).append("title", "xxxxxxxx").append("lastModDate", Instant.now());
			TapDeleteRecordEvent deleteRecordEvent = TapDeleteRecordEvent.create().init().before(before);
			MergeBundle mergeBundle = MongodbMergeOperate.mergeBundle(deleteRecordEvent);
			assertNotNull(mergeBundle);
			assertEquals(MergeBundle.EventOperation.DELETE, mergeBundle.getOperation());
			assertEquals(before, mergeBundle.getBefore());
		}
	}

	@Nested
	@DisplayName("Method buildUnsetDocument test")
	class buildUnsetDocumentTest {
		@Test
		@DisplayName("test not array, have target path")
		void test1() {
			Document data = new Document("f1", true)
					.append("subDoc", true)
					.append("subDoc.f1", true)
					.append("f2", true);
			Set<String> shareJoinKey = new HashSet<>();
			shareJoinKey.add("target.f2");
			Document unsetDoc = MongodbMergeOperate.buildUnsetDocument(shareJoinKey, data, "target", false, false);
			assertNotNull(unsetDoc);
			Document expect = new Document("target.f1", true)
					.append("target.subDoc", true);
			assertEquals(expect, unsetDoc);
		}

		@Test
		@DisplayName("test not array, not have target path")
		void test2() {
			Document data = new Document("f1", true)
					.append("subDoc", true)
					.append("subDoc.f1", true)
					.append("f2", true);
			Set<String> shareJoinKey = new HashSet<>();
			shareJoinKey.add("f2");
			Document unsetDoc = MongodbMergeOperate.buildUnsetDocument(shareJoinKey, data, "", false, false);
			assertNotNull(unsetDoc);
			Document expect = new Document("f1", true)
					.append("subDoc", true);
			assertEquals(expect, unsetDoc);
		}

		@Test
		@DisplayName("test array, have target path")
		void test3() {
			Document data = new Document("f1", true)
					.append("subArray", true)
					.append("subArray.f1", true)
					.append("f2", true);
			Set<String> shareJoinKey = new HashSet<>();
			shareJoinKey.add("target.f2");
			Document unsetDoc = MongodbMergeOperate.buildUnsetDocument(shareJoinKey, data, "target", true, true);
			assertNotNull(unsetDoc);
			Document expect = new Document("target.$[element1].f1", true)
					.append("target.$[element1].subArray", true);
			assertEquals(expect, unsetDoc);
		}

		@Test
		@DisplayName("test array, not have target path")
		void test4() {
			Document data = new Document("f1", true)
					.append("subArray", true)
					.append("subArray.f1", true)
					.append("f2", true);
			Set<String> shareJoinKey = new HashSet<>();
			shareJoinKey.add("target.f2");
			Document unsetDoc = MongodbMergeOperate.buildUnsetDocument(shareJoinKey, data, "", true, true);
			assertTrue(unsetDoc.isEmpty());
		}

		@Test
		@DisplayName("test array, firstMergeResult is false")
		void test5() {
			Document data = new Document("f1", true)
					.append("subArray", true)
					.append("subArray.f1", true)
					.append("f2", true);
			Set<String> shareJoinKey = new HashSet<>();
			shareJoinKey.add("target.f2");
			Document unsetDoc = MongodbMergeOperate.buildUnsetDocument(shareJoinKey, data, "target", true, false);
			assertTrue(unsetDoc.isEmpty());
		}
	}

	@Nested
	@DisplayName("Method unsetFilter test")
	class unsetFilterTest {

		private Document before;
		private Document after;
		private List<Map<String, String>> joinKeys;

		@BeforeEach
		void setUp() {
			before = new Document("id1", 1)
					.append("type1", "xxx")
					.append("f1", "zzzz")
					.append("f2", 625.85);
			after = new Document("id1", 2)
					.append("type1", "zzz")
					.append("f1", "yyyy")
					.append("f2", 123.45);
			joinKeys = new ArrayList<>();
			joinKeys.add(new HashMap<String, String>() {{
				put("source", "id");
				put("target", "id1");
			}});
			joinKeys.add(new HashMap<String, String>() {{
				put("source", "type");
				put("target", "type1");
			}});
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			Document filter = MongodbMergeOperate.unsetFilter(before, after, joinKeys, 1);
			assertEquals(new Document("id1", 2).append("type1", "zzz"), filter);
		}
	}

	@Nested
	@DisplayName("Method updateIntoArrayUnsetMerge test")
	class updateIntoArrayUnsetMergeTest {
		@Test
		@DisplayName("test data exists")
		void test1() {
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("src", "x");
			before.put("seq", 1);
			before.put("name", "test");
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("src", "y");
			after.put("seq", 1);
			after.put("name", "test1");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE, before, after);
			mergeBundle.setDataExists(true);
			MergeTableProperties currentProperty = new MergeTableProperties();
			currentProperty.setId("1");
			currentProperty.setTargetPath("array");
			currentProperty.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
				add(new HashMap<String, String>() {{
					put("source", "src");
					put("target", "src");
				}});
			}});
			currentProperty.setArrayKeys(new ArrayList<String>() {{
				add("id");
				add("src");
				add("seq");
			}});
			currentProperty.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = new HashMap<>();
			MergeInfo.UpdateJoinKey updateJoinKey = new MergeInfo.UpdateJoinKey(new Document("id", 1).append("src", "x"), new Document("id", 1).append("src", "y"), new Document("id", 1).append("src", "x"));
			updateJoinKeys.put("1", updateJoinKey);
			MergeResult mergeResult = new MergeResult();
			MergeTableProperties parentProperties = new MergeTableProperties();
			parentProperties.setId("2");
			parentProperties.setMergeType(MergeTableProperties.MergeType.updateOrInsert);
			MergeFilter mergeFilter = new MergeFilter(true);
			MergeResult result = MongodbMergeOperate.updateIntoArrayUnsetMerge(mergeBundle, currentProperty, updateJoinKeys, mergeResult, parentProperties, mergeFilter);
			assertEquals("{\"id\": 1, \"src\": \"x\"}", result.getFilter().toJson());
			assertEquals("{\"$pull\": {\"array\": {\"id\": 1, \"src\": \"x\", \"seq\": 1}}}", result.getUpdate().toJson());
		}

		@Test
		@DisplayName("test data not exists")
		void test2() {
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("src", "x");
			before.put("seq", 1);
			before.put("name", "test");
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("src", "y");
			after.put("seq", 1);
			after.put("name", "test1");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE, before, after);
			mergeBundle.setDataExists(false);
			MergeTableProperties currentProperty = new MergeTableProperties();
			currentProperty.setId("1");
			currentProperty.setTargetPath("array");
			currentProperty.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
				add(new HashMap<String, String>() {{
					put("source", "src");
					put("target", "src");
				}});
			}});
			currentProperty.setArrayKeys(new ArrayList<String>() {{
				add("id");
				add("src");
				add("seq");
			}});
			currentProperty.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = new HashMap<>();
			MergeInfo.UpdateJoinKey updateJoinKey = new MergeInfo.UpdateJoinKey(new Document("id", 1).append("src", "x"), new Document("id", 1).append("src", "y"), new Document("id", 1).append("src", "x"));
			updateJoinKeys.put("1", updateJoinKey);
			MergeResult mergeResult = new MergeResult();
			MergeTableProperties parentProperties = new MergeTableProperties();
			parentProperties.setId("2");
			parentProperties.setMergeType(MergeTableProperties.MergeType.updateOrInsert);
			MergeFilter mergeFilter = new MergeFilter(true);
			MergeResult result = MongodbMergeOperate.updateIntoArrayUnsetMerge(mergeBundle, currentProperty, updateJoinKeys, mergeResult, parentProperties, mergeFilter);
			assertEquals("{\"id\": 1, \"src\": \"x\"}", result.getFilter().toJson());
			assertEquals("{\"$set\": {\"array\": []}}", result.getUpdate().toJson());
		}
	}

	@Nested
	@DisplayName("Method removeIdIfNeed test")
	class removeIdIfNeedTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			Map<String, Object> data = new HashMap<>();
			data.put("_id", new ObjectId());
			data.put("f1", 1);
			data.put("f2", 1);
			data.put("f3", 1);
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f1");
				put("target", "f1");
			}});
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f2");
				put("target", "f2");
			}});
			mergeTableProperties.setJoinKeys(joinKeys);
			MongodbMergeOperate.removeIdIfNeed(data, mergeTableProperties);
			assertFalse(data.containsKey("_id"));
		}

		@Test
		@DisplayName("test merge type is updateIntoArray")
		void test2() {
			Map<String, Object> data = new HashMap<>();
			data.put("_id", new ObjectId());
			data.put("f1", 1);
			data.put("f2", 1);
			data.put("f3", 1);
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f1");
				put("target", "f1");
			}});
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f2");
				put("target", "f2");
			}});
			mergeTableProperties.setJoinKeys(joinKeys);
			MongodbMergeOperate.removeIdIfNeed(data, mergeTableProperties);
			assertTrue(data.containsKey("_id"));
		}

		@Test
		@DisplayName("test target path is not empty")
		void test3() {
			Map<String, Object> data = new HashMap<>();
			data.put("_id", new ObjectId());
			data.put("f1", 1);
			data.put("f2", 1);
			data.put("f3", 1);
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f1");
				put("target", "f1");
			}});
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f2");
				put("target", "f2");
			}});
			mergeTableProperties.setJoinKeys(joinKeys);
			mergeTableProperties.setTargetPath("xxx");
			MongodbMergeOperate.removeIdIfNeed(data, mergeTableProperties);
			assertTrue(data.containsKey("_id"));
		}

		@Test
		@DisplayName("test data is empty")
		void test4() {
			Map<String, Object> data = new HashMap<>();
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f1");
				put("target", "f1");
			}});
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f2");
				put("target", "f2");
			}});
			mergeTableProperties.setJoinKeys(joinKeys);
			mergeTableProperties.setTargetPath("xxx");
			assertDoesNotThrow(() -> MongodbMergeOperate.removeIdIfNeed(data, mergeTableProperties));
		}

		@Test
		@DisplayName("test merge table properties is null")
		void test5() {
			Map<String, Object> data = new HashMap<>();
			data.put("_id", new ObjectId());
			data.put("f1", 1);
			data.put("f2", 1);
			data.put("f3", 1);
			MongodbMergeOperate.removeIdIfNeed(data, null);
			assertTrue(data.containsKey("_id"));
		}

		@Test
		@DisplayName("test join keys contains value _id")
		void test6() {
			Map<String, Object> data = new HashMap<>();
			data.put("_id", new ObjectId());
			data.put("f1", 1);
			data.put("f2", 1);
			data.put("f3", 1);
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f1");
				put("target", "f1");
			}});
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "_id");
				put("target", "f2");
			}});
			mergeTableProperties.setJoinKeys(joinKeys);
			MongodbMergeOperate.removeIdIfNeed(data, mergeTableProperties);
			assertTrue(data.containsKey("_id"));
		}
	}
}