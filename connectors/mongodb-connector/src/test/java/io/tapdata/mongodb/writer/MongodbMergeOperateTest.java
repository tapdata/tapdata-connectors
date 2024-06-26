package io.tapdata.mongodb.writer;

import com.mongodb.client.model.UpdateOptions;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.mongodb.entity.MergeBundle;
import io.tapdata.mongodb.entity.MergeFilter;
import io.tapdata.mongodb.entity.MergeResult;
import io.tapdata.mongodb.merge.MergeFilterManager;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.apis.entity.merge.MergeTableProperties;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

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
					mergeFilter
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
			MongodbMergeOperate.updateMerge(mergeBundle, mergeTableProperties, mergeResult,new HashSet<>(), new MergeFilter(true));

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
			MongodbMergeOperate.updateMerge(mergeBundle, mergeTableProperties, mergeResult,new HashSet<>(), new MergeFilter(true));

			Document update = mergeResult.getUpdate();
			assertEquals("{\"$set\": {\"td\": 1, \"subMap.id\": 1}, \"$unset\": {\"subMap.model\": 1}}", update.toJson());
		}
	}
}