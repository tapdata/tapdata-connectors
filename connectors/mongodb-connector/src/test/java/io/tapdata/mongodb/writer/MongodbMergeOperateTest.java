package io.tapdata.mongodb.writer;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.mongodb.entity.MergeBundle;
import io.tapdata.mongodb.entity.MergeFilter;
import io.tapdata.mongodb.entity.MergeResult;
import io.tapdata.mongodb.merge.MergeFilterManager;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.apis.entity.merge.MergeTableProperties;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
}